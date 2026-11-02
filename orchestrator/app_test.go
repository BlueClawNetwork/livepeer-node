package orchestrator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/holiman/uint256"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/nodeconfig"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

func execute(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := Root(&output, &output)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func TestPaidHTTPStartup(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(fmt.Sprintf("separate-wallet=%t", separate), func(t *testing.T) {
			mainPort, metricsPort := freeTCPPort(t), freeTCPPort(t)
			address := fmt.Sprintf("http://127.0.0.1:%d", mainPort)
			p := paymentKeystoreParams(t, "http://127.0.0.1:1")
			p.HeartbeatInterval = time.Second
			p.KeystoreFile, p.KeystorePasswordFile = test.WriteFixedKeystore(t)
			key, err := eth.OpenKeystoreFile(p.KeystoreFile, p.KeystorePasswordFile)
			require.NoError(t, err)
			recipient := key.Address()
			if separate {
				recipient = ethcommon.HexToAddress("0x1234")
				p.OrchestratorAddress = &recipient
			}
			var requests atomic.Int32
			dependencies := make(chan struct{})
			releaseDependencies := sync.OnceFunc(func() { close(dependencies) })
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					Method string
					ID     json.RawMessage
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				if req.Method != "eth_chainId" {
					select {
					case <-dependencies:
					case <-r.Context().Done():
						return
					}
				}
				requests.Add(1)
				var result any = "0x1"
				switch req.Method {
				case "eth_chainId", "eth_getBalance", "eth_gasPrice", "eth_maxPriorityFeePerGas":
				case "eth_getTransactionCount":
					result = "0x0"
				case "eth_estimateGas":
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "error": map[string]any{"code": -32000, "message": "ticket estimation unavailable"}})
					return
				case "eth_getBlockByNumber":
					result = map[string]string{"number": "0x32", "hash": ethcommon.HexToHash("0xabcd").Hex()}
				case "eth_call":
					// Reuse the 32-byte encoding of 1 for calls returning an address, round, hash, or bool.
					result = fmt.Sprintf("0x%064x", 1)
					var call struct{ Input string }
					require.NoError(t, json.Unmarshal(req.Params[0], &call))
					if strings.HasPrefix(call.Input, fmt.Sprintf("0x%x", crypto.Keccak256([]byte("usedTickets(bytes32)"))[:4])) {
						result = fmt.Sprintf("0x%064x", 0)
					}
				default:
					t.Errorf("unexpected RPC %s", req.Method)
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}))
			}))
			defer rpc.Close()
			defer releaseDependencies()
			p.RPCURL = mustURL(t, rpc.URL)
			p.Listen = netip.MustParseAddrPort(fmt.Sprintf("0.0.0.0:%d", mainPort))
			p.MetricsListen = netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", metricsPort))
			p.ServiceURL = boa.Text[*url.URL]{Value: mustURL(t, "https://public.example/external")}
			p.RunnerConfig = filepath.Join(t.TempDir(), "runners.toml")
			require.NoError(t, os.WriteFile(p.RunnerConfig, []byte("[[Runners]]\nID = 'r'\nRunnerURL = 'https://runner.example'\nApp = 'test'\n[Runners.PriceInfo]\nPrice = 1\n"), 0600))
			require.NoError(t, p.Validate())
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- Serve(ctx, p, io.Discard) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Error("orchestrator did not stop")
				}
			})
			require.Eventually(t, func() bool {
				response, err := http.Get(address + "/external/discovery")
				if err != nil {
					return false
				}
				defer response.Body.Close()
				return response.StatusCode == http.StatusOK
			}, 5*time.Second, 20*time.Millisecond)
			require.EqualValues(t, 1, requests.Load())
			require.FileExists(t, p.RedeemerDB)
			response, err := http.Get("http://" + p.MetricsListen.String() + "/readyz")
			require.NoError(t, err)
			response.Body.Close()
			require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
			// Seed before releasing either worker's chain observation. The other
			// wallet case keeps its empty queue, exercising both first-pass paths.
			if separate {
				store, err := OpenRedeemerDB(p.RedeemerDB)
				require.NoError(t, err)
				defer store.Close()
				require.NoError(t, store.StoreWinningTicket(&pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: recipient, Recipient: recipient, FaceValue: big.NewInt(1000), WinProb: big.NewInt(100), TicketNonce: 1, RecipientRandHash: crypto.Keccak256Hash([]byte("failed ticket")), CreationRound: 1, ParamsExpirationBlock: big.NewInt(0)}, Sig: make([]byte, 65), RecipientRand: big.NewInt(1)}))
			}
			releaseDependencies()
			response, err = http.Get(address + "/external/discovery")
			require.NoError(t, err)
			defer response.Body.Close()
			var entries []discoveryEntry
			require.NoError(t, json.NewDecoder(response.Body).Decode(&entries))
			require.Len(t, entries, 1)
			require.Equal(t, p.ServiceURL.String(), entries[0].Address)
			payer := paymentTestKey(t)
			request, err := http.NewRequest(http.MethodPost, address+"/external/apps/r/session", nil)
			require.NoError(t, err)
			request.Header.Set("Livepeer-Payer-Address", payer.Address().Hex())
			response, err = http.DefaultClient.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusPaymentRequired, response.StatusCode)
			var challenge Challenge
			require.NoError(t, json.NewDecoder(response.Body).Decode(&challenge))
			encoded, err := base64.StdEncoding.DecodeString(challenge.PaymentParams)
			require.NoError(t, err)
			info, err := wire.DecodeOrchestratorInfo(encoded)
			require.NoError(t, err)
			require.Equal(t, recipient.Bytes(), info.Address)
			require.Equal(t, recipient.Bytes(), info.TicketParams.Recipient)
			require.Eventually(t, func() bool {
				ready, err := http.Get("http://" + p.MetricsListen.String() + "/readyz")
				if err != nil {
					return false
				}
				defer ready.Body.Close()
				return ready.StatusCode == http.StatusOK
			}, 7*time.Second, 20*time.Millisecond, "paid readiness must become healthy after the first dependency and worker observations")
			response, err = http.Get("http://" + p.MetricsListen.String() + "/metrics")
			require.NoError(t, err)
			metrics, err := io.ReadAll(response.Body)
			response.Body.Close()
			require.NoError(t, err)
			require.Equal(t, separate, strings.Contains(string(metrics), "livepeer_redemption_errors_total{"), "readiness must include completion of the first worker pass, including ticket failure")

		})
	}
}

func TestConfigPrecedenceAndRedaction(t *testing.T) {
	for _, ext := range []string{".toml", ".json"} {
		t.Run(ext, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config"+ext)
			t.Setenv("LIVEPEER_ORCHESTRATOR_KEYSTORE_PASSWORD", "keystore-secret")
			config := []byte(`ServiceURL = 'https://file.example'
Listen = '127.0.0.1:8000'
RedeemerMaxFeePerGas = '100'
ChainID = 1
Controller = '0x0000000000000000000000000000000000000001'
OrchestratorAddress = '0x0000000000000000000000000000000000000011'
WeiPerUSD = '1/2'
KeystoreFile = '/missing/account.json'
RunnerGrants = ['10.0.0.0/8', 'fc00::/7']
`)
			test.WriteConfig(t, path, string(config))
			for key, value := range map[string]string{
				"SERVICE_URL": "https://env.example", "BOOTSTRAP_SECRET": "super-secret-env", "LISTEN": "127.0.0.1:8001",
				"REDEEMER_MAX_FEE_PER_GAS": "200", "CHAIN_ID": "2",
				"CONTROLLER_ADDRESS": "0x0000000000000000000000000000000000000002", "WEI_PER_USD": "2/3",
				"SESSION_PROXY_GRANTS": "10.0.0.0/8,fc00::/7",
			} {
				t.Setenv("LIVEPEER_ORCHESTRATOR_"+key, value)
			}
			output, err := execute(t, "--config", path, "--listen", "127.0.0.1:8002",
				"--service-url", "https://user:password@flag.example", "--redeemer-max-fee-per-gas", "300",
				"--chain-id", "3", "--controller-address", "0x0000000000000000000000000000000000000003",
				"--health-grants", "10.0.0.0/8,fc00::/7", "--print-config")
			require.NoError(t, err)
			require.Contains(t, output, "127.0.0.1:8002")
			for _, absent := range []string{"127.0.0.1:8001", "127.0.0.1:8000", "password", "keystore", "/missing/", "flag.example", "env.example", "super-secret-env", "keystore-secret", path} {
				require.NotContains(t, output, absent)
			}
			require.Contains(t, output, `RedeemerMaxFeePerGas = "300"`)
			var printed map[string]any
			require.NoError(t, toml.Unmarshal([]byte(output), &printed))
			for _, key := range []string{"RunnerGrants", "SessionProxyGrants", "HealthGrants"} {
				require.Equal(t, []any{"10.0.0.0/8", "fc00::/7"}, printed[key], key)
			}
			require.EqualValues(t, 3, printed["ChainID"])
			require.Equal(t, "0x0000000000000000000000000000000000000003", printed["Controller"])
			require.Equal(t, "0x0000000000000000000000000000000000000011", printed["OrchestratorAddress"])
			require.Equal(t, "2/3", printed["WeiPerUSD"])
			require.Equal(t, "5s", printed["HeartbeatInterval"])
			for _, key := range []string{"ServiceURL", "RPCURL", "BootstrapSecret", "KeystoreFile", "KeystorePassword", "KeystorePasswordFile", "RunnerConfig", "TicketFaceValue"} {
				require.NotContains(t, printed, key)
			}
		})
	}
}

func TestChainViewSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	printConfig := func(t *testing.T, args ...string) map[string]any {
		t.Helper()
		output, err := execute(t, append([]string{"--config", path, "--print-config"}, args...)...)
		require.NoError(t, err)
		var printed map[string]any
		require.NoError(t, toml.Unmarshal([]byte(output), &printed))
		return printed
	}
	require.NoError(t, os.WriteFile(path, nil, 0600))
	printed := printConfig(t)
	require.Equal(t, "5s", printed["ChainRefreshInterval"], "enabled by default")
	require.Equal(t, "30s", printed["ChainMaxAge"])
	require.NoError(t, os.WriteFile(path, []byte("ChainRefreshInterval = '2s'\nChainMaxAge = '10s'\n"), 0600))
	printed = printConfig(t)
	require.Equal(t, "2s", printed["ChainRefreshInterval"])
	require.Equal(t, "10s", printed["ChainMaxAge"])
	t.Setenv("LIVEPEER_ORCHESTRATOR_CHAIN_MAX_AGE", "20s")
	printed = printConfig(t, "--chain-refresh-interval", "0")
	require.Equal(t, "0s", printed["ChainRefreshInterval"], "operators can opt out")
	require.Equal(t, "20s", printed["ChainMaxAge"])

	valid := paymentKeystoreParams(t, "http://127.0.0.1:1")
	valid.KeystorePassword = new("test")
	require.NoError(t, valid.Validate(), "an interval of 0 needs no maximum age")
	for _, tc := range []struct {
		name             string
		interval, maxAge time.Duration
		want             string
	}{
		{"negative interval", -time.Second, time.Minute, "chain-refresh-interval"},
		{"maximum age at the interval", time.Second, time.Second, "chain-max-age"},
		{"maximum age below the interval", time.Minute, time.Second, "chain-max-age"},
		{"maximum age past the limit", time.Second, eth.MaxChainViewAge + time.Second, "chain-max-age"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := valid
			p.ChainRefreshInterval, p.ChainMaxAge = tc.interval, tc.maxAge
			require.ErrorContains(t, p.Validate(), tc.want)
		})
	}
}

func TestLocalExampleConfig(t *testing.T) {
	_, err := execute(t, "--config", "../configs/orchestrator/local.example.toml", "--print-config")
	require.NoError(t, err)
}

func TestConfigErrorRemedies(t *testing.T) {
	for _, tc := range []struct {
		name, network, home, config, remedy string
	}{
		{name: "discovery network mismatch", home: t.TempDir(), config: "Network = 'arbitrum-one-mainnet'", remedy: "pass --network arbitrum-one-mainnet or select the file with --config"},
		{name: "missing HOME", network: "offchain", remedy: "set HOME or pass --data-dir"},
		{name: "invalid network", network: "..", home: t.TempDir(), remedy: "set --network to a name such as arbitrum-one-mainnet or offchain"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", tc.home)
			args := []string{"--print-config"}
			if tc.network != "" {
				args = append(args, "--network", tc.network)
			}
			if tc.config != "" {
				dir := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(dir, "config.toml"), []byte(tc.config), 0600))
				args = append(args, "--data-dir", dir)
			}
			_, err := execute(t, args...)
			require.ErrorContains(t, err, tc.remedy)
			require.True(t, boa.IsUserInputError(err))
			require.NotContains(t, err.Error(), "FuncCtx")
		})
	}
}

func TestConfigRejectsUnknownAndDirectSecrets(t *testing.T) {
	for _, content := range []string{
		"unknown_key = 1\n",
		"BootstrapSecret = 'forbidden'\n",
		"RPCURL = 'https://forbidden.example'\n",
		"KeystorePassword = 'forbidden'\n",
		"payment_chain_id = 1\n",
		"PrintConfig = true\n",
	} {
		path := filepath.Join(t.TempDir(), "config.toml")
		require.NoError(t, os.WriteFile(path, []byte(content), 0600))
		_, err := execute(t, "--config", path, "--print-config")
		require.Error(t, err, content)
	}
	_, err := execute(t, "--bootstrap-secret", "literal", "--print-config")
	require.Error(t, err)
	t.Setenv("LIVEPEER_ORCHESTRATOR_PRINT_CONFIG", "true")
	_, err = execute(t)
	require.ErrorContains(t, err, "bootstrap secret or static runner config")
}

func TestSecretEnvironmentFileConflict(t *testing.T) {
	secretFile := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(secretFile, []byte("file-secret\n"), 0600))
	t.Setenv("LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET", "env-secret")
	_, err := execute(t, "--bootstrap-secret-file", secretFile, "--print-config")
	require.ErrorContains(t, err, "cannot both be set")
}

func TestSecretFilesPreserveBytesAndRedactErrors(t *testing.T) {
	t.Chdir(t.TempDir())
	secretFile := "secret"
	require.NoError(t, os.WriteFile(secretFile, []byte("exact-secret\n"), 0600))
	var p Params
	require.NoError(t, (boa.Cmd[Params]{Params: &p, RawArgs: []string{"--bootstrap-secret-file", secretFile}}).Validate())
	require.Equal(t, "exact-secret\n", p.BootstrapSecret)
	// A file-only payment setting must select the same defaults as its value.
	require.NoError(t, os.WriteFile(secretFile, []byte("https://rpc.example"), 0600))
	args := []string{"--config=", "--network", "arbitrum-one-mainnet", "--data-dir", t.TempDir(), "--print-config"}
	fileConfig, err := execute(t, append(args, "--rpc-url-file", secretFile)...)
	require.NoError(t, err)
	t.Setenv("LIVEPEER_ORCHESTRATOR_RPC_URL", "https://rpc.example")
	valueConfig, err := execute(t, args...)
	require.NoError(t, err)
	require.Equal(t, valueConfig, fileConfig)
	require.Contains(t, fileConfig, "ChainID = 42161")
	require.Contains(t, fileConfig, "payments.sqlite")
	t.Setenv("LIVEPEER_ORCHESTRATOR_RPC_URL", "")
	secret := "https://user:private-password@rpc.example/\n"
	require.NoError(t, os.WriteFile(secretFile, []byte(secret), 0600))
	_, err = execute(t, "--rpc-url-file", secretFile, "--print-config")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private-password")
	require.NotContains(t, err.Error(), "rpc.example")
}

func TestValidationBeforeStartup(t *testing.T) {
	t.Setenv("LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET", "test")
	for _, flag := range []string{"--runner-grants", "--session-proxy-grants", "--health-grants"} {
		_, err := execute(t, "--config", "", flag, "10.0.0.0/33")
		require.ErrorContains(t, err, flag+" destination grant")
		require.True(t, boa.IsUserInputError(err))
	}
	db := filepath.Join(t.TempDir(), "payments.sqlite")
	_, err := execute(t, "--redeemer-db", db)
	require.ErrorContains(t, err, "on-chain payments require")
	require.True(t, boa.IsUserInputError(err))
	require.NoFileExists(t, db)
	_, err = execute(t, "--metrics-listen", "0.0.0.0:8936")
	require.ErrorContains(t, err, "loopback")
	require.True(t, boa.IsUserInputError(err))

	_, err = execute(t, "--orchestrator-address", "0x0000000000000000000000000000000000000001")
	require.ErrorContains(t, err, "on-chain payments require")
	_, err = execute(t, "--network", "test", "--orchestrator-address", "0x0000000000000000000000000000000000000000")
	require.ErrorContains(t, err, "orchestrator-address must be a nonempty")
	p := paymentKeystoreParams(t, "http://127.0.0.1:1")
	p.KeystoreFile, p.KeystorePasswordFile = "/missing/key", "/missing/password"
	p.TicketWinProb = new(uint256.Int).SetAllOne()
	require.ErrorContains(t, Serve(t.Context(), p, io.Discard), "ticket-win-prob must be less than")
}

func TestStaticRunnerTOML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runners.toml")
	config := `[[Runners]]
ID = "r"
RunnerURL = "https://runner.example/base"
App = "test"
HealthCode = 204
[Runners.GPU]
ID = "0"
Name = "H100"
VRAMMB = 80000
[Runners.PriceInfo]
Price = 1.5
Currency = "usd"
Unit = "fixed"
`
	require.NoError(t, os.WriteFile(path, []byte(config), 0600))
	registry := NewRegistry("", "https://public.example", time.Second, time.Minute)
	registry.SetWeiPerUSD(big.NewRat(1, 1))
	require.NoError(t, loadStatic(path, registry))
	runner := registry.runners["r"]
	require.Equal(t, 204, runner.HealthCode)
	require.Equal(t, "1.5", runner.USDQuote.Price.String())
	require.Equal(t, &runnerGPU{ID: "0", Name: "H100", VRAMMB: 80000}, runner.GPU)
	for _, price := range []string{"", "Price = 0\n", "Price = 0.0\n"} {
		require.NoError(t, os.WriteFile(path, []byte(strings.Replace(config, "Price = 1.5\n", price, 1)), 0600))
		require.ErrorContains(t, loadStatic(path, registry), "positive")
	}
	require.NoError(t, os.WriteFile(path, []byte(config+"PriceUSD = 2\n"), 0600))
	require.ErrorContains(t, loadStatic(path, registry), "unknown static runner keys")
}

func TestRedemptionValidationBeforeResources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payments.sqlite")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	t.Setenv("LIVEPEER_ORCHESTRATOR_REDEEMER_DB", path)
	t.Setenv("LIVEPEER_ORCHESTRATOR_RPC_URL", "http://127.0.0.1:1")
	t.Setenv("LIVEPEER_ORCHESTRATOR_CHAIN_ID", "1")
	t.Setenv("LIVEPEER_ORCHESTRATOR_SUBMIT", "true")
	hash := ethcommon.Hash{31: 1}.Hex()
	_, err := execute(t, "redemptions", "--retry-transaction", hash)
	require.ErrorContains(t, err, "retry requires --submit")
	require.True(t, boa.IsUserInputError(err))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Empty(t, data, "validation must precede SQLite initialization")
	output, err := execute(t, "redemptions")
	require.NoError(t, err, "environment variables configure inspection, but cannot authorize submission")
	require.JSONEq(t, "[]", output)
}

func paymentKeystoreParams(t *testing.T, endpoint string) Params {
	t.Helper()
	return Params{
		RedeemerTxTimeout: 5 * time.Minute, RedeemerMaxTxReplacements: 1,

		Listen: netip.MustParseAddrPort("127.0.0.1:0"), MetricsListen: netip.MustParseAddrPort("127.0.0.1:0"), ServiceURL: boa.Text[*url.URL]{Value: mustURL(t, "http://127.0.0.1:8935")},
		BootstrapSecret: "test", HeartbeatInterval: time.Hour, HeartbeatTTL: 2 * time.Hour,
		RedeemerDB: filepath.Join(t.TempDir(), "payments.sqlite"), RPCURL: mustURL(t, endpoint),
		ChainID: new(uint64(1)), Controller: new(ethcommon.HexToAddress("0x0000000000000000000000000000000000001000")),
		WeiPerUSD: big.NewRat(1, 1), TicketFaceValue: uint256.NewInt(1), TicketWinProb: uint256.NewInt(1),
	}
}

func TestPaymentKeystoreFailuresBeforeStartup(t *testing.T) {
	t.Setenv("LIVEPEER_ORCHESTRATOR_NETWORK", "test")
	path, passwordPath := test.WriteKeystore(t, nil)
	require.NoError(t, os.WriteFile(passwordPath, []byte("wrong-secret"), 0600))
	var requests atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer rpc.Close()
	t.Setenv("LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET", "test")
	t.Setenv("LIVEPEER_ORCHESTRATOR_RPC_URL", rpc.URL)
	for source, name := range []string{"config", "env", "cli"} {
		t.Run(name, func(t *testing.T) {
			p := paymentKeystoreParams(t, rpc.URL)
			paths := [3][2]string{{"/missing/key", "/missing/password"}, {"/missing/key", "/missing/password"}, {"/missing/key", "/missing/password"}}
			paths[source] = [2]string{path, passwordPath}
			config := filepath.Join(t.TempDir(), "orchestrator.toml")
			require.NoError(t, os.WriteFile(config, []byte(fmt.Sprintf("KeystoreFile = %q\nKeystorePasswordFile = %q\n", paths[0][0], paths[0][1])), 0600))
			args := []string{"--config", config, "--redeemer-db", p.RedeemerDB, "--chain-id", strconv.FormatUint(*p.ChainID, 10),
				"--controller-address", p.Controller.Hex(), "--wei-per-usd", "1", "--ticket-face-value", "1", "--ticket-win-prob", "1"}
			if source > 0 {
				t.Setenv("LIVEPEER_ORCHESTRATOR_KEYSTORE_FILE", paths[1][0])
				t.Setenv("LIVEPEER_ORCHESTRATOR_KEYSTORE_PASSWORD_FILE", paths[1][1])
			}
			if source > 1 {
				args = append(args, "--keystore-file", paths[2][0], "--keystore-password-file", paths[2][1])
			}
			_, err := execute(t, args...)
			require.ErrorContains(t, err, "cannot decrypt keystore")
			require.NotContains(t, err.Error(), "wrong-secret")
			require.Zero(t, requests.Load())
			require.NoFileExists(t, p.RedeemerDB)
		})
	}
	for _, paths := range [][2]string{{}, {path, ""}, {"", passwordPath}} {
		p := paymentKeystoreParams(t, rpc.URL)
		p.KeystoreFile, p.KeystorePasswordFile = paths[0], paths[1]
		p.DataDir = t.TempDir()
		err := Serve(t.Context(), p, io.Discard)
		if paths[1] == "" {
			require.ErrorContains(t, err, "on-chain payment requires")
		} else {
			require.ErrorContains(t, err, "keystore directory")
		}
		require.NotContains(t, err.Error(), "wrong-secret")
		require.Zero(t, requests.Load())
		require.NoFileExists(t, p.RedeemerDB)
		if paths[0] != "" || paths[1] != "" {
			// Either path alone requests paid mode; never silently start free.
			p = Params{BootstrapSecret: "test", KeystoreFile: paths[0], KeystorePasswordFile: paths[1]}
			require.ErrorContains(t, p.Validate(), "on-chain payment requires")
		}
	}
	require.ErrorContains(t, (Params{BootstrapSecret: "test", KeystorePassword: new("")}).Validate(), "on-chain payment requires")
}

func TestDefaultStorageAndRecovery(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0755))
	legacy := filepath.Join(root, "lpdb.sqlite3")
	require.NoError(t, os.WriteFile(legacy, []byte("legacy"), 0644))
	_, err := execute(t, "migrate", "up", "--data-dir", root)
	require.NoError(t, err)
	path := filepath.Join(root, "payments.sqlite")
	info, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm(), "existing datadir permissions are preserved")
	require.FileExists(t, path)
	config := `{"Network":"arbitrum-one-mainnet","DataDir":".","KeystoreFile":"missing-key","KeystorePasswordFile":"missing-password","RPCURLFile":"missing-rpc","RunnerConfig":"missing-runners","BootstrapSecretFile":"missing-bootstrap"}`
	require.NoError(t, os.WriteFile(filepath.Join(root, "config.json"), []byte(config), 0600))
	out, err := execute(t, "redemptions", "--config", filepath.Join(root, "config.json"), "--network", "arbitrum-one-mainnet")
	require.NoError(t, err)
	require.JSONEq(t, "[]", out)
	var requests atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var request struct{ ID any }
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "0x1"}))
	}))
	defer rpc.Close()
	require.NoError(t, os.WriteFile(filepath.Join(root, "new-rpc"), []byte(rpc.URL), 0600))
	_, err = execute(t, "redemptions", "--config", filepath.Join(root, "config.json"), "--retry-transaction", ethcommon.Hash{31: 1}.Hex(), "--submit", "--rpc-url-file", filepath.Join(root, "new-rpc"), "--chain-id", "2")
	require.ErrorContains(t, err, "chain ID", "explicit RPC file replaces the missing service credential")
	require.Equal(t, int32(1), requests.Load())
	data, err := os.ReadFile(legacy)
	require.NoError(t, err)
	require.Equal(t, "legacy", string(data))
	info, err = os.Stat(legacy)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0644), info.Mode().Perm())
	info, err = os.Stat(root)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm())
}

func TestOnchainCommandDefaults(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	for _, name := range []string{"CONFIG", "NETWORK", "DATA_DIR", "REDEEMER_DB"} {
		t.Setenv("LIVEPEER_ORCHESTRATOR_"+name, "")
	}
	root := filepath.Join(home, ".lpData", nodeconfig.Mainnet, "orchestrator")
	require.NoError(t, os.MkdirAll(root, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "config.toml"), []byte("Network = 'arbitrum-one-mainnet'\nRedeemerDB = 'custom.sqlite'"), 0600))
	_, err := execute(t, "migrate", "up")
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(root, "custom.sqlite"))
	output, err := execute(t, "redemptions")
	require.NoError(t, err)
	require.JSONEq(t, "[]", output)
	require.NoDirExists(t, filepath.Join(home, ".lpData", "offchain"))
	output, err = execute(t, "--print-config")
	require.NoError(t, err)
	require.Contains(t, output, `Network = "offchain"`)
}

func TestDisabledPaymentsDoNotCreateStorage(t *testing.T) {
	root := filepath.Join(t.TempDir(), "missing")
	out, err := execute(t, "--data-dir", root, "--network", "arbitrum-one-mainnet", "--print-config")
	require.NoError(t, err)
	require.NotContains(t, out, "RedeemerDB")
	require.NoDirExists(t, root)
}

func TestChainMismatchBeforeStorage(t *testing.T) {
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ ID any }
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "0x2"}))
	}))
	defer rpc.Close()
	p := paymentKeystoreParams(t, rpc.URL)
	p.KeystoreFile, p.KeystorePasswordFile = test.WriteKeystore(t, nil)
	p.RedeemerDB = filepath.Join(t.TempDir(), "missing", "payments.sqlite")
	require.ErrorContains(t, Serve(t.Context(), p, io.Discard), "chain ID")
	require.NoDirExists(t, filepath.Dir(p.RedeemerDB))
}

// chainViewHTTPParams starts a paid orchestrator's configuration against a counting RPC.
func chainViewHTTPParams(t *testing.T, rpc *test.ChainRPC) (Params, string) {
	t.Helper()
	mainPort, metricsPort := freeTCPPort(t), freeTCPPort(t)
	p := paymentKeystoreParams(t, rpc.URL)
	p.HeartbeatInterval = 50 * time.Millisecond
	p.KeystoreFile, p.KeystorePasswordFile = test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(p.KeystoreFile, p.KeystorePasswordFile)
	require.NoError(t, err)
	rpc.Mine(func(state *test.ChainState) { state.SetActive(key.Address(), true) })
	controller := rpc.Controller
	p.Controller = &controller
	p.Listen = netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", mainPort))
	p.MetricsListen = netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", metricsPort))
	p.ServiceURL = boa.Text[*url.URL]{Value: mustURL(t, "http://"+p.Listen.String())}
	p.RunnerConfig = filepath.Join(t.TempDir(), "runners.toml")
	require.NoError(t, os.WriteFile(p.RunnerConfig, []byte("[[Runners]]\nID = 'r'\nRunnerURL = 'https://runner.example'\nApp = 'test'\n[Runners.PriceInfo]\nPrice = 1\n"), 0600))
	return p, "http://" + p.MetricsListen.String()
}

func serveForTest(t *testing.T, p Params) {
	t.Helper()
	require.NoError(t, p.Validate())
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, p, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("orchestrator did not stop")
		}
	})
}

// A spare keep-alive connection would hold up server shutdown for five seconds.
var chainViewTestClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}

func requestChallenge(t *testing.T, p Params) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, p.ServiceURL.String()+"/apps/r/session", nil)
	require.NoError(t, err)
	request.Header.Set("Livepeer-Payer-Address", ethcommon.HexToAddress("0x2000").Hex())
	response, err := chainViewTestClient.Do(request)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func requestStatus(t *testing.T, url string) int {
	t.Helper()
	response, err := chainViewTestClient.Get(url)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func TestPaidHTTPReadsThroughTheChainView(t *testing.T) {
	rpc := test.NewChainRPC(t)
	p, metrics := chainViewHTTPParams(t, rpc)
	p.ChainRefreshInterval, p.ChainMaxAge = 50*time.Millisecond, 400*time.Millisecond
	serveForTest(t, p)
	require.Eventually(t, func() bool { return requestStatus(t, metrics+"/readyz") == http.StatusOK }, 7*time.Second, 10*time.Millisecond)

	// Challenges add nothing to what the timers read.
	rpc.Reset()
	for range 50 {
		require.Equal(t, http.StatusPaymentRequired, requestChallenge(t, p))
	}
	stats := rpc.Stats()
	require.Less(t, stats.Calls["BondingManager.isActiveTranscoder"], 50, "a challenge reads the view")

	// An RPC outage turns into refusal, and readiness says so.
	rpc.SetDown(true)
	require.Eventually(t, func() bool { return requestStatus(t, metrics+"/readyz") == http.StatusServiceUnavailable }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, http.StatusBadGateway, requestChallenge(t, p), "as a failed chain read answers")
	rpc.SetDown(false)
	require.Eventually(t, func() bool { return requestStatus(t, metrics+"/readyz") == http.StatusOK }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, http.StatusPaymentRequired, requestChallenge(t, p))

	response, err := chainViewTestClient.Get(metrics + "/metrics")
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	for _, family := range []string{"livepeer_chain_view_age_seconds", `livepeer_chain_view_refreshes_total{component="orchestrator",outcome="success"}`, `livepeer_chain_view_refreshes_total{component="orchestrator",outcome="error"}`, `livepeer_rpc_contract_calls_total{component="orchestrator",contract="controller",function="getContract"}`, `livepeer_rpc_requests_total{component="orchestrator",method="batch",outcome="success"}`, "livepeer_payment_operation_duration_seconds_count"} {
		require.Contains(t, string(body), family)
	}
}

func TestPaidHTTPWithoutAChainViewReadsDirectly(t *testing.T) {
	rpc := test.NewChainRPC(t)
	p, _ := chainViewHTTPParams(t, rpc)
	p.HeartbeatInterval, p.HeartbeatTTL = time.Hour, 2*time.Hour // Quiet the timers.
	require.Zero(t, p.ChainRefreshInterval)
	serveForTest(t, p)
	require.Eventually(t, func() bool { return requestChallenge(t, p) == http.StatusPaymentRequired }, 5*time.Second, 10*time.Millisecond)
	rpc.Reset()
	require.Equal(t, http.StatusPaymentRequired, requestChallenge(t, p))
	stats := rpc.Stats()
	require.Equal(t, 6, stats.RoundTrips, "a challenge reads the chain itself, as it did before chain views")
	require.Zero(t, stats.Batches)
}
