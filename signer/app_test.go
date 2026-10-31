package signer

import (
	"bytes"
	"context"
	"encoding/hex"
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm/wire"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func validParams(t *testing.T) Params {
	t.Helper()
	return Params{
		DiscoveryRefreshInterval: 5 * time.Minute, DiscoveryMaxFailedRefreshes: 6,
		Listen:        netip.MustParseAddrPort("0.0.0.0:8937"),
		MetricsListen: netip.MustParseAddrPort("127.0.0.1:8938"),
		KeystoreFile:  "key", KeystorePasswordFile: "password", DepositMultiplier: 1,
		RPCURL:         testURL(t, "http://localhost:8545"),
		Controller:     ethcommon.HexToAddress("0x1234567890123456789012345678901234567890"),
		ETHUSDFeed:     ethcommon.HexToAddress("0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"),
		MaxHourlyPrice: big.NewRat(36, 1), MaxFixedPrice: big.NewRat(1, 1),
		WeiPerUSD:     big.NewRat(100, 1),
		Orchestrators: []boa.Text[*url.URL]{{Value: testURL(t, "http://localhost:8935")}},
	}
}

func TestSignerConfigurationValidation(t *testing.T) {
	require.NoError(t, validParams(t).Validate())
	p := validParams(t)
	p.DiscoveryMaxFailedRefreshes = 0
	require.NoError(t, p.Validate())
	for _, test := range []struct {
		name   string
		change func(*Params)
		want   string
	}{
		{"missing password file", func(p *Params) { p.KeystorePasswordFile = "" }, "keystore-password-file"},
		{"empty Kafka", func(p *Params) { p.Kafka = &KafkaConfig{} }, "kafka broker"},
		{"RPC scheme", func(p *Params) { p.RPCURL = testURL(t, "file:///rpc") }, "RPC URL"},
		{"Controller", func(p *Params) { p.Controller = ethcommon.Address{} }, "controller-address"},
		{"feed", func(p *Params) { p.ETHUSDFeed = ethcommon.Address{}; p.WeiPerUSD = nil }, "eth-usd-feed"},
		{"hourly price", func(p *Params) { p.MaxHourlyPrice = big.NewRat(0, 1) }, "max-hourly-price"},
		{"fixed price", func(p *Params) { p.MaxFixedPrice = big.NewRat(0, 1) }, "max-fixed-price"},
		{"fixed rate", func(p *Params) { p.WeiPerUSD = big.NewRat(0, 1) }, "wei-per-usd"},
		{"feed age", func(p *Params) { p.WeiPerUSD = nil }, "eth-usd-max-age"},
		{"metrics", func(p *Params) { p.MetricsListen = netip.MustParseAddrPort("0.0.0.0:8938") }, "loopback"},
		{"zero deposit multiplier", func(p *Params) { p.DepositMultiplier = 0 }, "deposit multiplier"},
		{"headers without webhook", func(p *Params) { p.AuthWebhookHeaders = Headers{"Authorization": {"Bearer token"}} }, "webhook URL"},
		{"orchestrator headers without webhook", func(p *Params) { p.OrchestratorWebhookHeaders = Headers{"Authorization": {"Bearer token"}} }, "orchestrator webhook URL"},
		{"orchestrator webhook interval", func(p *Params) { p.OrchestratorWebhook = testURL(t, "https://webhook.example") }, "orchestrator-webhook-refresh-interval"},
		{"negative webhook interval", func(p *Params) {
			p.OrchestratorWebhook = testURL(t, "https://webhook.example")
			p.OrchestratorWebhookRefreshInterval = -time.Minute
		}, "orchestrator-webhook-refresh-interval"},
		{"invalid orchestrator webhook", func(p *Params) { p.OrchestratorWebhook = testURL(t, "https://user:secret@webhook.example") }, "orchestrator webhook URL"},
		{"zero discovery interval", func(p *Params) { p.DiscoveryRefreshInterval = 0 }, "discovery-refresh-interval"},
		{"discovery failure threshold", func(p *Params) { p.DiscoveryMaxFailedRefreshes = -1 }, "discovery-max-failed-refreshes"},
		{"invalid discovery grant", func(p *Params) { p.DiscoveryGrants = []string{"ftp://localhost"} }, "signer-discovery destination grant"},
		{"negative chain refresh", func(p *Params) { p.ChainRefreshInterval = -time.Second }, "chain-refresh-interval"},
		{"chain max age at the interval", func(p *Params) { p.ChainRefreshInterval, p.ChainMaxAge = time.Second, time.Second }, "chain-max-age"},
		{"chain max age below the interval", func(p *Params) { p.ChainRefreshInterval, p.ChainMaxAge = time.Minute, time.Second }, "chain-max-age"},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := validParams(t)
			test.change(&p)
			require.ErrorContains(t, p.Validate(), test.want)
		})
	}
}

func TestSignerKeystoreSourcesFailBeforeStartup(t *testing.T) {
	path, passwordPath := test.WriteKeystore(t, nil)
	require.NoError(t, os.WriteFile(passwordPath, []byte("wrong-secret"), 0600))
	var requests atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer rpc.Close()
	t.Setenv("LIVEPEER_SIGNER_RPC_URL", rpc.URL)
	// Occupied ports would produce a different error if startup reached listeners.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	for source, name := range []string{"config", "env", "cli"} {
		t.Run(name, func(t *testing.T) {
			paths := [3][2]string{{"/missing/key", "/missing/password"}, {"/missing/key", "/missing/password"}, {"/missing/key", "/missing/password"}}
			paths[source] = [2]string{path, passwordPath}
			config := filepath.Join(t.TempDir(), "signer.toml")
			require.NoError(t, os.WriteFile(config, []byte(fmt.Sprintf("KeystoreFile = %q\nKeystorePasswordFile = %q\nMaxHourlyPrice = '36'\nMaxFixedPrice = '1'\n", paths[0][0], paths[0][1])), 0600))
			args := []string{"--config", config, "--listen", listener.Addr().String()}
			if source > 0 {
				t.Setenv("LIVEPEER_SIGNER_KEYSTORE_FILE", paths[1][0])
				t.Setenv("LIVEPEER_SIGNER_KEYSTORE_PASSWORD_FILE", paths[1][1])
			}
			if source > 1 {
				args = append(args, "--keystore-file", paths[2][0], "--keystore-password-file", paths[2][1])
			}
			root := Root(io.Discard, io.Discard)
			root.SetArgs(args)
			err := root.Execute()
			require.ErrorContains(t, err, "cannot decrypt keystore")
			require.NotContains(t, err.Error(), "wrong-secret")
			require.Zero(t, requests.Load())
		})
	}
}

func TestSignerLoadsConfigAndSecretFiles(t *testing.T) {
	for _, ext := range []string{".toml", ".json"} {
		t.Run(ext, func(t *testing.T) {
			t.Chdir(t.TempDir())
			keyFile, passwordPath := test.WriteKeystore(t, nil)
			folder := filepath.Dir(keyFile)
			rpcFile := filepath.Join(folder, "rpc")
			headersFile := filepath.Join(folder, "headers")
			webhookFile := filepath.Join(folder, "webhook")
			require.NoError(t, os.WriteFile(rpcFile, []byte("http://localhost:8545?key=private-token"), 0600))
			require.NoError(t, os.WriteFile(webhookFile, []byte("http://localhost:9000/auth"), 0600))
			require.NoError(t, os.WriteFile(headersFile, []byte("Authorization: Bearer private-token, X-User: alice"), 0600))
			config := `Listen = "127.0.0.1:9001"
MetricsListen = "127.0.0.1:9002"
KeystoreFile = "keystore.json"
KeystorePasswordFile = "password"
RPCURLFile = "rpc"
AuthWebhookFile = "webhook"
AuthWebhookHeadersFile = "headers"
OrchestratorWebhookFile = "webhook"
OrchestratorWebhookHeadersFile = "headers"
OrchestratorWebhookRefreshInterval = "2m"
ChainID = 42161
Controller = "0xD8E8328501E9645d16Cf49539efC04f734606ee4"
ETHUSDFeed = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"
ETHUSDMaxAge = "1h"
MaxHourlyPrice = "36"
MaxFixedPrice = "1/2"
MaxTicketEV = "3000000000000"
Orchestrators = ["http://127.0.0.1:8935"]
DiscoveryGrants = ["127.0.0.1:8935"]
`
			configFile := filepath.Join(folder, "signer"+ext)
			test.WriteConfig(t, configFile, config)
			var p Params
			cmd := boa.Cmd[Params]{Params: &p, RawArgs: []string{"--config", configFile, "--data-dir", t.TempDir()}, RejectUnknown: true,
				RunFuncCtxE: func(ctx *boa.HookContext, p *Params, _ *cobra.Command, _ []string) error {
					dumped, err := ctx.DumpBytes(".toml", nil)
					require.NoError(t, err)
					require.NotContains(t, string(dumped), p.OrchestratorWebhook.String())
					require.NotContains(t, string(dumped), http.Header(p.OrchestratorWebhookHeaders).Get("Authorization"))
					require.Contains(t, string(dumped), "OrchestratorWebhookRefreshInterval")
					return p.Validate()
				},
			}
			require.NoError(t, cmd.RunArgsE(cmd.RawArgs))
			require.Equal(t, uint64(42161), *p.ChainID)
			require.Equal(t, "0xD8E8328501E9645d16Cf49539efC04f734606ee4", p.Controller.Hex())
			require.Equal(t, "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612", p.ETHUSDFeed.Hex())
			require.Equal(t, netip.MustParseAddrPort("127.0.0.1:9001"), p.Listen)
			require.Equal(t, keyFile, p.KeystoreFile)
			require.Equal(t, passwordPath, p.KeystorePasswordFile)
			require.NotNil(t, p.KeystorePassword)
			require.Equal(t, "private-token", p.RPCURL.Query().Get("key"))
			require.Equal(t, "http://localhost:9000/auth", p.AuthWebhook.String())
			require.Equal(t, "Bearer private-token", http.Header(p.AuthWebhookHeaders).Get("Authorization"))
			require.Equal(t, "http://localhost:9000/auth", p.OrchestratorWebhook.String())
			require.Equal(t, "Bearer private-token", http.Header(p.OrchestratorWebhookHeaders).Get("Authorization"))
			require.Equal(t, 2*time.Minute, p.OrchestratorWebhookRefreshInterval)
			require.Equal(t, big.NewRat(36, 1), p.MaxHourlyPrice)
			require.Equal(t, big.NewRat(1, 2), p.MaxFixedPrice)
			require.Equal(t, time.Hour, p.ETHUSDMaxAge)
			require.Equal(t, "http://127.0.0.1:8935", p.Orchestrators[0].Value.String())
			require.Equal(t, []string{"127.0.0.1:8935"}, p.DiscoveryGrants)
		})
	}
}

func loadSignerParams(t *testing.T, args []string, config string) (Params, error) {
	t.Helper()
	p := validParams(t)
	p.KeystorePasswordFile = filepath.Join(t.TempDir(), "password")
	require.NoError(t, os.WriteFile(p.KeystorePasswordFile, []byte("test"), 0600))
	args = append([]string(nil), args...)
	if config != "" {
		path := filepath.Join(t.TempDir(), "signer.toml")
		require.NoError(t, os.WriteFile(path, []byte(config), 0600))
		args = append(args, "--config", path)
	}
	cmd := boa.Cmd[Params]{Params: &p, RawArgs: args, RejectUnknown: true,
		ParamEnrich: boa.ParamEnricherCombine(boa.ParamEnricherDefault, boa.ParamEnricherEnv, boa.ParamEnricherEnvPrefix("LIVEPEER_SIGNER")),
		RunFuncE:    func(p *Params, _ *cobra.Command, _ []string) error { return p.Validate() },
	}
	err := cmd.RunArgsE(args)
	return p, err
}

func TestSignerDiscoveryGrantConfiguration(t *testing.T) {
	for _, test := range []struct {
		name   string
		args   []string
		env    string
		config string
		want   []string
		bad    bool
	}{
		{name: "omitted"},
		{name: "CLI", args: []string{"--discovery-grants", "127.0.0.1:8935,::1,10.0.0.0/8,fc00::/7"}, want: []string{"127.0.0.1:8935", "::1", "10.0.0.0/8", "fc00::/7"}},
		{name: "environment", env: "127.0.0.1:8935,::1,10.0.0.0/8,fc00::/7", want: []string{"127.0.0.1:8935", "::1", "10.0.0.0/8", "fc00::/7"}},
		{name: "TOML", config: "DiscoveryGrants = ['127.0.0.1:8935', '::1', '10.0.0.0/8', 'fc00::/7']\n", want: []string{"127.0.0.1:8935", "::1", "10.0.0.0/8", "fc00::/7"}},
		{name: "invalid CIDR CLI", args: []string{"--discovery-grants", "10.0.0.0/33"}, bad: true},
		{name: "invalid CLI", args: []string{"--discovery-grants", "ftp://localhost"}, bad: true},
		{name: "invalid environment", env: "127.0.0.1:0", bad: true},
		{name: "invalid TOML", config: "DiscoveryGrants = ['[::1]:65536']\n", bad: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("LIVEPEER_SIGNER_DISCOVERY_GRANTS", test.env)
			p, err := loadSignerParams(t, test.args, test.config)
			if test.bad {
				require.ErrorContains(t, err, "signer-discovery destination grant")
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, p.DiscoveryGrants)
		})
	}
}

func TestSignerOrchestratorWebhookConfiguration(t *testing.T) {
	t.Setenv("LIVEPEER_SIGNER_ORCHESTRATOR_WEBHOOK", "https://webhook.example?token=private-token")
	t.Setenv("LIVEPEER_SIGNER_ORCHESTRATOR_WEBHOOK_HEADERS", "Authorization: Bearer private-header")
	p, err := loadSignerParams(t, nil, "")
	require.NoError(t, err)
	require.Equal(t, 5*time.Minute, p.OrchestratorWebhookRefreshInterval)
	require.Equal(t, "private-token", p.OrchestratorWebhook.Query().Get("token"))
	require.Equal(t, "Bearer private-header", http.Header(p.OrchestratorWebhookHeaders).Get("Authorization"))
	t.Setenv("LIVEPEER_SIGNER_ORCHESTRATOR_WEBHOOK_REFRESH_INTERVAL", "45s")
	p, err = loadSignerParams(t, nil, "")
	require.NoError(t, err)
	require.Equal(t, 45*time.Second, p.OrchestratorWebhookRefreshInterval)
	p, err = loadSignerParams(t, []string{"--orchestrator-webhook-refresh-interval", "30s"}, "")
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, p.OrchestratorWebhookRefreshInterval)
}

func TestSignerOrchestratorWebhookSecrets(t *testing.T) {
	for _, flag := range []string{"--orchestrator-webhook", "--orchestrator-webhook-headers"} {
		_, err := loadSignerParams(t, []string{flag, "private-secret"}, "")
		require.ErrorContains(t, err, "unknown flag")
		require.NotContains(t, err.Error(), "private-secret")
	}
	const endpoint = "https://webhook.example?token=private-token"
	urlFile := filepath.Join(t.TempDir(), "webhook")
	require.NoError(t, os.WriteFile(urlFile, []byte(endpoint), 0600))
	t.Setenv("LIVEPEER_SIGNER_ORCHESTRATOR_WEBHOOK", endpoint)
	_, err := loadSignerParams(t, []string{"--orchestrator-webhook-file", urlFile}, "")
	require.ErrorContains(t, err, "cannot both be set")
	require.NotContains(t, err.Error(), "private-token")
}

func TestSignerReadiness(t *testing.T) {
	for _, fixedRate := range []bool{true, false} {
		t.Run(fmt.Sprintf("fixedRate=%v", fixedRate), func(t *testing.T) {
			p := validParams(t)
			if !fixedRate {
				p.WeiPerUSD, p.ETHUSDMaxAge = nil, time.Hour
			}
			key, keyFile, passwordPath := testSignerKey(t)
			p.KeystoreFile, p.KeystorePasswordFile = keyFile, passwordPath
			var failure atomic.Int32
			webhookCalled := make(chan struct{}, 1)
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/list" {
					webhookCalled <- struct{}{}
					_, _ = io.WriteString(w, "[]")
					return
				}
				var req struct {
					Method string
					Params []json.RawMessage
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
				var result any
				switch req.Method {
				case "eth_chainId":
					result = "0x1"
				case "eth_getBlockByNumber":
					if failure.Load() != 1 {
						result = map[string]string{"number": "0x32", "hash": ethcommon.HexToHash("0x1234").Hex()}
					}
				case "eth_call":
					var call struct {
						Data string `json:"input"`
					}
					require.NoError(t, json.Unmarshal(req.Params[0], &call))
					selector := func(sig string) bool {
						return strings.HasPrefix(call.Data, "0x"+hex.EncodeToString(crypto.Keccak256([]byte(sig))[:4]))
					}
					switch {
					case selector("getContract(bytes32)"):
						result = fmt.Sprintf("0x%064x", 1)
					case selector("lastInitializedRound()"):
						result = fmt.Sprintf("0x%064x", 5)
					case selector("blockHashForRound(uint256)"):
						result = ethcommon.HexToHash("0x1234").Hex()
					case selector("getSenderInfo(address)"):
						deposit, reserve, withdraw := 1000, 1000, 0
						switch failure.Load() {
						case 2:
							deposit = 0
						case 3:
							reserve = 0
						case 4:
							withdraw = 6
						}
						result = fmt.Sprintf("0x%064x%064x%064x%064x", deposit, withdraw, reserve, 0)
					case selector("description()"):
						result = "0x" // The oracle is unavailable during startup.
					default:
						t.Errorf("unexpected RPC call: %s", call.Data)
					}
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
			}))
			defer rpc.Close()
			p.RPCURL = testURL(t, rpc.URL)
			p.OrchestratorWebhook = testURL(t, rpc.URL+"/list")
			p.OrchestratorWebhookRefreshInterval = time.Hour
			p.DiscoveryGrants = []string{"127.0.0.1"}
			free := func() netip.AddrPort {
				l, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				address := l.Addr().(*net.TCPAddr).AddrPort()
				require.NoError(t, l.Close())
				return address
			}
			p.Listen, p.MetricsListen = free(), free()
			var outbox *eventOutbox
			if fixedRate {
				eventBytes, err := json.Marshal(testEvent())
				require.NoError(t, err)
				p.Kafka = &KafkaConfig{Broker: kafkaBrokerURL(t, "kafka://127.0.0.1:1"), Topic: "signing", OutboxDB: filepath.Join(t.TempDir(), "events.sqlite"), OutboxMaxBytes: int64(len(eventBytes))}
				brokerAddress, err := p.Kafka.brokerAddress()
				require.NoError(t, err)
				outbox, err = openEventOutbox(t.Context(), p.Kafka.OutboxDB, outboxBinding{key.Address().Hex(), brokerAddress, p.Kafka.Topic}, p.Kafka.OutboxMaxBytes)
				require.NoError(t, err)
				defer func() { require.NoError(t, outbox.Close()) }()
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- serve(ctx, p) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(3 * time.Second):
					t.Error("signer did not stop")
				}
			})
			client := &http.Client{Timeout: time.Second}
			select {
			case <-webhookCalled:
			case <-time.After(3 * time.Second):
				t.Fatal("signer did not start the webhook worker")
			}
			get := func(path string) int {
				response, err := client.Get("http://" + p.MetricsListen.String() + path)
				if err != nil {
					return 0
				}
				defer response.Body.Close()
				return response.StatusCode
			}
			if !fixedRate {
				require.Eventually(t, func() bool { return get("/readyz") == 503 }, 3*time.Second, 10*time.Millisecond)
				require.Equal(t, 200, get("/healthz"))
				return
			}
			require.Eventually(t, func() bool { return get("/readyz") == 200 }, 3*time.Second, 10*time.Millisecond)
			for _, state := range []int32{1, 2, 3, 4, 0} {
				failure.Store(state)
				want := 503
				if state == 0 {
					want = 200
				}
				require.Equal(t, want, get("/readyz"))
				require.Equal(t, 200, get("/healthz"))
			}
			_, err := outbox.enqueue(t.Context(), testEvent())
			require.NoError(t, err)
			require.Equal(t, 503, get("/readyz"), "a full outbox must make the signer unready")
			response, err := client.Get("http://" + p.MetricsListen.String() + "/metrics")
			require.NoError(t, err)
			metrics, err := io.ReadAll(response.Body)
			require.NoError(t, response.Body.Close())
			require.NoError(t, err)
			require.Contains(t, string(metrics), "livepeer_signer_kafka_pending_events 1")
			pending, err := outbox.pending(t.Context())
			require.NoError(t, err)
			require.NoError(t, outbox.acknowledge(t.Context(), pending))
			require.Equal(t, 200, get("/readyz"), "storage readiness recovers even while Kafka is unavailable")
		})
	}
}

func TestSignerServesPaymentsThroughTheChainView(t *testing.T) {
	key, keyFile, passwordPath := testSignerKey(t)
	_, info := testService(t)
	info.TicketParams.ExpirationBlock = big.NewInt(5000).Bytes()
	rpc := test.NewChainRPC(t)
	rpc.Mine(func(state *test.ChainState) {
		state.SetSender(key.Address(), test.ChainSender{Deposit: big.NewInt(1e18), Reserve: big.NewInt(1e18), WithdrawRound: new(big.Int)})
	})
	free := func() netip.AddrPort {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		address := l.Addr().(*net.TCPAddr).AddrPort()
		require.NoError(t, l.Close())
		return address
	}
	p := validParams(t)
	p.KeystoreFile, p.KeystorePasswordFile, p.RPCURL, p.Controller, p.Orchestrators = keyFile, passwordPath, testURL(t, rpc.URL), rpc.Controller, nil
	p.Listen, p.MetricsListen = free(), free()
	p.ChainRefreshInterval, p.ChainMaxAge = 50*time.Millisecond, 400*time.Millisecond
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, p) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("signer did not stop")
		}
	})
	status := func(path string) int {
		response, err := http.Get("http://" + p.MetricsListen.String() + path)
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		return response.StatusCode
	}
	pay := func() int {
		body, err := json.Marshal(map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
		require.NoError(t, err)
		response, err := http.Post("http://"+p.Listen.String()+"/generate-live-payment", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer response.Body.Close()
		return response.StatusCode
	}
	require.Eventually(t, func() bool { return status("/readyz") == http.StatusOK }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, http.StatusOK, pay())
	rpc.Reset()
	for range 50 {
		require.Equal(t, http.StatusOK, pay())
	}
	require.Less(t, rpc.Stats().Calls["TicketBroker.getSenderInfo"]+rpc.Stats().Calls["eth_getBlockByNumber"], 50, "a known payer reads the view")

	rpc.SetDown(true)
	require.Eventually(t, func() bool { return status("/readyz") == http.StatusServiceUnavailable }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, http.StatusBadRequest, pay(), "as a failed chain read answers")
	require.Equal(t, http.StatusOK, status("/healthz"))
	rpc.SetDown(false)
	require.Eventually(t, func() bool { return status("/readyz") == http.StatusOK }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, http.StatusOK, pay())

	response, err := http.Get("http://" + p.MetricsListen.String() + "/metrics")
	require.NoError(t, err)
	defer response.Body.Close()
	metrics, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	for _, family := range []string{"livepeer_chain_view_age_seconds", "livepeer_chain_view_tracked_payers", `livepeer_chain_view_refreshes_total{component="signer",outcome="error"}`, `livepeer_rpc_contract_calls_total{component="signer",contract="ticketBroker",function="getSenderInfo"}`, `livepeer_signer_payment_duration_seconds_count{component="signer",type="fixed"}`} {
		require.Contains(t, string(metrics), family)
	}
}

func TestSignerChainViewSettings(t *testing.T) {
	parse := func(t *testing.T, config string, args ...string) Params {
		t.Helper()
		path := filepath.Join(t.TempDir(), "signer.toml")
		require.NoError(t, os.WriteFile(path, []byte(config), 0600))
		var p Params
		cmd := boa.Cmd[Params]{Params: &p, RawArgs: append([]string{"--config", path, "--data-dir", t.TempDir(), "--max-hourly-price", "1", "--max-fixed-price", "1", "--wei-per-usd", "1"}, args...), RejectUnknown: true,
			ParamEnrich: boa.ParamEnricherCombine(boa.ParamEnricherDefault, boa.ParamEnricherEnv, boa.ParamEnricherEnvPrefix("LIVEPEER_SIGNER")),
			RunFuncE:    func(*Params, *cobra.Command, []string) error { return nil },
		}
		t.Setenv("LIVEPEER_SIGNER_RPC_URL", "http://localhost:8545")
		t.Setenv("LIVEPEER_SIGNER_KEYSTORE_PASSWORD", "test")
		require.NoError(t, cmd.RunArgsE(cmd.RawArgs))
		return p
	}
	p := parse(t, "")
	require.Equal(t, 5*time.Second, p.ChainRefreshInterval, "enabled by default")
	require.Equal(t, 30*time.Second, p.ChainMaxAge)
	p = parse(t, "ChainRefreshInterval = '2s'\nChainMaxAge = '10s'\n")
	require.Equal(t, 2*time.Second, p.ChainRefreshInterval)
	require.Equal(t, 10*time.Second, p.ChainMaxAge)
	t.Setenv("LIVEPEER_SIGNER_CHAIN_MAX_AGE", "20s")
	p = parse(t, "ChainMaxAge = '10s'\n", "--chain-refresh-interval", "0")
	require.Zero(t, p.ChainRefreshInterval, "operators can opt out")
	require.Equal(t, 20*time.Second, p.ChainMaxAge)
	opted := validParams(t)
	opted.ChainMaxAge = 0
	require.NoError(t, opted.Validate(), "an interval of 0 needs no maximum age")
}

func TestStorageDefaultsAndChainCheck(t *testing.T) {
	root := t.TempDir()
	cmd := Root(io.Discard, io.Discard)
	cmd.SetArgs([]string{"migrate", "up", "--data-dir", root})
	require.NoError(t, cmd.Execute())
	path := filepath.Join(root, "events.sqlite")
	info, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0755), info.Mode().Perm(), "existing datadir permissions are preserved")
	require.FileExists(t, path)
	config := filepath.Join(root, "config.toml")
	require.NoError(t, os.WriteFile(config, []byte("Network = 'custom'\nDataDir = 'configured'\n"), 0600))
	cmd = Root(io.Discard, io.Discard)
	cmd.SetArgs([]string{"migrate", "up", "--config", config})
	require.NoError(t, cmd.Execute())
	require.FileExists(t, filepath.Join(root, "configured", "events.sqlite"))
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct{ ID any }
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "0x2"}))
	}))
	defer rpc.Close()
	p := validParams(t)
	p.KeystoreFile, p.KeystorePasswordFile = test.WriteKeystore(t, nil)
	p.RPCURL = testURL(t, rpc.URL)
	p.ChainID = new(uint64(1))
	p.Kafka = &KafkaConfig{OutboxDB: filepath.Join(root, "missing", "events.sqlite"), OutboxMaxBytes: 1024, Topic: "events", AuthMethod: "scram-sha-512"}
	require.NoError(t, p.Kafka.Broker.UnmarshalText([]byte("broker.example.com")))
	require.ErrorContains(t, serve(t.Context(), p), "chain ID")
	require.NoDirExists(t, filepath.Join(root, "missing"))
}
