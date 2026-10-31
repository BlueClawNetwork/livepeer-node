package signer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

type pipeListener struct {
	conn             net.Conn
	accepted, closed chan struct{}
	once             sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case <-l.accepted:
		<-l.closed
		return nil, net.ErrClosed
	default:
		close(l.accepted)
		return l.conn, nil
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *pipeListener) Addr() net.Addr { return l.conn.LocalAddr() }

type unavailablePayer struct{}

func (unavailablePayer) PayerFunds(context.Context, ethcommon.Address, ethcommon.Address) (pm.PayerFunds, error) {
	return pm.PayerFunds{}, errors.New("payer has no deposit")
}

type fundedPayer struct{}

func (fundedPayer) PayerFunds(context.Context, ethcommon.Address, ethcommon.Address) (pm.PayerFunds, error) {
	return pm.PayerFunds{Snapshot: eth.ChainSnapshot{Block: big.NewInt(50), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, Deposit: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), Reserve: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), WithdrawRound: new(big.Int)}, nil
}

func testSignerKey(t *testing.T) (*eth.Key, string, string) {
	t.Helper()
	keyFile, passwordPath := test.WriteKeystore(t, nil)
	key, err := eth.OpenKeystoreFile(keyFile, passwordPath)
	require.NoError(t, err)
	return key, keyFile, passwordPath
}

func testService(t *testing.T) (*Service, wire.OrchestratorInfo) {
	t.Helper()
	key, _, _ := testSignerKey(t)
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	info := wire.OrchestratorInfo{Transcoder: "https://orch.example.com", Address: ethcommon.HexToAddress("0x1234").Bytes(), Price: wire.PriceInfo{PricePerUnit: 10, UnitsPerPrice: 1},
		TicketParams: wire.TicketParams{Recipient: ethcommon.HexToAddress("0x1234").Bytes(), FaceValue: big.NewInt(20).Bytes(), WinProb: new(big.Int).Add(new(big.Int).Quo(max, big.NewInt(2)), big.NewInt(1)).Bytes(), RecipientRandHash: crypto.Keccak256(make([]byte, 32)), Seed: big.NewInt(1).Bytes(), ExpirationBlock: big.NewInt(500).Bytes(), Expiration: wire.ExpirationParams{CreationRound: 5, CreationRoundBlockHash: ethcommon.HexToHash("0x1234").Bytes()}},
		Auth:         wire.AuthToken{Token: []byte("token"), SessionID: "manifest-1", Expiration: time.Now().Add(time.Hour).Unix()}}
	service := newService(key)
	service.SetPaymentChain(fundedPayer{})
	prices, err := newPricePolicy(new(big.Rat).Mul(testRat(t, "1000000000000"), big.NewRat(3600, 1)), testRat(t, "1000000000000"))
	require.NoError(t, err)
	require.NoError(t, prices.setRate(big.NewRat(1, 1), time.Time{}))
	service.pricePolicy = prices
	return service, info
}

func postPayment(t *testing.T, s *Service, request map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(request)
	require.NoError(t, err)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/generate-live-payment", bytes.NewReader(body)))
	return w
}

func TestOversizedPaymentRequestClosesConnection(t *testing.T) {
	s, _ := testService(t)
	defer s.Close()
	countingKey := &failingPaymentSigner{TicketSigner: s.key}
	s.key = countingKey
	server := httptest.NewServer(s)
	defer server.Close()
	response, err := http.Post(server.URL+"/generate-live-payment", "application/json", strings.NewReader(strings.Repeat("x", (1<<20)+1)))
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode, string(body))
	require.True(t, response.Close, "oversized bodies must retire the HTTP connection")
	require.Zero(t, countingKey.calls.Load())
}

func requirePaymentFailure(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	var response map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Contains(t, response, "error")
	// Decode the error envelope independently of signerError.
	var apiError struct {
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(response["error"], &apiError))
	require.NotEmpty(t, apiError.Message)
	if status == http.StatusInternalServerError {
		require.Equal(t, "Internal Server Error", apiError.Message)
	} else if status == http.StatusServiceUnavailable {
		require.Equal(t, "Service Unavailable Error", apiError.Message)
	}
	for _, field := range []string{"payment", "segCreds", "state"} {
		require.NotContains(t, response, field)
	}
}

func captureSignerLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func TestFixedPaymentAndSignedStateContinuation(t *testing.T) {
	s, info := testService(t)
	request := map[string]any{"orchestrator": base64.StdEncoding.EncodeToString(wire.EncodeOrchestratorInfo(info)), "type": "fixed", "ManifestID": "manifest-1"}
	w := postPayment(t, s, request)
	require.Equal(t, 200, w.Code, w.Body.String())
	var first paymentResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &first))
	require.Contains(t, string(first.State.State), `"SenderNonce":1`)
	require.NotContains(t, string(first.State.State), `"TicketNonce"`)
	decoded, err := base64.StdEncoding.DecodeString(first.Payment)
	require.NoError(t, err)
	payment, err := wire.DecodePayment(decoded)
	require.NoError(t, err)
	require.Equal(t, s.key.Address().Bytes(), payment.PayerAddress)
	require.Len(t, payment.PayerParams, 1)
	params := pm.TicketParams{Recipient: ethcommon.BytesToAddress(info.TicketParams.Recipient), FaceValue: new(big.Int).SetBytes(info.TicketParams.FaceValue), WinProb: new(big.Int).SetBytes(info.TicketParams.WinProb), RecipientRandHash: ethcommon.BytesToHash(info.TicketParams.RecipientRandHash), ExpirationBlock: new(big.Int).SetBytes(info.TicketParams.ExpirationBlock), ExpirationParams: &pm.TicketExpirationParams{CreationRound: 5, CreationRoundBlockHash: ethcommon.BytesToHash(info.TicketParams.Expiration.CreationRoundBlockHash)}}
	ticket := pm.NewTicket(&params, params.ExpirationParams, s.key.Address(), payment.PayerParams[0].TicketNonce)
	require.True(t, (pm.DefaultSigVerifier{}).Verify(s.key.Address(), ticket.Hash().Bytes(), payment.PayerParams[0].Sig))
	segmentBytes, err := base64.StdEncoding.DecodeString(first.SegCreds)
	require.NoError(t, err)
	segment, err := wire.DecodeSegData(segmentBytes)
	require.NoError(t, err)
	require.Equal(t, "manifest-1", string(segment.ManifestID))
	request["state"] = first.State
	w = postPayment(t, s, request)
	require.Equal(t, 200, w.Code, w.Body.String())
	var second paymentResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &second))
	var state paymentState
	require.NoError(t, json.Unmarshal(second.State.State, &state))
	require.Equal(t, uint64(1), state.SequenceNumber)
	require.Equal(t, uint32(2), state.TicketNonce)
	request["state"] = second.State
	info.Price.PricePerUnit++
	request["orchestrator"] = wire.EncodeOrchestratorInfo(info)
	requirePaymentFailure(t, postPayment(t, s, request), 481)
	info.Price.PricePerUnit--
	request["orchestrator"] = wire.EncodeOrchestratorInfo(info)
	request["app"] = "changed"
	w = postPayment(t, s, request)
	require.Equal(t, 400, w.Code)
	delete(request, "app")
	state.Balance = "1000000"
	forged, err := json.Marshal(state)
	require.NoError(t, err)
	request["state"] = signedState{State: forged, Sig: second.State.Sig}
	require.Equal(t, 400, postPayment(t, s, request).Code, "modified credit must fail signature verification")
}

func TestSignerRequestPriceCeiling(t *testing.T) {
	s, info := testService(t)
	request := map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "live"}
	request["maxPrice"] = map[string]any{"price": "9", "currency": "wei", "unit": "seconds"}
	require.Equal(t, 481, postPayment(t, s, request).Code)
	request["maxPrice"] = map[string]any{"price": "10", "currency": "wei", "unit": "seconds"}
	require.Equal(t, 200, postPayment(t, s, request).Code)
	request["maxPrice"] = map[string]any{"price": "10", "currency": "usd", "unit": "seconds"}
	requirePaymentFailure(t, postPayment(t, s, request), 400)
}

func TestSignedStateMovesBetweenIndependentSigners(t *testing.T) {
	_, info := testService(t)
	_, keyFile, passwordPath := testSignerKey(t)
	var calls atomic.Int32
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body struct{ State paymentState }
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.NotEmpty(t, body.State.Balance)
		require.NotZero(t, body.State.TicketNonce)
		require.False(t, body.State.LastUpdate.IsZero())
		_ = json.NewEncoder(w).Encode(map[string]any{"status": 200, "expiry": time.Now().Add(time.Hour).Unix(), "auth_id": "replica-user"})
	}))
	defer webhook.Close()
	var replicas [2]*Service
	for i := range replicas {
		key, err := eth.OpenKeystoreFile(keyFile, passwordPath)
		require.NoError(t, err)
		replicas[i] = newService(key)
		content, err := newPricePolicy(new(big.Rat).Mul(testRat(t, "1000"), big.NewRat(3600, 1)), testRat(t, "1000"))
		require.NoError(t, err)
		require.NoError(t, content.setRate(big.NewRat(1, 1), time.Time{}))
		replicas[i].pricePolicy = content
		replicas[i].SetPaymentChain(fundedPayer{})
		require.NoError(t, replicas[i].SetAuthWebhook(testURL(t, webhook.URL), nil))
	}
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			request := map[string]any{"orchestrator": base64.StdEncoding.EncodeToString(wire.EncodeOrchestratorInfo(info)), "type": kind}
			var previous paymentState
			for i := range 3 {
				if i == 2 {
					refreshed := info
					refreshed.TicketParams.RecipientRandHash = crypto.Keccak256([]byte(kind + "-refreshed"))
					request["orchestrator"] = base64.StdEncoding.EncodeToString(wire.EncodeOrchestratorInfo(refreshed))
				}
				response := postPayment(t, replicas[i%2], request)
				require.Equal(t, 200, response.Code, response.Body.String())
				var payment paymentResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payment))
				require.NotEmpty(t, payment.Payment)
				var state paymentState
				require.NoError(t, json.Unmarshal(payment.State.State, &state))
				require.Equal(t, uint64(i), state.SequenceNumber)
				require.Equal(t, "replica-user", state.AuthID)
				if i > 0 {
					require.Equal(t, previous.StateID, state.StateID)
				}
				if i == 2 {
					require.NotEqual(t, previous.PMSessionID, state.PMSessionID)
				}
				previous = state
				request["state"] = payment.State
			}
		})
	}
	require.Equal(t, int32(2), calls.Load(), "webhook authorization is cached in signed state across replicas")
}

func TestSignerRejectsCarriedCreditWithoutTickets(t *testing.T) {
	s, info := testService(t)
	maxWinProb := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	info.TicketParams.WinProb = new(big.Int).Quo(maxWinProb, big.NewInt(3)).Bytes()
	request := map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"}
	response := postPayment(t, s, request)
	require.Equal(t, 200, response.Code, response.Body.String())
	var first paymentResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &first))
	// The first batch leaves 10/3 wei of credit. The lower quote produces
	// a batch without tickets, which the remote payment protocol rejects.
	info.Price.PricePerUnit = 1
	info.TicketParams.FaceValue = big.NewInt(5).Bytes()
	info.TicketParams.RecipientRandHash = crypto.Keccak256([]byte("lower ticket EV"))
	request["orchestrator"], request["state"] = wire.EncodeOrchestratorInfo(info), first.State
	response = postPayment(t, s, request)
	requirePaymentFailure(t, response, 482)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "no_payment_needed"}))
}

func TestSlowResponseReleasesSlotOnDeadlineOrCancellation(t *testing.T) {
	for _, earlyCancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", earlyCancel), func(t *testing.T) {
			s, info := testService(t)
			s.slots = make(chan struct{}, 1)
			info.Auth.SessionID = strings.Repeat("m", 32<<10) // Force a write beyond the HTTP buffer.
			body, err := json.Marshal(map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
			require.NoError(t, err)
			serverConn, clientConn := net.Pipe()
			defer clientConn.Close()
			listener := &pipeListener{conn: serverConn, accepted: make(chan struct{}), closed: make(chan struct{})}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			finished := make(chan struct{})
			server := &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(finished); s.ServeHTTP(w, r) })}
			defer server.Close()
			go func() { _ = server.Serve(listener) }()
			require.NoError(t, clientConn.SetWriteDeadline(time.Now().Add(3*time.Second)))
			_, err = fmt.Fprintf(clientConn, "POST /generate-live-payment HTTP/1.1\r\nHost: local\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return len(s.slots) == 1 }, 500*time.Millisecond, time.Millisecond)
			if earlyCancel {
				cancel()
			}
			select {
			case <-finished:
				response := httptest.NewRecorder()
				s.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/sign-orchestrator-info", nil))
				require.Equal(t, http.StatusOK, response.Code, "the next request must be admitted after the blocked response ends")
			case <-time.After(2 * time.Second):
				t.Fatal("slow reader pinned the signer after its context ended")
			}
		})
	}
}

func TestSignerRefreshResetsNonceAtBatchLimit(t *testing.T) {
	s, info := testService(t)
	info.Price.PricePerUnit = 1000
	req := map[string]any{"type": "fixed", "ManifestID": "manifest-1", "orchestrator": wire.EncodeOrchestratorInfo(info)}
	var state paymentState
	for range 5 {
		w := postPayment(t, s, req)
		require.Equal(t, 200, w.Code, w.Body.String())
		var res paymentResponse
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &res))
		require.NoError(t, json.Unmarshal(res.State.State, &state))
		req["state"] = res.State
	}
	require.Equal(t, uint32(500), state.TicketNonce)
	require.Equal(t, 480, postPayment(t, s, req).Code, "exhausted parameters must request a refresh")
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "refresh_required"}))
	info.TicketParams.RecipientRandHash = crypto.Keccak256([]byte("fresh recipient randomness"))
	req["orchestrator"] = wire.EncodeOrchestratorInfo(info)
	w := postPayment(t, s, req)
	require.Equal(t, 200, w.Code, "new recipient-random hash should reset the nonce; got %s", w.Body.String())
	var refreshed paymentResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &refreshed))
	require.NoError(t, json.Unmarshal(refreshed.State.State, &state))
	require.Equal(t, uint32(100), state.TicketNonce)
	require.Equal(t, uint64(5), state.SequenceNumber)
}

func TestSignerEnforcesTicketExposureLimit(t *testing.T) {
	s, info := testService(t)
	info.Price.PricePerUnit = 1
	info.TicketParams.FaceValue = big.NewInt(1_000_000_000_000_000_000).Bytes()
	req := map[string]any{"type": "fixed", "ManifestID": "manifest-1", "orchestrator": wire.EncodeOrchestratorInfo(info), "maxPrice": map[string]any{"price": "1", "currency": "wei", "unit": "fixed"}}
	w := postPayment(t, s, req)
	require.Equal(t, 400, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "ticket parameters violate payer policy")
}

func TestPaymentParamsValidation(t *testing.T) {
	s, original := testService(t)
	for _, tc := range []struct {
		name   string
		change func(*wire.OrchestratorInfo)
		status int
	}{
		{"valid", func(*wire.OrchestratorInfo) {}, 200},
		{"auth expiry", func(i *wire.OrchestratorInfo) { i.Auth.Expiration = time.Now().Unix() }, 480},
		{"auth within three minutes", func(i *wire.OrchestratorInfo) { i.Auth.Expiration = time.Now().Add(2 * time.Minute).Unix() }, 480},
		{"auth beyond three minutes", func(i *wire.OrchestratorInfo) { i.Auth.Expiration = time.Now().Add(4 * time.Minute).Unix() }, 200},
		{"parameter expiry", func(i *wire.OrchestratorInfo) { i.TicketParams.ExpirationBlock = []byte{51} }, 480},
		{"zero expiration block", func(i *wire.OrchestratorInfo) { i.TicketParams.ExpirationBlock = nil }, 400},
		{"older round", func(i *wire.OrchestratorInfo) { i.TicketParams.Expiration.CreationRound-- }, 200},
		{"future round", func(i *wire.OrchestratorInfo) { i.TicketParams.Expiration.CreationRound++ }, 200},
		{"missing expiration metadata", func(i *wire.OrchestratorInfo) { i.TicketParams.Expiration = wire.ExpirationParams{} }, 400},
		{"zero hash", func(i *wire.OrchestratorInfo) { i.TicketParams.Expiration.CreationRoundBlockHash = make([]byte, 32) }, 400},
		{"wrong hash", func(i *wire.OrchestratorInfo) {
			i.TicketParams.Expiration.CreationRoundBlockHash = ethcommon.HexToHash("0xabcd").Bytes()
		}, 200},
		{"distinct recipient", func(i *wire.OrchestratorInfo) { i.TicketParams.Recipient = ethcommon.HexToAddress("0xabcd").Bytes() }, 200},
		{"zero recipient", func(i *wire.OrchestratorInfo) { i.TicketParams.Recipient = make([]byte, 20) }, 400},
		{"short recipient", func(i *wire.OrchestratorInfo) { i.TicketParams.Recipient = []byte{1} }, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := original
			tc.change(&info)
			w := postPayment(t, s, map[string]any{"orchestrator": base64.StdEncoding.EncodeToString(wire.EncodeOrchestratorInfo(info)), "type": "fixed"})
			if tc.status == 200 {
				require.Equal(t, 200, w.Code, w.Body.String())
				var response paymentResponse
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
				data, err := base64.StdEncoding.DecodeString(response.Payment)
				require.NoError(t, err)
				payment, err := wire.DecodePayment(data)
				require.NoError(t, err)
				require.Equal(t, info.TicketParams.Recipient, payment.TicketParams.Recipient)
				require.Equal(t, info.TicketParams.Expiration, payment.Expiration, "preserve the supplied creation round and hash")
			} else {
				requirePaymentFailure(t, w, tc.status)
			}
			if tc.status == 480 {
				require.Equal(t, info.Transcoder, w.Header().Get("Livepeer-Orchestrator-URL"))
			}
		})
	}
}

type payerChainFunc func(context.Context, ethcommon.Address, ethcommon.Address) (pm.PayerFunds, error)

func (f payerChainFunc) PayerFunds(ctx context.Context, payer, recipient ethcommon.Address) (pm.PayerFunds, error) {
	return f(ctx, payer, recipient)
}

type failingPaymentSigner struct {
	pm.TicketSigner
	failAt int
	calls  atomic.Int32
}

func (s *failingPaymentSigner) SignMessage(message []byte) ([]byte, error) {
	if int(s.calls.Add(1)) == s.failAt {
		return nil, errors.New("key /private/keys/signer.json: private-signing-detail")
	}
	return s.TicketSigner.SignMessage(message)
}

func TestPaymentFailureStatuses(t *testing.T) {
	s, original := testService(t)
	logs := captureSignerLogs(t)
	key := s.key
	for _, phase := range []struct {
		name           string
		failOn, status int
	}{{"precheck", 1, 400}} {
		for _, cause := range []string{"deposit", "reserve", "withdrawal", "chain observation"} {
			t.Run(phase.name+"/"+cause, func(t *testing.T) {
				logs.Reset()
				calls := 0
				s.SetPaymentChain(payerChainFunc(func(ctx context.Context, payer, recipient ethcommon.Address) (pm.PayerFunds, error) {
					calls++
					funds, err := (fundedPayer{}).PayerFunds(ctx, payer, recipient)
					if calls == phase.failOn {
						switch cause {
						case "deposit":
							funds.Deposit = new(big.Int)
						case "reserve":
							funds.Reserve = new(big.Int)
						case "withdrawal":
							funds.WithdrawRound = new(big.Int).Add(funds.Snapshot.Round, big.NewInt(1))
						case "chain observation":
							err = errors.New("RPC https://rpc.internal/?token=private-rpc-detail failed")
						}
					}
					return funds, err
				}))
				outcome := "rejected"
				if cause == "chain observation" {
					outcome = "error"
				}
				before := test.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": outcome})
				response := postPayment(t, s, map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(original), "type": "fixed"})
				requirePaymentFailure(t, response, phase.status)
				require.Equal(t, before+1, test.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": outcome}))
				require.NotContains(t, response.Body.String(), "rpc.internal")
				require.NotContains(t, response.Body.String(), "private-rpc-detail")
				require.Contains(t, logs.String(), `"phase":"`+phase.name+`"`)
				if cause == "chain observation" {
					require.NotContains(t, logs.String(), "private-rpc-detail")
				}
			})
		}
	}
	// A request observes funds once, so the batch recheck cannot see a new funding
	// failure over HTTP. Its handling stays distinct: not a client error, so 500.
	for _, cause := range []string{"deposit", "reserve", "withdrawal"} {
		t.Run("generation/"+cause, func(t *testing.T) {
			logs.Reset()
			funds, err := (fundedPayer{}).PayerFunds(t.Context(), s.key.Address(), ethcommon.Address{})
			require.NoError(t, err)
			switch cause {
			case "deposit":
				funds.Deposit = new(big.Int)
			case "reserve":
				funds.Reserve = new(big.Int)
			case "withdrawal":
				funds.WithdrawRound = new(big.Int).Add(funds.Snapshot.Round, big.NewInt(1))
			}
			params := pm.TicketParams{ExpirationBlock: new(big.Int).SetBytes(original.TicketParams.ExpirationBlock)}
			err = s.checkTicketParams(t.Context(), &params, 2, funds, "generation")
			require.ErrorIs(t, err, pm.ErrPayerUnavailable)
			_, clientError := errors.AsType[paymentFailure](err)
			require.False(t, clientError, "generate answers 500 for anything but a paymentFailure")
			require.Equal(t, "rejected", paymentOutcome(err))
			require.Contains(t, logs.String(), `"phase":"generation"`)
		})
	}
	s.SetPaymentChain(fundedPayer{})
	t.Run("batch exposure", func(t *testing.T) {
		info := original
		info.Price.PricePerUnit = 20 // One ticket passes precheck; the full batch exceeds the limit.
		s.payerPolicy.MaxBatchEV = big.NewRat(15, 1)
		requirePaymentFailure(t, postPayment(t, s, map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"}), 400)
	})
	s.payerPolicy = pm.DefaultPayerPolicy()
	for _, tc := range []struct {
		name   string
		log    string
		price  int64
		failAt int
	}{{"first ticket signing", "ticket batch generation failed", 10, 1}, {"second ticket signing", "ticket batch generation failed", 20, 2}, {"segment signing", "segment credential signing failed", 10, 2}, {"state signing", "payment state signing failed", 10, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			info := original
			info.Price.PricePerUnit = tc.price
			s.key = &failingPaymentSigner{TicketSigner: key, failAt: tc.failAt}
			s.events = eventSinkFunc(func(context.Context, signingEvent) error { t.Fatal("signing failure emitted an event"); return nil })
			response := postPayment(t, s, map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
			requirePaymentFailure(t, response, 500)
			require.NotContains(t, response.Body.String(), "/private/keys")
			require.NotContains(t, response.Body.String(), "private-signing-detail")
			require.Contains(t, logs.String(), tc.log)
			require.NotContains(t, logs.String(), "private-signing-detail")
		})
	}
	s.key = key
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"oversized event", fmt.Errorf("outbox /private/accounting.sqlite: private-storage-detail: %w", errEventTooLarge), 413},
		{"unavailable outbox", errors.New("SQLite /private/accounting.sqlite: private-storage-detail"), 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			s.events = eventSinkFunc(func(context.Context, signingEvent) error { return tc.err })
			response := postPayment(t, s, map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(original), "type": "fixed"})
			requirePaymentFailure(t, response, tc.status)
			require.NotContains(t, response.Body.String(), "/private/accounting.sqlite")
			require.NotContains(t, response.Body.String(), "private-storage-detail")
			require.Contains(t, logs.String(), "payment withheld")
			require.NotContains(t, logs.String(), "private-storage-detail")
			require.Zero(t, test.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "success"}))
			require.Zero(t, test.MetricSum(t, s.metrics.Registry, "signer_tickets_total", nil))
		})
	}
}

func TestBatchRecheckReusesPrecheckObservation(t *testing.T) {
	s, info := testService(t)
	defer s.Close()
	calls := 0
	s.SetPaymentChain(payerChainFunc(func(ctx context.Context, payer, recipient ethcommon.Address) (pm.PayerFunds, error) {
		calls++
		funds, err := (fundedPayer{}).PayerFunds(ctx, payer, recipient)
		if calls > 1 {
			funds.Deposit = new(big.Int)
		}
		return funds, err
	}))
	response := postPayment(t, s, map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, 1, calls)
}

func TestCancelledPaymentOutcome(t *testing.T) {
	s, info := testService(t)
	defer s.Close()
	s.SetPaymentChain(payerChainFunc(func(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
		return pm.PayerFunds{}, ctx.Err()
	}))
	body, err := json.Marshal(map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest("POST", "/generate-live-payment", bytes.NewReader(body)).WithContext(ctx)
	response := httptest.NewRecorder()
	s.ServeHTTP(response, request)
	requirePaymentFailure(t, response, 400)
	require.EqualValues(t, 1, test.MetricSum(t, s.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "client_cancelled"}))
	require.Zero(t, test.MetricSum(t, s.metrics.Registry, "signer_tickets_total", nil))
}
