package signer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/orchestrator"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

func TestPaymentEncoding(t *testing.T) {
	data, err := os.ReadFile("testdata/payment-before-authorization.json")
	require.NoError(t, err)
	var fixtures map[string]struct{ Payment, SegCreds string }
	require.NoError(t, json.Unmarshal(data, &fixtures))
	keyFile, passwordFile := test.WriteFixedKeystore(t)
	key, err := eth.OpenKeystoreFile(keyFile, passwordFile)
	require.NoError(t, err)
	s, info := testService(t)
	s.key = key
	info.Auth.Expiration = 1900000000
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			draft, err := s.preparePayment(t.Context(), kind, info.Auth.SessionID, info, paymentState{}, -1, time.Now())
			require.NoError(t, err)
			signed, err := draft.Sign(t.Context(), key, nil)
			require.NoError(t, err)
			require.Equal(t, fixtures[kind].Payment, signed.Payment)
			require.Equal(t, fixtures[kind].SegCreds, signed.SegCreds)
		})
	}
}

func TestDraftPaymentPreservesValues(t *testing.T) {
	s, original := testService(t)
	for _, kind := range []string{"fixed", "live"} {
		t.Run(kind, func(t *testing.T) {
			// Decode a separate envelope so mutation cannot affect another case.
			info, err := wire.DecodeOrchestratorInfo(wire.EncodeOrchestratorInfo(original))
			require.NoError(t, err)
			// Retain noncanonical integer bytes rather than re-encoding big.Ints.
			info.TicketParams.FaceValue = append([]byte{0}, info.TicketParams.FaceValue...)
			now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
			state := paymentState{PMSessionID: ethcommon.BytesToHash(info.TicketParams.RecipientRandHash).Hex(), TicketNonce: 41, Balance: "1/3", LastUpdate: now.Add(-1500 * time.Millisecond), SequenceNumber: 7}
			draft, err := s.preparePayment(t.Context(), kind, info.Auth.SessionID, info, state, 7, now)
			require.NoError(t, err)
			require.Equal(t, now, draft.State.LastUpdate)
			require.Equal(t, uint64(8), draft.State.SequenceNumber)
			fee := "10"
			count := 1
			if kind == "live" {
				fee = "20"
				count = 2
			}
			require.Equal(t, fee, draft.Usage.Fee.RatString())
			require.Equal(t, count, draft.Usage.NumTickets)
			require.Equal(t, uint32(41+count), draft.State.TicketNonce)
			paymentBefore := wire.EncodePayment(draft.payment)
			segmentBefore := wire.EncodeSegData(draft.segment)
			signatureMessage := append([]byte(info.Auth.SessionID), make([]byte, 32)...)
			signatureMessage = append(signatureMessage, crypto.Keccak256(nil)...)
			for _, value := range [][]byte{info.TicketParams.Recipient, info.TicketParams.FaceValue, info.TicketParams.WinProb, info.TicketParams.RecipientRandHash, info.TicketParams.Seed, info.TicketParams.ExpirationBlock, info.TicketParams.Expiration.CreationRoundBlockHash, info.Auth.Token} {
				value[0] ^= 0xff
			}
			info.Price.PricePerUnit = 999
			info.Auth.Expiration = 0
			signed, err := draft.Sign(t.Context(), s.key, nil)
			require.NoError(t, err)
			paymentBytes, err := base64.StdEncoding.DecodeString(signed.Payment)
			require.NoError(t, err)
			payment, err := wire.DecodePayment(paymentBytes)
			require.NoError(t, err)
			require.Equal(t, draft.Usage.NumTickets, len(payment.PayerParams))
			require.Equal(t, draft.State.TicketNonce, payment.PayerParams[len(payment.PayerParams)-1].TicketNonce)
			payment.PayerParams = nil
			require.Equal(t, paymentBefore, wire.EncodePayment(payment))
			segmentBytes, err := base64.StdEncoding.DecodeString(signed.SegCreds)
			require.NoError(t, err)
			segment, err := wire.DecodeSegData(segmentBytes)
			require.NoError(t, err)
			require.True(t, (pm.DefaultSigVerifier{}).Verify(s.key.Address(), crypto.Keccak256(signatureMessage), segment.Signature))
			segment.Signature = nil
			require.Equal(t, segmentBefore, wire.EncodeSegData(segment))
		})
	}
}

type collateralChain struct{ withdraw *big.Int }

func (c collateralChain) Snapshot(context.Context) (pm.ChainSnapshot, error) {
	return pm.ChainSnapshot{Block: big.NewInt(50), Round: big.NewInt(5), RoundHash: ethcommon.HexToHash("0x1234")}, nil
}
func (c collateralChain) IsActiveAt(context.Context, ethcommon.Address, pm.ChainSnapshot) (bool, error) {
	return true, nil
}
func (c collateralChain) PayerFunds(ctx context.Context, _, _ ethcommon.Address) (pm.PayerFunds, error) {
	snapshot, err := c.Snapshot(ctx)
	return pm.PayerFunds{Snapshot: snapshot, Deposit: big.NewInt(10), Reserve: big.NewInt(10), WithdrawRound: c.withdraw}, err
}

func TestRecipientReservesWinningLiabilityAcrossConcurrentSessions(t *testing.T) {
	key, _, _ := testSignerKey(t)
	store, err := orchestrator.OpenRedeemerDB(filepath.Join(t.TempDir(), "recipient.sqlite"))
	require.NoError(t, err)
	defer store.Close()
	chain := collateralChain{new(big.Int)}
	engine, err := orchestrator.NewPaymentEngine(store, chain, ethcommon.HexToAddress("0x1234"), big.NewInt(10), new(big.Int).Sub(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), big.NewInt(1)))
	require.NoError(t, err)
	s := newService(key)
	s.SetPaymentChain(chain)
	payments := map[string]signedPayment{}
	for _, manifest := range []string{"one", "two", "three"} {
		_, err := engine.MakeChallenge(t.Context(), "runner", manifest, key.Address(), 9, "fixed", "https://orch.example")
		require.NoError(t, err)
		info, err := engine.ChallengeInfo(manifest)
		require.NoError(t, err)
		draft, err := s.preparePayment(t.Context(), "fixed", manifest, info, paymentState{}, -1, time.Now())
		require.NoError(t, err)
		payment, err := draft.Sign(t.Context(), key, nil)
		require.NoError(t, err)
		payments[manifest] = payment
	}
	var workers sync.WaitGroup
	errors := make(chan error, 3)
	for manifest, payment := range payments {
		workers.Go(func() {
			_, _, err := engine.Receive(t.Context(), "runner", manifest, payment.Payment, payment.SegCreds)
			errors <- err
		})
	}
	workers.Wait()
	close(errors)
	accepted, rejected := 0, 0
	for err := range errors {
		if err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, pm.ErrPayerUnavailable)
			rejected++
		}
	}
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, rejected)
	count, err := store.WinningTicketCount(key.Address(), 0)
	require.NoError(t, err)
	require.Equal(t, 2, count, "rejected batches must roll back tickets and credit")
}

// paidPath is one paid request end to end against a counting JSON-RPC server:
// the orchestrator's challenge, the signer's payment and the orchestrator's receipt.
type paidPath struct {
	rpc              *test.ChainRPC
	engine           *orchestrator.PaymentEngine
	service          *Service
	payer            *eth.Key
	recipient        ethcommon.Address
	orchestratorView *eth.ChainView
	signerView       *eth.ChainView
	next             atomic.Int64
}

// newViewedPaidPath gives the orchestrator and the signer a chain view each, as
// separate processes have, and observes the chain once for each.
func newViewedPaidPath(t testing.TB, now func() time.Time) *paidPath {
	t.Helper()
	return newPaidPathWith(t, &eth.ChainViewOptions{Interval: 5 * time.Second, MaxAge: 30 * time.Second, Now: now})
}

func newPaidPath(t testing.TB) *paidPath {
	t.Helper()
	return newPaidPathWith(t, nil)
}

func newPaidPathWith(t testing.TB, viewOptions *eth.ChainViewOptions) *paidPath {
	t.Helper()
	payer, recipient := paidPathKey(t), ethcommon.HexToAddress("0x1234")
	rpcServer := test.NewChainRPC(t)
	rpcServer.Mine(func(state *test.ChainState) {
		state.SetActive(recipient, true)
		state.SetSender(payer.Address(), test.ChainSender{Deposit: big.NewInt(1e18), Reserve: big.NewInt(1e18), WithdrawRound: new(big.Int)})
	})
	client, err := eth.OpenRPC(rpcServer.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	contracts, err := eth.NewContracts(client, rpcServer.Controller)
	require.NoError(t, err)
	store, err := orchestrator.OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	direct := eth.PaymentChain{Contracts: contracts}
	path := &paidPath{rpc: rpcServer, payer: payer, recipient: recipient}
	var orchestratorChain pm.PaymentChain = pm.EthereumChain{Client: direct}
	var signerChain pm.PayerChain = pm.EthereumChain{Client: direct}
	if viewOptions != nil {
		options := *viewOptions
		options.Recipients = []ethcommon.Address{recipient}
		path.orchestratorView, err = eth.NewChainView(direct, options)
		require.NoError(t, err)
		options.Recipients = nil
		path.signerView, err = eth.NewChainView(direct, options)
		require.NoError(t, err)
		path.refresh(t)
		orchestratorChain, signerChain = path.orchestratorView, path.signerView
	}
	maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	// One ticket covers the price and almost never wins, keeping winner storage
	// out of the measurements of chain reads.
	path.engine, err = orchestrator.NewPaymentEngine(store, orchestratorChain, recipient, big.NewInt(1e12), new(big.Int).Quo(maximum, big.NewInt(1e5)))
	require.NoError(t, err)
	path.service = newService(payer)
	t.Cleanup(func() { _ = path.service.Close() })
	path.service.SetPaymentChain(signerChain)
	return path
}

func (p *paidPath) refresh(t testing.TB) {
	t.Helper()
	require.NoError(t, p.orchestratorView.Refresh(context.Background()))
	require.NoError(t, p.signerView.Refresh(context.Background()))
}

func paidPathKey(t testing.TB) *eth.Key {
	t.Helper()
	keyFile, passwordPath := test.WriteKeystore(t, nil)
	key, err := eth.OpenKeystoreFile(keyFile, passwordPath)
	require.NoError(t, err)
	return key
}

func (p *paidPath) manifest() string {
	return "session-" + strconv.FormatInt(p.next.Add(1), 10)
}

func (p *paidPath) challenge(t testing.TB, ctx context.Context, manifest string) wire.OrchestratorInfo {
	t.Helper()
	_, err := p.engine.MakeChallenge(ctx, "runner", manifest, p.payer.Address(), 9, "fixed", "https://orch.example")
	require.NoError(t, err)
	info, err := p.engine.ChallengeInfo(manifest)
	require.NoError(t, err)
	return info
}

func (p *paidPath) sign(t testing.TB, info wire.OrchestratorInfo) (payment, segCreds string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
	require.NoError(t, err)
	w := httptest.NewRecorder()
	p.service.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/generate-live-payment", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var signed paymentResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &signed))
	return signed.Payment, signed.SegCreds
}

func (p *paidPath) trySign(info wire.OrchestratorInfo) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]any{"orchestrator": wire.EncodeOrchestratorInfo(info), "type": "fixed"})
	w := httptest.NewRecorder()
	p.service.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/generate-live-payment", bytes.NewReader(body)))
	return w
}

func (p *paidPath) receive(ctx context.Context, manifest, payment, segCreds string) error {
	_, _, err := p.engine.Receive(ctx, "runner", manifest, payment, segCreds)
	return err
}

// The expected log is the one derived from the code in the issue this test pins.
func TestPaidRequestRPCCalls(t *testing.T) {
	path := newPaidPath(t)
	manifest := path.manifest()
	snapshot := map[string]int{"eth_getBlockByNumber": 1, "Controller.getContract": 1, "RoundsManager.lastInitializedRound": 1, "RoundsManager.blockHashForRound": 1}
	isActive := map[string]int{"Controller.getContract": 1, "BondingManager.isActiveTranscoder": 1}
	funds := map[string]int{"Controller.getContract": 1, "TicketBroker.getSenderInfo": 1, "TicketBroker.claimableReserve": 1}
	sum := func(parts ...map[string]int) map[string]int {
		total := map[string]int{}
		for _, part := range parts {
			for name, count := range part {
				total[name] += count
			}
		}
		return total
	}

	path.rpc.Reset()
	info := path.challenge(t, t.Context(), manifest)
	stats := path.rpc.Stats()
	require.Equal(t, 6, stats.RoundTrips, "challenge: snapshot (4) + IsActiveAt (2)")
	require.Equal(t, sum(snapshot, isActive), stats.Calls)
	require.Zero(t, stats.Unpinned)

	path.rpc.Reset()
	payment, segCreds := path.sign(t, info)
	stats = path.rpc.Stats()
	require.Equal(t, 7, stats.RoundTrips, "signer: payer funds (7), reused by the batch recheck")
	require.Equal(t, sum(snapshot, funds), stats.Calls)
	require.Zero(t, stats.Unpinned)

	path.rpc.Reset()
	require.NoError(t, path.receive(t.Context(), manifest, payment, segCreds))
	stats = path.rpc.Stats()
	require.Equal(t, 9, stats.RoundTrips, "receive: payer funds (7) + IsActiveAt (2)")
	require.Equal(t, sum(snapshot, funds, isActive), stats.Calls)
	require.Zero(t, stats.Unpinned)
}

type pathClock struct{ elapsed atomic.Int64 }

func (c *pathClock) Now() time.Time {
	return time.Unix(1_900_000_000, c.elapsed.Load())
}

func (c *pathClock) Advance(elapsed time.Duration) { c.elapsed.Add(int64(elapsed)) }

func TestSignerSeesPayerChangesWithinOneRefresh(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sender func(*test.ChainSender)
	}{
		{"deposit below the face value", func(sender *test.ChainSender) { sender.Deposit = big.NewInt(1) }},
		{"no deposit", func(sender *test.ChainSender) { sender.Deposit = new(big.Int) }},
		{"no reserve", func(sender *test.ChainSender) { sender.Reserve = new(big.Int) }},
		{"unlocking", func(sender *test.ChainSender) { sender.WithdrawRound = big.NewInt(6) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := newViewedPaidPath(t, nil)
			info := path.challenge(t, t.Context(), path.manifest())
			path.sign(t, info)
			path.rpc.Mine(func(state *test.ChainState) {
				sender := test.ChainSender{Deposit: big.NewInt(1e18), Reserve: big.NewInt(1e18), WithdrawRound: new(big.Int)}
				tc.sender(&sender)
				state.SetSender(path.payer.Address(), sender)
			})
			path.refresh(t)
			before := test.MetricSum(t, path.service.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "rejected"})
			requirePaymentFailure(t, path.trySign(info), http.StatusBadRequest)
			require.Equal(t, before+1, test.MetricSum(t, path.service.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "rejected"}))
		})
	}
}

func TestRefusedChainViewFailsASignatureLikeAFailedChainRead(t *testing.T) {
	direct := newPaidPath(t)
	info := direct.challenge(t, t.Context(), direct.manifest())
	direct.rpc.SetDown(true)
	want := direct.trySign(info)
	require.Equal(t, http.StatusBadRequest, want.Code, want.Body.String())

	clock := new(pathClock)
	path := newViewedPaidPath(t, clock.Now)
	info = path.challenge(t, t.Context(), path.manifest())
	path.sign(t, info)
	clock.Advance(31 * time.Second)
	got := path.trySign(info)
	require.Equal(t, want.Code, got.Code)
	require.JSONEq(t, want.Body.String(), got.Body.String(), "the same status and message")
	require.EqualValues(t, 1, test.MetricSum(t, direct.service.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "error"}))
	require.EqualValues(t, 1, test.MetricSum(t, path.service.metrics.Registry, "signer_payments_total", map[string]string{"outcome": "error"}))
	path.refresh(t)
	path.sign(t, info)
}

// paramsExpiryBuffer leaves one L1 block between signing and expiry, measured on the
// signer's own clock. A chain view holds that clock back: by under one L1 block at the
// default interval, and by several as a failing view approaches its maximum age. The
// signer can then sign parameters the orchestrator already refuses.
func TestSignerExpiryBufferIsRelativeToItsObservedClock(t *testing.T) {
	path := newViewedPaidPath(t, nil)
	manifest := path.manifest()
	info := path.challenge(t, t.Context(), manifest)
	expiry := new(big.Int).SetBytes(info.TicketParams.ExpirationBlock).Int64()
	path.sign(t, info)
	path.rpc.Mine(func(state *test.ChainState) { state.L1Block = expiry })
	require.NoError(t, path.orchestratorView.Refresh(t.Context()))
	payment, segCreds := path.sign(t, info)
	require.ErrorIs(t, path.receive(t.Context(), manifest, payment, segCreds), orchestrator.ErrInvalidPayment, "the orchestrator's clock passed expiry")
	require.NoError(t, path.signerView.Refresh(t.Context()))
	require.Equal(t, 480, path.trySign(info).Code, "after its next observation the signer asks for a refresh")
}

func TestPaidRequestRPCBudgetWithChainView(t *testing.T) {
	path := newViewedPaidPath(t, nil)
	roundTrips := func(t *testing.T, step func()) int {
		t.Helper()
		path.rpc.Reset()
		step()
		stats := path.rpc.Stats()
		require.Zero(t, stats.Unpinned)
		return stats.RoundTrips
	}
	for _, phase := range []struct {
		name            string
		sign, receive   int
		refreshedBefore bool
	}{{name: "new payer", sign: 2, receive: 2}, {name: "known payer"}, {name: "known payer after a refresh", refreshedBefore: true}} {
		t.Run(phase.name, func(t *testing.T) {
			if phase.refreshedBefore {
				path.refresh(t)
			}
			manifest := path.manifest()
			var info wire.OrchestratorInfo
			require.Zero(t, roundTrips(t, func() { info = path.challenge(t, t.Context(), manifest) }), "the challenge reads no payer funds")
			var payment, segCreds string
			require.LessOrEqual(t, roundTrips(t, func() { payment, segCreds = path.sign(t, info) }), phase.sign)
			require.LessOrEqual(t, roundTrips(t, func() { require.NoError(t, path.receive(t.Context(), manifest, payment, segCreds)) }), phase.receive)
		})
	}
}

// paidRequestLatency is a measurement rather than an assertion: RPC latency is
// injected, so the result shows how the round trips of a request add up.
const paidRequestLatency = 110 * time.Millisecond

func (p *paidPath) request(t testing.TB, ctx context.Context) {
	t.Helper()
	manifest := p.manifest()
	info := p.challenge(t, ctx, manifest)
	payment, segCreds := p.sign(t, info)
	require.NoError(t, p.receive(ctx, manifest, payment, segCreds))
}

func benchmarkPaidRequests(b *testing.B, path *paidPath, workers int) {
	path.rpc.SetLatency(paidRequestLatency)
	path.rpc.Reset()
	var (
		next      atomic.Int64
		mu        sync.Mutex
		latencies []time.Duration
		running   sync.WaitGroup
	)
	b.ResetTimer()
	for range workers {
		running.Go(func() {
			for next.Add(1) <= int64(b.N) {
				start := time.Now()
				path.request(b, context.Background())
				elapsed := time.Since(start)
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		})
	}
	running.Wait()
	b.StopTimer()
	slices.Sort(latencies)
	b.ReportMetric(float64(path.rpc.Stats().RoundTrips)/float64(b.N), "rpc_trips/op")
	b.ReportMetric(float64(latencies[len(latencies)/2].Milliseconds()), "p50_ms")
	b.ReportMetric(float64(latencies[len(latencies)*99/100].Milliseconds()), "p99_ms")
}

// run keeps both views current until the test ends, as their refresh loops do.
func (p *paidPath) run(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	loops.Go(func() { p.orchestratorView.Run(ctx) })
	loops.Go(func() { p.signerView.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		loops.Wait()
	})
}

// The direct path is the interval-0 configuration: 22 round trips since the signer
// stopped reading funds twice. The commit introducing this benchmark measured 29.
//
// Run with: go test ./signer -run '^$' -bench '^BenchmarkPaidRequest$' -benchtime 64x
func BenchmarkPaidRequest(b *testing.B) {
	for _, workers := range []int{1, 32} {
		b.Run("direct/workers="+strconv.Itoa(workers), func(b *testing.B) {
			benchmarkPaidRequests(b, newPaidPath(b), workers)
		})
		b.Run("view/workers="+strconv.Itoa(workers), func(b *testing.B) {
			path := newViewedPaidPath(b, nil)
			path.run(b)
			path.request(b, context.Background()) // A known payer, as after the first request.
			benchmarkPaidRequests(b, path, workers)
		})
	}
	// Only the views are current here: the signer and the receipt each read this payer once.
	b.Run("view/new payer", func(b *testing.B) {
		trips := 0
		for range b.N {
			b.StopTimer()
			path := newViewedPaidPath(b, nil)
			path.rpc.SetLatency(paidRequestLatency)
			path.rpc.Reset()
			b.StartTimer()
			path.request(b, context.Background())
			trips += path.rpc.Stats().RoundTrips
		}
		b.ReportMetric(float64(trips)/float64(b.N), "rpc_trips/op")
	})
}

// RPC load must follow the chain view's timer, not request volume. The interval is
// shortened so the comparison settles in seconds rather than minutes.
func TestRPCRateStaysFlatUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("measures load over several seconds")
	}
	const window = 2 * time.Second
	path := newPaidPathWith(t, &eth.ChainViewOptions{Interval: 100 * time.Millisecond, MaxAge: time.Minute})
	path.run(t)
	path.request(t, t.Context())
	measure := func(load func(stop <-chan struct{})) (trips, calls float64) {
		path.rpc.Reset()
		stop := make(chan struct{})
		var workers sync.WaitGroup
		if load != nil {
			for range 16 {
				workers.Go(func() { load(stop) })
			}
		}
		time.Sleep(window)
		stats := path.rpc.Stats()
		close(stop)
		workers.Wait()
		total := 0
		for _, count := range stats.Calls {
			total += count
		}
		return float64(stats.RoundTrips) / window.Seconds(), float64(total) / window.Seconds()
	}
	idleTrips, idleCalls := measure(nil)
	var requests atomic.Int64
	loadedTrips, loadedCalls := measure(func(stop <-chan struct{}) {
		for {
			select {
			case <-stop:
				return
			default:
				path.request(t, t.Context())
				requests.Add(1)
			}
		}
	})
	rate := float64(requests.Load()) / window.Seconds()
	t.Logf("idle: %.1f round trips/s, %.1f calls/s", idleTrips, idleCalls)
	t.Logf("%.0f requests/s: %.1f round trips/s, %.1f calls/s", rate, loadedTrips, loadedCalls)
	require.Greater(t, rate, 20.0, "the direct path would need 22 round trips for each of these")
	require.Positive(t, idleTrips)
	require.LessOrEqual(t, loadedTrips, idleTrips*1.25, "round trips do not grow with requests")
	require.LessOrEqual(t, loadedCalls, idleCalls*1.25, "nor do the calls inside them")
}
