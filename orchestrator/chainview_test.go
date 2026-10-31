package orchestrator

import (
	"crypto/ecdsa"
	"encoding/base64"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/internal/test"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
	"github.com/stretchr/testify/require"
)

type viewEngineClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *viewEngineClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *viewEngineClock) Advance(elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(elapsed)
}

// viewEngine is a payment engine reading through a chain view of a counting RPC.
type viewEngine struct {
	rpc       *test.ChainRPC
	view      *eth.ChainView
	direct    eth.PaymentChain
	clock     *viewEngineClock
	engine    *PaymentEngine
	key       *ecdsa.PrivateKey
	payer     ethcommon.Address
	recipient ethcommon.Address
	face      *big.Int
}

func newViewEngine(t *testing.T) *viewEngine {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	f := &viewEngine{rpc: test.NewChainRPC(t), clock: &viewEngineClock{now: time.Unix(1_900_000_000, 0)}, key: key, payer: crypto.PubkeyToAddress(key.PublicKey), recipient: ethcommon.HexToAddress("0x1234"), face: big.NewInt(1000)}
	f.rpc.Mine(func(state *test.ChainState) {
		state.SetActive(f.recipient, true)
		state.SetSender(f.payer, f.funded())
	})
	client, err := eth.OpenRPC(f.rpc.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	contracts, err := eth.NewContracts(client, f.rpc.Controller)
	require.NoError(t, err)
	f.direct = eth.PaymentChain{Contracts: contracts}
	f.view, err = eth.NewChainView(f.direct, eth.ChainViewOptions{Interval: 5 * time.Second, MaxAge: 30 * time.Second, Recipients: []ethcommon.Address{f.recipient}, Now: f.clock.Now})
	require.NoError(t, err)
	store, err := OpenRedeemerDB(filepath.Join(t.TempDir(), "payments.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// Tickets almost never win, so these tests observe only funds and clocks.
	f.engine, err = NewPaymentEngine(store, f.view, f.recipient, f.face, new(big.Int).Quo(maxWinProb, big.NewInt(1e9)))
	require.NoError(t, err)
	f.refresh(t)
	return f
}

func (f *viewEngine) funded() test.ChainSender {
	return test.ChainSender{Deposit: big.NewInt(1e12), Reserve: big.NewInt(1e12), WithdrawRound: new(big.Int)}
}

func (f *viewEngine) refresh(t *testing.T) {
	t.Helper()
	require.NoError(t, f.view.Refresh(t.Context()))
}

func (f *viewEngine) params(t *testing.T, session string) wire.OrchestratorInfo {
	t.Helper()
	challenge, err := f.engine.MakeChallenge(t.Context(), "runner", session, f.payer, 1, "fixed", "https://orch.example")
	require.NoError(t, err)
	encoded, err := base64.StdEncoding.DecodeString(challenge.PaymentParams)
	require.NoError(t, err)
	info, err := wire.DecodeOrchestratorInfo(encoded)
	require.NoError(t, err)
	return info
}

func TestPaymentPathReadsAKnownPayerFromMemory(t *testing.T) {
	f := newViewEngine(t)
	info := f.params(t, "first")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))
	f.refresh(t)
	f.rpc.Reset()
	info = f.params(t, "second")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))
	require.Zero(t, f.rpc.Stats().RoundTrips)
}

func TestReceiveSeesFallingFundsWithinOneRefresh(t *testing.T) {
	for name, drop := range map[string]func(*test.ChainSender){
		"deposit": func(sender *test.ChainSender) { sender.Deposit = big.NewInt(999) },
		"reserve": func(sender *test.ChainSender) { sender.Reserve = big.NewInt(999) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newViewEngine(t)
			info := f.params(t, "session")
			require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))
			f.rpc.Mine(func(state *test.ChainState) {
				sender := f.funded()
				drop(&sender)
				state.SetSender(f.payer, sender)
			})
			require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 2), "until the next observation, the last one stands")
			f.refresh(t)
			require.ErrorIs(t, receiveRecipientPayment(t, f.engine, f.key, info, 3), pm.ErrPayerUnavailable)
		})
	}
}

func TestReceiveRefusesAnUnlockingPayerWithinOneRefresh(t *testing.T) {
	f := newViewEngine(t)
	info := f.params(t, "session")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))
	// Unlocked at round 5, with a withdrawal round of 7: pm.ValidatePayerFunds still
	// accepts this at round 5, since withdrawal is more than one round away.
	f.rpc.Mine(func(state *test.ChainState) {
		sender := f.funded()
		sender.WithdrawRound = big.NewInt(7)
		state.SetSender(f.payer, sender)
	})
	f.refresh(t)
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 2))
	f.rpc.Mine(func(state *test.ChainState) { state.SetRound(6, test.ChainRoundHash(6)) })
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 3), "a stale view is one round before withdrawal is possible")
	f.refresh(t)
	err := receiveRecipientPayment(t, f.engine, f.key, info, 4)
	require.ErrorContains(t, err, "unlock too soon")
	require.Equal(t, "rejected", paymentOutcome(err))
}

func TestRoundRolloverReachesPaymentsWithinOneRefresh(t *testing.T) {
	f := newViewEngine(t)
	info := f.params(t, "old round")
	require.EqualValues(t, 5, info.TicketParams.Expiration.CreationRound)
	require.Equal(t, test.ChainRoundHash(5).Bytes(), info.TicketParams.Expiration.CreationRoundBlockHash)
	for round := int64(6); round <= 7; round++ {
		f.rpc.Mine(func(state *test.ChainState) { state.SetRound(round, test.ChainRoundHash(round)) })
		f.refresh(t)
		require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, uint32(round)), "a ticket stays valid for two rounds")
	}
	fresh := f.params(t, "new round")
	require.EqualValues(t, 7, fresh.TicketParams.Expiration.CreationRound)
	require.Equal(t, test.ChainRoundHash(7).Bytes(), fresh.TicketParams.Expiration.CreationRoundBlockHash)
	f.rpc.Mine(func(state *test.ChainState) { state.SetRound(8, test.ChainRoundHash(8)) })
	f.refresh(t)
	require.ErrorIs(t, receiveRecipientPayment(t, f.engine, f.key, info, 9), ErrInvalidPayment, "three rounds is too old")
}

func TestContractAddressChangeReachesPaymentsWithinOneRefresh(t *testing.T) {
	f := newViewEngine(t)
	info := f.params(t, "session")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))
	f.rpc.Mine(func(state *test.ChainState) {
		state.Move("TicketBroker", ethcommon.HexToAddress("0xb1"))
		state.SetSender(f.payer, test.ChainSender{Deposit: big.NewInt(1), Reserve: big.NewInt(1), WithdrawRound: new(big.Int)})
	})
	f.refresh(t)
	require.ErrorIs(t, receiveRecipientPayment(t, f.engine, f.key, info, 2), pm.ErrPayerUnavailable, "funds come from the new broker")
	f.rpc.Mine(func(state *test.ChainState) {
		state.Move("BondingManager", ethcommon.HexToAddress("0xb2"))
		state.SetActive(f.recipient, false)
	})
	f.refresh(t)
	_, err := f.engine.MakeChallenge(t.Context(), "runner", "after", f.payer, 1, "fixed", "https://orch.example")
	require.ErrorContains(t, err, "not active", "activity comes from the new bonding manager")
}

// Expiry follows the observed L1 clock, which a view holds back by up to its maximum
// age. What keeps a late ticket safe is storeWinningTickets refusing a parameter set
// once redemption can reveal its randomness, which does not depend on the clock.
func TestStaleViewCannotAcceptTicketsOnceRedemptionCanRevealRandomness(t *testing.T) {
	f := newViewEngine(t)
	info := f.params(t, "session")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))
	expiry := new(big.Int).SetBytes(info.TicketParams.ExpirationBlock).Int64()
	f.rpc.Mine(func(state *test.ChainState) { state.L1Block = expiry })
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 2), "the view still shows a clock before expiry")

	// The parameters have expired on chain, so a real-time redemption pass may proceed.
	ticket := &pm.SignedTicket{Ticket: &pm.Ticket{Recipient: f.recipient, PayerAddress: f.payer, FaceValue: f.face, WinProb: big.NewInt(1), TicketNonce: 1, RecipientRandHash: ethcommon.BytesToHash(info.TicketParams.RecipientRandHash), CreationRound: 5, ParamsExpirationBlock: big.NewInt(expiry)}, Sig: make([]byte, 65), RecipientRand: big.NewInt(1)}
	require.NoError(t, f.engine.store.StoreWinningTicket(ticket))
	require.NoError(t, f.engine.store.recordPrepared(t.Context(), ticket, eth.SignedTransaction{Hash: ethcommon.HexToHash("0x1234"), Raw: []byte{1}, From: f.recipient, Nonce: 1}))
	require.ErrorIs(t, receiveRecipientPayment(t, f.engine, f.key, info, 3), ErrInvalidPayment, "refused while the view's clock is still behind")

	f.refresh(t)
	fresh := f.params(t, "after expiry")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, fresh, 1))
	require.ErrorIs(t, receiveRecipientPayment(t, f.engine, f.key, info, 4), ErrInvalidPayment, "the observed clock has passed expiry too")
}

// paidReceiptStatus answers a payment as the orchestrator's HTTP routes do.
func paidReceiptStatus(t *testing.T, e *PaymentEngine, key *ecdsa.PrivateKey, info wire.OrchestratorInfo) int {
	t.Helper()
	payment, segment := recipientPayment(t, key, info, 99)
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("Livepeer-Payment", base64.StdEncoding.EncodeToString(wire.EncodePayment(payment)))
	request.Header.Set("Livepeer-Segment", base64.StdEncoding.EncodeToString(wire.EncodeSegData(segment)))
	response := httptest.NewRecorder()
	_, ok := (&Server{payment: e}).paidReceipt(response, request, "runner", priceInfo{Price: "1", Currency: "wei", Unit: "fixed"})
	require.False(t, ok)
	return response.Code
}

func TestRefusedChainViewFailsLikeAFailedChainRead(t *testing.T) {
	f := newViewEngine(t)
	info := f.params(t, "session")
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 1))

	// A direct engine with the RPC down is what a refusal must look like.
	direct, err := NewPaymentEngine(f.engine.store, pm.EthereumChain{Client: f.direct}, f.recipient, f.face, f.engine.winProb)
	require.NoError(t, err)
	directChallenge := func() error {
		_, err := direct.MakeChallenge(t.Context(), "runner", "direct", f.payer, 1, "fixed", "https://orch.example")
		return err
	}
	require.NoError(t, directChallenge())
	directInfo, err := direct.ChallengeInfo("direct")
	require.NoError(t, err)
	f.rpc.SetDown(true)
	require.Error(t, directChallenge())
	wantChallenge := paymentAppOutcome(directChallenge())
	wantReceive := receiveRecipientPayment(t, direct, f.key, directInfo, 1)
	require.Error(t, wantReceive)
	wantStatus := paidReceiptStatus(t, direct, f.key, directInfo)
	require.Equal(t, http.StatusBadRequest, wantStatus)

	f.clock.Advance(31 * time.Second)
	f.rpc.SetDown(false)
	_, err = f.engine.MakeChallenge(t.Context(), "runner", "stale", f.payer, 1, "fixed", "https://orch.example")
	require.Error(t, err)
	require.Equal(t, wantChallenge, paymentAppOutcome(err))
	_, err = f.engine.RefreshChallenge(t.Context(), "session", f.payer, "https://orch.example")
	require.Error(t, err)
	err = receiveRecipientPayment(t, f.engine, f.key, info, 2)
	require.Error(t, err)
	require.Equal(t, paymentOutcome(wantReceive), paymentOutcome(err), "a service failure, not a rejected payment")
	require.Equal(t, paymentAppOutcome(wantReceive), paymentAppOutcome(err))
	require.NotErrorIs(t, err, ErrInvalidPayment)
	require.NotErrorIs(t, err, pm.ErrPayerUnavailable)
	require.Equal(t, wantStatus, paidReceiptStatus(t, f.engine, f.key, info), "the same HTTP answer")

	f.refresh(t)
	require.NoError(t, receiveRecipientPayment(t, f.engine, f.key, info, 2), "and recovers once the view does")
}

func TestPaidReadinessFollowsTheChainView(t *testing.T) {
	f := newViewEngine(t)
	metrics := observability.New("orchestrator")
	registry := NewRegistry("", "http://localhost", time.Second, time.Minute)
	p := &paidReadiness{interval: time.Second, workerCompleted: time.Now(), account: f.recipient, view: f.view}
	check := func() {
		t.Helper()
		p.check(t.Context(), f.engine.store, f.direct, f.engine, metrics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	f.rpc.Reset()
	check()
	stats := f.rpc.Stats()
	require.Equal(t, map[string]int{"eth_getBalance": 1}, stats.Calls, "only the redeemer's gas balance is read: the chain state comes from the view")
	require.True(t, p.ready(registry))
	require.EqualValues(t, 1, test.MetricSum(t, metrics.Registry, "dependency_healthy", map[string]string{"dependency": "chain"}))
	require.EqualValues(t, f.clock.Now().Unix(), test.MetricSum(t, metrics.Registry, "chain_snapshot_last_success_timestamp_seconds", nil), "when the block was read, not when readiness looked at it")

	f.clock.Advance(31 * time.Second)
	require.False(t, p.ready(registry), "a stale view is not ready, even before the next check")
	check()
	require.False(t, p.chain)
	require.False(t, p.ready(registry))
	require.Zero(t, test.MetricSum(t, metrics.Registry, "dependency_healthy", map[string]string{"dependency": "chain"}))

	f.refresh(t)
	check()
	require.True(t, p.ready(registry))

	f.rpc.Mine(func(state *test.ChainState) { state.SetActive(f.recipient, false) })
	f.refresh(t)
	check()
	require.False(t, p.ready(registry), "an inactive recipient is still not ready")
}

func TestRedemptionKeepsReadingTheChainItself(t *testing.T) {
	f := newViewEngine(t)
	key := recoveryKey(t)
	f.rpc.Reset()
	failures := ProcessRedemptions(t.Context(), f.engine.store, f.direct, key, big.NewInt(1))
	require.Empty(t, failures)
	stats := f.rpc.Stats()
	require.Equal(t, 4, stats.RoundTrips, "a redemption pass observes the chain directly, though the view is current")
	require.Zero(t, stats.Batches)
	require.Equal(t, 1, stats.Calls["eth_getBlockByNumber"])
}
