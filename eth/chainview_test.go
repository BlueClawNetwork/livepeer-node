package eth

import (
	"bytes"
	"context"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	viewPayerAddress     = ethcommon.HexToAddress("0x2000")
	viewRecipientAddress = ethcommon.HexToAddress("0x3000")
)

// viewClock lets tests age an observation without waiting.
type viewClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *viewClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *viewClock) Advance(elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(elapsed)
}

func fundedViewChain(t *testing.T) *test.ChainRPC {
	t.Helper()
	rpc := test.NewChainRPC(t)
	rpc.Mine(func(state *test.ChainState) {
		state.SetActive(viewRecipientAddress, true)
		state.SetSender(viewPayerAddress, test.ChainSender{Deposit: big.NewInt(10), Reserve: big.NewInt(50), WithdrawRound: new(big.Int), Claimable: map[ethcommon.Address]*big.Int{viewRecipientAddress: big.NewInt(7)}})
	})
	return rpc
}

func newTestView(t *testing.T, rpc *test.ChainRPC, change ...func(*ChainViewOptions)) (*ChainView, *viewClock) {
	t.Helper()
	client, err := OpenRPC(rpc.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	contracts, err := NewContracts(client, rpc.Controller)
	require.NoError(t, err)
	clock := &viewClock{now: time.Unix(1_900_000_000, 0)}
	options := ChainViewOptions{Interval: 5 * time.Second, MaxAge: 30 * time.Second, Recipients: []ethcommon.Address{viewRecipientAddress}, Now: clock.Now}
	for _, apply := range change {
		apply(&options)
	}
	view, err := NewChainView(PaymentChain{Contracts: contracts}, options)
	require.NoError(t, err)
	return view, clock
}

func TestChainViewRequiresAMaximumAgeAboveItsInterval(t *testing.T) {
	contracts := PaymentChain{Contracts: &Contracts{}}
	for _, options := range []ChainViewOptions{{}, {Interval: time.Second}, {Interval: time.Second, MaxAge: time.Second}, {Interval: -time.Second, MaxAge: time.Second}} {
		_, err := NewChainView(contracts, options)
		require.Error(t, err, "%+v", options)
	}
	_, err := NewChainView(contracts, ChainViewOptions{Interval: time.Second, MaxAge: time.Second + 1})
	require.NoError(t, err)
}

func TestChainViewRefusesReadsBeforeItsFirstObservation(t *testing.T) {
	view, _ := newTestView(t, fundedViewChain(t))
	_, err := view.Snapshot(t.Context())
	require.Error(t, err)
	_, err = view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.Error(t, err)
}

func TestChainViewServesAnObservationFromMemory(t *testing.T) {
	for _, l2 := range []bool{false, true} {
		t.Run(map[bool]string{false: "ethereum", true: "arbitrum"}[l2], func(t *testing.T) {
			rpc := fundedViewChain(t)
			clock := int64(1000)
			if !l2 {
				clock = 0x200000
				rpc.Mine(func(state *test.ChainState) { state.L1Block, state.Number = 0, clock })
			}
			view, _ := newTestView(t, rpc)
			require.NoError(t, view.Refresh(t.Context()))
			_, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
			require.NoError(t, err)
			require.NoError(t, view.Refresh(t.Context()))
			rpc.Reset()

			snapshot, err := view.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, big.NewInt(clock), snapshot.Block, "the snapshot's clock is the protocol L1 block")
			require.Equal(t, big.NewInt(5), snapshot.Round)
			require.Equal(t, test.ChainRoundHash(5), snapshot.RoundHash)
			active, err := view.IsActiveAt(t.Context(), viewRecipientAddress, snapshot)
			require.NoError(t, err)
			require.True(t, active)
			funds, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
			require.NoError(t, err)
			require.Equal(t, big.NewInt(10), funds.Deposit)
			require.Equal(t, big.NewInt(7), funds.Reserve, "acceptance uses this recipient's claimable reserve")
			require.Equal(t, new(big.Int), funds.WithdrawRound)
			require.Equal(t, snapshot, funds.Snapshot)
			require.Zero(t, rpc.Stats().RoundTrips, "a known payer costs no RPC")
		})
	}
}

func TestChainViewObservationIsTwoRoundTrips(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	_, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	rpc.Reset()
	require.NoError(t, view.Refresh(t.Context()))
	stats := rpc.Stats()
	require.Equal(t, 2, stats.RoundTrips, "one block read and one batch")
	require.Equal(t, 1, stats.Batches)
	require.Zero(t, stats.Unpinned, "every call is pinned to the block with requireCanonical")
	require.Equal(t, map[string]int{"eth_getBlockByNumber": 1, "Controller.getContract": 3, "RoundsManager.lastInitializedRound": 1, "RoundsManager.blockHashForRound": 1, "BondingManager.isActiveTranscoder": 1, "TicketBroker.getSenderInfo": 1, "TicketBroker.claimableReserve": 1}, stats.Calls, "contract addresses are re-read by every observation")
}

func TestChainViewFirstObservationSettlesInFourRoundTrips(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	rpc.Reset()
	require.NoError(t, view.Refresh(t.Context()))
	stats := rpc.Stats()
	require.Equal(t, 4, stats.RoundTrips, "block, addresses, what depends on them, round hash")
	require.Zero(t, stats.Unpinned)
}

func TestChainViewNewPayerReadsInlineAndJoinsTheNextObservation(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))

	rpc.Reset()
	funds, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	stats := rpc.Stats()
	require.Equal(t, 1, stats.RoundTrips, "at the current observation's block, in one batch")
	require.Zero(t, stats.Unpinned)
	require.Equal(t, big.NewInt(7), funds.Reserve)

	rpc.Reset()
	_, err = view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	require.Zero(t, rpc.Stats().RoundTrips, "now known")

	rpc.Mine(func(state *test.ChainState) {
		state.SetSender(viewPayerAddress, test.ChainSender{Deposit: big.NewInt(4), Reserve: big.NewInt(5), WithdrawRound: new(big.Int)})
	})
	require.NoError(t, view.Refresh(t.Context()))
	rpc.Reset()
	funds, err = view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(4), funds.Deposit)
	require.Zero(t, rpc.Stats().RoundTrips, "the refresh read it")
}

func TestChainViewZeroRecipientUsesTotalReserve(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	funds, err := view.PayerFunds(t.Context(), viewPayerAddress, ethcommon.Address{})
	require.NoError(t, err)
	require.Equal(t, big.NewInt(50), funds.Reserve, "readiness uses total remaining reserve")
	require.NoError(t, view.Refresh(t.Context()))
	funds, err = view.PayerFunds(t.Context(), viewPayerAddress, ethcommon.Address{})
	require.NoError(t, err)
	require.Equal(t, big.NewInt(50), funds.Reserve)
	require.Zero(t, rpc.Stats().Calls["TicketBroker.claimableReserve"])
}

func TestChainViewReturnsCopies(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	first, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	first.Deposit.SetInt64(-1)
	first.Reserve.SetInt64(-1)
	first.Snapshot.Block.SetInt64(-1)
	first.Snapshot.Round.SetInt64(-1)
	second, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(10), second.Deposit)
	require.Equal(t, big.NewInt(7), second.Reserve)
	require.Equal(t, big.NewInt(1000), second.Snapshot.Block)
	require.Equal(t, big.NewInt(5), second.Snapshot.Round)
}

func TestChainViewRefusesPastItsMaximumAgeAndRecovers(t *testing.T) {
	rpc := fundedViewChain(t)
	view, clock := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	_, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	require.NoError(t, view.Refresh(t.Context()))
	held, err := view.Snapshot(t.Context())
	require.NoError(t, err)

	reads := func() map[string]error {
		_, snapshot := view.Snapshot(t.Context())
		_, funds := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
		_, newPayer := view.PayerFunds(t.Context(), ethcommon.HexToAddress("0x2001"), viewRecipientAddress)
		_, active := view.IsActiveAt(t.Context(), viewRecipientAddress, held)
		return map[string]error{"snapshot": snapshot, "funds": funds, "new payer": newPayer, "active": active}
	}
	requireReads := func(check func(error, ...any)) {
		for name, err := range reads() {
			check(err, name)
		}
	}

	// A failed refresh keeps the last good observation, but only until the maximum age.
	rpc.SetDown(true)
	clock.Advance(20 * time.Second)
	require.Error(t, view.Refresh(t.Context()))
	rpc.SetDown(false)
	requireReads(func(err error, name ...any) { require.NoError(t, err, name...) })
	rpc.SetDown(true)
	clock.Advance(10 * time.Second)
	require.Error(t, view.Refresh(t.Context()))
	_, err = view.Snapshot(t.Context())
	require.NoError(t, err, "exactly the maximum age is still served")
	clock.Advance(time.Nanosecond)
	requireReads(func(err error, name ...any) { require.ErrorIs(t, err, errChainViewUnavailable, name...) })
	rpc.Reset()
	rpc.SetDown(false)
	requireReads(func(err error, name ...any) { require.ErrorIs(t, err, errChainViewUnavailable, name...) })
	require.Zero(t, rpc.Stats().RoundTrips, "a refused read does not reach the RPC")

	rpc.Mine(func(state *test.ChainState) { state.L1Block = 1001 })
	require.NoError(t, view.Refresh(t.Context()))
	held, err = view.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, big.NewInt(1001), held.Block)
	requireReads(func(err error, name ...any) { require.NoError(t, err, name...) })
}

func TestChainViewDiscardsAnInlineReadThatOutlivesTheMaximumAge(t *testing.T) {
	rpc := fundedViewChain(t)
	view, clock := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	rpc.OnCall(func() { clock.Advance(31 * time.Second) })
	_, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.ErrorIs(t, err, errChainViewUnavailable)
}

func TestChainViewConcurrentNewPayersShareOneRead(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	rpc.SetLatency(50 * time.Millisecond)
	rpc.Reset()
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 32 {
		workers.Go(func() {
			<-start
			funds, err := view.PayerFunds(context.Background(), viewPayerAddress, viewRecipientAddress)
			if assert.NoError(t, err) {
				assert.Equal(t, big.NewInt(7), funds.Reserve)
			}
		})
	}
	close(start)
	workers.Wait()
	stats := rpc.Stats()
	require.Equal(t, 1, stats.RoundTrips)
	require.Equal(t, 1, stats.Calls["TicketBroker.getSenderInfo"])
}

func TestChainViewDoesNotCacheAFailedRead(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	snapshot, err := view.Snapshot(t.Context())
	require.NoError(t, err)
	other := ethcommon.HexToAddress("0x3001")
	rpc.FailCalls(func(string, []byte) bool { return true })
	_, err = view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.Error(t, err)
	_, err = view.IsActiveAt(t.Context(), other, snapshot)
	require.Error(t, err)
	rpc.FailCalls(nil)
	rpc.Reset()
	funds, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(7), funds.Reserve)
	active, err := view.IsActiveAt(t.Context(), other, snapshot)
	require.NoError(t, err)
	require.False(t, active)
	require.Equal(t, 2, rpc.Stats().RoundTrips, "both reads went back to the RPC")
}

func TestChainViewCancelledCallerLeavesASharedReadRunning(t *testing.T) {
	rpc := fundedViewChain(t)
	view, _ := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	rpc.SetLatency(100 * time.Millisecond)
	first, cancel := context.WithCancel(t.Context())
	failed := make(chan error, 1)
	go func() {
		_, err := view.PayerFunds(first, viewPayerAddress, viewRecipientAddress)
		failed <- err
	}()
	require.Eventually(t, func() bool { return rpc.Stats().RoundTrips > 4 }, time.Second, time.Millisecond, "the first caller started the read")
	second := make(chan error, 1)
	go func() {
		_, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
		second <- err
	}()
	time.Sleep(10 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-failed, context.Canceled)
	require.NoError(t, <-second)
}

func TestChainViewOneFailingEntryDoesNotFailTheObservation(t *testing.T) {
	rpc := fundedViewChain(t)
	badPayer, badRecipient := ethcommon.HexToAddress("0x2bad"), ethcommon.HexToAddress("0x3bad")
	view, _ := newTestView(t, rpc, func(options *ChainViewOptions) {
		options.Recipients = append(options.Recipients, badRecipient)
	})
	require.NoError(t, view.Refresh(t.Context()))
	for _, payer := range []ethcommon.Address{viewPayerAddress, badPayer} {
		_, err := view.PayerFunds(t.Context(), payer, viewRecipientAddress)
		require.NoError(t, err)
	}
	reverting := func(_ string, input []byte) bool {
		return bytes.Contains(input, badPayer.Bytes()) || bytes.Contains(input, badRecipient.Bytes())
	}
	rpc.FailCalls(reverting)
	rpc.Mine(func(state *test.ChainState) {
		state.SetSender(viewPayerAddress, test.ChainSender{Deposit: big.NewInt(4), Reserve: big.NewInt(5), WithdrawRound: new(big.Int)})
	})
	require.NoError(t, view.Refresh(t.Context()), "a contract revert for one payer cannot refuse every other payer")

	rpc.Reset()
	snapshot, err := view.Snapshot(t.Context())
	require.NoError(t, err)
	funds, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(4), funds.Deposit, "other payers are still current")
	active, err := view.IsActiveAt(t.Context(), viewRecipientAddress, snapshot)
	require.NoError(t, err)
	require.True(t, active)
	require.Zero(t, rpc.Stats().RoundTrips)

	_, err = view.PayerFunds(t.Context(), badPayer, viewRecipientAddress)
	require.Error(t, err, "the failure surfaces to that payer, as a direct read would")
	_, err = view.IsActiveAt(t.Context(), badRecipient, snapshot)
	require.Error(t, err)

	rpc.FailCalls(nil)
	_, err = view.PayerFunds(t.Context(), badPayer, viewRecipientAddress)
	require.NoError(t, err)
}

func TestChainViewCoreReadFailureFailsTheObservation(t *testing.T) {
	for _, function := range []string{"Controller.getContract", "RoundsManager.lastInitializedRound", "RoundsManager.blockHashForRound"} {
		t.Run(function, func(t *testing.T) {
			rpc := fundedViewChain(t)
			view, _ := newTestView(t, rpc)
			require.NoError(t, view.Refresh(t.Context()))
			before, err := view.Snapshot(t.Context())
			require.NoError(t, err)
			rpc.FailCalls(func(name string, _ []byte) bool { return name == function })
			rpc.Mine(func(state *test.ChainState) { state.L1Block = 2000 })
			require.Error(t, view.Refresh(t.Context()))
			after, err := view.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, before.Block, after.Block, "the last good observation stays current")
		})
	}
}

func TestChainViewFallsBackToSequentialWhenBatchesAreRefused(t *testing.T) {
	for _, refusal := range []struct {
		name string
		mode test.BatchMode
	}{{"HTTP status", test.BatchRefusedHTTP}, {"error object", test.BatchRefusedObject}, {"error per call", test.BatchRefusedEach}, {"missing responses", test.BatchTruncated}} {
		t.Run(refusal.name, func(t *testing.T) {
			rpc := fundedViewChain(t)
			rpc.SetBatchMode(refusal.mode)
			var fallbacks atomic.Int32
			view, _ := newTestView(t, rpc, func(options *ChainViewOptions) {
				options.OnBatchFallback = func() { fallbacks.Add(1) }
			})
			require.NoError(t, view.Refresh(t.Context()))
			require.Positive(t, fallbacks.Load())
			snapshot, err := view.Snapshot(t.Context())
			require.NoError(t, err)
			require.Equal(t, big.NewInt(5), snapshot.Round)
			require.Equal(t, test.ChainRoundHash(5), snapshot.RoundHash)

			rpc.Reset()
			funds, err := view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
			require.NoError(t, err)
			require.Equal(t, big.NewInt(7), funds.Reserve)
			stats := rpc.Stats()
			require.LessOrEqual(t, stats.RoundTrips, 2, "a new payer stays within its budget once a refusal is known")
			require.Zero(t, stats.Batches)
			require.Zero(t, stats.Unpinned)

			rpc.Mine(func(state *test.ChainState) {
				state.SetSender(viewPayerAddress, test.ChainSender{Deposit: big.NewInt(4), Reserve: big.NewInt(5), WithdrawRound: new(big.Int)})
			})
			rpc.Reset()
			before := fallbacks.Load()
			require.NoError(t, view.Refresh(t.Context()))
			stats = rpc.Stats()
			require.Zero(t, stats.Batches, "a known refusal does not waste a round trip")
			require.Zero(t, stats.Unpinned, "sequential reads stay pinned to one block")
			require.Equal(t, before+1, fallbacks.Load())
			rpc.Reset()
			funds, err = view.PayerFunds(t.Context(), viewPayerAddress, viewRecipientAddress)
			require.NoError(t, err)
			require.Equal(t, big.NewInt(4), funds.Deposit)
			require.Zero(t, rpc.Stats().RoundTrips)
		})
	}
}

func TestChainViewOnlyCountsRefusedBatches(t *testing.T) {
	rpc := fundedViewChain(t)
	var fallbacks atomic.Int32
	view, _ := newTestView(t, rpc, func(options *ChainViewOptions) {
		options.OnBatchFallback = func() { fallbacks.Add(1) }
	})
	require.NoError(t, view.Refresh(t.Context()))
	rpc.SetDown(true)
	require.Error(t, view.Refresh(t.Context()), "an outage is not a refusal")
	rpc.SetDown(false)
	rpc.FailCalls(func(string, []byte) bool { return true })
	require.Error(t, view.Refresh(t.Context()), "failing every call is not a refusal")
	rpc.FailCalls(nil)
	require.Zero(t, fallbacks.Load())
	rpc.Reset()
	require.NoError(t, view.Refresh(t.Context()))
	require.Equal(t, 1, rpc.Stats().Batches, "batches stay in use")
}

func TestChainViewTriesBatchesAgainLater(t *testing.T) {
	rpc := fundedViewChain(t)
	rpc.SetBatchMode(test.BatchRefusedHTTP)
	view, clock := newTestView(t, rpc)
	require.NoError(t, view.Refresh(t.Context()))
	rpc.SetBatchMode(test.BatchAccepted)
	rpc.Reset()
	require.NoError(t, view.Refresh(t.Context()))
	require.Zero(t, rpc.Stats().Batches)
	clock.Advance(viewBatchRetry + time.Second)
	rpc.Reset()
	require.NoError(t, view.Refresh(t.Context()))
	stats := rpc.Stats()
	require.Equal(t, 1, stats.Batches)
	require.Equal(t, 2, stats.RoundTrips)
}
