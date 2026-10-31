package eth

import (
	"math/big"
	"sync"
	"testing"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/internal/test"
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
