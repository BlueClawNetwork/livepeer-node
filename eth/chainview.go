package eth

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/livepeer/node/eth/contracts"
)

// ChainView answers the payment path's chain reads from memory, so RPC load
// follows a timer rather than request volume. Each observation pins every read to
// one canonical block, the coherence a direct Snapshot has, and re-reads contract
// addresses from the Controller.
//
// Only the payment path reads through a view. Redemption, receipts, finality and
// nonces stay real-time on PaymentChain.
type ChainView struct {
	chain      PaymentChain
	maxAge     time.Duration
	now        func() time.Time
	refreshing sync.Mutex // Observations run one at a time.

	mu         sync.Mutex
	current    *chainObservation
	payers     map[viewPayer]time.Time
	recipients map[ethcommon.Address]time.Time
}

// ChainViewOptions configures a ChainView. Interval and MaxAge are required.
type ChainViewOptions struct {
	Interval, MaxAge time.Duration
	Recipients       []ethcommon.Address // Observed from the first refresh.
	Now              func() time.Time    // Defaults to time.Now.
}

// viewPayer is a payer and the recipient of its claimable reserve. A zero
// recipient requests total remaining reserve, as SenderInfo does.
type viewPayer struct{ payer, recipient ethcommon.Address }

// viewContracts are the addresses the Controller returned at one block.
type viewContracts struct{ rounds, bonding, broker ethcommon.Address }

// chainObservation is everything read at one block. A later read may add to it
// only when pinned to that same block, so an observation never mixes blocks.
type chainObservation struct {
	at        time.Time // Taken before the block read, so age is never understated.
	reference map[string]any
	contracts viewContracts
	snapshot  ChainSnapshot
	funds     map[viewPayer]SenderInfo
	active    map[ethcommon.Address]bool
}

// NewChainView returns a view that refuses reads until its first refresh.
func NewChainView(chain PaymentChain, options ChainViewOptions) (*ChainView, error) {
	if chain.Contracts == nil || options.Interval <= 0 || options.MaxAge <= options.Interval {
		return nil, errors.New("chain view requires contracts, a positive interval and a greater maximum age")
	}
	v := &ChainView{chain: chain, maxAge: options.MaxAge, now: options.Now, payers: map[viewPayer]time.Time{}, recipients: map[ethcommon.Address]time.Time{}}
	if v.now == nil {
		v.now = time.Now
	}
	for _, recipient := range options.Recipients {
		v.recipients[recipient] = v.now()
	}
	return v, nil
}

// errChainViewUnavailable fails a read as a failed chain read does. The view holds
// no observation, or only one past the maximum age.
var errChainViewUnavailable = errors.New("chain observation unavailable")

func (v *ChainView) observation(ctx context.Context) (*chainObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v.mu.Lock()
	obs := v.current
	v.mu.Unlock()
	if obs == nil || v.expired(obs) {
		return nil, errChainViewUnavailable
	}
	return obs, nil
}

func (v *ChainView) expired(obs *chainObservation) bool {
	return v.now().Sub(obs.at) > v.maxAge
}

// Snapshot returns the current observation's block, round and round hash.
func (v *ChainView) Snapshot(ctx context.Context) (ChainSnapshot, error) {
	obs, err := v.observation(ctx)
	if err != nil {
		return ChainSnapshot{}, err
	}
	return obs.snapshot.clone(), nil
}

func (s ChainSnapshot) clone() ChainSnapshot {
	s.Block, s.Round = new(big.Int).Set(s.Block), new(big.Int).Set(s.Round)
	return s
}

func (i SenderInfo) clone() SenderInfo {
	i.Snapshot = i.Snapshot.clone()
	i.Deposit, i.Reserve, i.WithdrawRound = new(big.Int).Set(i.Deposit), new(big.Int).Set(i.Reserve), new(big.Int).Set(i.WithdrawRound)
	return i
}

// IsActiveAt returns a recipient's status at the snapshot's own observation. A
// recipient not yet observed is read at that same block.
func (v *ChainView) IsActiveAt(ctx context.Context, recipient ethcommon.Address, snapshot ChainSnapshot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	obs := snapshot.observation
	if obs == nil {
		return false, errors.New("canonical snapshot required")
	}
	if v.expired(obs) {
		return false, errChainViewUnavailable
	}
	v.mu.Lock()
	v.recipients[recipient] = v.now()
	active, ok := obs.active[recipient]
	v.mu.Unlock()
	if ok {
		return active, nil
	}
	read := v.newRead(obs.reference)
	active, err := activeTranscoder(ctx, read, obs.contracts.bonding, recipient)
	if errors.Is(err, errViewCallPending) {
		read.fetch(ctx)
		active, err = activeTranscoder(ctx, read, obs.contracts.bonding, recipient)
	}
	if err != nil {
		return false, err
	}
	if v.expired(obs) {
		return false, errChainViewUnavailable
	}
	v.mu.Lock()
	obs.active[recipient] = active
	v.mu.Unlock()
	return active, nil
}

// PayerFunds returns a payer's funds from the current observation. An unseen payer
// is read at that observation's block and joins later observations.
func (v *ChainView) PayerFunds(ctx context.Context, payer, recipient ethcommon.Address) (SenderInfo, error) {
	obs, err := v.observation(ctx)
	if err != nil {
		return SenderInfo{}, err
	}
	key := viewPayer{payer, recipient}
	v.mu.Lock()
	v.payers[key] = v.now()
	info, ok := obs.funds[key]
	v.mu.Unlock()
	if ok {
		return info.clone(), nil
	}
	read := v.newRead(obs.reference)
	info, err = read.funds(ctx, obs.contracts.broker, key)
	if errors.Is(err, errViewCallPending) {
		read.fetch(ctx)
		info, err = read.funds(ctx, obs.contracts.broker, key)
	}
	if err != nil {
		return SenderInfo{}, err
	}
	if v.expired(obs) {
		return SenderInfo{}, errChainViewUnavailable
	}
	info.Snapshot = obs.snapshot
	v.mu.Lock()
	obs.funds[key] = info
	v.mu.Unlock()
	return info.clone(), nil
}

// Refresh reads one observation and publishes it. On failure the previous
// observation stays current.
func (v *ChainView) Refresh(ctx context.Context) error {
	v.refreshing.Lock()
	defer v.refreshing.Unlock()
	at := v.now()
	header, err := v.chain.Contracts.RPC.header(ctx, "latest")
	if err != nil {
		return err
	}
	block, reference, err := canonicalBlock(header)
	if err != nil {
		return err
	}
	v.mu.Lock()
	prior := v.current
	payers := make([]viewPayer, 0, len(v.payers))
	for key := range v.payers {
		payers = append(payers, key)
	}
	recipients := make([]ethcommon.Address, 0, len(v.recipients))
	for recipient := range v.recipients {
		recipients = append(recipients, recipient)
	}
	v.mu.Unlock()
	obs, err := v.newRead(reference).observe(ctx, at, block, prior, payers, recipients)
	if err != nil {
		return err
	}
	v.mu.Lock()
	v.current = obs
	v.mu.Unlock()
	return nil
}

// errViewCallPending reports a call queued for the next batch.
var errViewCallPending = errors.New("chain view call pending")

type viewCallKey struct {
	to   ethcommon.Address
	data string
}

type viewCall struct {
	output hexutil.Bytes
	err    error
}

// viewRead answers the generated bindings from batches pinned to one block. A call
// without a result is queued and fails with errViewCallPending, so the bindings
// decide what is read and decode it exactly as they do for direct reads.
type viewRead struct {
	view      *ChainView
	reference map[string]any
	results   map[viewCallKey]*viewCall
	pending   []viewCallKey
}

func (v *ChainView) newRead(reference map[string]any) *viewRead {
	return &viewRead{view: v, reference: reference, results: map[viewCallKey]*viewCall{}}
}

func (r *viewRead) CallContract(_ context.Context, msg ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	key := viewCallKey{*msg.To, string(msg.Data)}
	call, ok := r.results[key]
	if !ok {
		call = &viewCall{err: errViewCallPending}
		r.results[key] = call
		r.pending = append(r.pending, key)
	}
	return call.output, call.err
}

// CodeAt answers the bindings' check after an empty result: no code here means no
// contract at that address, which is what an empty pinned result reports.
func (r *viewRead) CodeAt(context.Context, ethcommon.Address, *big.Int) ([]byte, error) {
	return nil, nil
}

// fetch reads the queued calls in one batch.
func (r *viewRead) fetch(ctx context.Context) {
	keys := r.pending
	r.pending = nil
	batch := make([]gethrpc.BatchElem, len(keys))
	for i, key := range keys {
		to := key.to
		batch[i] = gethrpc.BatchElem{Method: "eth_call", Args: []any{map[string]any{"from": ethcommon.Address{}, "to": &to, "input": hexutil.Bytes(key.data)}, r.reference}, Result: &r.results[key].output}
	}
	err := r.view.chain.Contracts.RPC.ethereum.Client().BatchCallContext(ctx, batch)
	for i, key := range keys {
		if err == nil {
			r.results[key].err = safeRPCError(batch[i].Error)
		} else {
			r.results[key].err = safeRPCError(err)
		}
	}
}

// funds queues both of a payer's calls together, since senderFunds stops at the
// first it cannot answer.
func (r *viewRead) funds(ctx context.Context, address ethcommon.Address, key viewPayer) (SenderInfo, error) {
	broker, err := contracts.NewTicketBrokerCaller(address, r)
	if err != nil {
		return SenderInfo{}, err
	}
	if key.recipient != (ethcommon.Address{}) {
		_, _ = broker.ClaimableReserve(&bind.CallOpts{Context: ctx}, key.payer, key.recipient)
	}
	return senderFunds(ctx, r, address, key.payer, key.recipient)
}

func (r *viewRead) contracts(ctx context.Context) (viewContracts, error) {
	var known viewContracts
	var failure error
	for name, address := range map[string]*ethcommon.Address{"roundsManager": &known.rounds, "bondingManager": &known.bonding, "ticketBroker": &known.broker} {
		var err error
		*address, err = r.view.chain.Contracts.resolveWith(ctx, r, name)
		if err != nil {
			failure = err
		}
	}
	return known, failure
}

// viewDependents are the reads that depend on contract addresses.
type viewDependents struct {
	round  *big.Int
	hash   ethcommon.Hash
	active map[ethcommon.Address]bool
	funds  map[viewPayer]SenderInfo
	err    error
}

// dependents reads everything behind the given addresses. An assumed round stands
// in for a round not read yet, so its hash can share the batch.
func (r *viewRead) dependents(ctx context.Context, known viewContracts, assumed *big.Int, payers []viewPayer, recipients []ethcommon.Address) viewDependents {
	deps := viewDependents{active: make(map[ethcommon.Address]bool, len(recipients)), funds: make(map[viewPayer]SenderInfo, len(payers))}
	deps.round, deps.err = initializedRound(ctx, r, known.rounds)
	if assumed != nil && errors.Is(deps.err, errViewCallPending) {
		deps.round = assumed
	}
	if deps.round != nil {
		var err error
		if deps.hash, err = roundBlockHash(ctx, r, known.rounds, deps.round); deps.err == nil {
			deps.err = err
		}
	}
	for _, recipient := range recipients {
		active, err := activeTranscoder(ctx, r, known.bonding, recipient)
		if err != nil {
			deps.err = err
			continue
		}
		deps.active[recipient] = active
	}
	for _, key := range payers {
		info, err := r.funds(ctx, known.broker, key)
		if err != nil {
			deps.err = err
			continue
		}
		deps.funds[key] = info
	}
	return deps
}

// An observation settles in at most three batches: the addresses, what depends on
// them, and the hash of a round only then known.
const maxViewBatches = 3

// observe first assumes the previous observation's addresses and round, so a normal
// observation fits one batch after the block read. Only results under the addresses
// and round this block confirms are used; a wrong assumption costs a follow-up batch
// pinned to the same block.
func (r *viewRead) observe(ctx context.Context, at time.Time, block *big.Int, prior *chainObservation, payers []viewPayer, recipients []ethcommon.Address) (*chainObservation, error) {
	if prior != nil {
		r.dependents(ctx, prior.contracts, prior.snapshot.Round, payers, recipients)
	}
	for batches := 0; ; batches++ {
		known, err := r.contracts(ctx)
		var deps viewDependents
		if err == nil {
			deps = r.dependents(ctx, known, nil, payers, recipients)
		}
		if len(r.pending) > 0 && batches < maxViewBatches {
			r.fetch(ctx)
			continue
		}
		if err == nil {
			err = deps.err
		}
		if err != nil {
			return nil, err
		}
		obs := &chainObservation{at: at, reference: r.reference, contracts: known, funds: deps.funds, active: deps.active}
		obs.snapshot = ChainSnapshot{Block: block, Round: deps.round, RoundHash: deps.hash, blockReference: r.reference, observation: obs}
		for key, info := range obs.funds {
			info.Snapshot = obs.snapshot
			obs.funds[key] = info
		}
		return obs, nil
	}
}
