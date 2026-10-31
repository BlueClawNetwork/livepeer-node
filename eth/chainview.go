package eth

import (
	"context"
	"errors"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/livepeer/node/eth/contracts"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
)

// ChainView answers the payment path's chain reads from memory, so RPC load
// follows a timer rather than request volume. Each observation pins every read to
// one canonical block, the coherence a direct Snapshot has, and re-reads contract
// addresses from the Controller.
//
// Only the payment path reads through a view. Redemption, receipts, finality and
// nonces stay real-time on PaymentChain.
type ChainView struct {
	chain           PaymentChain
	maxAge          time.Duration
	maxEntries      int
	idleAfter       time.Duration
	batchSize       int
	interval        time.Duration
	now             func() time.Time
	onBatchFallback func()
	onRefresh       func(string, time.Duration, error)
	refreshing      sync.Mutex // Observations run one at a time.
	inline          singleflight.Group

	mu         sync.Mutex
	current    *chainObservation
	payers     map[viewPayer]time.Time
	recipients map[ethcommon.Address]time.Time
	batchRetry time.Time // When to try batches again; zero while they are in use.
}

// Defaults bound what a flood of new addresses can cost: at most this many entries
// are held, and at most this many reads of them run at once.
const (
	defaultViewEntries   = 1024
	defaultViewIdleAfter = 10 * time.Minute
	defaultViewBatchSize = 100
	viewConcurrency      = 8
)

// ChainViewOptions configures a ChainView. Interval and MaxAge are required.
type ChainViewOptions struct {
	Interval, MaxAge time.Duration
	Recipients       []ethcommon.Address // Observed from the first refresh.
	MaxEntries       int                 // Tracked payers, and tracked recipients; defaults to 1024.
	IdleAfter        time.Duration       // Stop reading an entry unused this long; defaults to 10 minutes.
	BatchSize        int                 // Calls in one batch; defaults to 100.
	Now              func() time.Time    // Defaults to time.Now.
	OnBatchFallback  func()              // Called when reads run sequentially because batches are refused.
	// OnRefresh reports each observation: success, partial when entries failed, or
	// error. Errors are safe to log, as they are for every other chain read.
	OnRefresh func(outcome string, elapsed time.Duration, err error)
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
	number    *big.Int  // Chain height, which unlike the L1 clock moves every block.
	block     string
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
	v := &ChainView{chain: chain, interval: options.Interval, maxAge: options.MaxAge, maxEntries: options.MaxEntries, idleAfter: options.IdleAfter, batchSize: options.BatchSize, now: options.Now, onBatchFallback: options.OnBatchFallback, onRefresh: options.OnRefresh, payers: map[viewPayer]time.Time{}, recipients: map[ethcommon.Address]time.Time{}}
	if v.now == nil {
		v.now = time.Now
	}
	if v.maxEntries == 0 {
		v.maxEntries = defaultViewEntries
	}
	if v.idleAfter == 0 {
		v.idleAfter = defaultViewIdleAfter
	}
	if v.batchSize == 0 {
		v.batchSize = defaultViewBatchSize
	}
	for _, recipient := range options.Recipients {
		v.recipients[recipient] = v.now()
	}
	return v, nil
}

// errChainViewUnavailable fails a read as a failed chain read does. The view holds
// no observation, or only one past the maximum age.
var errChainViewUnavailable = errors.New("chain observation unavailable")

const viewBatchRetry = 10 * time.Minute

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
	if evicted, ok := touchViewEntry(v.recipients, recipient, v.now(), v.maxEntries); ok && v.current != nil {
		delete(v.current.active, evicted)
	}
	active, ok := obs.active[recipient]
	v.mu.Unlock()
	if ok {
		return active, nil
	}
	value, err := v.readInline(ctx, obs, "active/"+recipient.Hex(), func(ctx context.Context, read *viewRead) (any, error) {
		active, err := activeTranscoder(ctx, read, obs.contracts.bonding, recipient)
		if errors.Is(err, errViewCallPending) {
			read.fetch(ctx)
			active, err = activeTranscoder(ctx, read, obs.contracts.bonding, recipient)
		}
		if err != nil {
			return false, err
		}
		v.mu.Lock()
		if _, tracked := v.recipients[recipient]; tracked {
			obs.active[recipient] = active
		}
		v.mu.Unlock()
		return active, nil
	})
	if err != nil {
		return false, err
	}
	return value.(bool), nil
}

// viewInlineTimeout matches the RPC client's own request timeout.
const viewInlineTimeout = 15 * time.Second

// readInline runs one read pinned to an observation's block, shared by concurrent
// callers. It outlives a caller that gives up, since others may be waiting. Errors
// are returned to those sharing the read and then forgotten.
func (v *ChainView) readInline(ctx context.Context, obs *chainObservation, key string, read func(context.Context, *viewRead) (any, error)) (any, error) {
	result := v.inline.DoChan(obs.block+"/"+key, func() (any, error) {
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), viewInlineTimeout)
		defer cancel()
		return read(readCtx, v.newRead(obs.reference))
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case outcome := <-result:
		if outcome.Err != nil {
			return nil, outcome.Err
		}
		if v.expired(obs) {
			return nil, errChainViewUnavailable
		}
		return outcome.Val, nil
	}
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
	if evicted, ok := touchViewEntry(v.payers, key, v.now(), v.maxEntries); ok && v.current != nil {
		delete(v.current.funds, evicted)
	}
	info, ok := obs.funds[key]
	v.mu.Unlock()
	if ok {
		return info.clone(), nil
	}
	value, err := v.readInline(ctx, obs, "funds/"+payer.Hex()+"/"+recipient.Hex(), func(ctx context.Context, read *viewRead) (any, error) {
		info, err := read.funds(ctx, obs.contracts.broker, key)
		if errors.Is(err, errViewCallPending) {
			read.fetch(ctx)
			info, err = read.funds(ctx, obs.contracts.broker, key)
		}
		if err != nil {
			return nil, err
		}
		info.Snapshot = obs.snapshot
		v.mu.Lock()
		if _, tracked := v.payers[key]; tracked {
			obs.funds[key] = info
		}
		v.mu.Unlock()
		return info, nil
	})
	if err != nil {
		return SenderInfo{}, err
	}
	return value.(SenderInfo).clone(), nil
}

// Run observes the chain now and then every interval until ctx ends.
func (v *ChainView) Run(ctx context.Context) {
	ticker := time.NewTicker(v.interval)
	defer ticker.Stop()
	for {
		// An observation that outlasts the maximum age could never be served.
		observing, cancel := context.WithTimeout(ctx, v.maxAge)
		_ = v.Refresh(observing)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Age returns the time since the current observation's block read began, and false
// before the first observation.
func (v *ChainView) Age() (time.Duration, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.current == nil {
		return 0, false
	}
	return v.now().Sub(v.current.at), true
}

// Refresh reads one observation and publishes it. On failure the previous
// observation stays current.
func (v *ChainView) Refresh(ctx context.Context) error {
	v.refreshing.Lock()
	defer v.refreshing.Unlock()
	start := time.Now()
	failures, err := v.observe(ctx)
	if v.onRefresh != nil {
		outcome := "success"
		switch {
		case err != nil:
			outcome = "error"
		case failures > 0:
			outcome = "partial"
		}
		v.onRefresh(outcome, time.Since(start), err)
	}
	return err
}

// observe publishes a new observation, returning how many entries it left out.
func (v *ChainView) observe(ctx context.Context) (int, error) {
	at := v.now()
	header, err := v.chain.Contracts.RPC.header(ctx, "latest")
	if err != nil {
		return 0, err
	}
	block, reference, err := canonicalBlock(header)
	if err != nil {
		return 0, err
	}
	v.mu.Lock()
	if !v.batchRetry.IsZero() && at.After(v.batchRetry) {
		v.batchRetry = time.Time{}
	}
	prior := v.current
	cutoff := at.Add(-v.idleAfter)
	payers, recipients := expireViewEntries(v.payers, cutoff), expireViewEntries(v.recipients, cutoff)
	v.mu.Unlock()
	// Keep reads monotonic behind a load balancer. The last observation ages out if
	// the RPC stays behind it, and a reorg to a shorter chain catches up in blocks.
	if prior != nil && (*big.Int)(header.Number).Cmp(prior.number) < 0 {
		return 0, errors.New("chain head moved behind the last observation")
	}
	obs, failures, err := v.newRead(reference).observe(ctx, at, block, prior, payers, recipients)
	if err != nil {
		return 0, err
	}
	obs.number, obs.block = (*big.Int)(header.Number), header.Hash.Hex()
	v.mu.Lock()
	v.current = obs
	v.mu.Unlock()
	return failures, nil
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

// touchViewEntry records a use, making room at the limit by evicting the entry
// unused the longest. Scanning at the limit costs far less than the read it precedes.
func touchViewEntry[K comparable](entries map[K]time.Time, key K, now time.Time, limit int) (evicted K, ok bool) {
	if _, tracked := entries[key]; !tracked && len(entries) >= limit {
		var oldest time.Time
		for candidate, used := range entries {
			if !ok || used.Before(oldest) {
				evicted, oldest, ok = candidate, used, true
			}
		}
		delete(entries, evicted)
	}
	entries[key] = now
	return evicted, ok
}

func expireViewEntries[K comparable](entries map[K]time.Time, cutoff time.Time) []K {
	keys := make([]K, 0, len(entries))
	for key, used := range entries {
		if used.Before(cutoff) {
			delete(entries, key)
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

// Tracked returns how many payers later observations will read.
func (v *ChainView) Tracked() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.payers)
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

// fetch reads the queued calls in bounded batches. A refused batch falls back to
// sequential reads, still pinned to this read's block.
func (r *viewRead) fetch(ctx context.Context) {
	keys := r.pending
	r.pending = nil
	if r.view.batching() {
		var (
			mu      sync.Mutex
			refused []viewCallKey
			batches errgroup.Group
		)
		batches.SetLimit(viewConcurrency)
		for chunk := range slices.Chunk(keys, r.view.batchSize) {
			batches.Go(func() error {
				unanswered := r.fetchBatch(ctx, chunk)
				mu.Lock()
				defer mu.Unlock()
				refused = append(refused, unanswered...)
				return nil
			})
		}
		_ = batches.Wait()
		if keys = refused; len(keys) == 0 {
			return
		}
		// A batch fails the same way during an outage or a reorg. Only a call that
		// succeeds alone shows it was the batch being refused.
		r.fetchOne(ctx, keys[0])
		if failure := r.results[keys[0]].err; failure != nil {
			for _, key := range keys[1:] {
				r.results[key].err = failure
			}
			return
		}
		r.view.refuseBatches()
		keys = keys[1:]
	}
	r.view.notifyFallback()
	var calls errgroup.Group
	calls.SetLimit(viewConcurrency)
	for _, key := range keys {
		calls.Go(func() error {
			r.fetchOne(ctx, key)
			return nil
		})
	}
	_ = calls.Wait()
}

// fetchBatch reads keys in one batch and returns those it got no answer for: all
// of them unless some call got one, or those a provider left out of its response.
func (r *viewRead) fetchBatch(ctx context.Context, keys []viewCallKey) []viewCallKey {
	batch := make([]gethrpc.BatchElem, len(keys))
	for i, key := range keys {
		to := key.to
		batch[i] = gethrpc.BatchElem{Method: "eth_call", Args: []any{map[string]any{"from": ethcommon.Address{}, "to": &to, "input": hexutil.Bytes(key.data)}, r.reference}, Result: &r.results[key].output}
	}
	if err := r.view.chain.Contracts.RPC.ethereum.Client().BatchCallContext(ctx, batch); err != nil {
		for _, key := range keys {
			r.results[key].err = safeRPCError(err)
		}
		return keys
	}
	var missing []viewCallKey
	failed := 0
	for i, key := range keys {
		r.results[key].err = safeRPCError(batch[i].Error)
		if batch[i].Error != nil {
			failed++
		}
		if errors.Is(batch[i].Error, gethrpc.ErrMissingBatchResponse) {
			missing = append(missing, key)
		}
	}
	if failed == len(keys) {
		return keys
	}
	return missing
}

func (r *viewRead) fetchOne(ctx context.Context, key viewCallKey) {
	to := key.to
	output, err := r.view.chain.Contracts.callerAt(r.reference).CallContract(ctx, ethereum.CallMsg{To: &to, Data: []byte(key.data)}, nil)
	call := r.results[key]
	call.output, call.err = output, err
}

func (v *ChainView) batching() bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.batchRetry.IsZero()
}

// refuseBatches stops batching until a later observation tries again. Only
// observations try again, so a new payer's read keeps to its round trips.
func (v *ChainView) refuseBatches() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.batchRetry = v.now().Add(viewBatchRetry)
}

func (v *ChainView) notifyFallback() {
	if v.onBatchFallback != nil {
		v.onBatchFallback()
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
	// err is a failed round read, which fails the observation.
	err      error
	failures int
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
	// A failure for one entry only leaves it unobserved: its next read goes to the
	// RPC and reports the error, and other payers are unaffected.
	for _, recipient := range recipients {
		active, err := activeTranscoder(ctx, r, known.bonding, recipient)
		if err != nil {
			deps.failures++
			continue
		}
		deps.active[recipient] = active
	}
	for _, key := range payers {
		info, err := r.funds(ctx, known.broker, key)
		if err != nil {
			deps.failures++
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
func (r *viewRead) observe(ctx context.Context, at time.Time, block *big.Int, prior *chainObservation, payers []viewPayer, recipients []ethcommon.Address) (*chainObservation, int, error) {
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
			return nil, 0, err
		}
		obs := &chainObservation{at: at, reference: r.reference, contracts: known, funds: deps.funds, active: deps.active}
		obs.snapshot = ChainSnapshot{Block: block, Round: deps.round, RoundHash: deps.hash, blockReference: r.reference, observation: obs}
		for key, info := range obs.funds {
			info.Snapshot = obs.snapshot
			obs.funds[key] = info
		}
		return obs, deps.failures, nil
	}
}
