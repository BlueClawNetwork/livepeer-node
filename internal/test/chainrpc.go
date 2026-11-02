package test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// BatchMode selects how ChainRPC answers a JSON-RPC batch, covering the ways
// providers and load balancers refuse them.
type BatchMode int

const (
	BatchAccepted      BatchMode = iota
	BatchRefusedHTTP             // HTTP 400 for any JSON array.
	BatchRefusedObject           // HTTP 200 with one error object instead of an array.
	BatchRefusedEach             // An array with -32601 for every element.
	BatchTruncated               // An array holding only the first response.
)

// ChainSender is a TicketBroker sender. A nil Claimable entry uses Reserve.
type ChainSender struct {
	Deposit, Reserve, WithdrawRound *big.Int
	Claimable                       map[common.Address]*big.Int
}

func (s ChainSender) clone() ChainSender {
	s.Claimable = maps.Clone(s.Claimable)
	return s
}

type chainRounds struct {
	Last   int64
	Hashes map[int64]common.Hash
}

// ChainState is the contract state of one block. State belongs to a contract
// address, so a retired contract keeps answering with what it last held.
type ChainState struct {
	Number, L1Block int64 // A zero L1Block omits l1BlockNumber, as on Ethereum.
	Contracts       map[string]common.Address
	rounds          map[common.Address]chainRounds
	active          map[common.Address]map[common.Address]bool
	senders         map[common.Address]map[common.Address]ChainSender
}

func (s *ChainState) clone() *ChainState {
	next := &ChainState{Number: s.Number, L1Block: s.L1Block, Contracts: maps.Clone(s.Contracts), rounds: map[common.Address]chainRounds{}, active: map[common.Address]map[common.Address]bool{}, senders: map[common.Address]map[common.Address]ChainSender{}}
	for address, rounds := range s.rounds {
		next.rounds[address] = chainRounds{rounds.Last, maps.Clone(rounds.Hashes)}
	}
	for address, active := range s.active {
		next.active[address] = maps.Clone(active)
	}
	for address, senders := range s.senders {
		next.senders[address] = maps.Clone(senders)
	}
	return next
}

// SetRound initializes a round on the current RoundsManager.
func (s *ChainState) SetRound(round int64, hash common.Hash) {
	address := s.Contracts["RoundsManager"]
	rounds := s.rounds[address]
	rounds.Last, rounds.Hashes[round] = round, hash
	s.rounds[address] = rounds
}

// SetActive sets recipient status on the current BondingManager.
func (s *ChainState) SetActive(recipient common.Address, active bool) {
	s.active[s.Contracts["BondingManager"]][recipient] = active
}

// SetSender sets a payer's funds on the current TicketBroker.
func (s *ChainState) SetSender(payer common.Address, sender ChainSender) {
	s.senders[s.Contracts["TicketBroker"]][payer] = sender.clone()
}

// Move deploys a contract at a new address, starting from its current state.
// The old address keeps answering with the state it held.
func (s *ChainState) Move(contract string, address common.Address) {
	previous := s.Contracts[contract]
	switch contract {
	case "RoundsManager":
		s.rounds[address] = chainRounds{s.rounds[previous].Last, maps.Clone(s.rounds[previous].Hashes)}
	case "BondingManager":
		s.active[address] = maps.Clone(s.active[previous])
	case "TicketBroker":
		s.senders[address] = maps.Clone(s.senders[previous])
	}
	s.Contracts[contract] = address
}

// ChainRPC is a counting Ethereum JSON-RPC HTTP server for the payment reads.
// Reads pinned to a block hash see that block's state and fail once the block
// is no longer canonical, so a test can observe mixed-block reads directly.
type ChainRPC struct {
	URL         string
	Controller  common.Address
	mu          sync.Mutex
	blocks      map[common.Hash]*ChainState
	head        common.Hash
	orphaned    map[common.Hash]bool
	nextHash    uint64
	latency     time.Duration
	down        bool
	batchMode   BatchMode
	failCall    func(function string, input []byte) bool
	onCall      func()
	onBlockRead func()
	trips       int
	batches     int
	calls       map[string]int
	unpinned    int
	batchSizes  []int
}

const chainRPCController = "0x00000000000000000000000000000000000c0001"

var chainSelectors = func() map[string]string {
	selectors := map[string]string{}
	for name, signature := range map[string]string{
		"Controller.getContract":             "getContract(bytes32)",
		"RoundsManager.lastInitializedRound": "lastInitializedRound()",
		"RoundsManager.blockHashForRound":    "blockHashForRound(uint256)",
		"BondingManager.isActiveTranscoder":  "isActiveTranscoder(address)",
		"TicketBroker.getSenderInfo":         "getSenderInfo(address)",
		"TicketBroker.claimableReserve":      "claimableReserve(address,address)",
	} {
		selectors[hex.EncodeToString(crypto.Keccak256([]byte(signature))[:4])] = name
	}
	return selectors
}()

// ChainRoundHash is the deterministic initial block hash of a round.
func ChainRoundHash(round int64) common.Hash {
	return crypto.Keccak256Hash([]byte("round"), big.NewInt(round).Bytes())
}

// NewChainRPC starts a chain at round 5 with its contracts registered.
func NewChainRPC(t testing.TB) *ChainRPC {
	t.Helper()
	state := &ChainState{Number: 0x100000, L1Block: 1000, Contracts: map[string]common.Address{
		"RoundsManager": common.HexToAddress("0xa1"), "BondingManager": common.HexToAddress("0xa2"), "TicketBroker": common.HexToAddress("0xa3"),
	}, rounds: map[common.Address]chainRounds{}, active: map[common.Address]map[common.Address]bool{}, senders: map[common.Address]map[common.Address]ChainSender{}}
	state.rounds[state.Contracts["RoundsManager"]] = chainRounds{Hashes: map[int64]common.Hash{}}
	state.active[state.Contracts["BondingManager"]] = map[common.Address]bool{}
	state.senders[state.Contracts["TicketBroker"]] = map[common.Address]ChainSender{}
	state.SetRound(5, ChainRoundHash(5))
	c := &ChainRPC{Controller: common.HexToAddress(chainRPCController), blocks: map[common.Hash]*ChainState{}, orphaned: map[common.Hash]bool{}, calls: map[string]int{}}
	c.head = c.addBlock(state)
	server := httptest.NewServer(http.HandlerFunc(c.serve))
	t.Cleanup(server.Close)
	c.URL = server.URL
	return c
}

// addBlock stores a state under a new unique hash. Caller holds c.mu, or the
// server is not yet running.
func (c *ChainRPC) addBlock(state *ChainState) common.Hash {
	c.nextHash++
	hash := crypto.Keccak256Hash([]byte("block"), new(big.Int).SetUint64(c.nextHash).Bytes())
	c.blocks[hash] = state
	return hash
}

// Mine appends a block whose state is the head's state after mutate.
func (c *ChainRPC) Mine(mutate func(*ChainState)) common.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.blocks[c.head].clone()
	state.Number++
	if mutate != nil {
		mutate(state)
	}
	c.head = c.addBlock(state)
	return c.head
}

// Reorg replaces the head with a sibling at the same height. The old head stops
// answering pinned reads, as requireCanonical demands.
func (c *ChainRPC) Reorg(mutate func(*ChainState)) common.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.blocks[c.head].clone()
	if mutate != nil {
		mutate(state)
	}
	c.orphaned[c.head] = true
	c.head = c.addBlock(state)
	return c.head
}

// Rewind makes an earlier block the head without orphaning anything, as a load
// balancer does when a request lands on a lagging backend.
func (c *ChainRPC) Rewind(block common.Hash) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.head = block
}

// Head returns the current head hash.
func (c *ChainRPC) Head() common.Hash {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head
}

// SetLatency delays every HTTP round trip.
func (c *ChainRPC) SetLatency(latency time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latency = latency
}

// SetDown makes every request fail with HTTP 503.
func (c *ChainRPC) SetDown(down bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.down = down
}

// SetBatchMode selects how batches are answered.
func (c *ChainRPC) SetBatchMode(mode BatchMode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batchMode = mode
}

// FailCalls makes matching eth_calls return an execution error. A nil filter
// clears it.
func (c *ChainRPC) FailCalls(filter func(function string, input []byte) bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failCall = filter
}

// OnCall runs a hook after the next eth_call is answered, so a test can change
// the chain or the clock in the middle of a read. It runs once.
func (c *ChainRPC) OnCall(hook func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onCall = hook
}

// OnBlockRead runs a hook after the next eth_getBlockByNumber is answered. It runs once.
func (c *ChainRPC) OnBlockRead(hook func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onBlockRead = hook
}

// ChainRPCStats counts traffic since the last Reset. RoundTrips are HTTP
// requests, so a batch is one; Calls counts each JSON-RPC call inside them, by
// method or Contract.function.
type ChainRPCStats struct {
	RoundTrips, Batches, Unpinned int
	Calls                         map[string]int
	BatchSizes                    []int
}

// Stats returns a copy of the current counts.
func (c *ChainRPC) Stats() ChainRPCStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ChainRPCStats{RoundTrips: c.trips, Batches: c.batches, Unpinned: c.unpinned, Calls: maps.Clone(c.calls), BatchSizes: append([]int(nil), c.batchSizes...)}
}

// Reset clears the counts.
func (c *ChainRPC) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trips, c.batches, c.unpinned, c.calls, c.batchSizes = 0, 0, 0, map[string]int{}, nil
}

type chainRPCRequest struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

func (c *ChainRPC) serve(w http.ResponseWriter, r *http.Request) {
	var body json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	latency, down, mode := c.latency, c.down, c.batchMode
	c.trips++
	c.mu.Unlock()
	time.Sleep(latency)
	if down {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if len(body) > 0 && body[0] == '[' {
		var requests []chainRPCRequest
		if err := json.Unmarshal(body, &requests); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		c.batches++
		c.batchSizes = append(c.batchSizes, len(requests))
		c.mu.Unlock()
		switch mode {
		case BatchRefusedHTTP:
			http.Error(w, "batch requests are not supported", http.StatusBadRequest)
			return
		case BatchRefusedObject:
			_ = json.NewEncoder(w).Encode(chainRPCError(nil, -32600, "batch requests are not supported"))
			return
		case BatchRefusedEach:
			responses := make([]map[string]any, len(requests))
			for i, request := range requests {
				responses[i] = chainRPCError(request.ID, -32601, "batch requests are not supported")
			}
			_ = json.NewEncoder(w).Encode(responses)
			return
		case BatchTruncated:
			requests = requests[:1]
		}
		responses := make([]map[string]any, len(requests))
		for i, request := range requests {
			responses[i] = c.answer(request)
		}
		_ = json.NewEncoder(w).Encode(responses)
		return
	}
	var request chainRPCRequest
	if err := json.Unmarshal(body, &request); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(c.answer(request))
}

func chainRPCError(id json.RawMessage, code int, message string) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": code, "message": message}}
}

func (c *ChainRPC) answer(request chainRPCRequest) map[string]any {
	c.mu.Lock()
	var hook func()
	switch request.Method {
	case "eth_call":
		hook, c.onCall = c.onCall, nil
	case "eth_getBlockByNumber":
		hook, c.onBlockRead = c.onBlockRead, nil
	}
	defer func() {
		c.mu.Unlock()
		if hook != nil {
			hook()
		}
	}()
	result := func(value any) map[string]any {
		return map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": value}
	}
	switch request.Method {
	case "eth_chainId":
		c.calls[request.Method]++
		return result("0x1")
	case "eth_getBalance":
		c.calls[request.Method]++
		return result("0x1")
	case "eth_getBlockByNumber":
		c.calls[request.Method]++
		state := c.blocks[c.head]
		header := map[string]string{"number": "0x" + strconv.FormatInt(state.Number, 16), "hash": c.head.Hex()}
		if state.L1Block != 0 {
			header["l1BlockNumber"] = "0x" + strconv.FormatInt(state.L1Block, 16)
		}
		return result(header)
	case "eth_call":
		return c.call(request)
	}
	c.calls[request.Method]++
	return chainRPCError(request.ID, -32601, "unexpected method "+request.Method)
}

// call answers one eth_call. Caller holds c.mu.
func (c *ChainRPC) call(request chainRPCRequest) map[string]any {
	var args struct {
		To    common.Address `json:"to"`
		Input string         `json:"input"`
	}
	var pin struct {
		BlockHash        common.Hash `json:"blockHash"`
		RequireCanonical bool        `json:"requireCanonical"`
	}
	input := []byte(nil)
	if len(request.Params) > 0 && json.Unmarshal(request.Params[0], &args) == nil {
		input = common.FromHex(args.Input)
	}
	function := "unknown"
	if len(input) >= 4 {
		if name, ok := chainSelectors[hex.EncodeToString(input[:4])]; ok {
			function = name
		}
	}
	c.calls[function]++
	block := c.head
	if len(request.Params) > 1 && json.Unmarshal(request.Params[1], &pin) == nil && pin.BlockHash != (common.Hash{}) && pin.RequireCanonical {
		block = pin.BlockHash
	} else {
		c.unpinned++
	}
	state := c.blocks[block]
	if state == nil {
		return chainRPCError(request.ID, -32000, "header not found")
	}
	if c.orphaned[block] {
		return chainRPCError(request.ID, -32000, "block hash is not currently canonical")
	}
	if c.failCall != nil && c.failCall(function, input) {
		return chainRPCError(request.ID, 3, "execution reverted")
	}
	output := state.execute(function, args.To, input)
	return map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "0x" + output}
}

func chainWord(value *big.Int) string {
	if value == nil {
		value = new(big.Int)
	}
	return fmt.Sprintf("%064x", value)
}

func chainArgAddress(input []byte, index int) common.Address {
	offset := 4 + 32*index
	if len(input) < offset+32 {
		return common.Address{}
	}
	return common.BytesToAddress(input[offset : offset+32])
}

// execute returns hex output, empty when the address holds no such contract.
func (s *ChainState) execute(function string, to common.Address, input []byte) string {
	switch function {
	case "Controller.getContract":
		if len(input) < 36 {
			return ""
		}
		for name, address := range s.Contracts {
			if crypto.Keccak256Hash([]byte(name)) == common.BytesToHash(input[4:36]) {
				return chainWord(new(big.Int).SetBytes(address.Bytes()))
			}
		}
		return chainWord(nil)
	case "RoundsManager.lastInitializedRound":
		if rounds, ok := s.rounds[to]; ok {
			return chainWord(big.NewInt(rounds.Last))
		}
	case "RoundsManager.blockHashForRound":
		if rounds, ok := s.rounds[to]; ok && len(input) >= 36 {
			return strings.TrimPrefix(rounds.Hashes[new(big.Int).SetBytes(input[4:36]).Int64()].Hex(), "0x")
		}
	case "BondingManager.isActiveTranscoder":
		if active, ok := s.active[to]; ok {
			if active[chainArgAddress(input, 0)] {
				return chainWord(big.NewInt(1))
			}
			return chainWord(nil)
		}
	case "TicketBroker.getSenderInfo":
		if senders, ok := s.senders[to]; ok {
			sender := senders[chainArgAddress(input, 0)]
			return chainWord(sender.Deposit) + chainWord(sender.WithdrawRound) + chainWord(sender.Reserve) + chainWord(nil)
		}
	case "TicketBroker.claimableReserve":
		if senders, ok := s.senders[to]; ok {
			sender := senders[chainArgAddress(input, 0)]
			if claimable, ok := sender.Claimable[chainArgAddress(input, 1)]; ok {
				return chainWord(claimable)
			}
			return chainWord(sender.Reserve)
		}
	}
	return ""
}
