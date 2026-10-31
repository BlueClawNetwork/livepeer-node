package eth

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/eth/contracts"
	"github.com/livepeer/node/eth/contracts/chainlink"
)

// Contracts adapts upstream bindings to explicit transaction planning and
// canonical payment snapshots. Protocol addresses come from the Controller;
// Poll and price-feed addresses are explicit caller inputs.
type Contracts struct {
	RPC          *RPC
	Controller   ethcommon.Address
	abis         map[string]*abi.ABI
	MaxFeePerGas *big.Int // Optional ceiling in wei per gas.
	noncesMu     sync.Mutex
	nonces       map[ethcommon.Address]*nonceState
}

var bindingMetadata = map[string]*bind.MetaData{
	"controller":      contracts.ControllerMetaData,
	"bondingManager":  contracts.BondingManagerMetaData,
	"ticketBroker":    contracts.TicketBrokerMetaData,
	"roundsManager":   contracts.RoundsManagerMetaData,
	"serviceRegistry": contracts.ServiceRegistryMetaData,
	"livepeerToken":   contracts.LivepeerTokenMetaData,
	"minter":          contracts.MinterMetaData,
	"poll":            contracts.PollMetaData,
	"governor":        contracts.GovernorMetaData,
	"priceFeed":       chainlink.AggregatorV3InterfaceMetaData,
}

// controllerNames maps binding names to their Controller registry names.
var controllerNames = map[string]string{"bondingManager": "BondingManager", "ticketBroker": "TicketBroker", "roundsManager": "RoundsManager", "serviceRegistry": "ServiceRegistry", "livepeerToken": "LivepeerToken", "minter": "Minter", "governor": "LivepeerGovernor"}

func OpenContracts(rpc *RPC, controller string) (*Contracts, error) {
	if !ValidAddress(controller) {
		return nil, errors.New("controller-address must be a 20-byte Ethereum address")
	}
	return NewContracts(rpc, ethcommon.HexToAddress(controller))
}

func NewContracts(rpc *RPC, controller ethcommon.Address) (*Contracts, error) {
	c := &Contracts{RPC: rpc, Controller: controller, abis: map[string]*abi.ABI{}}
	for name, metadata := range bindingMetadata {
		parsed, err := metadata.GetAbi()
		if err != nil {
			return nil, fmt.Errorf("invalid %s binding: %w", name, err)
		}
		c.abis[name] = parsed
	}
	return c, nil
}

func (c *Contracts) Resolve(ctx context.Context, name string) (ethcommon.Address, error) {
	return c.ResolveAt(ctx, "latest", name)
}

func (c *Contracts) ResolveAt(ctx context.Context, block any, name string) (ethcommon.Address, error) {
	return c.resolveWith(ctx, c.callerAt(block), name)
}

func (c *Contracts) resolveWith(ctx context.Context, caller bind.ContractCaller, name string) (ethcommon.Address, error) {
	if _, ok := c.abis[name]; !ok {
		return ethcommon.Address{}, errors.New("unsupported contract")
	}
	if name == "controller" {
		return c.Controller, nil
	}
	contractName := controllerNames[name]
	if contractName == "" {
		return ethcommon.Address{}, errors.New("contract requires an explicit address")
	}
	controller, err := contracts.NewControllerCaller(c.Controller, caller)
	if err != nil {
		return ethcommon.Address{}, err
	}
	address, err := controller.GetContract(&bind.CallOpts{Context: ctx}, crypto.Keccak256Hash([]byte(contractName)))
	if err != nil {
		return ethcommon.Address{}, fmt.Errorf("resolve %s: %w", contractName, err)
	}
	if address == (ethcommon.Address{}) {
		return ethcommon.Address{}, fmt.Errorf("%s is not registered in Controller", contractName)
	}
	return address, nil
}

func (c *Contracts) Pack(name, method string, args ...any) ([]byte, error) {
	contract, ok := c.abis[name]
	if !ok {
		return nil, errors.New("unsupported contract")
	}
	return contract.Pack(method, args...)
}

func (c *Contracts) Call(ctx context.Context, name string, address ethcommon.Address, method string, args ...any) ([]any, error) {
	return c.CallAt(ctx, "latest", name, address, method, args...)
}

func (c *Contracts) CallAt(ctx context.Context, block any, name string, address ethcommon.Address, method string, args ...any) ([]any, error) {
	contract, ok := c.abis[name]
	if !ok {
		return nil, errors.New("unsupported contract")
	}
	var values []any
	bound := bind.NewBoundContract(address, *contract, c.callerAt(block), nil, nil)
	if err := bound.Call(&bind.CallOpts{Context: ctx}, &values, method, args...); err != nil {
		return nil, fmt.Errorf("%s.%s: %w", name, method, err)
	}
	return values, nil
}

// Geth's hash-based calls use requireCanonical=false. This small read adapter
// preserves EIP-1898 canonicality for every call in a payment/price snapshot.
type contractCaller struct {
	rpc   *RPC
	block any
}

func (c *Contracts) callerAt(block any) contractCaller { return contractCaller{c.RPC, block} }

func (c contractCaller) CallContract(ctx context.Context, msg ethereum.CallMsg, number *big.Int) ([]byte, error) {
	if c.block == nil || c.block == "latest" {
		data, err := c.rpc.ethereum.CallContract(ctx, msg, number)
		return data, safeRPCError(err)
	}
	if c.block == "pending" {
		data, err := c.rpc.ethereum.PendingCallContract(ctx, msg)
		return data, safeRPCError(err)
	}
	var result hexutil.Bytes
	err := c.rpc.ethereum.Client().CallContext(ctx, &result, "eth_call", map[string]any{"from": msg.From, "to": msg.To, "input": hexutil.Bytes(msg.Data)}, c.block)
	return result, safeRPCError(err)
}

func (c contractCaller) CodeAt(ctx context.Context, address ethcommon.Address, number *big.Int) ([]byte, error) {
	if c.block == nil || c.block == "latest" {
		return c.rpc.CodeAt(ctx, address, number)
	}
	var result hexutil.Bytes
	err := c.rpc.ethereum.Client().CallContext(ctx, &result, "eth_getCode", address, c.block)
	return result, safeRPCError(err)
}

type TransactionPlan struct {
	To        ethcommon.Address `json:"to"`
	From      ethcommon.Address `json:"from"`
	Data      string            `json:"data"`
	ValueWei  string            `json:"value_wei"`
	GasLimit  uint64            `json:"gas_limit"`
	FeeCapWei string            `json:"max_fee_per_gas_wei"`
	TipCapWei string            `json:"max_priority_fee_per_gas_wei"`
}

var errMissingBaseFee = errors.New("ethereum RPC header is missing base fee")

// PlanTransaction simulates a state change and estimates gas before any signing.
func (c *Contracts) PlanTransaction(ctx context.Context, from, to ethcommon.Address, data []byte, value *big.Int) (TransactionPlan, error) {
	return c.PlanTransactionWithOptions(ctx, from, to, data, value, FeeOptions{})
}

func (c *Contracts) PlanTransactionWithOptions(ctx context.Context, from, to ethcommon.Address, data []byte, value *big.Int, options FeeOptions) (TransactionPlan, error) {
	if value == nil || value.Sign() < 0 || value.BitLen() > 256 {
		return TransactionPlan{}, errors.New("invalid transaction value")
	}
	call := ethereum.CallMsg{From: from, To: &to, Data: data, Value: value, Gas: options.GasLimit}
	if _, err := c.RPC.ethereum.PendingCallContract(ctx, call); err != nil {
		return TransactionPlan{}, fmt.Errorf("transaction simulation failed: %w", safeRPCError(err))
	}
	gas := options.GasLimit
	if gas == 0 {
		var err error
		gas, err = c.RPC.ethereum.EstimateGas(ctx, call)
		if err != nil {
			return TransactionPlan{}, fmt.Errorf("estimate gas: %w", safeRPCError(err))
		}
	}
	if gas == 0 {
		return TransactionPlan{}, errors.New("estimated gas must be positive")
	}
	fees, err := c.Fees(ctx, options.PriorityFee)
	if err != nil {
		return TransactionPlan{}, err
	}
	return TransactionPlan{To: to, From: from, Data: hexutil.Encode(data), ValueWei: value.String(), GasLimit: gas, FeeCapWei: fees.FeeCapWei, TipCapWei: fees.TipCapWei}, nil
}

func (c *Contracts) checkFee(fee *big.Int) error {
	if c.MaxFeePerGas != nil {
		if c.MaxFeePerGas.Sign() <= 0 {
			return errors.New("maximum fee per gas must be positive")
		}
		if fee.Cmp(c.MaxFeePerGas) > 0 {
			return errors.New("transaction fee exceeds maximum fee per gas")
		}
	}
	return nil
}

type nonceState struct {
	sync.Mutex
	next uint64
}

func (c *Contracts) nonceState(address ethcommon.Address) *nonceState {
	c.noncesMu.Lock()
	defer c.noncesMu.Unlock()
	if c.nonces == nil {
		c.nonces = map[ethcommon.Address]*nonceState{}
	}
	if c.nonces[address] == nil {
		c.nonces[address] = new(nonceState)
	}
	return c.nonces[address]
}

// SetNonceFloor includes identities already durably saved by a caller. Local
// reservations cannot coordinate another process using the same signing key.
func (c *Contracts) SetNonceFloor(address ethcommon.Address, floor uint64) {
	state := c.nonceState(address)
	state.Lock()
	defer state.Unlock()
	state.next = max(state.next, floor)
}

type SignedTransaction struct {
	Hash  ethcommon.Hash
	Raw   []byte
	Nonce uint64
	From  ethcommon.Address
}

// Prepare performs reads and signing only; callers can durably save the identity
// before the first possible broadcast.
func (c *Contracts) Prepare(ctx context.Context, plan TransactionPlan, key *Key, chainID *big.Int) (SignedTransaction, error) {
	return c.prepare(ctx, plan, key, chainID, nil)
}

// PrepareAndStore holds the transaction account's nonce reservation through persistence.
// Save must durably store the identity without broadcasting it or calling back
// into this client's nonce methods. A failed save does not consume a nonce.
func (c *Contracts) PrepareAndStore(ctx context.Context, plan TransactionPlan, key *Key, chainID *big.Int, save func(SignedTransaction) error) (SignedTransaction, error) {
	if save == nil {
		return SignedTransaction{}, errors.New("transaction persistence is required")
	}
	return c.prepare(ctx, plan, key, chainID, save)
}

func (c *Contracts) prepare(ctx context.Context, plan TransactionPlan, key *Key, chainID *big.Int, save func(SignedTransaction) error) (SignedTransaction, error) {
	return c.prepareAtNonce(ctx, plan, key, chainID, save, nil)
}

// prepareAtNonce lets the ported redemption backend own nonce allocation through
// submission. The CLI's prepare-and-reserve behavior remains unchanged.
func (c *Contracts) prepareAtNonce(ctx context.Context, plan TransactionPlan, key *Key, chainID *big.Int, save func(SignedTransaction) error, explicit *uint64) (SignedTransaction, error) {
	if key == nil || key.Address() != plan.From || chainID == nil || chainID.Sign() <= 0 {
		return SignedTransaction{}, errors.New("transaction account does not match signing key")
	}
	data, err := hexutil.Decode(plan.Data)
	if err != nil {
		return SignedTransaction{}, errors.New("invalid transaction data")
	}
	parse := func(raw, name string) (*big.Int, error) {
		value, ok := new(big.Int).SetString(raw, 10)
		if !ok || value.Sign() < 0 || value.BitLen() > 256 {
			return nil, fmt.Errorf("invalid %s", name)
		}
		return value, nil
	}
	value, err := parse(plan.ValueWei, "transaction value")
	if err != nil {
		return SignedTransaction{}, err
	}
	if plan.GasLimit == 0 {
		return SignedTransaction{}, errors.New("gas limit must be positive")
	}
	opts, err := bind.NewKeyedTransactorWithChainID(key.private, chainID)
	if err != nil {
		return SignedTransaction{}, err
	}
	opts.Context, opts.NoSend, opts.Value, opts.GasLimit = ctx, true, value, plan.GasLimit
	opts.GasFeeCap, err = parse(plan.FeeCapWei, "maximum fee per gas")
	if err == nil {
		opts.GasTipCap, err = parse(plan.TipCapWei, "maximum priority fee per gas")
	}
	if err != nil {
		return SignedTransaction{}, err
	}
	if opts.GasFeeCap.Cmp(opts.GasTipCap) < 0 {
		return SignedTransaction{}, errors.New("fee cap is below tip cap")
	}
	if err = c.checkFee(opts.GasFeeCap); err != nil {
		return SignedTransaction{}, err
	}
	state := c.nonceState(plan.From)
	state.Lock()
	defer state.Unlock()
	var nonce uint64
	if explicit != nil {
		nonce = *explicit
	} else {
		nonce, err = c.RPC.ethereum.PendingNonceAt(ctx, plan.From)
		if err != nil {
			return SignedTransaction{}, safeRPCError(err)
		}
		nonce = max(nonce, state.next)
	}
	if nonce == math.MaxUint64 {
		return SignedTransaction{}, errors.New("transaction account nonce exhausted")
	}
	opts.Nonce = new(big.Int).SetUint64(nonce)
	bound := bind.NewBoundContract(plan.To, abi.ABI{}, nil, c.RPC.ethereum, nil)
	signed, err := bound.RawTransact(opts, data)
	if err != nil {
		return SignedTransaction{}, err
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		return SignedTransaction{}, err
	}
	prepared := SignedTransaction{Hash: signed.Hash(), Raw: raw, Nonce: nonce, From: plan.From}
	if save != nil {
		if err := save(prepared); err != nil {
			return SignedTransaction{}, err
		}
	}
	// Once saved or returned to a caller, this identity can be broadcast even
	// if its outcome is uncertain. Never reuse its nonce in this client.
	if explicit == nil {
		state.next = nonce + 1
	}
	return prepared, nil
}

// Broadcast sends these exact signed bytes once. Always retain the local hash:
// any transport error can occur after the node has accepted the transaction.
func (c *Contracts) Broadcast(ctx context.Context, tx SignedTransaction) error {
	var signed types.Transaction
	if err := signed.UnmarshalBinary(tx.Raw); err != nil || signed.Hash() != tx.Hash {
		return errors.New("invalid prepared transaction")
	}
	var hash ethcommon.Hash
	if err := c.RPC.ethereum.Client().CallContext(ctx, &hash, "eth_sendRawTransaction", hexutil.Encode(tx.Raw)); err != nil {
		return safeRPCError(err)
	}
	if hash != tx.Hash {
		return errors.New("transaction hash from RPC does not match signed transaction")
	}
	return nil
}

func (c *Contracts) Submit(ctx context.Context, plan TransactionPlan, key *Key, chainID *big.Int) (ethcommon.Hash, error) {
	tx, err := c.Prepare(ctx, plan, key, chainID)
	if err != nil {
		return ethcommon.Hash{}, err
	}
	return tx.Hash, c.Broadcast(ctx, tx)
}

// WaitReceipt waits for one confirmed receipt and reports reverted transactions.
func (c *Contracts) WaitReceipt(ctx context.Context, hash ethcommon.Hash) (uint64, error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		receipt, err := c.RPC.TransactionReceipt(ctx, hash)
		if err == nil {
			if receipt.Status != types.ReceiptStatusSuccessful {
				return 0, errors.New("transaction reverted")
			}
			return receipt.BlockNumber.Uint64(), nil
		}
		// Unlike WaitMined, CLI waiting surfaces provider failures immediately.
		if !errors.Is(err, ethereum.NotFound) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}
