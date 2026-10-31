package eth

import (
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/livepeer/node/eth/contracts"
)

// PaymentChain adapts the retained Livepeer contracts to pm's narrow chain
// interface. Ethereum contract reads and redemption stay in eth.
type PaymentChain struct{ Contracts *Contracts }

func (c PaymentChain) IsActive(ctx context.Context, recipient ethcommon.Address) (bool, error) {
	snapshot, err := c.Snapshot(ctx)
	if err != nil {
		return false, err
	}
	return c.IsActiveAt(ctx, recipient, snapshot)
}

func (c PaymentChain) IsActiveAt(ctx context.Context, recipient ethcommon.Address, snapshot ChainSnapshot) (bool, error) {
	if snapshot.blockReference == nil {
		return false, errors.New("canonical snapshot required")
	}
	address, err := c.Contracts.ResolveAt(ctx, snapshot.blockReference, "bondingManager")
	if err != nil {
		return false, err
	}
	bonding, err := contracts.NewBondingManagerCaller(address, c.Contracts.callerAt(snapshot.blockReference))
	if err != nil {
		return false, err
	}
	return bonding.IsActiveTranscoder(&bind.CallOpts{Context: ctx}, recipient)
}

// Receipt reports a submitted redemption without retrying or resubmitting it.
func (c PaymentChain) Receipt(ctx context.Context, hash ethcommon.Hash) (confirmed, reverted bool, err error) {
	receipt, err := c.Contracts.RPC.TransactionReceipt(ctx, hash)
	if errors.Is(err, ethereum.NotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	finalized, err := c.Contracts.RPC.header(ctx, "finalized")
	if err != nil {
		return false, false, err
	}
	if finalized.Number == nil {
		return false, false, errors.New("invalid finalized header")
	}
	if (*big.Int)(finalized.Number).Cmp(receipt.BlockNumber) < 0 {
		return false, false, nil
	}
	canonical, err := c.Contracts.RPC.header(ctx, hexutil.EncodeBig(receipt.BlockNumber))
	if err != nil {
		return false, false, err
	}
	if canonical.Hash != receipt.BlockHash {
		return false, false, nil
	}
	return receipt.Status == types.ReceiptStatusSuccessful, receipt.Status == types.ReceiptStatusFailed, nil
}

type ChainSnapshot struct {
	Block          *big.Int // Protocol L1 block, including on Arbitrum.
	Round          *big.Int // Last initialized round.
	RoundHash      ethcommon.Hash
	blockReference map[string]any
}

func (c PaymentChain) Snapshot(ctx context.Context) (ChainSnapshot, error) {
	header, err := c.Contracts.RPC.header(ctx, "latest")
	if err != nil {
		return ChainSnapshot{}, err
	}
	block, ref, err := canonicalBlock(header)
	if err != nil {
		return ChainSnapshot{}, err
	}
	address, err := c.Contracts.ResolveAt(ctx, ref, "roundsManager")
	if err != nil {
		return ChainSnapshot{}, err
	}
	rounds, err := contracts.NewRoundsManagerCaller(address, c.Contracts.callerAt(ref))
	if err != nil {
		return ChainSnapshot{}, err
	}
	opts := &bind.CallOpts{Context: ctx}
	round, err := rounds.LastInitializedRound(opts)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if err := validInitializedRound(round); err != nil {
		return ChainSnapshot{}, err
	}
	hash, err := rounds.BlockHashForRound(opts, round)
	if err != nil {
		return ChainSnapshot{}, err
	}
	if hash == ([32]byte{}) {
		return ChainSnapshot{}, errors.New("invalid round block hash")
	}
	return ChainSnapshot{Block: block, Round: round, RoundHash: ethcommon.Hash(hash), blockReference: ref}, nil
}

// canonicalBlock returns a header's protocol L1 clock and the reference binding
// every read to that canonical block, including Controller resolution.
func canonicalBlock(header *miniHeader) (*big.Int, map[string]any, error) {
	if header.Hash == (ethcommon.Hash{}) || header.Number == nil {
		return nil, nil, errors.New("invalid chain header")
	}
	clock := header.Number
	if header.L1BlockNumber != nil {
		clock = header.L1BlockNumber
	}
	return new(big.Int).Set((*big.Int)(clock)), map[string]any{"blockHash": header.Hash.Hex(), "requireCanonical": true}, nil
}

func validInitializedRound(round *big.Int) error {
	if !round.IsInt64() || round.Sign() <= 0 {
		return errors.New("invalid initialized round")
	}
	return nil
}

// SenderInfo is a coherent view of a sender's collateral and the amount of
// reserve this particular recipient can claim in the initialized round.
// A zero recipient requests total remaining reserve, for sender readiness.
// Sender/reserve semantics follow go-livepeer/eth/client_ticketbroker.go,
// originally by Yondon Fu and Nico Vergauwen, with L1-round handling by
// Rafał Leszko (4b6ede31f040a084ff9e55bd798a0d6fffee1e1b).
type SenderInfo struct {
	Snapshot                        ChainSnapshot
	Deposit, Reserve, WithdrawRound *big.Int
}

func (c PaymentChain) SenderInfo(ctx context.Context, sender, recipient ethcommon.Address) (SenderInfo, error) {
	snapshot, err := c.Snapshot(ctx)
	if err != nil {
		return SenderInfo{}, err
	}
	address, err := c.Contracts.ResolveAt(ctx, snapshot.blockReference, "ticketBroker")
	if err != nil {
		return SenderInfo{}, err
	}
	broker, err := contracts.NewTicketBrokerCaller(address, c.Contracts.callerAt(snapshot.blockReference))
	if err != nil {
		return SenderInfo{}, err
	}
	opts := &bind.CallOpts{Context: ctx}
	info, err := broker.GetSenderInfo(opts, sender)
	if err != nil {
		return SenderInfo{}, err
	}
	reserve := info.Reserve.FundsRemaining
	if recipient != (ethcommon.Address{}) {
		reserve, err = broker.ClaimableReserve(opts, sender, recipient)
		if err != nil {
			return SenderInfo{}, err
		}
	}
	return SenderInfo{Snapshot: snapshot, Deposit: info.Sender.Deposit, Reserve: reserve, WithdrawRound: info.Sender.WithdrawRound}, nil
}

// The redemption tuple follows Yondon Fu's go-livepeer/eth/client_ticketbroker.go
// RedeemWinningTicket (ffcefd6341d14696e2718c1a42875428cd2cc953).
type RedeemTicket struct {
	Recipient         ethcommon.Address
	Sender            ethcommon.Address
	FaceValue         *big.Int
	WinProb           *big.Int
	SenderNonce       uint32
	RecipientRandHash ethcommon.Hash
	AuxData           []byte
	Signature         []byte
	RecipientRand     *big.Int
}

func (c PaymentChain) PrepareRedemption(ctx context.Context, redeemer *Key, chainID *big.Int, t RedeemTicket) (SignedTransaction, error) {
	plan, err := c.redemptionPlan(ctx, redeemer, t)
	if err != nil {
		return SignedTransaction{}, err
	}
	return c.Contracts.Prepare(ctx, plan, redeemer, chainID)
}

func (c PaymentChain) redemptionPlan(ctx context.Context, redeemer *Key, t RedeemTicket) (TransactionPlan, error) {
	if redeemer == nil || t.Recipient == (ethcommon.Address{}) || t.FaceValue == nil || t.WinProb == nil || t.RecipientRand == nil {
		return TransactionPlan{}, errors.New("incomplete redemption configuration or ticket")
	}
	address, err := c.Contracts.Resolve(ctx, "ticketBroker")
	if err != nil {
		return TransactionPlan{}, err
	}
	coreTicket := contracts.MTicketBrokerCoreTicket{Recipient: t.Recipient, Sender: t.Sender, FaceValue: t.FaceValue, WinProb: t.WinProb, SenderNonce: new(big.Int).SetUint64(uint64(t.SenderNonce)), RecipientRandHash: t.RecipientRandHash, AuxData: t.AuxData}
	data, err := c.Contracts.Pack("ticketBroker", "redeemWinningTicket", coreTicket, t.Signature, t.RecipientRand)
	if err != nil {
		return TransactionPlan{}, err
	}
	return c.Contracts.PlanTransaction(ctx, redeemer.Address(), address, data, big.NewInt(0))
}

// SubmitRedemption ports go-livepeer/eth/backend.go's Next/send/Update sequence.
// Persistence happens before send, but only an acknowledged send advances Next.
func (c PaymentChain) SubmitRedemption(ctx context.Context, nonces *NonceManager, manager *TransactionManager, redeemer *Key, chainID *big.Int, t RedeemTicket, save func(SignedTransaction) error) error {
	nonces.Lock(redeemer.Address())
	defer nonces.Unlock(redeemer.Address())
	plan, err := c.redemptionPlan(ctx, redeemer, t)
	if err != nil {
		return err
	}
	nonce, err := nonces.NextContext(ctx, redeemer.Address())
	if err != nil {
		return err
	}
	prepared, err := c.Contracts.prepareAtNonce(ctx, plan, redeemer, chainID, save, &nonce)
	if err != nil {
		return err
	}
	var tx types.Transaction
	if err := tx.UnmarshalBinary(prepared.Raw); err != nil {
		return err
	}
	if err := manager.SendTransaction(ctx, &tx); err != nil {
		return err
	}
	nonces.Update(redeemer.Address(), nonce)
	return nil
}

// IsUsedTicket is ported from go-livepeer/eth/client_ticketbroker.go. The block
// argument also supports the finalized observation needed for node settlement.
func (c PaymentChain) IsUsedTicket(ctx context.Context, hash ethcommon.Hash, block string) (bool, error) {
	var ref any = block
	if block == "finalized" {
		header, err := c.Contracts.RPC.header(ctx, "finalized")
		if err != nil {
			return false, err
		}
		if header.Hash == (ethcommon.Hash{}) || header.Number == nil {
			return false, errors.New("invalid finalized header")
		}
		ref = map[string]any{"blockHash": header.Hash, "requireCanonical": true}
	}
	address, err := c.Contracts.ResolveAt(ctx, ref, "ticketBroker")
	if err != nil {
		return false, err
	}
	broker, err := contracts.NewTicketBrokerCaller(address, c.Contracts.callerAt(ref))
	if err != nil {
		return false, err
	}
	return broker.UsedTickets(&bind.CallOpts{Context: ctx}, hash)
}

// FinalizedNonce pins account state to the same canonical finalized reference
// used for ticket-use settlement. A missing/malformed count is not evidence.
func (c PaymentChain) FinalizedNonce(ctx context.Context, address ethcommon.Address) (uint64, error) {
	header, err := c.Contracts.RPC.header(ctx, "finalized")
	if err != nil {
		return 0, err
	}
	if header.Hash == (ethcommon.Hash{}) || header.Number == nil || !(*big.Int)(header.Number).IsUint64() {
		return 0, errors.New("invalid finalized header")
	}
	var nonce *hexutil.Uint64
	ref := map[string]any{"blockHash": header.Hash, "requireCanonical": true}
	if err := c.Contracts.RPC.ethereum.Client().CallContext(ctx, &nonce, "eth_getTransactionCount", address, ref); err != nil {
		return 0, safeRPCError(err)
	}
	if nonce == nil {
		return 0, errors.New("invalid finalized transaction count")
	}
	return uint64(*nonce), nil
}

// InclusionReceipt retains node's canonical-block check without treating an
// unfinalized inclusion as settled. The ported manager only waits for inclusion.
func (c PaymentChain) InclusionReceipt(ctx context.Context, hash ethcommon.Hash) (*types.Receipt, error) {
	receipt, err := c.Contracts.RPC.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, err
	}
	header, err := c.Contracts.RPC.header(ctx, hexutil.EncodeBig(receipt.BlockNumber))
	if err != nil {
		return nil, err
	}
	if header.Hash != receipt.BlockHash {
		return nil, ethereum.NotFound
	}
	return receipt, nil
}

func (c PaymentChain) Redeem(ctx context.Context, redeemer *Key, chainID *big.Int, t RedeemTicket) (ethcommon.Hash, error) {
	tx, err := c.PrepareRedemption(ctx, redeemer, chainID, t)
	if err != nil {
		return ethcommon.Hash{}, err
	}
	return tx.Hash, c.Contracts.Broadcast(ctx, tx)
}
