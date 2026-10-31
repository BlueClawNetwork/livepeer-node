package signer

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"math"
	"math/big"
	"slices"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
)

const paramsExpiryBuffer = int64(1)

// draftPayment fixes the payment and accounting inputs before authorization.
// Only State's authorization fields are updated by authorizePayment.
type draftPayment struct {
	batch   *pm.TicketBatch
	payment wire.Payment
	segment wire.SegData
	State   paymentState
	Usage   paymentUsage
}

type signedPayment struct {
	Payment, SegCreds string
}

type paymentUsage struct {
	Fee, Balance *big.Rat
	PreviousTime time.Time
	BillableSecs float64
	NumTickets   int
}

type paymentState struct {
	StateID              string
	PMSessionID          string
	LastUpdate           time.Time
	OrchestratorAddress  ethcommon.Address
	App                  string
	AuthExpiry           int64
	AuthPolicy           string
	AuthMaxPrice         string
	TicketNonce          uint32 `json:"SenderNonce"`
	Balance              string
	FeeWei               string
	FeeUSD               *string
	InitialPricePerUnit  int64
	InitialPixelsPerUnit int64
	Type                 string
	SequenceNumber       uint64
	AuthID               string
	ManifestID           string
}

func (s *Service) preparePayment(ctx context.Context, paymentType, manifest string, info wire.OrchestratorInfo, state paymentState, oldSequence int64, now time.Time) (draftPayment, error) {
	if s.key == nil || (paymentType != "live" && paymentType != "fixed") || manifest == "" || oldSequence < -1 || oldSequence == math.MaxInt64 || info.Price.PricePerUnit <= 0 || info.Price.UnitsPerPrice <= 0 {
		return draftPayment{}, invalid("invalid remote payment request")
	}
	now = now.UTC()
	previous := state.LastUpdate.UTC()
	if previous.IsZero() {
		previous = now
	}
	billableSecs := now.Sub(previous).Seconds()
	seconds := int64(1)
	if paymentType == "live" {
		if oldSequence < 0 {
			seconds = 10
			billableSecs = 10
		} else {
			seconds = max(int64(math.Ceil(now.Sub(state.LastUpdate).Seconds())), 1)
		}
		if seconds > 3600 {
			return draftPayment{}, invalid("payment interval exceeds one hour")
		}
	}
	fee := new(big.Rat).SetFrac(new(big.Int).Mul(big.NewInt(info.Price.PricePerUnit), big.NewInt(seconds)), big.NewInt(info.Price.UnitsPerPrice))
	balance := new(big.Rat)
	if state.Balance != "" {
		if _, ok := balance.SetString(state.Balance); !ok {
			return draftPayment{}, invalid("invalid balance in payment state")
		}
	}
	params := pm.TicketParams{Recipient: ethcommon.BytesToAddress(info.TicketParams.Recipient), FaceValue: new(big.Int).SetBytes(info.TicketParams.FaceValue), WinProb: new(big.Int).SetBytes(info.TicketParams.WinProb), RecipientRandHash: ethcommon.BytesToHash(info.TicketParams.RecipientRandHash), Seed: new(big.Int).SetBytes(info.TicketParams.Seed), ExpirationBlock: new(big.Int).SetBytes(info.TicketParams.ExpirationBlock), ExpirationParams: &pm.TicketExpirationParams{CreationRound: info.TicketParams.Expiration.CreationRound, CreationRoundBlockHash: ethcommon.BytesToHash(info.TicketParams.Expiration.CreationRoundBlockHash)}}
	if state.PMSessionID != params.RecipientRandHash.Hex() {
		state.TicketNonce = 0
		state.PMSessionID = params.RecipientRandHash.Hex()
	}
	if state.TicketNonce >= 500 {
		return draftPayment{}, pm.ErrRefreshRequired
	}
	// Precheck one ticket before generating the batch.
	// Observation/funding failures here are client errors.
	funds, err := s.observePayer(ctx, &params, "precheck")
	if err == nil {
		err = s.checkTicketParams(ctx, &params, 1, funds, "precheck")
	}
	if err != nil {
		if errors.Is(err, pm.ErrRefreshRequired) {
			return draftPayment{}, err
		}
		if _, ok := errors.AsType[paymentFailure](err); ok {
			return draftPayment{}, err
		}
		return draftPayment{}, errors.Join(invalid("payer funds unavailable"), err)
	}
	count, err := pm.RemoteBatchSize(params, fee, balance)
	if err != nil {
		if errors.Is(err, pm.ErrNoTickets) {
			return draftPayment{}, err
		}
		slog.With("component", "signer", "event", "payment", "operation", "payment").WarnContext(ctx, "signer ticket batch rejected", "outcome", "error", "error_kind", observability.ErrorKind(err))
		return draftPayment{}, invalid("invalid ticket batch")
	}
	// Recheck the full batch before authorization, on the precheck's observation:
	// only the ticket count differs. A funding failure here is an internal generation
	// failure; invalid exposure remains a client error.
	if err := s.checkTicketParams(ctx, &params, count, funds, "generation"); err != nil {
		return draftPayment{}, err
	}
	batch, remaining, err := pm.DraftRemoteBatch(params, s.key.Address(), state.TicketNonce, fee, balance)
	if err != nil {
		if !errors.Is(err, pm.ErrRefreshRequired) && !errors.Is(err, pm.ErrNoTickets) {
			slog.With("component", "signer", "event", "payment", "operation", "payment").ErrorContext(ctx, "signer ticket batch generation failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
		}
		return draftPayment{}, err
	}
	// Retain the original wire bytes, including integer encodings and auth token.
	wireParams := info.TicketParams
	wireParams.Recipient = slices.Clone(wireParams.Recipient)
	wireParams.FaceValue = slices.Clone(wireParams.FaceValue)
	wireParams.WinProb = slices.Clone(wireParams.WinProb)
	wireParams.RecipientRandHash = slices.Clone(wireParams.RecipientRandHash)
	wireParams.Seed = slices.Clone(wireParams.Seed)
	wireParams.ExpirationBlock = slices.Clone(wireParams.ExpirationBlock)
	wireParams.Expiration.CreationRoundBlockHash = slices.Clone(wireParams.Expiration.CreationRoundBlockHash)
	message := wire.Payment{TicketParams: wireParams, PayerAddress: s.key.Address().Bytes(), Expiration: wireParams.Expiration, ExpectedPrice: info.Price}
	segHash := crypto.Keccak256(nil)
	auth := info.Auth
	auth.Token = slices.Clone(auth.Token)
	segment := wire.SegData{ManifestID: []byte(manifest), Hash: segHash, Auth: auth}
	state.TicketNonce += uint32(count)
	state.Balance = remaining.RatString()
	state.LastUpdate = now
	state.SequenceNumber = uint64(oldSequence + 1)
	return draftPayment{batch: batch, payment: message, segment: segment, State: state,
		Usage: paymentUsage{Fee: fee, Balance: remaining, PreviousTime: previous, BillableSecs: billableSecs, NumTickets: count}}, nil
}

// Sign adds signatures to the exact payment prepared before authorization.
// It does not observe the clock or chain, or recalculate any payment values.
func (p draftPayment) Sign(ctx context.Context, signer pm.TicketSigner, metrics *observability.Metrics) (signedPayment, error) {
	message := p.payment
	message.PayerParams = make([]wire.TicketPayerParams, 0, len(p.batch.PayerParams))
	for _, ticket := range p.batch.Tickets() {
		sig, err := signer.SignMessage(ticket.Hash().Bytes())
		metrics.Add("signer_sign_operations_total", 1, observability.Labels{"kind": "ticket", "outcome": observability.ErrorKind(err)})
		if err != nil {
			slog.With("component", "signer", "event", "payment", "operation", "payment").ErrorContext(ctx, "signer ticket batch generation failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
			return signedPayment{}, err
		}
		message.PayerParams = append(message.PayerParams, wire.TicketPayerParams{TicketNonce: ticket.TicketNonce, Sig: sig})
	}
	flatten := append(slices.Clone(p.segment.ManifestID), make([]byte, 32)...)
	flatten = append(flatten, p.segment.Hash...)
	sig, err := signer.SignMessage(crypto.Keccak256(flatten))
	metrics.Add("signer_sign_operations_total", 1, observability.Labels{"kind": "segment", "outcome": observability.ErrorKind(err)})
	if err != nil {
		slog.With("component", "signer", "event", "payment", "operation", "payment").ErrorContext(ctx, "signer segment credential signing failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
		return signedPayment{}, err
	}
	segment := p.segment
	segment.Signature = sig
	return signedPayment{Payment: base64.StdEncoding.EncodeToString(wire.EncodePayment(message)), SegCreds: base64.StdEncoding.EncodeToString(wire.EncodeSegData(segment))}, nil
}

func (s *Service) observePayer(ctx context.Context, ticketParams *pm.TicketParams, phase string) (pm.PayerFunds, error) {
	if ticketParams == nil {
		return pm.PayerFunds{}, invalid("ticketParams is nil")
	}
	if ticketParams.ExpirationBlock.Int64() == 0 {
		return pm.PayerFunds{}, invalid("ticketParams expiration block is 0")
	}
	if s.paymentChain == nil {
		slog.With("component", "signer", "event", "payment", "operation", "payment").ErrorContext(ctx, "signer payer chain is not configured", "phase", phase)
		return pm.PayerFunds{}, errors.Join(pm.ErrPayerUnavailable, errPaymentService)
	}
	funds, err := s.paymentChain.PayerFunds(ctx, s.key.Address(), ticketParams.Recipient)
	if err != nil {
		slog.With("component", "signer", "event", "payment", "operation", "payment").ErrorContext(ctx, "signer payer chain observation failed", "phase", phase, "outcome", "error", "error_kind", observability.ErrorKind(err))
		return pm.PayerFunds{}, errors.Join(pm.ErrPayerUnavailable, errPaymentService)
	}
	if funds.Snapshot.Block == nil {
		slog.With("component", "signer", "event", "payment", "operation", "payment").ErrorContext(ctx, "signer payer chain observation has no L1 block", "phase", phase)
		return pm.PayerFunds{}, errors.Join(pm.ErrPayerUnavailable, errPaymentService)
	}
	s.observeFunds(funds, false)
	return funds, nil
}

// Adapted from go-livepeer/pm/sender.go's validateTicketParams, by Yondon Fu,
// Nico Vergauwen and Rafał Leszko. Supplied creation-round metadata is retained;
// refresh is governed by parameter expiry on the L1 clock, not the current round.
func (s *Service) checkTicketParams(ctx context.Context, ticketParams *pm.TicketParams, numTickets int, funds pm.PayerFunds, phase string) error {
	latestL1Block := funds.Snapshot.Block
	currentBuffer := new(big.Int).Sub(ticketParams.ExpirationBlock, latestL1Block).Int64()
	if currentBuffer <= paramsExpiryBuffer {
		return pm.ErrRefreshRequired
	}
	if err := s.payerPolicy.Check(*ticketParams, numTickets, funds); err != nil {
		slog.With("component", "signer", "event", "payment", "operation", "payment").WarnContext(ctx, "signer payer policy rejected ticket parameters", "phase", phase, "num_tickets", numTickets, "outcome", "error", "error_kind", observability.ErrorKind(err))
		if errors.Is(err, pm.ErrPayerUnavailable) {
			return err
		}
		return invalid("ticket parameters violate payer policy")
	}
	return nil
}
