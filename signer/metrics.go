package signer

import (
	"context"
	"errors"
	"math/big"

	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
)

// Wire statuses are compatibility responses, not failure classifications: a
// precheck RPC outage is HTTP 400, and an authorization denial can be HTTP 5xx.
func paymentOutcome(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "client_cancelled"
	case errors.Is(err, errPaymentService):
		return "error"
	case errors.Is(err, errAuthorizationDenied):
		return "rejected"
	case errors.Is(err, pm.ErrRefreshRequired):
		return "refresh_required"
	case errors.Is(err, pm.ErrNoTickets):
		return "no_payment_needed"
	case errors.Is(err, pm.ErrPayerUnavailable):
		return "rejected"
	}
	if _, ok := errors.AsType[paymentFailure](err); ok {
		return "rejected"
	}
	return "error"
}

func (s *Service) collectMetrics(emit observability.Emit) {
	s.collector.CollectMetrics(emit)
	if s.pricePolicy != nil {
		s.pricePolicy.mu.RLock()
		until := s.pricePolicy.validUntil
		s.pricePolicy.mu.RUnlock()
		value := 0.0
		if !until.IsZero() {
			value = float64(until.Unix())
		}
		emit("price_feed_valid_until_timestamp_seconds", value, nil)
		emit("dependency_healthy", observability.Bool(s.pricePolicy.ready()), observability.Labels{"dependency": "price_feed"})
	}
}

func (s *Service) signMessage(kind string, data []byte) ([]byte, error) {
	sig, err := s.key.SignMessage(data)
	s.metrics.Add("signer_sign_operations_total", 1, observability.Labels{"kind": kind, "outcome": observability.ErrorKind(err)})
	return sig, err
}

func (s *Service) observeFunds(funds pm.PayerFunds, total bool) {
	if funds.Snapshot.Block != nil && funds.Snapshot.Round != nil {
		s.metrics.Gauge("chain_snapshot_last_success_timestamp_seconds", nil).Set(float64(funds.Snapshot.ObservedAt().Unix()))
	}
	if funds.Snapshot.Block != nil {
		s.metrics.Gauge("chain_snapshot_block", nil).Set(float64(funds.Snapshot.Block.Int64()))
	}
	if funds.Snapshot.Round != nil {
		s.metrics.Gauge("chain_snapshot_round", nil).Set(float64(funds.Snapshot.Round.Int64()))
	}
	if funds.Deposit != nil {
		s.metrics.Gauge("signer_deposit_eth", nil).Set(observability.ETH(new(big.Rat).SetInt(funds.Deposit)))
	}
	if total && funds.Reserve != nil {
		s.metrics.Gauge("signer_reserve_eth", nil).Set(observability.ETH(new(big.Rat).SetInt(funds.Reserve)))
	}
}
