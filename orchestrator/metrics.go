package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
)

func runnerIdentity(id string, item *runner) observability.Identity {
	return observability.Identity{App: item.App, Runner: id, Mode: item.Mode}
}

func (r *Registry) identity(id, sid string) observability.Identity {
	r.mu.Lock()
	defer r.mu.Unlock()
	if item := r.runners[id]; item != nil {
		if sess := item.Sessions[sid]; sess != nil {
			return sess.identity
		}
		return runnerIdentity(id, item)
	}
	return observability.Identity{}
}

func (r *Registry) runnerEvent(id string, item *runner, event string) {
	r.metrics.Add("runner_events_total", 1, runnerIdentity(id, item).Labels().With("event", event))
	if r.logger != nil {
		r.logger.Info("runner state changed", "event", "runner_"+event, "operation", "runner_lifecycle", "runner_id", id, "app", item.App, "outcome", "success")
	}
}

func (r *Registry) collectMetrics(emit observability.Emit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, item := range r.runners {
		labels := runnerIdentity(id, item).Labels()
		ready := r.usable(item) && r.priceAvailable(item)
		emit("runner_registered", 1, labels)
		emit("runner_ready", observability.Bool(ready), labels)
		emit("runner_capacity", float64(item.Capacity), labels)
		available := 0
		if ready {
			available = max(item.Capacity-len(item.Sessions), 0)
		}
		emit("runner_capacity_available", float64(available), labels)
		if !item.Static {
			emit("runner_last_heartbeat_timestamp_seconds", float64(item.Last.Unix()), labels)
		}
		type group struct {
			identity observability.Identity
			pending  bool
		}
		counts := map[group]int{{runnerIdentity(id, item), false}: 0, {runnerIdentity(id, item), true}: 0}
		for _, sess := range item.Sessions {
			counts[group{sess.identity, sess.pending}]++
		}
		for key, count := range counts {
			state := "active"
			if key.pending {
				state = "pending"
			}
			emit("runner_sessions", float64(count), key.identity.Labels().With("state", state))
		}
	}
	if r.server != nil {
		r.server.collectMetrics(emit)
	}
}

func (s *Server) collectMetrics(emit observability.Emit) {
	s.mu.Lock()
	control, session := 0, 0
	for _, ch := range s.channels {
		if ch.sessionID == "" {
			control++
		} else {
			session++
		}
	}
	s.mu.Unlock()
	emit("trickle_channels", float64(control), observability.Labels{"kind": "control"})
	emit("trickle_channels", float64(session), observability.Labels{"kind": "session"})
	emit("trickle_buffered_bytes", float64(s.trickleServer.BufferedBytes()), nil)
}

type upstreamObserver struct {
	http.RoundTripper
	metrics  *observability.Metrics
	identity observability.Identity
	kind     string
}

func (t upstreamObserver) RoundTrip(r *http.Request) (*http.Response, error) {
	start := time.Now()
	response, err := t.RoundTripper.RoundTrip(r)
	observability.TrackResponse(r, response)
	labels := t.identity.Labels().With("destination", t.kind)
	outcome := observability.ErrorKind(err)
	if err == nil && response.StatusCode >= 500 {
		outcome = "http_5xx"
	} else if err == nil && response.StatusCode >= 400 {
		outcome = "http_4xx"
	}
	t.metrics.Add("upstream_requests_total", 1, labels.With("outcome", outcome))
	t.metrics.Observe("upstream_response_header_duration_seconds", time.Since(start).Seconds(), labels)
	return response, err
}

// paidReadiness tracks worker completion separately from per-ticket progress.
// A failed ticket cannot make a live worker appear dead.
type paidReadiness struct {
	mu                     sync.Mutex
	completed              time.Time
	workerCompleted        time.Time
	account                common.Address
	chain, active, storage bool
	snapshot               eth.ChainSnapshot
	interval               time.Duration
}

func (p *paidReadiness) ready(registry *Registry) bool {
	p.mu.Lock()
	limit := max(time.Minute, 3*p.interval)
	ok := p.chain && p.active && p.storage && !p.completed.IsZero() && time.Since(p.completed) <= limit && !p.workerCompleted.IsZero() && time.Since(p.workerCompleted) <= limit
	p.mu.Unlock()
	registry.mu.Lock()
	fresh := registry.rateUntil.IsZero() || time.Now().Before(registry.rateUntil)
	registry.mu.Unlock()
	return ok && fresh
}

func (p *paidReadiness) check(ctx context.Context, store *RedeemerDB, chain eth.PaymentChain, engine *PaymentEngine, metrics *observability.Metrics, logger *slog.Logger) {
	snapshot, chainErr := chain.Snapshot(ctx)
	active := false
	if chainErr == nil {
		active, chainErr = chain.IsActiveAt(ctx, engine.recipient, snapshot)
	}
	dbCtx, cancel := context.WithTimeout(ctx, time.Second)
	storageErr := store.checkStorage(dbCtx)
	cancel()
	p.mu.Lock()
	changed := p.completed.IsZero() || p.chain != (chainErr == nil) || p.active != active || p.storage != (storageErr == nil)
	p.completed, p.chain, p.active, p.storage = time.Now(), chainErr == nil, active, storageErr == nil
	if chainErr == nil {
		p.snapshot = snapshot
	}
	p.mu.Unlock()
	metrics.Gauge("dependency_healthy", observability.Labels{"dependency": "chain"}).Set(observability.Bool(chainErr == nil && active))
	metrics.Gauge("dependency_healthy", observability.Labels{"dependency": "payment_storage"}).Set(observability.Bool(storageErr == nil))
	if chainErr == nil {
		if snapshot.Block != nil {
			metrics.Gauge("chain_snapshot_block", nil).Set(float64(snapshot.Block.Int64()))
		}
		if snapshot.Round != nil {
			metrics.Gauge("chain_snapshot_round", nil).Set(float64(snapshot.Round.Int64()))
		}
		metrics.Gauge("chain_snapshot_last_success_timestamp_seconds", nil).Set(float64(time.Now().Unix()))
	}
	if balance, err := chain.Contracts.RPC.BalanceAt(ctx, p.account, nil); err == nil {
		metrics.Gauge("redeemer_gas_balance_eth", nil).Set(observability.ETH(new(big.Rat).SetInt(balance)))
	}
	if changed {
		logger.Info("payment dependency state changed", "event", "payment_dependencies", "operation", "check", "outcome", map[bool]string{true: "success", false: "unavailable"}[chainErr == nil && active && storageErr == nil])
	}
}

// A rolled-back empty write validates the payment table and SQLite's ability to
// enter a write transaction, without changing payment or redemption state.
func (s *RedeemerDB) checkStorage(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE winning_tickets SET enqueued_at=enqueued_at WHERE 0`); err != nil {
		return err
	}
	return tx.Rollback()
}

// paymentObservation preserves the public error while identifying its cause.
type paymentObservation struct {
	error
	outcome string
}

func (e paymentObservation) Unwrap() error { return e.error }

func paymentOutcome(err error) string {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return observability.ErrorKind(err)
	}
	if observed, ok := errors.AsType[paymentObservation](err); ok {
		return observed.outcome
	}
	switch {
	case errors.Is(err, errPaymentSessionCapacity), errors.Is(err, errPaymentTicketCapacity):
		return "capacity"
	case errors.Is(err, ErrFixedAlreadyCharged):
		return "already_charged"
	case errors.Is(err, ErrInsufficientBalance):
		return "insufficient_balance"
	case errors.Is(err, ErrInvalidPayment), errors.Is(err, pm.ErrPayerUnavailable):
		return "rejected"
	case errors.Is(err, ErrMissingChallenge):
		return "missing_challenge"
	default:
		return observability.ErrorKind(err)
	}
}

func paymentAppOutcome(err error) string {
	switch outcome := paymentOutcome(err); outcome {
	case "rejected", "missing_challenge", "insufficient_balance", "already_charged":
		return "rejected"
	case "cancelled":
		return "client_cancelled"
	case "capacity":
		return "capacity"
	default:
		return "error"
	}
}

func (e *PaymentEngine) paymentLabels(runner, manifest string) observability.Labels {
	if runner == "" {
		e.mu.Lock()
		if session := e.sessions[manifest]; session != nil {
			runner = session.runner
		}
		e.mu.Unlock()
	}
	if e.identity == nil {
		return nil
	}
	return e.identity(runner, manifest).Labels()
}

// observeDuration is deferred with its arguments taken at the start of an operation.
func (e *PaymentEngine) observeDuration(operation string, start time.Time, labels observability.Labels) {
	e.metrics.Observe("payment_operation_duration_seconds", time.Since(start).Seconds(), labels.With("operation", operation))
}

func (e *PaymentEngine) recordPayment(operation string, err error, labels observability.Labels) {
	outcome := paymentOutcome(err)
	e.metrics.Add("payment_operations_total", 1, labels.With("operation", operation).With("outcome", outcome))
	if e.store != nil && e.store.logger != nil && operation != "charge" {
		e.store.logger.Info("payment observed", "event", "payment", "operation", operation, "outcome", outcome, "app", labels["app"], "runner_id", labels["runner_id"])
	}
}
