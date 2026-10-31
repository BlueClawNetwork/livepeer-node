package signer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/felixge/httpsnoop"
	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/eth"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
)

const authTokenValidPeriod = 30 * time.Minute

type signedState struct {
	State []byte `json:"state"`
	Sig   []byte `json:"sig"`
}
type paymentRequest struct {
	State        signedState `json:"state"`
	Orchestrator []byte      `json:"orchestrator"`
	ManifestID   string
	App          string    `json:"app,omitempty"`
	Type         string    `json:"type"`
	MaxPrice     *maxPrice `json:"maxPrice,omitempty"`
}
type paymentResponse struct {
	Payment  string      `json:"payment"`
	SegCreds string      `json:"segCreds,omitempty"`
	State    signedState `json:"state"`
}

type Service struct {
	metrics           *observability.Metrics
	authClient        *http.Client
	authURL           *url.URL
	authPolicy        string
	authHeaders       Headers
	key               pm.TicketSigner
	mux               *http.ServeMux
	collector         *discovery.Collector[discoveredRunner]
	discoveryInterval time.Duration
	orchWebhook       *orchestratorWebhook
	paymentChain      pm.PayerChain
	expiryMargin      int64 // L1 blocks the payment chain's clock can be behind.
	payerPolicy       pm.PayerPolicy
	pricePolicy       *pricePolicy
	events            signedTicketSink
	slots             chan struct{}
}

// SetPaymentChain sets where funds and the L1 clock come from. A chain view's clock
// can be behind the orchestrator's, so the signer then asks for fresh parameters that
// much earlier, instead of signing ones the orchestrator has seen expire.
func (s *Service) SetPaymentChain(chain pm.PayerChain) {
	s.paymentChain, s.expiryMargin = chain, 0
	if view, ok := chain.(interface{ MaxL1BlocksBehind() int64 }); ok {
		s.expiryMargin = view.MaxL1BlocksBehind()
	}
}

func (s *Service) SetPayerPolicy(policy pm.PayerPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	s.payerPolicy = policy
	return nil
}

// SetDiscovery configures discovery sources. Private destinations, including
// redirect targets, require a discovery grant. Configure before starting workers
// or serving requests.
func (s *Service) SetDiscovery(orchestrators []*url.URL, grants ...string) error {
	policy, err := destination.New("signer-discovery", grants)
	if err != nil {
		return err
	}
	urls := make([]*url.URL, 0, len(orchestrators))
	for _, candidate := range orchestrators {
		if err := discovery.ValidateURL(candidate); err != nil {
			return err
		}
		parsed := candidate.Clone()
		parsed.RawPath = strings.TrimRight(parsed.EscapedPath(), "/") + "/discovery"
		parsed.Path, _ = url.PathUnescape(parsed.RawPath)
		urls = append(urls, parsed)
	}
	client := policy.Client()
	client.Timeout = 5 * time.Second
	s.collector.Close()
	s.collector = &discovery.Collector[discoveredRunner]{Client: client, MaxFailures: s.collector.MaxFailures, Observe: s.metrics.Discovery}
	if s.orchWebhook == nil {
		s.collector.SetSources(urls)
	} else if cached := s.orchWebhook.urls.Load(); cached != nil {
		s.collector.SetSources(*cached)
		s.collector.AcceptEmpty()
	}
	return nil
}

func validateDiscoveryPolling(interval time.Duration, maxFailedRefreshes int) error {
	if interval <= 0 {
		return errors.New("discovery-refresh-interval must be positive")
	}
	if maxFailedRefreshes < 0 {
		return errors.New("discovery-max-failed-refreshes must be nonnegative")
	}
	return nil
}

// SetDiscoveryPolling configures polling before serving requests or starting workers.
func (s *Service) SetDiscoveryPolling(interval time.Duration, maxFailedRefreshes int) error {
	if err := validateDiscoveryPolling(interval, maxFailedRefreshes); err != nil {
		return err
	}
	s.discoveryInterval, s.collector.MaxFailures = interval, max(1, maxFailedRefreshes)
	return nil
}

// RunDiscovery refreshes inventories until cancellation. When using a webhook,
// also run RunOrchestratorWebhook. Wait for both workers before Close.
func (s *Service) RunDiscovery(ctx context.Context) { s.collector.Run(ctx, s.discoveryInterval) }

func NewService(key *eth.Key) (*Service, error) {
	if key == nil {
		return nil, errors.New("signer key is required")
	}
	return newService(key), nil
}

func (s *Service) Close() error {
	s.collector.Close()
	if s.authClient != nil {
		s.authClient.CloseIdleConnections()
	}
	return nil
}

func newService(key *eth.Key) *Service {
	s := &Service{
		metrics:           observability.New("signer"),
		key:               key,
		mux:               http.NewServeMux(),
		payerPolicy:       pm.DefaultPayerPolicy(),
		slots:             make(chan struct{}, 64),
		collector:         &discovery.Collector[discoveredRunner]{MaxFailures: 6},
		discoveryInterval: 5 * time.Minute,
	}
	s.mux.HandleFunc("POST /sign-orchestrator-info", s.signInfo)
	s.mux.HandleFunc("POST /generate-live-payment", s.generate)
	s.mux.HandleFunc("GET /discover-orchestrators", s.discover)
	s.metrics.Snapshot(s.collectMetrics)
	s.metrics.Legacy(func(emit observability.Emit) { emit("signer_up", 1, nil) })
	return s
}
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.metrics.HTTP(w, r, s.serveHTTP)
}

func (s *Service) serveHTTP(w http.ResponseWriter, r *http.Request) {
	_, pattern := s.mux.Handler(r)
	observability.Operation(r, pattern)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	control := http.NewResponseController(w)
	_ = control.SetReadDeadline(time.Now().Add(5 * time.Second))
	deadline, _ := ctx.Deadline()
	_ = control.SetWriteDeadline(deadline)
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = control.SetWriteDeadline(time.Now())
		close(done)
	})
	defer func() {
		if !stop() {
			<-done // Do not change deadlines after the connection is reused.
		}
	}()
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		s.metrics.Add("http_admission_rejections_total", 1, observability.Labels{"pool": "control"})
		signerError(w, http.StatusServiceUnavailable, "signer busy")
		return
	}
	s.mux.ServeHTTP(w, r.WithContext(ctx))
}
func signerError(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status == http.StatusInternalServerError {
		reason = "Internal Server Error"
	} else if status == http.StatusServiceUnavailable {
		reason = "Service Unavailable Error"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": reason}})
}

func (s *Service) signInfo(w http.ResponseWriter, r *http.Request) {
	address := s.key.Address().Hex()
	sig, err := s.signMessage("orchestrator_info", []byte(address))
	if err != nil {
		slog.With("component", "signer", "event", "sign_payment", "operation", "sign_payment").ErrorContext(r.Context(), "signer orchestrator info signing failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
		signerError(w, 500, "signing failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"address": address, "signature": eth.HexSignature(sig)})
}

type discoveredGPU struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	VRAMMB int    `json:"vram_mb,omitempty"`
}

type discoveredRunner struct {
	ObservedAt        string         `json:"observed_at"`
	URL               string         `json:"url"`
	App               string         `json:"app"`
	Version           string         `json:"version,omitempty"`
	Metadata          string         `json:"metadata,omitempty"`
	GPU               *discoveredGPU `json:"gpu,omitempty"`
	Mode              string         `json:"mode"`
	Capacity          int            `json:"capacity"`
	CapacityUsed      int            `json:"capacity_used"`
	CapacityAvailable int            `json:"capacity_available"`
	PriceInfo         struct {
		Price    json.Number `json:"price"`
		PriceUSD json.Number `json:"price_usd,omitempty"`
		Currency string      `json:"currency"`
		Unit     string      `json:"unit"`
	} `json:"price_info"`
}

func (r discoveredRunner) DiscoveryIdentity() (string, string, string) {
	return r.URL, r.App, r.ObservedAt
}

type discoveredOrchestrator struct {
	Address  string             `json:"address"`
	Runners  []discoveredRunner `json:"runners"`
	LastSeen time.Time          `json:"last_seen"`
}

func (s *Service) discover(w http.ResponseWriter, r *http.Request) {
	if !s.collector.Initialized() {
		if s.orchWebhook != nil {
			s.orchWebhook.refresh(r.Context(), s.collector.Client.Transport, true)
		}
		s.collector.Refresh(r.Context(), true)
	}
	snapshots := s.collector.Snapshots()
	if len(snapshots) == 0 {
		signerError(w, http.StatusServiceUnavailable, "orchestrator discovery unavailable")
		return
	}
	appFilter := r.URL.Query()["app"]
	gpuFilter := r.URL.Query()["gpu"]
	inventories := []discovery.Entry[discoveredRunner]{}
	observations := map[string]time.Time{}
	for _, snapshot := range snapshots {
		inventories = append(inventories, snapshot.Entries...)
		for _, entry := range snapshot.Entries {
			if _, ok := observations[entry.Address]; !ok {
				observations[entry.Address] = snapshot.QueriedAt
			}
		}
	}
	result := make([]discoveredOrchestrator, 0)
	for _, entry := range discovery.Merge(inventories) {
		filtered := make([]discoveredRunner, 0, len(entry.Runners))
		for _, runner := range entry.Runners {
			if len(appFilter) > 0 && !slices.Contains(appFilter, runner.App) {
				continue
			}
			name := ""
			if runner.GPU != nil {
				name = strings.TrimSpace(runner.GPU.Name)
			}
			if len(gpuFilter) > 0 && !slices.Contains(gpuFilter, name) {
				continue
			}
			filtered = append(filtered, runner)
		}
		if len(filtered) > 0 {
			result = append(result, discoveredOrchestrator{Address: entry.Address, Runners: filtered, LastSeen: observations[entry.Address]})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

func randomStateID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// paymentFailure carries a client-safe message and HTTP status.
type paymentFailure struct {
	status int
	reason string
}

func (e paymentFailure) Error() string { return e.reason }
func invalid(reason string) error      { return paymentFailure{http.StatusBadRequest, reason} }

var (
	errPaymentService      = errors.New("payment dependency unavailable")
	errAuthorizationDenied = errors.New("payment authorization denied")
)

func serviceFailure(status int, reason string) error {
	return errors.Join(paymentFailure{status, reason}, errPaymentService)
}

func checkMaxPrice(req paymentRequest, price wire.PriceInfo) error {
	if req.MaxPrice == nil {
		return nil
	}
	ceiling, ok := new(big.Rat).SetString(req.MaxPrice.Price.String())
	if !ok || ceiling.Sign() <= 0 || strings.ToLower(strings.TrimSpace(req.MaxPrice.Currency)) != "wei" {
		return invalid("maxPrice requires a positive wei price")
	}
	unit := map[string]string{"live": "seconds", "fixed": "fixed"}[req.Type]
	if strings.ToLower(strings.TrimSpace(req.MaxPrice.Unit)) != unit {
		return invalid("maxPrice unit does not match payment type")
	}
	actual := new(big.Rat).SetFrac64(price.PricePerUnit, price.UnitsPerPrice)
	if actual.Cmp(ceiling) > 0 {
		return paymentFailure{481, "orchestrator price exceeds maxPrice"}
	}
	return nil
}

func (s *Service) generate(w http.ResponseWriter, r *http.Request) {
	paymentType, outcome, start := "unknown", "rejected", time.Now()
	defer func() {
		if outcome != "success" && errors.Is(r.Context().Err(), context.Canceled) {
			outcome = "client_cancelled"
		} else if outcome != "success" && errors.Is(r.Context().Err(), context.DeadlineExceeded) {
			outcome = "error"
		}
		s.metrics.Add("signer_payments_total", 1, observability.Labels{"type": paymentType, "outcome": outcome})
		s.metrics.Observe("signer_payment_duration_seconds", time.Since(start).Seconds(), observability.Labels{"type": paymentType})
	}()
	// MaxBytesReader needs net/http's original writer to close oversized requests.
	body, err := io.ReadAll(http.MaxBytesReader(httpsnoop.Unwrap(w), r.Body, 1<<20))
	if err != nil {
		signerError(w, 400, "payment request too large")
		return
	}
	var req paymentRequest
	if err := json.Unmarshal(body, &req); err != nil {
		signerError(w, 400, "invalid payment request")
		return
	}
	if req.Type != "live" && req.Type != "fixed" {
		signerError(w, 400, "payment type must be live or fixed")
		return
	}
	paymentType = req.Type
	info, err := wire.DecodeOrchestratorInfo(req.Orchestrator)
	if err != nil {
		signerError(w, 400, "invalid orchestrator Protobuf")
		return
	}
	// NB: Once go-livepeer is phased out, ensure OrchestratorInfo.Address matches info.TicketParams.Recipient
	address := ethcommon.BytesToAddress(info.TicketParams.Recipient)
	if address == (ethcommon.Address{}) || len(info.Auth.Token) == 0 || info.TicketParams.Expiration.CreationRound <= 0 || len(info.TicketParams.Expiration.CreationRoundBlockHash) != 32 || ethcommon.BytesToHash(info.TicketParams.Expiration.CreationRoundBlockHash) == (ethcommon.Hash{}) || len(info.TicketParams.Recipient) != 20 || info.Price.PricePerUnit <= 0 || info.Price.UnitsPerPrice <= 0 || len(info.TicketParams.RecipientRandHash) != 32 || info.Auth.SessionID == "" {
		signerError(w, 400, "incomplete or expired orchestrator payment params")
		return
	}
	authTokenExpireBuffer := 0.1
	refreshPoint := info.Auth.Expiration - int64(authTokenValidPeriod.Seconds()*authTokenExpireBuffer)
	if time.Now().After(time.Unix(refreshPoint, 0)) {
		outcome = "refresh_required"
		w.Header().Set("Livepeer-Orchestrator-URL", info.Transcoder)
		signerError(w, 480, "refresh expired orchestrator authentication")
		return
	}
	if req.ManifestID == "" {
		req.ManifestID = info.Auth.SessionID
	}
	if req.ManifestID != info.Auth.SessionID {
		signerError(w, 400, "manifest ID does not match payment params")
		return
	}
	if err := checkMaxPrice(req, info.Price); err != nil {
		if f, ok := errors.AsType[paymentFailure](err); ok {
			signerError(w, f.status, f.reason)
		} else {
			signerError(w, 400, "invalid maxPrice")
		}
		return
	}
	var accountingRate *big.Rat
	if s.pricePolicy != nil {
		var err error
		accountingRate, err = s.pricePolicy.checkWithRate(req.Type, info.Price)
		if err != nil {
			outcome = paymentOutcome(err)
			f, _ := errors.AsType[paymentFailure](err)
			signerError(w, f.status, f.reason)
			return
		}
	}
	var state paymentState
	oldSequence := int64(-1)
	if len(req.State.State) > 0 || len(req.State.Sig) > 0 {
		if !(pm.DefaultSigVerifier{}).Verify(s.key.Address(), req.State.State, req.State.Sig) {
			signerError(w, 400, "invalid state signature")
			return
		}
		if err := json.Unmarshal(req.State.State, &state); err != nil || state.StateID == "" || state.SequenceNumber > math.MaxInt64 {
			signerError(w, 400, "invalid payment state")
			return
		}
		if state.OrchestratorAddress != address || state.App != req.App || state.Type != req.Type || state.ManifestID != req.ManifestID {
			signerError(w, 400, "payment state scope mismatch")
			return
		}
		oldSequence = int64(state.SequenceNumber)
	} else {
		state = paymentState{StateID: randomStateID(), OrchestratorAddress: address, App: req.App, Type: req.Type, ManifestID: req.ManifestID, InitialPricePerUnit: info.Price.PricePerUnit, InitialPixelsPerUnit: info.Price.UnitsPerPrice}
	}
	if state.InitialPricePerUnit <= 0 || state.InitialPixelsPerUnit <= 0 || new(big.Rat).SetFrac64(info.Price.PricePerUnit, info.Price.UnitsPerPrice).Cmp(new(big.Rat).SetFrac64(state.InitialPricePerUnit, state.InitialPixelsPerUnit)) > 0 {
		signerError(w, 481, "orchestrator price exceeds initial session price")
		return
	}
	draft, err := s.preparePayment(r.Context(), req.Type, req.ManifestID, info, state, oldSequence, time.Now())
	if err != nil {
		outcome = paymentOutcome(err)
		switch {
		case errors.Is(err, pm.ErrRefreshRequired):
			w.Header().Set("Livepeer-Orchestrator-URL", info.Transcoder)
			signerError(w, 480, "refresh session for remote signer")
		case errors.Is(err, pm.ErrNoTickets):
			signerError(w, 482, "no tickets needed")
		default:
			if f, ok := errors.AsType[paymentFailure](err); ok {
				signerError(w, f.status, f.reason)
			} else {
				signerError(w, 500, "payment generation failed")
			}
		}
		return
	}
	draft.State.FeeWei, draft.State.FeeUSD = draft.Usage.Fee.FloatString(0), nil
	if accountingRate != nil && accountingRate.Sign() > 0 {
		value := new(big.Rat).Quo(draft.Usage.Fee, accountingRate).FloatString(18)
		draft.State.FeeUSD = new(strings.TrimSuffix(strings.TrimRight(value, "0"), "."))
	}
	if err := s.authorizePayment(r, req, info.Price, &draft.State); err != nil {
		outcome = paymentOutcome(err)
		if f, ok := errors.AsType[paymentFailure](err); ok {
			signerError(w, f.status, f.reason)
		} else {
			signerError(w, 500, "payment authorization failed")
		}
		return
	}
	outcome = "error"
	signed, err := draft.Sign(r.Context(), s.key, s.metrics)
	if err != nil {
		signerError(w, 500, "payment generation failed")
		return
	}
	stateBytes, err := json.Marshal(draft.State)
	if err != nil {
		slog.With("component", "signer", "event", "sign_payment", "operation", "sign_payment").ErrorContext(r.Context(), "signer payment state encoding failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
		signerError(w, 500, "payment state encoding failed")
		return
	}
	stateSig, err := s.signMessage("state", stateBytes)
	if err != nil {
		slog.With("component", "signer", "event", "sign_payment", "operation", "sign_payment").ErrorContext(r.Context(), "signer payment state signing failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
		signerError(w, 500, "payment state signing failed")
		return
	}
	response, err := json.Marshal(paymentResponse{Payment: signed.Payment, SegCreds: signed.SegCreds, State: signedState{State: stateBytes, Sig: stateSig}})
	if err != nil {
		slog.With("component", "signer", "event", "sign_payment", "operation", "sign_payment").ErrorContext(r.Context(), "signer payment response encoding failed", "outcome", "error", "error_kind", observability.ErrorKind(err))
		signerError(w, 500, "payment response encoding failed")
		return
	}
	if s.events != nil {
		event := newSignedTicketEvent(s.key.Address(), req, info, draft)
		if err := s.events.Enqueue(r.Context(), event); err != nil {
			if errors.Is(err, errEventTooLarge) {
				outcome = "rejected"
				slog.With("component", "signer", "event", "sign_payment", "operation", "sign_payment").WarnContext(r.Context(), "signer accounting event exceeds size limit; payment withheld", "outcome", "error", "error_kind", observability.ErrorKind(err))
				signerError(w, http.StatusRequestEntityTooLarge, "signing event exceeds size limit")
			} else {
				slog.With("component", "signer", "event", "sign_payment", "operation", "sign_payment").ErrorContext(r.Context(), "signer accounting event could not be persisted; payment withheld", "outbox_full", errors.Is(err, errOutboxFull), "outcome", "error", "error_kind", observability.ErrorKind(err))
				signerError(w, http.StatusServiceUnavailable, "signer accounting outbox unavailable or full")
			}
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(response)
	outcome = "success"
	s.metrics.Add("signer_tickets_total", float64(draft.Usage.NumTickets), observability.Labels{"type": req.Type})
}
