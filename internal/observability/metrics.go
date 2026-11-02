// Package observability supplies process-local Prometheus registries and safe
// transport observations. Labels passed here must come from fixed operations or
// registered inventory, never request paths, credentials, or error messages.
package observability

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/felixge/httpsnoop"
	"github.com/livepeer/node/cmd/version"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Labels map[string]string

func (l Labels) With(key, value string) Labels {
	copy := make(Labels, len(l)+1)
	for k, v := range l {
		copy[k] = v
	}
	copy[key] = value
	return copy
}

type Identity struct{ App, Runner, Mode string }

func (i Identity) Labels() Labels {
	return Labels{"app": i.App, "runner_id": i.Runner, "mode": i.Mode}
}

// Metrics owns a registry; services and tests never share global collectors.
// Families are registered on first use, with a fixed label schema thereafter.
type Metrics struct {
	Registry   *prometheus.Registry
	Component  string
	mu         sync.Mutex
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
}

func New(component string) *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(), Component: component,
		counters: map[string]*prometheus.CounterVec{}, gauges: map[string]*prometheus.GaugeVec{}, histograms: map[string]*prometheus.HistogramVec{}}
	m.Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), collectors.NewBuildInfoCollector())
	m.Gauge("build_info", Labels{"version": version.Version, "revision": version.Commit}).Set(1)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func labelValues(labels Labels) (keys, values []string) {
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values = append(values, labels[key])
	}
	return
}

func metricHelp(name string) string { return strings.ReplaceAll(name, "_", " ") + "." }

func (m *Metrics) Add(name string, value float64, labels Labels) {
	if m == nil {
		return
	}
	m.counter(name, labels).Add(value)
}

func (m *Metrics) counter(name string, labels Labels) prometheus.Counter {
	keys, values := labelValues(labels)
	m.mu.Lock()
	v := m.counters[name]
	if v == nil {
		v = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "livepeer", Name: name, Help: metricHelp(name), ConstLabels: prometheus.Labels{"component": m.Component}}, keys)
		m.Registry.MustRegister(v)
		m.counters[name] = v
	}
	m.mu.Unlock()
	return v.WithLabelValues(values...)
}

func (m *Metrics) Gauge(name string, labels Labels) prometheus.Gauge {
	keys, values := labelValues(labels)
	m.mu.Lock()
	defer m.mu.Unlock()
	v := m.gauges[name]
	if v == nil {
		v = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "livepeer", Name: name, Help: metricHelp(name), ConstLabels: prometheus.Labels{"component": m.Component}}, keys)
		m.Registry.MustRegister(v)
		m.gauges[name] = v
	}
	return v.WithLabelValues(values...)
}

func (m *Metrics) Observe(name string, value float64, labels Labels) {
	if m == nil {
		return
	}
	keys, values := labelValues(labels)
	m.mu.Lock()
	v := m.histograms[name]
	if v == nil {
		buckets := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
		if name == "session_duration_seconds" || name == "app_call_duration_seconds" || name == "redemption_settlement_duration_seconds" {
			buckets = []float64{1, 5, 15, 30, 60, 300, 900, 3600, 10800}
		}
		v = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "livepeer", Name: name, Help: metricHelp(name), ConstLabels: prometheus.Labels{"component": m.Component}, Buckets: buckets}, keys)
		m.Registry.MustRegister(v)
		m.histograms[name] = v
	}
	m.mu.Unlock()
	v.WithLabelValues(values...).Observe(value)
}

type Emit func(name string, value float64, labels Labels)
type snapshotCollector struct {
	collect   func(Emit)
	component string
}

func (c snapshotCollector) Describe(chan<- *prometheus.Desc) {}
func (c snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	c.collect(func(name string, value float64, labels Labels) {
		keys, values := labelValues(labels)
		var constants prometheus.Labels
		if c.component != "" {
			constants = prometheus.Labels{"component": c.component}
		}
		kind := prometheus.GaugeValue
		if strings.HasSuffix(name, "_total") {
			kind = prometheus.CounterValue
		}
		ch <- prometheus.MustNewConstMetric(prometheus.NewDesc("livepeer_"+name, metricHelp(name), keys, constants), kind, value, values...)
	})
}

// Snapshot registers read-only collection. Callbacks must bound database reads
// and must not make network requests or mutate business state.
func (m *Metrics) Snapshot(collect func(Emit)) {
	m.Registry.MustRegister(snapshotCollector{collect, m.Component})
}
func (m *Metrics) Legacy(collect func(Emit)) { m.Registry.MustRegister(snapshotCollector{collect, ""}) }

func Bool(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
func ETH(value *big.Rat) float64 {
	if value == nil {
		return 0
	}
	x, _ := new(big.Rat).Quo(value, new(big.Rat).SetInt(big.NewInt(1e18))).Float64()
	return x
}

// ErrorKind deliberately discards remote and transport error text.
func ErrorKind(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection_refused"
	}
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		return "invalid_json"
	}
	var field *json.UnmarshalTypeError
	if errors.As(err, &field) {
		return "invalid_json_type"
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	return "error"
}

func (m *Metrics) RPC(method string, elapsed time.Duration, err error) {
	labels := Labels{"method": method}
	m.Add("rpc_requests_total", 1, labels.With("outcome", ErrorKind(err)))
	m.Observe("rpc_request_duration_seconds", elapsed.Seconds(), labels)
}

// RPCCall counts one eth_call, including each call inside a batch. RPC counts HTTP
// requests, so a batch is one request and many calls.
func (m *Metrics) RPCCall(contract, function string) {
	m.Add("rpc_contract_calls_total", 1, Labels{"contract": contract, "function": function})
}

// ChainViewRefresh records one chain view observation.
func (m *Metrics) ChainViewRefresh(outcome string, elapsed time.Duration) {
	m.Add("chain_view_refreshes_total", 1, Labels{"outcome": outcome})
	m.Observe("chain_view_refresh_duration_seconds", elapsed.Seconds(), nil)
}

// ChainViewRefreshHook records each observation and logs a change between working
// and failing, without repeating a failure at every interval. Refreshes run serially.
func (m *Metrics) ChainViewRefreshHook(logger *slog.Logger) func(string, time.Duration, error) {
	failing := false
	return func(outcome string, elapsed time.Duration, err error) {
		m.ChainViewRefresh(outcome, elapsed)
		if failing != (err != nil) {
			failing = err != nil
			logger.Info("chain view state changed", "event", "chain_view", "operation", "refresh", "outcome", outcome, "error_kind", ErrorKind(err))
		}
	}
}

// ChainViewBatchFallback counts reads run sequentially because batches are refused.
func (m *Metrics) ChainViewBatchFallback() {
	m.Add("chain_view_batch_fallbacks_total", 1, nil)
}

// ChainView reports a chain view's state at scrape time, from memory. Age is absent
// until the first observation, since nothing has been observed to have an age.
func (m *Metrics) ChainView(age func() (time.Duration, bool), tracked func() int) {
	m.Snapshot(func(emit Emit) {
		if elapsed, ok := age(); ok {
			emit("chain_view_age_seconds", elapsed.Seconds(), nil)
		}
		emit("chain_view_tracked_payers", float64(tracked()), nil)
	})
}

func (m *Metrics) Discovery(source int, elapsed time.Duration, err error) {
	labels := Labels{"source_index": strconv.Itoa(source)}
	m.Add("discovery_refreshes_total", 1, labels.With("outcome", ErrorKind(err)))
	m.Observe("discovery_refresh_duration_seconds", elapsed.Seconds(), labels)
}

type requestKey struct{}
type Request struct {
	metrics                 *Metrics
	Operation               string
	Identity                Identity
	App                     bool
	Protocol                string
	Outcome                 string
	status                  int
	started, headers        time.Time
	transfers               atomic.Pointer[transferCounters]
	readFailed, writeFailed atomic.Bool
	upstreamFailed          atomic.Bool
}

type transferCounters struct{ in, out prometheus.Counter }

func (r *Request) trackTransfers(input, output string) {
	labels := r.Identity.Labels()
	r.transfers.Store(&transferCounters{
		in:  r.metrics.counter("stream_transfer_bytes_total", labels.With("protocol", input).With("direction", "in")),
		out: r.metrics.counter("stream_transfer_bytes_total", labels.With("protocol", output).With("direction", "out")),
	})
}

func (r *Request) transferred(n int, incoming bool) {
	if counters := r.transfers.Load(); counters != nil && n > 0 {
		counter := counters.out
		if incoming {
			counter = counters.in
		}
		counter.Add(float64(n))
	}
}

// closeWriter is forwarded only when the wrapped transport supports half-close.
// ReverseProxy relies on it to drain the other direction after an upgrade.
type closeWriter interface{ CloseWrite() error }
type halfCloseBody struct {
	io.ReadWriteCloser
	closeWriter
}
type halfCloseConnection struct {
	net.Conn
	closeWriter
}

type upgradedBody struct {
	upstreamBody
	writer io.Writer
}

type upstreamBody struct {
	io.ReadCloser
	state *Request
}

func (b upstreamBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		b.state.upstreamFailed.Store(true)
	}
	return n, err
}

func (b upgradedBody) Write(p []byte) (int, error) {
	n, err := b.writer.Write(p)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		b.state.upstreamFailed.Store(true)
	}
	return n, err
}

// TrackResponse preserves upgraded bodies and observes failures after headers.
func TrackResponse(r *http.Request, response *http.Response) {
	if response == nil {
		return
	}
	if s := State(r); s != nil {
		if body, ok := response.Body.(io.ReadWriteCloser); ok && response.StatusCode == http.StatusSwitchingProtocols {
			response.Body = upgradedBody{upstreamBody{body, s}, body}
			if half, ok := body.(closeWriter); ok {
				response.Body = halfCloseBody{response.Body.(io.ReadWriteCloser), half}
			}
		} else {
			response.Body = upstreamBody{response.Body, s}
		}
	}
}

func State(r *http.Request) *Request {
	state, _ := r.Context().Value(requestKey{}).(*Request)
	return state
}
func Operation(r *http.Request, operation string) {
	if s := State(r); s != nil && operation != "" {
		s.Operation = operation
	}
}
func Outcome(r *http.Request, outcome string) {
	if s := State(r); s != nil {
		s.Outcome = outcome
	}
}

func App(r *http.Request, identity Identity) {
	s := State(r)
	if s == nil || s.App || identity.Runner == "" {
		return
	}
	s.App, s.Identity = true, identity
	s.metrics.Gauge("app_calls_in_flight", identity.Labels()).Inc()
	s.trackTransfers("http", s.Protocol)
	if !s.headers.IsZero() {
		s.metrics.Observe("app_response_header_duration_seconds", s.headers.Sub(s.started).Seconds(), identity.Labels().With("protocol", s.Protocol))
	}
}

func Stream(r *http.Request, identity Identity, protocol string) {
	if s := State(r); s != nil && !s.App && identity.Runner != "" {
		s.Identity, s.Protocol = identity, protocol
		s.trackTransfers(protocol, protocol)
	}
}

type reader struct {
	io.ReadCloser
	state *Request
}

func (r reader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.state.transferred(n, true)
	if err != nil && !errors.Is(err, io.EOF) {
		r.state.readFailed.Store(true)
	}
	return n, err
}

type connection struct {
	net.Conn
	state *Request
}

func (c connection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.state.transferred(n, true)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		c.state.readFailed.Store(true)
	}
	return n, err
}
func (c connection) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.state.transferred(n, false)
	if err != nil && !errors.Is(err, net.ErrClosed) {
		c.state.writeFailed.Store(true)
	}
	return n, err
}

func httpMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		return method
	}
	return "other"
}

// HTTP observes the entire handler lifetime, including upgraded connections.
// httpsnoop preserves the exact optional ResponseWriter interfaces.
func (m *Metrics) HTTP(w http.ResponseWriter, r *http.Request, serve http.HandlerFunc) {
	s := &Request{metrics: m, Operation: "unknown", Protocol: "http", started: time.Now()}
	r = r.WithContext(context.WithValue(r.Context(), requestKey{}, s))
	if r.Body != nil {
		r.Body = reader{r.Body, s}
	}
	m.Gauge("http_requests_in_flight", nil).Inc()
	mark := func(code int) {
		if s.status == 0 && (code >= 200 || code == 101) {
			s.status, s.headers = code, time.Now()
			if code == http.StatusSwitchingProtocols {
				s.Protocol = "websocket"
			} else if s.Protocol == "http" && strings.HasPrefix(w.Header().Get("Content-Type"), "text/event-stream") {
				s.Protocol = "sse"
			}
			if s.Identity.Runner != "" {
				input := "http"
				if s.Protocol == "trickle" || s.Protocol == "websocket" {
					input = s.Protocol
				}
				s.trackTransfers(input, s.Protocol)
			}
			if s.App {
				m.Observe("app_response_header_duration_seconds", s.headers.Sub(s.started).Seconds(), s.Identity.Labels().With("protocol", s.Protocol))
			}
		}
	}
	write := func(next func([]byte) (int, error), p []byte) (int, error) {
		n, err := next(p)
		s.transferred(n, false)
		if err != nil {
			s.writeFailed.Store(true)
		}
		return n, err
	}
	wrapped := httpsnoop.Wrap(w, httpsnoop.Hooks{
		WriteHeader: func(next httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
			return func(code int) { mark(code); next(code) }
		},
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(p []byte) (int, error) {
				mark(200)
				return write(next, p)
			}
		},
		ReadFrom: func(_ httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				mark(200)
				// Count completed writes during the copy, not the final aggregate.
				n, err := io.Copy(writeFunc(func(p []byte) (int, error) { return write(w.Write, p) }), src)
				if err != nil && !s.writeFailed.Load() {
					s.upstreamFailed.Store(true)
				}
				return n, err
			}
		},
		Flush: func(next httpsnoop.FlushFunc) httpsnoop.FlushFunc { return func() { mark(200); next() } },
		FlushError: func(next httpsnoop.FlushErrorFunc) httpsnoop.FlushErrorFunc {
			return func() error {
				mark(200)
				err := next()
				if err != nil {
					s.writeFailed.Store(true)
				}
				return err
			}
		},
		Hijack: func(next httpsnoop.HijackFunc) httpsnoop.HijackFunc {
			return func() (net.Conn, *bufio.ReadWriter, error) {
				conn, rw, err := next()
				if err != nil {
					s.writeFailed.Store(true)
					return conn, rw, err
				}
				mark(101)
				var observed net.Conn = connection{conn, s}
				if half, ok := conn.(closeWriter); ok {
					observed = halfCloseConnection{observed, half}
				}
				return observed, rw, nil
			}
		},
	})
	defer func() {
		panicked := recover()
		if panicked != nil && s.Outcome == "" {
			s.Outcome = "stream_error"
		}
		m.Gauge("http_requests_in_flight", nil).Dec()
		elapsed := time.Since(s.started).Seconds()
		if s.status == 0 && r.Context().Err() == nil && panicked == nil {
			mark(200)
		}
		labels := Labels{"operation": s.Operation, "method": httpMethod(r.Method)}
		m.Add("http_requests_total", 1, labels.With("status", strconv.Itoa(s.status)))
		m.Observe("http_request_duration_seconds", elapsed, labels)
		if s.Outcome != "session_stopped" && (r.Context().Err() != nil || (s.Outcome == "" && (s.readFailed.Load() || s.writeFailed.Load()) && !s.upstreamFailed.Load())) {
			s.Outcome = "client_cancelled"
		}
		if s.Outcome == "" {
			switch {
			case s.upstreamFailed.Load():
				s.Outcome = "stream_error"
			case s.status >= 500:
				s.Outcome = "error"
			case s.status == 402:
				s.Outcome = "payment_required"
			case s.status == 480:
				s.Outcome = "refresh_required"
			case s.status == 482:
				s.Outcome = "no_payment_needed"
			case s.status >= 400:
				s.Outcome = "rejected"
			default:
				s.Outcome = "success"
			}
		}
		if s.App {
			identity := s.Identity.Labels()
			m.Gauge("app_calls_in_flight", identity).Dec()
			appLabels := identity.With("protocol", s.Protocol)
			m.Add("app_calls_total", 1, appLabels.With("outcome", s.Outcome))
			m.Observe("app_call_duration_seconds", elapsed, appLabels)
		}
		if s.Identity.Runner != "" {
			labels := s.Identity.Labels().With("protocol", s.Protocol)
			if s.Outcome == "stream_error" || s.Outcome == "transport_error" {
				m.Add("stream_errors_total", 1, labels.With("outcome", s.Outcome))
			}
		}
		if panicked != nil {
			panic(panicked)
		}
	}()
	serve(wrapped, r)
}

// writeFunc deliberately has no ReaderFrom, so copies report each write.
type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }
