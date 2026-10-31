# Metrics and operational events

Scrape `/metrics` on each component's existing loopback `MetricsListen`:

```yaml
scrape_configs:
  - job_name: livepeer-orchestrator
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:8936']
  - job_name: livepeer-signer
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:8938']
  - job_name: livepeer-discovery
    scrape_interval: 15s
    static_configs:
      - targets: ['127.0.0.1:8940']
```

Attach environment/deployment labels in your collector. Scrapes use memory and
local SQLite reads with a one-second deadline; they never call dependencies.
Failed collectors omit unavailable statistics and report
`livepeer_collector_healthy{collector="redemptions"}=0` or
`livepeer_signer_kafka_outbox_healthy=0`. Missing values do not mean zero work.

## Identity and counting

New Livepeer families carry `component`; legacy and Go/process collectors retain
their existing labels. App/runner labels use registered `app`, `runner_id`, and
`mode`; aggregate runners for app totals. Sessions and Trickle retain ownership.
Inventory exists before traffic; unregister/expiry removes it, retaining counters.

Counters reset on restart; durable queue gauges recover from SQLite. ETH values
are approximate operational measurements. Legacy aggregate runner/session, up,
and outbox names remain available.

Operations use route patterns or fixed names; unknown routes/methods use
`unknown`/`other`. Unregistered identities and request-supplied paths, URLs,
credentials, session IDs, error text, or signer app strings never create labels.
Discovery source indices refer to the current configured list order.

HTTP requests and app calls count once at completion. Reservations, upstream
attempts, and payment operations count independently; aliases share these paths.
Admission rejection for a known application counts a call without an upstream
attempt. Refresh counts once; charge excludes fixed-session and zero-unit no-ops.

App/payment outcomes follow causes independently of HTTP status. Funding/policy
rejection, capacity, challenges, refresh (480), no-payment-needed (482), client
cancellation, and intentional session termination are distinct from service
failures (`error`, `transport_error`, `stream_error`).

Final-header histograms exclude provisional headers and update immediately;
transfer bytes also update during open streams. Lifetime histograms wait for
completion. Failures after HTTP 200/101 remain visible in the terminal outcome;
status zero means no final response was observed before abort.

HTTP byte counts cover bodies; upgraded connection counts may include framing.
Uploads use `http`; responses use `http`/`sse`; Trickle and upgrades use their
protocol in both directions.
Application success means observed HTTP/transport success. Payload failures,
inference latency, FPS, and GPU use require runner telemetry.

Redemption transitions/errors follow committed changes, including asynchronous
callbacks and sibling recovery. No-op reconciliation/replay adds no observations;
reorg retries are not service failures, and inclusion is not final settlement.

## Metric families

Names below have the `livepeer_` prefix. `_total` denotes counters; durations
are histograms with `_bucket`, `_sum`, and `_count` samples. Other families are
gauges; timestamps use Unix seconds. Most event families appear on first use.

| Area | Families and dimensions |
| --- | --- |
| Runtime | `build_info{version,revision}`, standard `go_*`, `process_*`, and `go_build_info`. |
| Legacy | `runners`, `sessions`, `signer_up`, and `discovery_up` retain their existing labels. |
| HTTP | `http_requests_total{operation,method,status}`, `http_request_duration_seconds{operation,method}`, `http_requests_in_flight`, `http_admission_rejections_total{pool}`. Probe/scrape requests are excluded. |
| Apps | `app_calls_total{app,runner_id,mode,protocol,outcome}`, `app_call_duration_seconds`, `app_response_header_duration_seconds`, `app_calls_in_flight`. Histograms use identity/protocol; in-flight uses identity. |
| Upstreams | `upstream_requests_total{app,runner_id,mode,destination,outcome}`, `upstream_response_header_duration_seconds`. Destination is `runner` or `session_proxy`; HTTP failures use `http_4xx`/`http_5xx`. |
| Inventory | `runner_registered`, `runner_ready`, `runner_capacity`, `runner_capacity_available`, `runner_sessions{state="pending\|active"}`, `runner_last_heartbeat_timestamp_seconds`, all with app identity. Unavailable runners expose zero available capacity; heartbeat timestamps apply to dynamic runners. |
| Lifecycle | `runner_events_total{event}`, `runner_health_checks_total{outcome}`, `session_reservations_total{outcome}`, `session_ends_total{reason}`, `session_duration_seconds`, with app identity. Reservations include allocations pending payment; only activated sessions contribute lifetime observations. |
| Streams | `trickle_channels{kind="control\|session"}`, `trickle_buffered_bytes` for retained published segment bytes; `stream_transfer_bytes_total{app,runner_id,mode,protocol,direction}`, `stream_errors_total{app,runner_id,mode,protocol,outcome}`. Trickle requests are transport activity, not extra app invocations. |
| Payments | `payment_operations_total{operation,outcome}` for challenge/receive/refresh/charge; `payment_operation_duration_seconds{operation}` for challenge/receive/refresh; `payment_tickets_total{kind="accepted\|winning"}`, `payment_credit_eth_total`, `payment_charge_eth_total`, `payment_billed_seconds_total`, `payment_fixed_charges_total`, with app identity. Credit is accepted expected ticket value; charges are successfully debited service usage. |
| Signer | `signer_payments_total{type,outcome}`, `signer_payment_duration_seconds{type}`, `signer_tickets_total{type}`, `signer_sign_operations_total{kind,outcome}`. Signing kinds are ticket, segment, state, and orchestrator_info. Partial signing before a failed request remains visible. Successful payment/ticket totals follow required outbox persistence, even if response delivery is interrupted. |
| Authorization | `signer_auth_webhook_requests_total{status,outcome}`, `signer_auth_webhook_duration_seconds`, `signer_auth_cache_hits_total`. Status zero means no webhook response; outcomes distinguish success, denied, and error. |
| Outbox | Existing `signer_kafka_pending_events`, `signer_kafka_pending_bytes`, `signer_kafka_oldest_event_age_seconds`, `signer_kafka_outbox_healthy`, `signer_kafka_publish_errors_total`, `signer_kafka_enqueue_errors_total`, and `signer_kafka_storage_errors_total`. New `signer_kafka_delivered_events_total` and `signer_kafka_last_delivery_timestamp_seconds` follow successful local acknowledgement; delivery remains at least once. |
| Redemption queue | `redemption_queue_tickets{state}`, `redemption_oldest_ticket_timestamp_seconds{state}`: safe parameter-expiry `waiting`, clock-eligible `eligible`, `in_flight`, `awaiting_finality`, or `unknown` without a fresh clock observation. Each ticket counts once, regardless of replacements. |
| Transactions | `redemption_unresolved_transactions{phase}`, `redemption_oldest_transaction_timestamp_seconds{phase}` include historical retries/replacements until the identity is confirmed, reverted, or superseded, or its ticket is settled (`confirmed`/`used`). Ticket expiry or invalidation alone does not remove an unresolved identity. `sending` appears as `broadcast`. |
| Redemption activity | `redemption_transitions_total{event}`, `redemption_broadcasts_total{outcome}`, `redemption_errors_total{error_kind}`, `redemption_settlements_total`, `redemption_settled_eth_total`, `redemption_settlement_duration_seconds`, `redemption_last_settlement_timestamp_seconds`, `redemption_last_success_timestamp_seconds`, `redemption_worker_last_completed_timestamp_seconds`. Settlement counts once per ticket, including `used` → `confirmed` upgrades. |
| Chain/pricing | `rpc_requests_total{method,outcome}`, `rpc_request_duration_seconds{method}` count HTTP round trips, including direct contract calls and JSON-RPC errors over HTTP 200; a JSON-RPC batch counts once with method `batch`. `rpc_contract_calls_total{contract,function}` counts each `eth_call`, including those inside a batch; functions outside a fixed table are `unknown`. `chain_snapshot_block`, `chain_snapshot_round`, `chain_snapshot_last_success_timestamp_seconds`, `price_feed_updates_total{outcome}`, `price_feed_valid_until_timestamp_seconds`, `dependency_healthy{dependency}`, `signer_deposit_eth`, `signer_reserve_eth`, `redeemer_gas_balance_eth`. Block uses the protocol L1 clock; the success timestamp is when the block was read; signer reserve is total reserve observed by readiness. |
| Chain view | `chain_view_age_seconds`, absent before the first refresh; `chain_view_tracked_payers`; `chain_view_refreshes_total{outcome}`, where `partial` left out payers whose reads failed; `chain_view_refresh_duration_seconds`; `chain_view_batch_fallbacks_total` for reads run sequentially because the RPC refuses batches. Absent when `ChainRefreshInterval` is 0. |
| Discovery | `discovery_refreshes_total{source_index,outcome}`, `discovery_refresh_duration_seconds{source_index}`, `discovery_source_state{source_index,state}`, `discovery_source_failures{source_index}`, `discovery_cached_runners{source_index}`, `discovery_last_success_timestamp_seconds{source_index}`. States distinguish healthy, retained, uninitialized, and evicted sources. |

Empty queues have zero counts/oldest timestamps. Fixed exchange rates have a
validity timestamp of zero. Chain/fund gauges retain the last observation;
check snapshot freshness and dependency health alongside them.

## Queries and starting alerts

Calls per second and app service-failure ratio:

```promql
sum by (app, runner_id) (rate(livepeer_app_calls_total[5m]))

sum by (app) (rate(livepeer_app_calls_total{outcome=~"error|transport_error|stream_error"}[5m]))
/
clamp_min(sum by (app) (rate(livepeer_app_calls_total[5m])), 0.001)
```

Start with a warning above 5% for ten minutes, gated on meaningful traffic.
Client cancellations and payment challenges are excluded from the numerator.

App p95 final-header latency and available capacity:

```promql
histogram_quantile(0.95,
  sum by (app, le) (rate(livepeer_app_response_header_duration_seconds_bucket[5m])))

sum by (app) (livepeer_runner_capacity_available)
```

Choose latency thresholds per workload. Warn on zero capacity together with
increasing reservation/admission rejections for five minutes; idle apps need
not alert solely for zero capacity.

Payment charges and accepted/winning tickets per second:

```promql
sum by (app, runner_id) (rate(livepeer_payment_charge_eth_total[5m]))

sum by (app, kind) (rate(livepeer_payment_tickets_total[5m]))
```

Eligible queue age and unresolved transaction age:

```promql
(time() - livepeer_redemption_oldest_ticket_timestamp_seconds{state="eligible"}) > 600
and on (job, instance, state)
livepeer_redemption_queue_tickets{state="eligible"} > 0

(time() - livepeer_redemption_oldest_transaction_timestamp_seconds) > 900
and on (job, instance, phase)
livepeer_redemption_unresolved_transactions > 0
```

Use a ten-minute persistence window for eligible backlog. Parameter-expiry
waiting does not qualify. Tune transaction thresholds above
`RedeemerTxTimeout × (1 + RedeemerMaxTxReplacements)` plus expected finality time.
An error-free pass updates `redemption_last_success_timestamp_seconds`; it is
not proof of settlement progress. Compare worker/pass freshness with queue age,
transaction phase, and `redemption_last_settlement_timestamp_seconds`.

Other useful starting conditions:

- Failed collectors/dependencies or stale worker completion for one minute,
  allowing for the configured heartbeat cadence.
- `livepeer_chain_view_age_seconds` above about half of `ChainMaxAge`, before
  payments start being refused, or any `chain_view_batch_fallbacks_total` growth,
  which multiplies refresh round trips by the number of payers.
- Positive feed validity timestamp earlier than `time()` when paid traffic is
  expected; fixed-rate deployments use zero.
- Outbox age above five minutes with pending events, or publish failures with
  growing pending bytes. Storage failure/full capacity is more urgent because
  it withholds payment.
- Zero redeemer gas balance or low signer deposit/reserve. Choose funding
  thresholds from actual redemption fees and payment exposure.
- Discovery sources retained/evicted beyond the expected refresh interval.
  Successful empty inventories remain valid observations.

## Readiness, migration, and logs

`/healthz` checks liveness. Paid orchestrator `/readyz` requires writable payment
storage, chain access, an active recipient, valid pricing, and worker completion.
Checks run at heartbeat cadence even with empty queues. Dependency observations
and worker completion expire after `max(60 seconds, 3 × HeartbeatInterval)`;
queue stalls and individual ticket errors remain separate signals.
With a [chain view](configuration.md#chain-reads-for-payments), chain access means
a view within `ChainMaxAge`, which readiness checks on every request; only the
redeemer's gas balance is read from the RPC there.
Off-chain orchestrator/discovery readiness reports listener health.
Signer readiness requires chain access, positive deposit/total reserve, no
withdrawal through the next round, fresh pricing, and healthy enabled outbox
storage with capacity. Kafka connectivity alone is not required; recipient
claimable reserve is checked when generating payments.

Migration 003 adds enqueue time. Existing rows receive migration time, so their
ages are lower bounds. New timestamps survive restarts. Rolling back 003 drops
only enqueue times; another upgrade restarts those historical measurements.
Follow [payment recovery](payment-recovery.md) before schema changes.

Operational logs retain their encoding and use fixed component/event/operation,
outcome, and error-kind fields, with app/runner/session/transaction correlation
where relevant. Remote, key, and storage error text is replaced with safe
classifications. Raw URLs, bodies, headers, credentials, signatures, randomness,
and signed transaction bytes are excluded. Routine segment progress is debug
level.
