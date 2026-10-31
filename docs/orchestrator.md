# Running an orchestrator

The orchestrator registers and advertises runners, reserves sessions, and
forwards HTTP, SSE, WebSocket, and [Trickle](../trickle/README.md) traffic.
It supports persistent sessions and single-shot requests, on-chain or off-chain.

Start with the [local example](../README.md#run-a-local-example). For deployment,
copy the [configuration example](../configs/orchestrator/config.example.toml),
set its paths and destinations, and run:

```sh
livepeer orchestrator --config /etc/livepeer/orchestrator.toml
```

## Register runners

Configure a bootstrap credential, a static runner file, or both.

Dynamic runners register through `POST /runners/heartbeat` using the bootstrap
credential. The response supplies a credential scoped to that runner for later
heartbeats and unregister requests. Set `BootstrapSecretFile` as described in
[configuration and secrets](configuration.md#credential-files). If you use only
dynamic runners, omit `RunnerConfig`.

Static runners load from `RunnerConfig`: `[[Runners]]` tables with Go field
names such as `RunnerURL`, `HealthURL`, and `HealthCode`, plus optional
`[Runners.GPU]` and `[Runners.PriceInfo]`. Follow the
[runner example](../configs/orchestrator/runners.example.toml); omit
`BootstrapSecretFile` for static-only operation. Health URLs are checked every
`HeartbeatInterval` (default five seconds). A runner with a health URL is unavailable
until its first successful check; three consecutive failures release its sessions.

`GET /discovery` lists this orchestrator's available runners; aggregate multiple
orchestrators with [discovery](discovery.md). Off-chain operation accepts quoted
prices but advertises free service. Enable payments below to charge clients.

## Allow local and private destinations

Use separate [destination grants](configuration.md#destination-grants) for local
or private connections:

| Setting | Permits private destinations for |
| --- | --- |
| `RunnerGrants` | Application requests to runners |
| `SessionProxyGrants` | Targets supplied when creating a session proxy |
| `HealthGrants` | Static runner health checks |

For a runner and health endpoint at `127.0.0.1:9000`, set both:

```toml
RunnerGrants = ["127.0.0.1"]
HealthGrants = ["127.0.0.1:9000"]
```

To allow a private subnet, use a CIDR such as `"192.168.1.0/24"` in each setting.

Outbound HTTPS uses system certificate trust. Ethereum RPC connections do not
require grants.

## Public URLs and TLS

`ServiceURL` is the public base URL advertised to clients, defaulting to
`http://<Listen>`. Set `RunnerServiceURL` if runners reach the listener through
a different URL such as Docker. That URL is used for heartbeat responses,
session callbacks, Trickle `internal_url`, and the orchestrator-to-runner (O2R)
control channel. Client discovery, session, proxy, and channel URLs continue
to use `ServiceURL`.

The application listener serves HTTP and defaults to loopback. Use explicit
IP:port addresses for `Listen` and `MetricsListen`, including IPv6 addresses
such as `[::1]:8935`. `Listen = "0.0.0.0:8935"` or `"[::]:8935"` binds all
interfaces. Terminate public HTTPS in a reverse proxy and set `ServiceURL` to
its HTTPS address. The separate metrics listener must remain on loopback; see
[probe addresses](configuration.md#local-storage-and-probes).

## Sessions and proxies

Each session keeps its reserved price and paid status; heartbeat and exchange-rate
updates affect new sessions. Releasing a session immediately frees capacity,
revokes its token, and removes its channels and proxies.
A runner stop gives in-flight HTTP, SSE, and WebSocket requests up to one second
to finish; all other releases cancel them immediately. This applies to both
persistent and single-shot sessions.

Enabling a runner's proxy creates an automatic proxy. Without a URL
template, generated URLs use `ServiceURL/run/{proxy}`. The earlier
`/proxy/{proxy}` path remains an alias for active proxies. A runner can also
create a session proxy through its callback route with a `target_url`.

`ProxyURLTemplate` accepts a hostname label or final path segment, for example
`https://{proxy}.apps.example` or `https://apps.example/proxy/{proxy}`. Configure
DNS, TLS, and forwarding to this listener for those URLs. Hostname routing uses
the actual Host header. Single-shot domain proxies require a runner ID that is
a lowercase DNS label of at most 63 characters.

## Enable payments

For paid operation, select an on-chain `Network` and supply:

- A redemption account and its password; see
  [Ethereum keystores](configuration.md#ethereum-keystores).
- `RPCURLFile` or `LIVEPEER_ORCHESTRATOR_RPC_URL`: RPC credential.
- `ChainID` and `Controller` for custom networks.
- `TicketFaceValue` and `TicketWinProb`: positive uint256 ticket parameters.
  The winning probability must be less than `2^256 - 1`.
- Exactly one conversion source: `WeiPerUSD` or `ETHUSDFeed` (Arbitrum supplies a feed).

The payment database has a [default path](configuration.md); `RedeemerDB` /
`--redeemer-db` overrides it.

Set `OrchestratorAddress` / `--orchestrator-address` to the registered public
address to keep the orchestrator key offline. The keystore account pays gas;
tickets credit the orchestrator.

Incomplete paid configuration fails startup. The orchestrator address must be active
on the configured chain. Runner prices use USD per hour for live work or USD per
fixed request. Hourly discovery prices are normalized to USD and wei per second.

`WeiPerUSD` sets a fixed conversion. Alternatively, set `ETHUSDFeed` to a
Chainlink-compatible ETH/USD feed on the payment chain. The feed is checked
every hour; `PriceMaxAge` must be positive and defaults to two hours. Startup
requires a valid observation. Stale rates suspend new sessions; existing
sessions retain their initial price.

See [transaction recovery](payment-recovery.md#orchestrator-transaction-recovery)
for timeouts, replacements and fee settings. Winning tickets wait until payment
parameters expire before redemption exposes their randomness on-chain. Only a
successful, canonical, finalized receipt settles their liability.

Challenges, balances, and replay guards live in memory; restart requires new
paid sessions. Winning tickets and transaction identities survive in SQLite.
See [payment recovery](payment-recovery.md) for backup and reconciliation.

Challenges and receipts read chain state from an in-memory
[chain view](configuration.md#chain-reads-for-payments), not from the RPC on each
request. A stale view refuses them, answering as a failed chain read does: 502 for
a challenge, 400 for a payment. Redemption always reads the chain directly.

## Resource limits

Paid `/readyz` returns 503 until payment dependencies and the redemption worker
are ready. See [metrics and operational events](metrics.md) for requirements,
app/runner measurements, queue monitoring, and scrape/query examples.

The registry accepts up to 256 runners and 10,000 sessions, with 25 generated
proxies per session. Control requests have 64 admission slots and a five-second
body deadline. Streaming requests have 256 separate slots.

The session channel quota is 1,024 entries, with 256 reserved for runner controls.
Trickle retains five segments per channel, each capped at 10 MB (10,000,000 bytes).
There is no global byte cap; size memory for active channels and segments. See
the [protocol reference](../trickle/README.md) for streaming behavior.
