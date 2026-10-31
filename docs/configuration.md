# Configuration and secrets

Services accept TOML or JSON and reject unknown keys. Migration and recovery
commands read only their relevant settings. Use the
[orchestrator](../configs/orchestrator/config.example.toml),
[signer](../configs/signer/config.example.toml),
[discovery](../configs/discovery/config.example.toml), and
[chain](../configs/chain/config.example.toml) examples as starting points.

## Select configuration and storage

Precedence is **CLI > environment > config > default**; empty environment values
are ignored. Select a file with `--config FILE` or the component's `CONFIG`
environment variable. An explicit config can set `Network` and `DataDir`.
`--config=""` disables config discovery.

`Network` defaults to `arbitrum-one-mainnet` for chain, signer, and the
orchestrator's `redemptions` and `migrate` commands. The orchestrator service
defaults to `offchain`. Arbitrum supplies chain, Controller, and ETH/USD feed
defaults. Custom networks require explicit chain and Controller settings.
Signers and paid orchestrators also need a feed or fixed conversion rate.
Paid orchestrators must select an on-chain network. RPC chain IDs are checked.

Config and storage default to `~/.lpData/<network>/<component>`; accounts use
`~/.lpData/<network>/keystore`. Explicit `DataDir` values are used directly and
move accounts to `<DataDir>/keystore`, even if the value matches the default.
Database filenames default to `payments.sqlite` (orchestrator) and `events.sqlite`
(signer). Network names must be nonempty directory names other than `.` or `..`.

Without `--config`, commands load `<DataDir>/config.toml` if present. That file
cannot set `DataDir`; its `Network` must match the flag/environment/default.
A paid orchestrator using config discovery therefore needs
`--network arbitrum-one-mainnet`. Explicit configs must exist and determine the
network before default directories are calculated.

Relative input paths use the working directory for CLI/environment values and
the config file's directory for file values. Relative database paths use the
effective datadir; absolute paths stay unchanged. This includes account,
migration, and recovery commands.

## Names and environment variables

All component TOML keys use Go field names, such as `ServiceURL`, `RPCURLFile`,
and `KeystoreFile`. Command-line flags use kebab case. Environment variables use
the component prefix and underscores:

| Component | Prefix | Example |
| --- | --- | --- |
| Orchestrator | `LIVEPEER_ORCHESTRATOR_` | `LIVEPEER_ORCHESTRATOR_BOOTSTRAP_SECRET` |
| Discovery | `LIVEPEER_DISCOVERY_` | `LIVEPEER_DISCOVERY_NODES` |
| Signer | `LIVEPEER_SIGNER_` | `LIVEPEER_SIGNER_RPC_URL` |
| Chain | `LIVEPEER_CHAIN_` | `LIVEPEER_CHAIN_RPC_URL` |

Run `livepeer COMPONENT --help` for flags and defaults. Orchestrator, discovery,
and chain support `--print-config`:

```sh
livepeer orchestrator --config /etc/livepeer/orchestrator.toml --print-config
```

Output omits secrets, secret file paths, and the static runner file path.
Chain action inputs, transaction switches, `--output`, and `--print-config` are
CLI-only, as are orchestrator `--print-config` and redemption `--submit`.

## Destination grants

Grants allow local, private, or other restricted destinations; public addresses
need none. Permissions apply per purpose, including redirects.

All `*-grants` accept a literal IP, `IP:port` (`[IPv6]:port` for IPv6), or a CIDR
such as `192.168.1.0/24` or `fc00::/7`. A bare IP or CIDR permits any port.
Grants are checked against each resolved IP at connection time.

## Credential files

Supply RPC URLs, bootstrap credentials, keystore passwords, webhook URLs/headers,
and Kafka credentials through file options or direct environment variables.
Direct secret flags and TOML values are rejected. An environment secret cannot
be combined with its file option.

Credential files are read as exact bytes, including trailing newlines. For
example, create a bootstrap credential without a newline:

```sh
umask 077
openssl rand -hex 32 | tr -d '\n' > runner-bootstrap.secret
```

Set `BootstrapSecretFile` to its absolute path and use the same credential for
initial runner registration. Secret-manager mounts must preserve exact bytes;
URL files require a valid URL without a trailing newline.

## Ethereum keystores

Chain, signer, and paid orchestrators discover encrypted geth accounts in the
keystore directory described above. A sole account is selected automatically;
use `Account` / `--account` to select among accounts or `KeystoreFile` /
`--keystore-file` to bypass discovery.

Signing also requires `KeystorePasswordFile` / `--keystore-password-file` or an
environment password. For signer, these are
`LIVEPEER_SIGNER_KEYSTORE_PASSWORD_FILE` and `LIVEPEER_SIGNER_KEYSTORE_PASSWORD`.

Keystore and password files must be regular files readable only by their owner;
`0400` and `0600` are accepted. Secret-manager symlinks are accepted when their
target files meet these requirements. Password files follow the exact-byte
rules above; supply an empty file for a keystore without a password.

Signer and paid orchestrators require these credentials at startup; chain needs
them for submissions and local signing. Credential changes require a restart
or a new chain invocation.

The signer account is the payer, supplying the TicketBroker deposit and reserve.
The orchestrator's keystore account signs redemptions and pays gas. Ticket payments
credit `OrchestratorAddress`, which defaults to that account. The chain account
must match the keystore account when signing. Multiple orchestrator processes
may share a redemption key. Failed submissions are retried automatically.
See [transaction recovery](payment-recovery.md).

### Prepare a keystore

Create an account without RPC access; choose a password path in an existing directory:

```sh
umask 077
openssl rand -hex 32 | tr -d '\n' > /path/to/password
livepeer chain account create \
  --keystore-password-file /path/to/password
```

The command uses geth's standard scrypt settings, saves the keystore with `0600`
permissions, and prints its address (`--output json` for scripts).
`--keystore-file` selects a new path in an existing parent directory; existing
files are never overwritten. No account address is required.

## Chain reads for payments

Paid orchestrators and signers read chain state from an in-memory view, so RPC
load follows a timer rather than request volume. One loop per process reads the
latest block, then contract addresses, round, recipient status, and the funds of
payers in use, with every call pinned to that block. Redemption, receipts,
finality, and nonces always read the chain directly.

| Setting | Default | Effect |
| --- | --- | --- |
| `ChainRefreshInterval` / `--chain-refresh-interval` | `5s` | Time between refreshes. `0` disables the view; every request then reads the chain. |
| `ChainMaxAge` / `--chain-max-age` | `30s` | Refuse payments when the view is older than this. It must exceed the interval, and be at most `2m`. |

Past the maximum age, or before the first refresh, payments fail as they do when
a chain read fails and readiness fails with them. A failed refresh keeps the last
good view only until then.

Requests may therefore act on chain state as old as the interval, or the maximum
age while refreshes are failing, so a payer's new deposit can take a refresh to
appear. Ticket parameters last 40 L1 blocks, about eight minutes, so `ChainMaxAge`
is capped far below that. A signer asks for fresh parameters earlier by the L1
blocks its view can be behind, about one for every 12 seconds of `ChainMaxAge`, so
it doesn't sign parameters the orchestrator has already seen expire. Lowering either setting raises idle RPC use: each
refresh is two round trips of about seven calls, plus two per payer in use.

A payer's first request reads its funds inline at the view's block, sharing the
read with concurrent requests. At most 1,024 payers are kept, evicting the least
recently used, and payers idle for ten minutes stop being read. If the RPC refuses
JSON-RPC batches, reads run sequentially and
`livepeer_chain_view_batch_fallbacks_total` counts them.

## Local storage and probes

Missing storage directories are created with `0700` permissions; existing
directory permissions are preserved. Keep SQLite files on persistent storage,
and use separate database paths for separate instances.
For signer accounting, use an owner-only parent directory and preserve those
permissions on SQLite's WAL and SHM files during restore.
See [payment recovery](payment-recovery.md) for backup and restart procedures.

Health and metrics endpoints use separate loopback listeners:

| Component | Default application address | Default metrics address |
| --- | --- | --- |
| Orchestrator | `127.0.0.1:8935` | `127.0.0.1:8936` |
| Signer | `127.0.0.1:8937` | `127.0.0.1:8938` |
| Discovery | `127.0.0.1:8939` | `127.0.0.1:8940` |

```sh
curl -fsS http://127.0.0.1:8936/readyz
curl -fsS http://127.0.0.1:8938/readyz
curl -fsS http://127.0.0.1:8940/readyz
```

All metrics listeners serve `/healthz`, `/readyz`, and `/metrics`.
See [metrics and operational events](metrics.md) for readiness requirements,
scrape configuration, measurements, and query/alert examples.
