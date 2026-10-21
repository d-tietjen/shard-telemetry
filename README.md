# ShardTelemetry

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

ShardTelemetry is a Rust observability storage engine for logs, traces, and
metrics. It assigns each durable shard to a single writer, stores
signal-specific records behind a checksummed STEL envelope, and exposes
indexed query and ingestion interfaces for standalone and embedded
deployments.

## Project status

This is a pre-release project. The package is versioned 0.3.0; published
releases use the `shard-telemetry` crate name. Storage formats, network
protocol details, and operational defaults may change before a stable release.
Evaluate it against a representative workload and follow the compatibility
documents before relying on it for production data.

The project is distributed under Apache-2.0. The security policy, release
process, and contributor expectations are public and linked below.

## What it provides

- Native STEL ingestion and query over TCP, with checksummed envelopes and
  durable acknowledgements.
- Bounded authenticated native status for actual local WAL and applied-index
  frontiers, with source, retention, scoped-loss and HA coverage explicitly unknown.
- OTLP/gRPC and OTLP/HTTP ingestion for logs, traces, and metrics.
- Signal-aware storage, immutable query-index artifacts, and a bounded local
  cache for object-tier reads.
- A local EmbeddedTelemetryRuntime for applications that need a
  lifecycle-owned, durable node-local telemetry store.
- A store-and-forward UpstreamOffloader for forwarding durable embedded
  envelopes to a central ShardTelemetry service.
- Explicit compatibility boundaries for Loki, Prometheus/PromQL, Tempo/TraceQL,
  and ClickHouse integrations.

Unsupported protocol and query features are rejected explicitly; consult the
compatibility documents instead of assuming API parity with another system.

Attached hosts with a stream write fence must implement
`TelemetryAppendGate::append_leader_epoch` using trusted current host authority.
Every ingestion path supplies that token at its final engine submission; native
client envelopes cannot override it. The engine checks the token before WAL
reservation. Existing gates default to no token for unfenced compatibility,
which cannot authorize a fenced engine. This handoff does not establish HA
query ownership, indexed quorum, source coverage or retained-query completeness.

Hosts that recover an `EngineHandle` can attach it with
`TelemetryEngineAttachment` and `DurableTelemetryStore::attach_with_engine_handle`.
The store retains the complete engine owner, including recovered directory
ownership, through ingestion and query work. The existing
`TelemetryHostAttachment` API continues to accept `Arc<StreamEngine>` and uses
the same attachment implementation. Hosts remain responsible for startup
deadlines, sink installation, write fencing and readiness.

`TelemetryRecoveredCatalogs::recover` provides a separate bounded catalog
preparation step on Linux/Android. A single `TelemetryRecoveryBudget` keeps the
original absolute deadline and cumulative limits for file/path bytes,
directory advances (including dot entries, retries and EOF), JSON object/value
counts, decoded strings, declared retained state and work. Regular-file reads
use nofollow/nonblocking opens and explicit bounded attempts; buffer and path
capacities are checked after allocation. A tree-free JSON census precedes typed
decoding, with escaped-string scratch accounted separately. Refusal errors can
retain actual buffers and physical errors without exposing buffer contents in
Debug output. These are input and declared-state bounds, not process RSS or a
hard limit on filesystem syscall duration.

The recovered owner holds the real delete/retry catalogs and releases its
exclusive directory lease last. Bounded receipt migration reconciles retained
v1 input even after partial v2 publication, preserves identical retries and
rejects conflicting identities. Expired v1 entries need no fabricated v2 file;
stale temporary files are safely rewritten on restart. Existing constructors
retain their prior behavior. Catalog names must remain stable and writers must
cooperate with the lease: separate rename/remove/reopen operations do not form
a kernel-atomic namespace fence. This stage does not open a complete owned
store or qualify sink/object recovery, worker construction or serving readiness.

`TelemetryAppendPool::build_until` is a separate actual worker producer. It
requires an explicit worker count and requested stack size, shares the caller's
`TelemetryRecoveryBudget`, and registers every actual Rayon worker with a
host-provided `TelemetryTaskRegistry` using the same saved expiry. Each worker
keeps the complete engine handle until its physical loop exits. The host must
retain entered thread handles and their actual outcomes even when registration
returns a late error. Drop the pool before joining the registry; a cooperative
stop flag alone cannot replace Rayon's termination/physical join sequence.
Partial startup and late registration failures keep entered work in that
registry. A successfully constructed pool refused at its final clock check is
retained in the error until dropped. Worker count, requested stacks, names and
known callback state are admitted; private Rayon scheduler allocations and OS
stack rounding are not measured allocator/RSS bounds. This does not yet connect
bounded journal/object recovery and catalogs into a complete owned attachment.

The internal consuming [log-index publication producer](src/stripe/BOUNDED-PUBLICATION.md)
installs normal compressed query frames and their watermark using the original
native resume work and deadline. It retains real input, child results and
partial query state on refusal. The separate internal
[consuming log transaction](src/sink/BOUNDED-APPLY.md) joins genuine anchored
journal commit, query-index commit and publication in the original shared
application-checkpoint/cache owners. The actual
[owned log factory](src/sink/BOUNDED-OWNED-LOG.md) connects that transaction to
the strict sink trait and host-registered workers for recovered single-log
commands. The opt-in `OwnedLogSinkFactory::new_with_replay_until` accepts an
explicit complete configured log-partition scope and finite replay limits. Its
first genuine native checkpoint callback validates the whole cross-shard journal
chain and rebuilds the actual query/application state on the registered workers
without rewriting journal bytes. Unsupported transaction shapes, conflicting
edges, incomplete scope, expiry or partial failure keep the factory terminal;
original rows and physical outcomes remain retained. The existing `new_until`
continues to reject nonempty journals. Local replay grants no current control,
HA query ownership or source-completeness proof. Ordinary live validation,
serving, other signal/object modes, private product and host integration remain
gates. Empty engine recovery does not invoke a checkpoint callback: an opt-in
factory remains quarantined until a genuine configured-partition checkpoint
turn completes its actual adoption. The explicit
`AnchoredSinkJournal::open_application_until` producer creates `STELAPP2` files
for actual global application edges distributed over physical shards. The
writer borrows the held global application-checkpoint map and original native
work, stores the unchanged native expected/next and shard coordinates, and
allows only local sparse nonregression after the global edge matched. Reopen
still checks the complete cross-anchor chain before any index mutation.
`open_until` retains the `STELSNK1` domain and its original exact local placement
comparison. Each opener rejects the other format; mixed-format sources are
ineligible, and application-format factories require explicit replay opt-in.
No existing journal is relabeled, rewritten or silently migrated.

## Related projects

ShardTelemetry is standalone; it does not declare a runtime dependency on the
following related projects.

- [shard-kv](https://github.com/d-tietjen/shard-kv) is a separate
  cache-oriented key-value project. It is a useful companion where a workload
  needs local cache state as well as durable telemetry storage.
- [fast-telemetry](https://crates.io/crates/fast-telemetry) is a separate Rust
  instrumentation library for hot-path counters, gauges, histograms,
  distributions, and spans. The optional `fast-telemetry` feature adds a
  direct metric-snapshot bridge into the embedded runtime; recording remains on
  fast-telemetry's original hot path.

Each project has its own release process, APIs, and support boundary.

## Build and verify

The pinned Rust toolchain is declared in rust-toolchain.toml. From a checkout:

~~~text
cargo build --release --locked --bin shard-telemetry-server
cargo test --workspace --all-targets --all-features --locked
cargo run --release --bin shard-telemetry-server -- --help
~~~

For the complete local release-quality check, including documentation and
supply-chain checks, run:

~~~text
scripts/release-gate.sh
~~~

## Deployment model

The standalone server owns the canonical object catalog and global archive for
a deployment. An application can either send data remotely to that service or
use EmbeddedTelemetryRuntime locally and forward the durable local WAL with
UpstreamOffloader. An embedded node must not publish objects directly into the
central service's catalog prefix.

Production deployments require an authenticated service configuration and a
durable object backend. Keep the native listener on loopback or behind
transport encryption, use a dedicated object-store prefix, and follow the
object-storage and recovery guidance in SECURITY.md and
TIERED_STORAGE_ARCHITECTURE.md.

## Bounded embedded recent history

An embedded runtime can retain an exact recent window for local decisions while
the central telemetry service continues to receive the normal durable stream.
Enable the crate's `fast-telemetry` feature for the direct snapshot bridge. The
local policy has independent RAM, SSD, and time bounds:

~~~rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry::{Runtime, RuntimeConfig};
use shard_telemetry::{
    EmbeddedEvictionPolicy, EmbeddedTelemetryConfig, EmbeddedTelemetryRuntime,
    FastTelemetryConfig, FastTelemetryExporter,
};

let embedded = Arc::new(EmbeddedTelemetryRuntime::open(
    EmbeddedTelemetryConfig::bounded(
        "/var/lib/my-app/telemetry",
        Duration::from_secs(15 * 60),
        EmbeddedEvictionPolicy::Delete,
    )
    .with_storage_budgets(256 * 1024 * 1024, 8 * 1024 * 1024 * 1024),
)?);
let metrics = Runtime::new(RuntimeConfig::default());
let exporter = embedded.attach(|| FastTelemetryExporter::new(
    metrics,
    Arc::clone(&embedded),
    FastTelemetryConfig::new("device", "my-app").with_max_series(10_000),
))?;
embedded.mark_ready()?;
let maintenance = embedded.spawn_maintenance(Duration::from_secs(60))?;
~~~

Drive `exporter.export_once()` at `exporter.interval()` and retain the built-in
maintenance handle until shutdown. Alternatively, call
`embedded.compact_retention()` from a host scheduler. Queries against the
embedded runtime are immediately clipped to the configured recent window.
Complete groups beyond the time or local payload budget are removed
oldest-first. `embedded.storage_health()` reports complete logical and
allocated directory bytes, rollup bytes and series, WAL payload bytes, index
backlog, retention failures, quota headroom, and the last successful
maintenance time.

The durable store preserves separate log, trace, and raw metric retention
windows when `DurableTelemetryConfig.retention` is `None`. An explicit global
window overrides all three signal windows. Reads enforce these windows before
optimized owner queries; trace retention uses the span end time, including
spans that begin before the cutoff. Events and links follow their parent span,
and exemplars follow their parent metric point. Maintenance removes complete
expired signal groups and safely checkpoints their WAL offsets; restart keeps
the same windows. Retention requires a recovery journal or immutable object
tier. Lifetime metric rollups keep their separate aggregate semantics.
When lifetime rollups are enabled, both object-backed WAL reclamation and
time-based physical retention keep metric source batches until their rollup
checkpoint is persisted. An indexed append arriving after a flush's rollup
snapshot remains available for the next checkpoint, including after restart.

Correlation lookups enforce the requested tenant in both navigation indexes
and cold log reads, including when tenants share a partition, trace ID, or label.
Correlation event-time bounds are inclusive for every signal, including logs
that carry only compatibility fields.

An embedded RAM budget disables the optional retained correlation navigation
postings and identity caches. Correlation lookups then match exact typed signal
records in the resident indexes and object tier, including logs, span links and
metric exemplars. This retains query results while trading additional lookup
and decoding work for lower retained memory. Stores without a RAM budget keep
the default navigation policy; `DurableTelemetryLimits.correlations` can select
an explicit policy or disable navigation independently.

RAM-budget mode also supplies `DurableTelemetryLimits.native_query_work`.
Its `NativeQueryWorkLimits` declare per-owner ceilings for encoded blocks,
decoded sidecar/variable-value bytes and rows, control objects, distinct winners, and cumulative
winner materialization. Each byte fragment is derived from one sixteenth of
the configured RAM allowance divided across configured physical owner stripes; adding
logical partitions does not multiply that allowance. Cache chunks/read bounds
follow these ceilings. Cold native queries visit one catalog page, manifest and
block at a time. Correlation pages retain the smallest exact references across
all candidates, including later-published groups with earlier offsets.
Checked log queries also traverse groups and frames lazily, keeping an admitted
top result set in the requested offset or timestamp order. They preflight
compressed query-index expansion, structural rows and typed metadata expansion,
and avoid retaining parsed log frames or posting caches. Exact typed correlation
filters continue across bounded pages, so a string identity cannot consume the
limit for an integer identity. Compatibility-field lookups propagate the same
capacity errors.
Native tenant-partition discovery follows the same lazy control admission and
does not retain tenant-partition caches under the work policy. Its aggregate
counts remain exact and independent of a response page's winner limit.

Winner accounting streams MessagePack into a byte counter rather than creating
another encoded payload. It charges serialized deep payloads and concrete
row/key headers before insertion, including replacing older logical versions;
shared payloads are deliberately charged repeatedly. The merged owner results
have an aggregate ceiling derived from the owner count. These are conservative
work/materialization limits, not allocator measurements or a process RSS quota.
A block, sidecar, winner set or result that exceeds its declared ceiling fails
with a query-capacity error instead of returning an incomplete successful page.
Bounded decoding also rejects sidecar frames whose content size cannot be
verified. Stores with no work policy preserve their existing decoding and query
behavior. An explicit work policy requires disabled retained correlation
navigation. Additional scan latency, startup/publication state, allocator
overhead and process memory still require operational qualification.

`with_max_ram_bytes` requires a local or S3 immutable object tier. A recovery
journal alone cannot release sealed trace and metric payloads from resident
storage. Configurations that omit the RAM budget preserve their existing
journal-only or memory-only choices.

RAM-budget mode supplies `DurableTelemetryLimits.native_resident` as well.
For allowance `R` and the sum `N` of logical trace and metric partitions,
`NativeResidentLimits` reserves `R/4` for sealed payloads: each uniquely owned
partition has ceiling `G = R/(4*N)`. The native publication target and encoded
append ceiling are at most `G/4`; later group/request overrides cannot enlarge
these effective ceilings. Each completed bounded append seals its partition
before evaluating the publication threshold. Actual encoded sealing checks
the ceiling before removing heads. A failed publication keeps its indexed
boundary for an identical retry, so retries do not add another sealed copy.
Other partitions have independent finite allowances.

Decoded append work is derived from `R/(16*physical_owners)` and capped by the
native head allowances. Record counts, declared decompressed sidecars and log
structural bytes, and cumulative serialized record/header charges are checked
before WAL admission. This owner allowance lets ordinary multi-source metric
metadata use more than a tiny fraction of one logical partition's encoded
allowance; compressed expansion that exceeds it fails admission. The builder
reserves separate preflight, owner decode and decoded validation-cache charges,
in addition to query work and queues, before allocating heads/dictionaries/caches.
These are finite declared encoded/materialization charges. Allocator/RSS,
startup metadata and transport concurrency still require measured qualification.
Unbudgeted configurations retain their existing append, sealing and group policy.
Enabling a resident policy does not rewrite existing WAL or immutable blocks.
An over-budget persisted input fails closed when bounded reopen/replay or query
decoding reads it; existing data must be qualified against the new ceilings.

Normalized in-process calls reject oversized input with `LokiApiError::is_bad_request()`
before dispatching any partition or reserving a retry identity. Each exact encoded
STEL envelope must fit the engine's append ceiling, and their combined bytes must
fit `signals.max_otlp_request_bytes`. This normalized-data ceiling is separate
from the original decompressed OTLP transport body. With a resident policy,
HTTP body/gzip limits and OTLP/gRPC decoding also clamp to the effective native
input ceiling before protocol decoding. Store and embedded runtime
`maximum_append_bytes()` expose the conservative single-envelope ceiling; native
transport adapters must also enforce their own complete encoded-frame ceiling.

Use `EmbeddedEvictionPolicy::OffloadToS3` instead of `Delete` to preserve full
raw history. Publication is write-through: a group becomes authoritative in a
dedicated S3 prefix before its payload and index are admitted to the bounded
local SSD cache. Do not reuse the central server's catalog prefix.

`query_lifetime_metric_rollups` returns a crash-safe, bounded-cardinality local
summary without reading S3 or expired raw records. Monotonic cumulative
counters and histograms are reset-aware lifetime totals; gauges retain their
latest value and lifetime minimum/maximum. Rollup persistence precedes reclamation,
so a rollup failure leaves the raw WAL in place. The configured byte limits
bound storage-engine state and steady-state local data; transient request/query
allocations, filesystem metadata, and one in-flight WAL/spool group require
operational headroom.

## Hard-quota offline product usage

Do not use arbitrary metric rollups when the product requires a hard sub-1 MiB
usage-data contract. `EmbeddedUsageLedger` is a separate fixed-schema path for
an allowlist of feature counters, total active seconds, and optional fixed
monthly buckets. It stores two compressed, checksummed generations in one
preallocated and exclusively locked file; it has no WAL, spool, or temporary
rewrite file.

~~~rust
use shard_telemetry::{EmbeddedUsageLedger, EmbeddedUsageLedgerConfig};

let usage = EmbeddedUsageLedger::open(
    EmbeddedUsageLedgerConfig::new(
        "/var/lib/my-app/usage.ledger",
        ["search", "export", "share"],
    )
    .with_monthly_buckets(12)
    .with_max_file_bytes(1024 * 1024),
)?;
usage.record_batch(30, [("search", 1)])?;
~~~

The maximum possible file size is calculated before creation and physical disk
space is reserved at open. For the covered 100-feature, 12-month profile, the
complete file is below 64 KiB. Unknown features fail the whole update by
default or may be directed to one fixed overflow counter. See
[EMBEDDED_USAGE.md](EMBEDDED_USAGE.md) for format, recovery, quota, and health
details.

## Documentation

| Topic | Reference |
| --- | --- |
| Supported telemetry API surface | [TELEMETRY_COMPATIBILITY.md](TELEMETRY_COMPATIBILITY.md) |
| Native framing and acknowledgements | [NATIVE_PROTOCOL.md](NATIVE_PROTOCOL.md) |
| Loki-compatible HTTP surface | [LOKI_COMPATIBILITY.md](LOKI_COMPATIBILITY.md) |
| ClickHouse integration and differential checks | [CLICKHOUSE_COMPATIBILITY.md](CLICKHOUSE_COMPATIBILITY.md) |
| Query planning and index behavior | [QUERY_ARCHITECTURE.md](QUERY_ARCHITECTURE.md) |
| Compression layout and policy | [COMPRESSION_ARCHITECTURE.md](COMPRESSION_ARCHITECTURE.md) |
| Object tier, recovery, and retention | [TIERED_STORAGE_ARCHITECTURE.md](TIERED_STORAGE_ARCHITECTURE.md) |
| Hard-quota offline product usage | [EMBEDDED_USAGE.md](EMBEDDED_USAGE.md) |
| Deployment examples | [deploy/README.md](deploy/README.md) |
| Benchmark method and harnesses | [BENCHMARKS.md](BENCHMARKS.md) |
| Release procedure | [RELEASING.md](RELEASING.md) |
| Security reporting and operational guidance | [SECURITY.md](SECURITY.md) |

## Benchmarks

The repository provides reproducible harnesses for component and
head-to-head measurements. They intentionally require a caller-provided,
immutable corpus and produce a new result directory with provenance metadata.
They do not treat a historical result from another machine as a product
guarantee. See BENCHMARKS.md for commands, inputs, comparison rules, and
reporting requirements.

## Contributing, security, and releases

Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Report
security issues through GitHub's private vulnerability-reporting flow as
described in [SECURITY.md](SECURITY.md). Maintainers follow
[RELEASING.md](RELEASING.md) for versioned releases.

## License

ShardTelemetry is licensed under the [Apache License 2.0](LICENSE). Required
third-party attributions are retained in [THIRD_PARTY_NOTICES](THIRD_PARTY_NOTICES).
