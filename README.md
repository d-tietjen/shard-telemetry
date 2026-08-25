# ShardTelemetry

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

ShardTelemetry is a Rust observability storage engine for logs, traces, and
metrics. It assigns each durable shard to a single writer, stores
signal-specific records behind a checksummed STEL envelope, and exposes
indexed query and ingestion interfaces for standalone and embedded
deployments.

## Project status

This is a pre-release project. The package is versioned 0.1.0; published
releases use the `shard-telemetry` crate name. Storage formats, network
protocol details, and operational defaults may change before a stable release.
Evaluate it against a representative workload and follow the compatibility
documents before relying on it for production data.

The project is distributed under Apache-2.0. The security policy, release
process, and contributor expectations are public and linked below.

## What it provides

- Native STEL ingestion and query over TCP, with checksummed envelopes and
  durable acknowledgements.
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
    FastTelemetryConfig::new("device", "my-app"),
))?;
embedded.mark_ready()?;
~~~

Drive `exporter.export_once()` at `exporter.interval()` and call
`embedded.compact_retention()` periodically. Queries against the embedded
runtime are immediately clipped to the configured recent window. Complete
groups beyond the time or local payload budget are removed oldest-first.

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
