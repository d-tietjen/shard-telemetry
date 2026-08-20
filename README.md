# ShardTelemetry

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

ShardTelemetry is a Rust observability storage engine for logs, traces, and
metrics. It assigns each durable shard to a single writer, stores
signal-specific records behind a checksummed STEL envelope, and exposes
indexed query and ingestion interfaces for standalone and embedded
deployments.

## Project status

This is a pre-release source project. The package is versioned 0.1.0 and is
not published to crates.io. Storage formats, network protocol details, and
operational defaults may change before a stable release. Evaluate it against a
representative workload and follow the compatibility documents before relying
on it for production data.

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
- [fast-telemetry](https://github.com/eden-dev-inc/fast-telemetry) is a
  separate Rust instrumentation library for hot-path counters, gauges,
  histograms, distributions, and spans. It is complementary to
  ShardTelemetry's durable ingestion and query role.

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
