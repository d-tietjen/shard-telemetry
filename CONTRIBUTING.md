# Contributing to ShardTelemetry

Thank you for helping improve ShardTelemetry.

## Development

ShardTelemetry uses the Rust toolchain pinned in `rust-toolchain.toml`. A change is
ready for review when these commands pass:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo build --workspace --all-targets --all-features --release --locked
scripts/release-gate.sh
```

Add regression tests for behavioral changes. Storage-format, recovery, query,
and protocol changes require malformed-input and restart-boundary coverage.
Benchmark claims must follow BENCHMARKS.md: include the command, corpus
identity, machine, CPU allocation, build revision and profile, verification
mode, and retained result artifact. Do not include private host names, paths,
or non-redistributable data in a public pull request.

## Module ownership

Keep the stable types, signatures, and reexports in `src/name.rs`; put
implementation and behavior tests in `src/name/`. Use `name.rs` with a sibling
`name/` directory, never `mod.rs`.

| Area | Root modules | Child ownership |
| --- | --- | --- |
| Log storage | `stripe`, `structural`, `query_index` | Appends, storage lanes, indexes, query paths, and format codecs. |
| Durability | `telemetry_store`, `sink`, `tier` | Engine attachment, append and retention paths, sink workers, catalog transactions, object storage, and cache I/O. |
| Signals and query APIs | `metric`, `trace`, `analytics`, `loki_api`, `prometheus_api`, `native_protocol`, `traceql` | Signal codecs and indexes, analytical projections, API parsing and responses, wire messages, and query evaluation. |
| Runtime and tools | `telemetry`, `usage_ledger`, `locality`, `embedded`, `native_server`, `correlation`, `offload`, `promql` and the structural/signal benchmark binaries | Models and routing, accounting and recovery, placement, lifecycle, protocol dispatch, cross-signal navigation, transfer, evaluation, and benchmark workloads. |

When moving code, keep serialization, query ordering, protocol responses, CLI
flags, and public exports stable. Place new tests next to the behavior they
exercise.

## Pull requests

Keep changes focused and explain compatibility or operational consequences.
Do not commit credentials, production log content, benchmark corpora, build
artifacts, or generated secrets. By submitting a contribution, you agree that
it is licensed under the Apache License 2.0.

Use GitHub's private vulnerability-reporting flow for security issues; see
`SECURITY.md`.
