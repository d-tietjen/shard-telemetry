# Benchmarking ShardTelemetry

This repository contains benchmark programs and harnesses, not a portable
performance claim. Results depend on the input corpus, physical CPU topology,
storage medium, operating-system settings, build revision, and the comparison
configuration. Run the relevant harness against an immutable corpus and retain
the result directory before drawing conclusions.

The old retained-machine history is deliberately not published here: it could
not be independently reproduced from this repository and included
machine-specific paths. Public benchmark evidence must be generated from a
commit and include the provenance described below.

## Principles

1. Compare identical accepted input, not merely files with a similar name.
2. Pin or record the input hash, byte count, container image digest, product
   revision, toolchain, kernel, and physical CPU allocation.
3. Use a new result directory for every run. Harnesses refuse to overwrite an
   existing run.
4. State whether the result measures ingest, warm query, cold query, storage
   bytes, or a combination; these are different properties.
5. Do not compare a locally built candidate to a packaged competitor unless
   the build, configuration, and CPU allocation are disclosed.
6. Treat benchmark output as review evidence, not as a compatibility or
   availability guarantee.

Benchmark outputs default to benchmark-results beneath the checkout. That
directory is ignored by Git so results and local corpora are not accidentally
committed.

## Component benchmarks

Build the release binaries first:

~~~text
cargo build --release --locked --bins
~~~

For a quick compression measurement on a local input:

~~~text
cargo run --release --locked --bin shard-telemetry-compress-bench -- /path/to/input.log
~~~

The structural benchmark creates durable worker packs and verifies sampled
reconstruction:

~~~text
cargo run --release --locked --bin shard-telemetry-structural-bench -- \
  /path/to/docker-json.log \
  --limit-bytes 1GiB \
  --workers 1 \
  --locality disabled \
  --output-dir benchmark-results/structural \
  --report benchmark-results/structural-report.txt
~~~

Use a fresh output directory. The codec and locality programs are also useful
for focused investigations:

~~~text
cargo run --release --locked --bin shard-telemetry-codec-bench -- /path/to/input.log --limit-bytes 1GiB
cargo run --release --locked --bin shard-telemetry-locality-bench
cargo run --release --locked --bin shard-telemetry-offload-bench -- --partitions 4 --batches-per-partition 64
~~~

## Head-to-head harnesses

The heavyweight scripts run on Linux and require Docker, a requested number of
physical CPU cores, and the platform tools checked by each script. They use
pinned container image digests. They never download or provide a benchmark
corpus.

Set SOURCE to an immutable Docker json-file input. Set EXPECTED_SHA256 and
EXPECTED_FILE_BYTES when comparing runs; the scripts accept empty values for
exploratory local work, but public evidence should always set both.

~~~text
SOURCE=/data/telemetry/docker-json.log \
EXPECTED_SHA256=replace-with-the-corpus-sha256 \
EXPECTED_FILE_BYTES=replace-with-the-corpus-byte-count \
scripts/run-head-to-head.sh
~~~

The available harnesses are:

| Script | Purpose | Additional input |
| --- | --- | --- |
| scripts/run-head-to-head.sh | Structural-storage comparison with ClickHouse | SOURCE |
| scripts/run-loki-benchmark.sh | Loki wire-ingest comparison | SOURCE |
| scripts/run-clickhouse-head-to-head.sh | Server ingest and query comparison with ClickHouse | SOURCE and locally built server/load binaries |
| scripts/run-signal-clickhouse-head-to-head.sh | Synthetic trace and metric storage/query comparison | Locally built binaries; no external corpus |
| scripts/run-query-head-to-head.sh | Pack-query comparison | SOURCE, a source archive for provenance, and a compatible shard-stream checkout |
| scripts/run-cold-query-head-to-head.sh | Cold-query comparison from retained artifacts | A completed baseline run, compatible shard-stream checkout, deterministic-simulation checkout, and source archive |

For the two scripts that need a companion checkout, pass SHARD_STREAM_SOURCE
explicitly. The query and cold-query scripts record its revision and source
tree hash. Cold-query runs also require a previously generated baseline and
therefore are not a first-run benchmark.

## Publishing a result

Attach the result directory or a durable artifact to the pull request or
release candidate. Include:

- the Git commit and a clean/dirty worktree status;
- command line and relevant environment variables;
- corpus origin, SHA-256, and byte count;
- host model, kernel, storage type, and physical CPU set;
- Rust toolchain and competitor image or binary digest;
- configuration values that affect ingestion, compaction, cache state, or
  query iterations;
- verification results, including accepted record counts and exact-query
  comparisons where the harness provides them; and
- a clear statement of the measured metric and its units.

Do not publish production logs, credentials, private host names, or
machine-specific paths. Use synthetic or otherwise redistributable corpora
when results must be shared publicly.

## Continuous validation

The release gate verifies format, lint, tests, documentation, release build,
and supply-chain checks:

~~~text
scripts/release-gate.sh
~~~

It does not replace a workload-specific benchmark. Compatibility and
competitive checks are documented in CLICKHOUSE_COMPATIBILITY.md,
LOKI_COMPATIBILITY.md, TELEMETRY_COMPATIBILITY.md, and
competitive/README.md.
