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

The embedded/server qualification matrix compares append throughput, storage
bytes, restart recovery, and warm resource-query latency. It runs embedded
storage by default; add `RUN_NETWORK=1` to include native and Loki HTTP server
ingest with native query latency. The matrix records both recovery-journal
modes so the startup-throughput tradeoff is visible:

~~~text
./scripts/run-perf-matrix.sh
RUN_NETWORK=1 ./scripts/run-perf-matrix.sh
~~~

Set `RECORDS`, `SHARDS`, `WRITERS`, `LOOKUP_ITERATIONS`, and `SCAN_ITERATIONS`
for a smaller smoke run or a larger qualification. On macOS, resident-set
size is unavailable in managed shells and is reported as zero; run the same
matrix on Linux when RSS is required.

Native load runs can exercise one or several logical partitions per request.
`--partitions-per-request N` uses the untracked append path and exposes whether
the physical owner stripes improve a multi-partition batch:

~~~text
target/release/shard-telemetry-loki-load /path/to/docker-json.log \
  --protocol native --workers 4 --partitions 256 \
  --partitions-per-request 4
~~~

The durable Loki range benchmark measures indexed selector queries and
selector-plus-line-filter queries across four storage shards. It reports the
candidate lines and bytes inspected so a lower latency result can be checked
against actual pruning work:

~~~text
cargo run --release --locked --bin shard-telemetry-loki-query-bench
LOKI_BENCH_RECORDS=100000 LOKI_BENCH_ITERATIONS=500 \
  cargo run --release --locked --bin shard-telemetry-loki-query-bench
~~~

For Linux runtime and topology audits, build the optional dial9 integration
with frame pointers and Tokio's instrumentation hooks:

~~~text
RUSTFLAGS='--cfg tokio_unstable -C force-frame-pointers=yes' \
  cargo build --release --locked --features dial9 --bin shard-telemetry-server

DIAL9_ENABLED=true \
DIAL9_TRACE_DIR=/path/to/dial9-traces \
DIAL9_CPU_PROFILE_ENABLED=true \
DIAL9_PROCESS_RESOURCE_USAGE_ENABLED=true \
target/release/shard-telemetry-server --shards 16
~~~

The server sizes its Tokio and append-submission workers from the shard count
by default, capped by host parallelism. Use `--runtime-worker-threads N` when
the host has a separate CPU budget for shard-stream's owner, sync, coordinator,
durable-sink dispatcher workers. Append submission follows that budget up to 64
workers, while the durable dispatcher follows it up to 256 workers. The default
Tokio blocking pool is four workers per runtime worker, capped at 64, and is
reserved for CPU-heavy protocol work plus blocking storage calls. The dispatcher
budget is separate from the per-shard durable sinks and index workers, so it can
be reduced when shard count is a topology choice rather than a CPU budget.
When S3 archival is enabled, its separate async I/O runtime follows the same
runtime budget, clamped to 4..=64 workers; standalone `S3ObjectStore` callers
retain a four-worker default.

The physical shard count is a storage and routing topology choice. Each
shard-stream shard owns its shard and sync workers plus a coordinator and a
replica-append worker, and its coordinator and durable-sink admission pools are
also separate. ShardTelemetry adds one index worker for that shard plus its
durable-sink dispatcher pool. Tokio, append-submission, and blocking workers are
additional pools. This is a bounded multi-pool architecture rather than strict
one-thread-per-core pinning; reserve CPUs for the complete set when comparing
shard counts. A run with sixteen physical shards can require substantially more
runnable threads than sixteen even when Tokio is capped at sixteen workers.
`EmbeddedTelemetryConfig::new` uses the same shard-derived append and durable
sink dispatcher budget, so embedded multi-shard runs exercise owner
parallelism instead of silently collapsing onto one submission thread.

CPU-heavy OTLP, Loki, and Prometheus wire decoding runs on the bounded blocking
pool. Partition-parallel envelope preparation and durable appends share the
configured append pool. Trace, metric, ingest-pack, and typed-log-metadata zstd
contexts are reused per worker, and trace/metric head admission uses a
structural resident-byte estimate instead of serializing each record just to
measure its memory cost. Pre-WAL trace/metric validation hands decoded records
to the owner stripe through a bounded checksum-keyed cache with independent lock
shards, avoiding a second live-path decode. Loki and native log ingestion also
carry the live frame-index sidecar, so the owner stripe does not decompress a
newly written ingest pack just to publish its index. Contiguous Loki streams reuse
their label-derived compression cohort instead of hashing labels once per record.
Native tenant queries route each logical partition directly to its deterministic
owner stripe. Each owner retains only its requested timestamp top-k before the
bounded global merge, avoiding a full fan-out scan and unbounded result sort.
Known trace-ID, metric-series, and partition-affine queries use the same owner
selection in attached stores, while unordered analytics queries skip idle
workers entirely. Ordered multi-partition scans retain their global merge path
so tiered frame pruning can still stop at the correct page boundary.
The durable sink passes trace and metric records by reference through conflict
admission, so duplicate and obsolete retries do not allocate record clones.
Remote Write conflict checks use deterministic series lock shards, allowing
unrelated series to proceed concurrently while retaining same-series
serialization. Its request-local duplicate table is hash-based and preserves
first-seen order, so conflict admission is linear in the number of samples. A
request probes each partition and series with one exact timestamp query rather
than issuing one owner-worker query per sample.
OTLP and embedded partition fan-out use the same unordered grouping strategy,
while query workers share immutable query slices and a cached ordered worker
snapshot instead of rebuilding those structures for every shard.

Run the server and load generator on separate recorded CPU sets. Compare the
same corpus with one, four, and sixteen shards while retaining the dial9 trace
segments. On Linux, enable schedule profiling only when the host permits the
required perf events; otherwise the trace still provides Tokio worker,
process-resource, and userspace CPU evidence but cannot explain kernel
off-CPU movement.

The load generator assigns each worker a distinct stream/resource identity so
native and Loki requests spread across logical partitions. Reusing one identity
for every worker collapses a multi-worker run onto one route and invalidates
shard-scaling conclusions. Native load runs use the normal `AppendUntracked`
path by default. Add `--retryable` when measuring replay-safe request receipts;
that mode intentionally persists and syncs one idempotency receipt per request
and must be compared separately from the untracked throughput path.

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
