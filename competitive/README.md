# External competitive qualification

`scripts/run-competitive-oracles.sh` is the Linux/amd64 functional
head-to-head gate. It deliberately compares the product to the systems that
define its public compatibility and analytical-consumer boundaries:

| System | Signal / boundary | Equal input and asserted result |
| --- | --- | --- |
| Prometheus | OTLP metrics and PromQL | The identical OTLP Metrics protobuf yields one `checkout_requests` gauge with value `1`. |
| Loki | OTLP logs and LogQL | The identical OTLP Logs protobuf yields three records, two containing `checkout`. |
| Tempo | OTLP traces and trace-by-ID | The identical OTLP Traces protobuf yields trace `111…111` with one resource-spans group. |
| ClickHouse | all signal analytical SQL | The existing stock ClickHouse RowBinary matrix runs all 30 checks against the same generated fixture. |
| DuckDB | analytical interchange | DuckDB reads ShardTelemetry's bounded NDJSON scan and returns three log rows, two with `checkout`. |

The fixture generator is built from this repository; it emits no production
records. Each campaign gives all engines one recorded current Unix-nanosecond
base timestamp, so receivers that correctly reject far-future samples (such
as Prometheus) can participate. Set `COMPETITIVE_FIXTURE_TIMESTAMP_NANOS` to
rerun a retained capture exactly. The campaign records that timestamp, fixture
hashes, command output, image identities, response bodies, and the full
ClickHouse acceptance evidence in a new directory. Any oracle mismatch fails
the run. Pinned Linux/amd64 image digests are in [images.env](images.env); the
campaign refuses a non-Linux or non-amd64 host.

Run it only on a disposable Linux Docker host:

```sh
cargo build --release --locked \
  --bin shard-telemetry-server \
  --bin shard-telemetry-clickhouse-fixture
RESULT_ROOT=/var/tmp/shard-telemetry-competitive \
  scripts/run-competitive-oracles.sh
```

This is a functional oracle campaign, not a throughput claim. The full-size,
same-host performance harnesses remain separate because the systems do not
share one storage model or feature set:

- `scripts/run-clickhouse-head-to-head.sh` measures the retained 80 GiB log
  corpus with equal CPUs, source checksum, result checks, and storage account.
- `scripts/run-loki-benchmark.sh` measures the same retained corpus through
  Loki's native HTTP ingestion path.
- `scripts/run-signal-clickhouse-head-to-head.sh` compares deterministic
  correlated trace and metric workloads to stock ClickHouse.

Run one retained-corpus suite through
`scripts/run-benchmark-qualification.sh` on the dedicated 16-core Linux host.
It rebuilds the exact benchmark binary from the checked-out revision, rejects
tracked local source changes, validates the harness's result artifacts, and
retains a compact checksum manifest without exporting corpus data, durable
stores, request logs, or local tokens. The manually dispatched
`Benchmark qualification` workflow targets only the explicitly labelled
`shard-telemetry-benchmark` self-hosted runner; it never runs on PR runners.
Set `BENCHMARK_SUITE` to `clickhouse-logs`, `loki-logs`,
`clickhouse-signals`, `clickhouse-hot-queries`, or
`clickhouse-cold-queries`, and set `BENCHMARK_RESULT_ROOT` to a dedicated
absolute result-volume path. Corpus and auxiliary paths remain explicit
environment configuration of that host, so a result cannot silently substitute
a smaller or different fixture.

Do not turn a successful small-fixture oracle run into a claim of universal
feature or performance parity. Unsupported PromQL, LogQL, TraceQL, or SQL
operations remain fail-closed and are documented in the corresponding
compatibility documents. A new competitive system belongs in this matrix only
after its image, equal-input adapter, result normalizer, acceptance assertions,
and retained Linux evidence are added together.
