# SearchBench 1M comparison

Run date: 2026-09-21. All three engines ran on the same 16-physical-core
Ubuntu 24.04.4 Linux host against the same 1M OTLP Parquet slice and the same
92-query SearchBench workload. ShardTelemetry used 16 shards and 16 tenant
partitions.

| Engine | Queries | Median | P95 | Max | Sum |
| --- | ---: | ---: | ---: | ---: | ---: |
| ShardTelemetry | 92/92 | 15.295 ms | 2.965 s | 10.667 s | 52.839 s |
| SereneDB 26.09.1 | 92/92 | 7 ms | 17 ms | 28 ms | 0.761 s |
| ClickHouse 26.8.2.7 sorted | 70/92 | 7.5 ms | 53 ms | 216 ms | 1.266 s |

ClickHouse marks 22 BM25/top-k queries unsupported in its SearchBench adapter;
the supported-query comparison is 28.574 ms / 3.168 s / 10.667 s for
ShardTelemetry versus 7 ms / 14 ms / 21 ms for SereneDB and 7.5 ms / 53 ms /
216 ms for ClickHouse (median / p95 / max).

Load measurements were: ShardTelemetry 13.289 s and 5,614,287,930 bytes;
SereneDB 5.10 s and 141,666,298 bytes; ClickHouse 2.28 s and 178,762,564
bytes. ShardTelemetry's adapter projects seven searchable columns and therefore
its footprint is not directly comparable to the engines' full 15-column tables.

The 100M ShardTelemetry run loaded 100,000,000 rows in 3,941.575 s and timed
out Q1 at the 60 s query limit. The matching 100M ClickHouse build was stopped
when the benchmark host reached 99% disk utilization during full-text merge
work; it had
written roughly 50 GB. The 100M SereneDB Docker image was incompatible with the
checked-in dictionary syntax, so the complete comparison uses the official
26.09.1 Linux binary instead.
