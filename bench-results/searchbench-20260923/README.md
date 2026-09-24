# Corrected SearchBench and signal comparison

Run date: 2026-09-23. The corrected ShardTelemetry log run used a 16-physical-
core Linux host, 16 shards, 16 runtime workers, 16 tenant partitions,
and three tries per query. The host was shared; the run recorded a 60-second
compiler wait bound and could not drop Linux page caches.

The ShardTelemetry log result completed all 92 queries. Per-query medians over
the three tries were 4.311 ms median, 530.478 ms p95, 954.677 ms p99,
956.100 ms max, and 5.389 s sum. The first try includes cold-cache effects and
had a 92.130 ms median, 2.969 s p95, 11.554 s max, and 56.199 s sum.

The corrected run stored 55,669,872 bytes and loaded the 1M-row slice in
16.052 s. The stored-byte comparison to SereneDB's 141,666,298 bytes and
ClickHouse's 178,762,564 bytes is indicative because the ShardTelemetry
adapter projects seven searchable columns while those comparison tables retain
15 columns. Their recorded SearchBench latency was one value per query:
SereneDB 7 ms median / 15.9 ms p95 / 28 ms max, and ClickHouse 7.5 ms /
50.75 ms / 216 ms on its 70 supported queries.

On the matched one-core signal corpus, ShardTelemetry stored 902,077 trace
bytes versus ClickHouse's 5,775,860 bytes (84.4% less, 6.40x smaller) and
1,966,452 metric bytes versus 3,897,201 (49.5% less, 1.98x smaller). Warm
trace lookup latency was 2/2/6 ms at p50/p95/p99 versus ClickHouse's
3/7/11 ms, with 467.279 versus 276.688 queries/s. Warm metric lookup latency
was 2/4/9 ms versus 3/6/8 ms, with 360.380 versus 272.100 queries/s. The
metric p99 is the one measured latency point where ClickHouse was lower.

A second full run started after a brief quiet window at 09:45:33 UTC and
completed all 92 queries, but unrelated compiler processes resumed during the
run. Its per-query-median sum was 5.754 s versus 5.389 s for the corrected
run; the focused medians were Q31 1.047 s, Q66 0.996 s, Q73 0.905 s, Q81
0.928 s, and Q84 1.049 s. It is retained as shared-load evidence in
`shardtelemetry_otel_logs_1m_phrase_token_candidate_isolated_0945_sharedload.json`
and is not used as a clean performance gate.

The six bounded timestamp/text projection queries that previously returned
truncated HTTP bodies now return successfully. The fix normalizes candidate
record ordinals before structural projection decoding; the regression is
covered by `stripe::tests::structural_candidate_ordinals_are_sorted_and_deduplicated`.

The dial9 sample identified the remaining tail work in analytics row
materialization, grouped/cardinality scans, JSONLines/RowBinary writers, and
trace-cardinality joins; sampled leaf frames included BLAKE3, JSON escaping,
`AnalyticsRow` destruction, allocation, and exact token/field checks. A
per-frame embedded-field posting cache and a timestamp-tail shortcut were
tested in focused A/B runs and removed because they did not produce a clean
latency win under shared-host conditions. The retained normalization
checks for already sorted postings before sorting, so the correctness fix does
not sort the common hot-path case.

A same-host A/B of a trace-ID deduplication and one-sided intersection
optimization also regressed the focused warm-median sum from 3.717 s to
3.863 s, including a 4.7% Q84 regression, so it was removed.

See [all-signals-summary.tsv](all-signals-summary.tsv) for trace, metric, and
log storage and latency rows, and the [trace/metric ClickHouse summary](../signal-clickhouse-20260923/summary.tsv)
for the direct synthetic signal comparison.
