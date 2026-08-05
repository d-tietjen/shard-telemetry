# ClickHouse query compatibility

ShardTelemetry delegates analytical SQL semantics to a pinned ClickHouse query node
and remains responsible for log ingestion, indexing, compression, and tiered
storage. The query evaluator remains unmodified. Production automatic pushdown
uses the narrow in-tree `StorageShardTelemetry` adapter described below; the generic
URL path remains available for an entirely stock ClickHouse binary. The pinned
compatibility target is source tag `v26.3.17.56-lts` at commit
`c57540de480d8a501b601163471d3843674378cf`. The custom Adam binary has
SHA-256 `1c8b28af4b209a1cdf127b8eac1220deb63d54f1142d277df1954b58e0bcfd66`.

This boundary makes ClickHouse, rather than a second SQL implementation, the
semantic authority for expressions, types, aggregate functions, joins,
subqueries, common table expressions, window functions, JSON functions,
materialized views, output formats, and query errors. Clients that need this
surface connect to ClickHouse. Loki, OTLP, and ShardTelemetry-native clients continue
to connect directly to ShardTelemetry.

## Versioned columnar source

The first executable adapter is a versioned Arrow IPC stream:

```text
GET /shardtelemetry/api/v1/clickhouse/scan
```

The route is absent by default. It is registered only when
`shard-telemetry-server` receives `--clickhouse-token-file`. Every request must send
the exact token as `Authorization: Bearer ...`. The file must contain a
non-empty token. Treat this as an administrative credential: the holder may
select a tenant with `X-Scope-OrgID`.

Run the endpoint on loopback or behind an authenticated TLS/mTLS proxy. Do not
send the bearer token over an untrusted plaintext network.

The Arrow schema is version 1. The endpoint accepts one fixed `relation`:
`logs`, `spans`, `span_events`, `span_links`, `metric_points`, or
`metric_exemplars`. Omitting it selects `logs`. The complete pinned schemas
and ClickHouse types are executable in `clickhouse/shard-telemetry-engine.sql`.

The log relation begins with:

| Column | Arrow type | ClickHouse type | Meaning |
| --- | --- | --- | --- |
| `tenant` | `Utf8` | `String` | Authenticated tenant |
| `signal` | `Utf8` | `String` | Parent telemetry signal |
| `timestamp` | `Timestamp(Nanosecond, UTC)` | `DateTime64(9, 'UTC')` | Event time |
| `observed_timestamp` | nullable nanosecond timestamp | `Nullable(DateTime64(9, 'UTC'))` | Independent OTLP observed time |
| `partition` | `UInt32` | `UInt32` | Logical partition |
| `offset` | `UInt64` | `UInt64` | Durable offset |
| `message` | nullable `Utf8` | `Nullable(String)` | Original log line |
| `labels` | `Map<Utf8, Utf8>` | `Map(String, String)` | Loki stream labels |
| `metadata` | `Map<Utf8, Utf8>` | `Map(String, String)` | Structured metadata |

The response content type is `application/vnd.apache.arrow.stream` and carries
`X-ShardTelemetry-Schema-Version: 1` plus the pinned ClickHouse target.

Every relation is streamed in bounded 8,192-row batches. The HTTP layer never
materializes a complete tenant. Durable scans page logical partitions by
offset and query owner stripes in parallel. Conflict resolution occurs before
the offset cursor is applied, preventing an older span or metric version from
reappearing on a later page.

## Storage pushdown contract

The URL query accepts these fail-closed parameters:

| Parameter | Behavior |
| --- | --- |
| `start_ns` | Inclusive unsigned Unix-nanosecond timestamp |
| `end_ns` | Exclusive unsigned Unix-nanosecond timestamp |
| `term` | Repeatable case-insensitive indexed message token; AND semantics |
| `label.NAME` | Repeatable exact stream-label equality |
| `metadata.NAME` | Repeatable exact structured-metadata equality |
| `attribute.NAME` | Repeatable exact rendered record-attribute equality |
| `resource.NAME` | Repeatable exact rendered resource-attribute equality |
| `scope.NAME` | Repeatable exact rendered scope-attribute equality |
| `trace_id` | Exact lowercase or uppercase 128-bit hex trace ID |
| `span_id` | Exact lowercase or uppercase 64-bit hex span ID |
| `series_id` | Exact 128-bit metric-series fingerprint |
| `name` | Exact span, event, or metric name |
| `columns` | Comma-separated projection in requested output order |
| `limit` | Optional global row limit |

Unknown parameters, columns, empty column lists, duplicate columns, and invalid
ranges are rejected. Safe constraints are translated to `LogQuery`,
`TraceQuery`, or `MetricQuery`; every decoded row is then checked against the
complete request before it is emitted. Column selection controls which Arrow
arrays are allocated and transmitted.

The generic ClickHouse `URL` engine does not infer these parameters from a SQL
`WHERE` clause. It therefore supports explicit pushdown in the source URL.

The pinned `StorageShardTelemetry` adapter in `clickhouse/adapter` subclasses
ClickHouse's `StorageURL` and overrides only its URI-parameter hook. It obtains
the physical projection and analyzed filter DAG from `SelectQueryInfo` and
automatically translates safe timestamp and exact map equalities into the same
scan contract. The original filter remains in ClickHouse as a residual, so an
unsupported expression loses performance rather than correctness. See
`clickhouse/adapter/README.md` for installation, DDL, and the exact pushdown
rules.

## ClickHouse source

With ShardTelemetry listening locally and the token supplied by a protected secret
source, ClickHouse can query the stream directly:

```sql
SELECT
    labels['service_name'] AS service,
    count() AS records,
    quantileTDigest(0.99)(lengthUTF8(message)) AS p99_message_bytes
FROM shardtelemetry.logs
GROUP BY service
ORDER BY records DESC;
```

`clickhouse/shard-telemetry-url.sql` contains a generic URL example and
`clickhouse/shard-telemetry-engine.sql` contains every automatic-pushdown
relation and the derived trace view.
ClickHouse stores engine headers in table metadata, so production
deployments should inject a short-lived credential or use a trusted local
proxy rather than committing a token to SQL.

## Differential gate

`scripts/run-clickhouse-compatibility.sh` retains the log matrix.
`scripts/run-clickhouse-telemetry-compatibility.sh` adds every trace/metric
relation, relation-specific aggregates and windows, topology queries, and
cross-signal joins by trace/span, exemplar, and resource identity. Both
evaluate the same deterministic query against:

1. the live ShardTelemetry Arrow source; and
2. an equivalent ClickHouse `Memory` table populated from that source.

It compares exact serialized results for filters, native-map grouping,
conditional and exact aggregates, arrays, windows, CTEs, joins, timestamp/map
predicates, mixed residual predicates, disjunctions, missing-map default-value
semantics, aliases, subqueries, and aggregate combinators. The harness refuses
a ClickHouse version other than `26.3.17.56` unless
`STRICT_CLICKHOUSE_VERSION=0` is supplied for developer smoke testing.

Set `SHARD_TELEMETRY_ADAPTER_MODE=1`, or run
`scripts/run-clickhouse-adapter-compatibility.sh`, to create a
`StorageShardTelemetry` source table and exercise automatic pushdown. Adapter mode
requires a ClickHouse binary or image built with the pinned adapter.

This proves the adapter and evaluator path; it does not replace the larger
compatibility corpus. The release gate is the applicable ClickHouse SQL test
suite plus generated differential combinations of nullable values, nested
types, aliases, lambdas, aggregate combinators, joins, windows, and errors.

## Compatibility status

| Area | Status |
| --- | --- |
| ClickHouse `SELECT` evaluator semantics | Supplied by pinned ClickHouse |
| Bounded typed ShardTelemetry scan | Implemented |
| Logs, spans, events, links, metric points, and exemplars | Implemented in schema v1 |
| Derived trace summaries and cross-signal joins | Implemented in pinned ClickHouse DDL |
| Authentication and tenant selection | Implemented; route disabled by default |
| Explicit timestamp/term/label/metadata pushdown | Implemented |
| Explicit column selection | Implemented |
| Automatic plan-to-scan pushdown | Implemented and accepted for columns, timestamp bounds/order, exact ID/name/map equality, exact message tokens, cardinality, and safe limits |
| ClickHouse native/HTTP client surface | Supplied by ClickHouse query node |
| All-signal differential SQL matrix | Passed through the custom adapter and pinned evaluator: 30/30 exact-result cases |
| Full ClickHouse SQL regression corpus | Pending import and classification |
| Controlled 1 GiB real-log benchmark | Passed with exact results; ShardTelemetry was 2.25x smaller and ingested 1.78x faster |
| 80 GiB warm analytical benchmark | Passed with exact results: 607,363,459 rows; ShardTelemetry was 2.25x smaller, ClickHouse ingested 1.58x faster; ShardTelemetry won latest/token p50 and lost exact-stream p50 |
| Cold S3-compatible analytical benchmark | Pending; the current cold-tier result is a local immutable-object/cache ablation |

## Current custom-adapter acceptance

The retained Adam gate is:

```text
/home/dtietjen/deterministic-sim-runs/shard-telemetry/clickhouse-adapter-20260804-v1/acceptance-17
```

It passed 30/30 exact-result cases: 18 log queries, nine span/event/link/metric/
exemplar queries, and three joins through trace, exemplar, and resource
identity. It includes ordered residual filters, case-sensitive and
case-insensitive exact token pushdown, cardinality, maps, windows, aggregate
combinators, topology, and correlations. The custom adapter retains the
original ClickHouse predicate as a residual; a pushdown miss can add work but
cannot change results.

The controlled real-log run `benchmark-1g-attempt13` then queried the live
ShardTelemetry source and MergeTree sequentially on CPUs 0–15. All three
ordered result files were byte-identical. ShardTelemetry stored 33,958,025
bytes versus 76,288,534 bytes and ingested at 123.20 MiB/s versus 69.14 MiB/s.
This is an accepted adapter/storage comparison, but it is not evidence of the
separate 1 GiB/s-per-core target.

The authoritative full-corpus run is `benchmark-80g-final-attempt3`. Both
engines accepted 85,899,345,920 bytes and produced exactly 607,363,459 rows.
ShardTelemetry stored 2,702,973,946 bytes at 31.78x and ingested at 148.60
MiB/s. ClickHouse stored 6,091,870,726 active-part bytes at 14.10x and ingested
at 234.80 MiB/s. Across 20 warm iterations, ShardTelemetry versus ClickHouse
p50/p99 was 70/82 versus 767/807 ms for latest records, 69/76 versus 6/6 ms for
exact stream, and 100/123 versus 1,715/1,833 ms for a case-insensitive indexed
token. All three ordered result files were byte-identical.

## Initial acceptance evidence

On 2026-07-31, the differential smoke ran on Adam against the exact official
ClickHouse `26.3.17.56` image above. The final native-map Arrow stream had
SHA-256 `be3c7f12f4ecbcee5132c1474521e49008cfd6ea0fee5c96647b1f2b8883c01d`.
Three synthetic records covered two streams, labels, metadata, multiple
timestamps, and case-varying error terms. The initial six gates and the
expanded predicate/semantic gates all produced byte-identical serialized
results:

```text
PASS row-count
PASS group-map
PASS aggregates
PASS window
PASS cte-array
PASS self-join
PASS timestamp-map-filter
PASS mixed-residual
PASS disjunction
PASS missing-map-key
PASS missing-map-equality
PASS alias-subquery
PASS aggregate-combinators
ClickHouse compatibility smoke passed with 26.3.17.56
```

The exact 26.3 analyzer was also inspected on Adam. It rewrites constant map
lookups to dynamic inputs such as `labels.key_app` and `metadata.key_code` and
constant-folds time bounds to `DateTime64(9, 'UTC')` values. The adapter handles
those canonical forms using ClickHouse's own String text deserializer and
retains every original filter as a residual.

On 2026-08-04, the expanded schema-v1 matrix ran against a live
ShardTelemetry fixture containing a correlated log, span, span event, span
link, metric point, and metric exemplar. The exact official ClickHouse
`26.3.17.56` evaluator image for the local architecture had manifest digest
`sha256:422be85ae7344058369cdd366ac0efea9daa8428b55c9cf50258e83a7d12fcb3`.
All 13 log cases and all 12 signal/correlation cases returned byte-identical
results against ClickHouse `Memory` snapshots:

```text
PASS span-aggregates
PASS span-window
PASS span-map-filter
PASS event-group
PASS link-topology
PASS metric-aggregates
PASS metric-window
PASS metric-map-filter
PASS exemplar-group
PASS log-span-correlation
PASS exemplar-span-correlation
PASS resource-correlation
All-signal ClickHouse analytical compatibility passed
```

The adapter installer was separately applied twice, idempotently, to a shallow
checkout at exact tag `v26.3.17.56-lts`. The retained custom binary was then
built from commit `c57540de480d8a501b601163471d3843674378cf`; its SHA-256 is
`1c8b28af4b209a1cdf127b8eac1220deb63d54f1142d277df1954b58e0bcfd66`.
The 30-case acceptance and full 80 GiB run above both use that binary.

ShardTelemetry must not claim to be a standalone reimplementation of the
ClickHouse server. The supported drop-in boundary is a ShardTelemetry storage
backend beneath the pinned ClickHouse query node: existing ClickHouse clients
and `SELECT` analytics keep ClickHouse semantics while telemetry data resides
in ShardTelemetry. The full upstream SQL corpus remains required before a
broad compatibility release claim. The pinned custom-adapter functional and
performance gates are complete.
