# ClickHouse query compatibility

ShardTelemetry's ClickHouse integration is implemented entirely in Rust. The
server exposes authenticated, typed telemetry relations over HTTP; an
unmodified ClickHouse query node reads those relations with its built-in `URL`
engine. ShardTelemetry does not patch, fork, or compile code into ClickHouse.

ClickHouse remains the semantic authority for expressions, aggregates, joins,
subqueries, common table expressions, windows, JSON functions, materialized
views, output formats, and query errors. ShardTelemetry remains responsible for
telemetry ingestion, indexing, compression, retention, and tiered storage.

The compatibility target is ClickHouse `26.3.17.56` LTS. This design preserves
the existing ClickHouse client and SQL surface without adding C++ to the
ShardTelemetry product or requiring a custom ClickHouse binary.

## Rust analytical source

The v1 endpoint is:

```text
GET /shardtelemetry/api/v1/clickhouse/scan
```

The route is absent unless `shard-telemetry-server` receives
`--clickhouse-token-file`. Every request must send the exact token as
`Authorization: Bearer ...`; `X-Scope-OrgID` selects the authenticated tenant.
Run the endpoint on loopback or behind an authenticated TLS/mTLS proxy.

The endpoint exposes these fixed relations:

- `logs`
- `spans`
- `span_events`
- `span_links`
- `metric_points`
- `metric_exemplars`

The complete ClickHouse schemas and the derived `traces` view are executable in
`clickhouse/shard-telemetry-engine.sql`.

Two lossless Rust encoders are available:

- `wire=rowbinary` streams ClickHouse RowBinary and is used by the persistent
  stock-ClickHouse tables.
- `wire=arrow` streams Arrow IPC for columnar clients and ad hoc `url(...)`
  queries.

Responses are emitted in bounded batches. The Rust service never materializes
an entire tenant. Durable scans query owner stripes in parallel, page by stable
offset, and resolve span or metric conflicts before applying the cursor.

## Stock ClickHouse tables

The production relation uses ClickHouse's built-in `URL` engine:

```sql
CREATE TABLE shardtelemetry.logs
(
    tenant String,
    signal String,
    timestamp DateTime64(9, 'UTC'),
    observed_timestamp Nullable(DateTime64(9, 'UTC')),
    partition UInt32,
    offset UInt64,
    resource_id Nullable(String),
    scope_id Nullable(String),
    trace_id Nullable(String),
    span_id Nullable(String),
    message Nullable(String),
    body_json Nullable(String),
    severity_number Nullable(Int32),
    severity_text Nullable(String),
    event_name Nullable(String),
    flags Nullable(UInt32),
    dropped_attributes_count Nullable(UInt32),
    labels Map(String, String),
    metadata Map(String, String),
    attributes Map(String, String),
    resource_attributes Map(String, String),
    scope_attributes Map(String, String),
    attribute_ids Map(String, String),
    resource_attribute_ids Map(String, String),
    scope_attribute_ids Map(String, String),
    attributes_json Nullable(String),
    resource_attributes_json Nullable(String),
    scope_attributes_json Nullable(String)
)
ENGINE = URL(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=logs&wire=rowbinary',
    'RowBinary',
    headers(
        'Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE',
        'X-Scope-OrgID' = 'fake'
    )
);
```

ClickHouse stores engine headers in table metadata. Production deployments
must inject a short-lived token or route through a trusted local proxy rather
than committing a credential to SQL.

## Explicit Rust pushdown

The endpoint accepts these fail-closed parameters:

| Parameter | Behavior |
| --- | --- |
| `start_ns` | Inclusive unsigned Unix-nanosecond timestamp |
| `end_ns` | Exclusive unsigned Unix-nanosecond timestamp |
| `term` | Repeatable case-insensitive indexed log token; AND semantics |
| `label.NAME` | Repeatable exact stream-label equality |
| `metadata.NAME` | Repeatable exact structured-metadata equality |
| `attribute.NAME` | Repeatable exact rendered record-attribute equality |
| `resource.NAME` | Repeatable exact rendered resource-attribute equality |
| `scope.NAME` | Repeatable exact rendered scope-attribute equality |
| `trace_id` | Exact 128-bit hexadecimal trace ID |
| `span_id` | Exact 64-bit hexadecimal span ID |
| `series_id` | Exact 128-bit metric-series fingerprint |
| `name` | Exact span, event, or metric name |
| `columns` | Comma-separated output order |
| `limit` | Optional global row limit |
| `wire` | `rowbinary`, `arrow`, or `arrow_stream` |

Unknown parameters, columns, duplicate columns, invalid ranges, and invalid IDs
are rejected. Safe constraints are translated directly into `LogQuery`,
`TraceQuery`, or `MetricQuery`, and emitted rows are checked against the full
request before serialization.

Stock ClickHouse does not translate an arbitrary SQL `WHERE` clause into these
URL parameters. Persistent URL tables therefore retain exact SQL semantics but
do not receive automatic storage pushdown. Callers that construct `url(...)`
sources may include explicit parameters, and ShardTelemetry-native APIs use the
same indexes directly. Automatic SQL-plan pushdown would require a separate
Rust SQL gateway and is not claimed by this release.

## Cross-signal analytics

Stable trace/span IDs, series IDs, resource/scope IDs, and typed-attribute
fingerprints support exact joins across relations:

```sql
SELECT
    spans.name,
    count() AS matching_logs,
    uniqExact(metric_exemplars.series_id) AS metric_series
FROM shardtelemetry.logs AS logs
INNER JOIN shardtelemetry.spans AS spans
    USING (tenant, trace_id, span_id)
LEFT JOIN shardtelemetry.metric_exemplars AS metric_exemplars
    USING (tenant, trace_id, span_id)
GROUP BY spans.name
ORDER BY matching_logs DESC;
```

`clickhouse/shard-telemetry-engine.sql` defines every persistent URL table and
the trace summary view. `clickhouse/shard-telemetry-url.sql` contains an ad hoc
table-function example.

## Differential gates

`scripts/run-clickhouse-compatibility.sh` validates logs.
`scripts/run-clickhouse-telemetry-compatibility.sh` adds every trace and metric
relation plus topology and cross-signal joins. Both compare the same query over:

1. the live Rust RowBinary source through stock ClickHouse `url(...)`; and
2. a ClickHouse `Memory` table populated from that source.

The matrix covers exact cardinality, maps, missing-map defaults, timestamp and
message filters, arrays, windows, CTEs, aliases, aggregate combinators, joins,
events, links, exemplars, and resource/trace correlations. The harness refuses
a ClickHouse version other than `26.3.17.56` unless
`STRICT_CLICKHOUSE_VERSION=0` is explicitly selected for development.

`scripts/run-clickhouse-acceptance.sh` creates the correlated fixture, starts
the Rust server, runs both matrices through a pinned official ClickHouse image,
and retains hashes, versions, metrics, and exact results in a new evidence
directory.

The current Adam acceptance run passed all 30 cases against the unmodified
official ClickHouse `26.3.17.56` image: 18 log cases and 12 trace, metric, and
cross-signal cases. Retained evidence is:

```text
/home/dtietjen/deterministic-sim-runs/shard-telemetry/clickhouse-stock-url-20260806-v1/acceptance-3-26.3
```

The end-to-end comparison is `scripts/run-clickhouse-head-to-head.sh`. It uses
the official pinned ClickHouse image, equal source bytes, identical physical
CPUs, exact row counts, and byte-identical result checks. No custom ClickHouse
binary or ClickHouse source checkout is accepted by the harness.

## Compatibility status

| Area | Status |
| --- | --- |
| ShardTelemetry implementation | Rust only |
| ClickHouse query node | Unmodified official binary |
| Logs, spans, events, links, metric points, and exemplars | Implemented in schema v1 |
| Authentication and tenant selection | Implemented; route disabled by default |
| Exact SQL evaluator semantics | Supplied by pinned stock ClickHouse |
| Derived traces and cross-signal joins | Implemented in stock ClickHouse DDL |
| Explicit indexed pushdown | Implemented in the Rust scan endpoint |
| Automatic SQL-plan pushdown | Not claimed; stock URL tables evaluate residual SQL in ClickHouse |
| Differential SQL matrix | Passed 30/30 through stock URL/RowBinary on ClickHouse 26.3.17.56 |
| Full upstream ClickHouse SQL corpus | Pending import and classification |
| Cold object-tier analytical benchmark | Pending |

Historical measurements produced with the removed C++ prototype remain useful
as storage-codec and native-query evidence, but they are not release evidence
for the supported stock-ClickHouse boundary. Publish comparative ClickHouse
URL-table latency only from the stock harness.
