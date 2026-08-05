-- StorageShardTelemetry requires a ClickHouse binary built with clickhouse/adapter.
-- Inject the token from a protected source; do not commit a real credential.

CREATE DATABASE IF NOT EXISTS shardtelemetry;

CREATE TABLE IF NOT EXISTS shardtelemetry.logs
(
    tenant String, signal String, timestamp DateTime64(9, 'UTC'),
    observed_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64,
    resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String),
    span_id Nullable(String), message Nullable(String), body_json Nullable(String),
    severity_number Nullable(Int32), severity_text Nullable(String), event_name Nullable(String),
    flags Nullable(UInt32), dropped_attributes_count Nullable(UInt32),
    labels Map(String, String), metadata Map(String, String), attributes Map(String, String),
    resource_attributes Map(String, String), scope_attributes Map(String, String),
    attribute_ids Map(String, String), resource_attribute_ids Map(String, String),
    scope_attribute_ids Map(String, String), attributes_json Nullable(String),
    resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)
)
ENGINE = ShardTelemetry(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=logs',
    'ArrowStream',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
);

CREATE TABLE IF NOT EXISTS shardtelemetry.spans
(
    tenant String, signal String, timestamp DateTime64(9, 'UTC'),
    end_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64,
    resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String),
    span_id Nullable(String), parent_span_id Nullable(String), name Nullable(String),
    kind Nullable(Int32), duration_nanos Nullable(UInt64), status_code Nullable(Int32),
    status_message Nullable(String), trace_state Nullable(String), flags Nullable(UInt32),
    dropped_attributes_count Nullable(UInt32), dropped_events_count Nullable(UInt32),
    dropped_links_count Nullable(UInt32), attributes Map(String, String),
    resource_attributes Map(String, String), scope_attributes Map(String, String),
    attribute_ids Map(String, String), resource_attribute_ids Map(String, String),
    scope_attribute_ids Map(String, String), attributes_json Nullable(String),
    resource_attributes_json Nullable(String), scope_attributes_json Nullable(String),
    events_json Nullable(String), links_json Nullable(String)
)
ENGINE = ShardTelemetry(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=spans',
    'ArrowStream',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
);

CREATE TABLE IF NOT EXISTS shardtelemetry.span_events
(
    tenant String, signal String, timestamp DateTime64(9, 'UTC'),
    parent_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64,
    resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String),
    span_id Nullable(String), ordinal Nullable(UInt32), name Nullable(String),
    dropped_attributes_count Nullable(UInt32), attributes Map(String, String),
    resource_attributes Map(String, String), scope_attributes Map(String, String),
    attribute_ids Map(String, String), attributes_json Nullable(String),
    resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)
)
ENGINE = ShardTelemetry(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=span_events',
    'ArrowStream',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
);

CREATE TABLE IF NOT EXISTS shardtelemetry.span_links
(
    tenant String, signal String, timestamp DateTime64(9, 'UTC'), partition UInt32, offset UInt64,
    resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String),
    span_id Nullable(String), ordinal Nullable(UInt32), linked_trace_id Nullable(String),
    linked_span_id Nullable(String), trace_state Nullable(String), flags Nullable(UInt32),
    dropped_attributes_count Nullable(UInt32), attributes Map(String, String),
    resource_attributes Map(String, String), scope_attributes Map(String, String),
    attribute_ids Map(String, String), attributes_json Nullable(String),
    resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)
)
ENGINE = ShardTelemetry(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=span_links',
    'ArrowStream',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
);

CREATE TABLE IF NOT EXISTS shardtelemetry.metric_points
(
    tenant String, signal String, timestamp DateTime64(9, 'UTC'),
    start_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64,
    resource_id Nullable(String), scope_id Nullable(String), series_id Nullable(String),
    name Nullable(String), description Nullable(String), unit Nullable(String),
    metric_kind Nullable(String), temporality Nullable(Int32), monotonic Nullable(Bool),
    flags Nullable(UInt32), value_type Nullable(String), scalar_integer Nullable(Int64),
    scalar_double_bits Nullable(UInt64), value_json Nullable(String), labels Map(String, String),
    metadata Map(String, String), attributes Map(String, String),
    resource_attributes Map(String, String), scope_attributes Map(String, String),
    attribute_ids Map(String, String), resource_attribute_ids Map(String, String),
    scope_attribute_ids Map(String, String), attributes_json Nullable(String),
    resource_attributes_json Nullable(String), scope_attributes_json Nullable(String),
    exemplars_json Nullable(String)
)
ENGINE = ShardTelemetry(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=metric_points',
    'ArrowStream',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
);

CREATE TABLE IF NOT EXISTS shardtelemetry.metric_exemplars
(
    tenant String, signal String, timestamp DateTime64(9, 'UTC'),
    parent_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64,
    resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String),
    span_id Nullable(String), series_id Nullable(String), ordinal Nullable(UInt32),
    name Nullable(String), value_type Nullable(String), scalar_integer Nullable(Int64),
    scalar_double_bits Nullable(UInt64), attributes Map(String, String),
    attribute_ids Map(String, String), attributes_json Nullable(String),
    labels Map(String, String), metadata Map(String, String)
)
ENGINE = ShardTelemetry(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=metric_exemplars',
    'ArrowStream',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
);

-- A trace is the current winning set of spans sharing a tenant and trace ID.
CREATE VIEW IF NOT EXISTS shardtelemetry.traces AS
SELECT
    tenant,
    trace_id,
    min(timestamp) AS start_timestamp,
    max(end_timestamp) AS end_timestamp,
    max(duration_nanos) AS max_duration_nanos,
    count() AS span_count,
    countIf(status_code = 2) AS error_count,
    argMinIf(name, timestamp, parent_span_id IS NULL) AS root_name,
    argMinIf(resource_attributes['service.name'], timestamp, parent_span_id IS NULL)
        AS root_service_name
FROM shardtelemetry.spans
GROUP BY tenant, trace_id;
