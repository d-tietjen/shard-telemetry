-- A stock ClickHouse binary can query every relation through url(...).
-- The complete pinned schemas are in shard-telemetry-engine.sql. Replace the
-- engine name with URL and add the explicit structure shown there.

SELECT tenant, trace_id, span_id, name, duration_nanos
FROM url(
    'http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan?relation=spans',
    ArrowStream,
    'tenant String, signal String, timestamp DateTime64(9, \'UTC\'), end_timestamp Nullable(DateTime64(9, \'UTC\')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), parent_span_id Nullable(String), name Nullable(String), kind Nullable(Int32), duration_nanos Nullable(UInt64), status_code Nullable(Int32), status_message Nullable(String), trace_state Nullable(String), flags Nullable(UInt32), dropped_attributes_count Nullable(UInt32), dropped_events_count Nullable(UInt32), dropped_links_count Nullable(UInt32), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), resource_attribute_ids Map(String, String), scope_attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String), events_json Nullable(String), links_json Nullable(String)',
    headers('Authorization' = 'Bearer REPLACE_FROM_SECRET_STORE', 'X-Scope-OrgID' = 'fake')
)
WHERE status_code = 2
ORDER BY timestamp DESC
LIMIT 100;

-- Cross-signal joins use exact binary-ID renderings and content-addressed
-- resource/scope/typed-attribute identities. The StorageShardTelemetry DDL
-- defines convenient persistent external tables for these queries.
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
