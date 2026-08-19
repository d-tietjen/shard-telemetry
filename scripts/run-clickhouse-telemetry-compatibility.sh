#!/usr/bin/env bash
set -euo pipefail
shopt -u patsub_replacement 2>/dev/null || true

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
"$SCRIPT_DIR/run-clickhouse-compatibility.sh"

if [[ -z ${SHARD_TELEMETRY_CLICKHOUSE_TOKEN:-} ]]; then
    : "${SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE:?set SHARD_TELEMETRY_CLICKHOUSE_TOKEN or SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE}"
    SHARD_TELEMETRY_CLICKHOUSE_TOKEN=$(<"$SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE")
fi

CLICKHOUSE_BIN=${CLICKHOUSE_BIN:-clickhouse}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-}
CLICKHOUSE_NETWORK=${CLICKHOUSE_NETWORK:-host}
SHARD_TELEMETRY_URL=${SHARD_TELEMETRY_URL:-http://127.0.0.1:3100/shardtelemetry/api/v1/clickhouse/scan}
SHARD_TELEMETRY_TENANT=${SHARD_TELEMETRY_TENANT:-fake}
REQUIRE_NONEMPTY=${SHARD_TELEMETRY_REQUIRE_NONEMPTY:-1}

run_clickhouse() {
    if [[ -n $CLICKHOUSE_IMAGE ]]; then
        docker run --rm --network "$CLICKHOUSE_NETWORK" -i "$CLICKHOUSE_IMAGE" clickhouse "$@"
    else
        "$CLICKHOUSE_BIN" "$@"
    fi
}

escape_sql() {
    local value=$1
    value=${value//\\/\\\\}
    value=${value//\'/\'\'}
    printf '%s' "$value"
}

relation_structure() {
    case "$1" in
        logs)
            printf '%s' "tenant String, signal String, timestamp DateTime64(9, 'UTC'), observed_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), message Nullable(String), body_json Nullable(String), severity_number Nullable(Int32), severity_text Nullable(String), event_name Nullable(String), flags Nullable(UInt32), dropped_attributes_count Nullable(UInt32), labels Map(String, String), metadata Map(String, String), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), resource_attribute_ids Map(String, String), scope_attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)"
            ;;
        spans)
            printf '%s' "tenant String, signal String, timestamp DateTime64(9, 'UTC'), end_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), parent_span_id Nullable(String), name Nullable(String), kind Nullable(Int32), duration_nanos Nullable(UInt64), status_code Nullable(Int32), status_message Nullable(String), trace_state Nullable(String), flags Nullable(UInt32), dropped_attributes_count Nullable(UInt32), dropped_events_count Nullable(UInt32), dropped_links_count Nullable(UInt32), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), resource_attribute_ids Map(String, String), scope_attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String), events_json Nullable(String), links_json Nullable(String)"
            ;;
        span_events)
            printf '%s' "tenant String, signal String, timestamp DateTime64(9, 'UTC'), parent_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), ordinal Nullable(UInt32), name Nullable(String), dropped_attributes_count Nullable(UInt32), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)"
            ;;
        span_links)
            printf '%s' "tenant String, signal String, timestamp DateTime64(9, 'UTC'), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), ordinal Nullable(UInt32), linked_trace_id Nullable(String), linked_span_id Nullable(String), trace_state Nullable(String), flags Nullable(UInt32), dropped_attributes_count Nullable(UInt32), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)"
            ;;
        metric_points)
            printf '%s' "tenant String, signal String, timestamp DateTime64(9, 'UTC'), start_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), series_id Nullable(String), name Nullable(String), description Nullable(String), unit Nullable(String), metric_kind Nullable(String), temporality Nullable(Int32), monotonic Nullable(Bool), flags Nullable(UInt32), value_type Nullable(String), scalar_integer Nullable(Int64), scalar_double_bits Nullable(UInt64), value_json Nullable(String), labels Map(String, String), metadata Map(String, String), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), resource_attribute_ids Map(String, String), scope_attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String), exemplars_json Nullable(String)"
            ;;
        metric_exemplars)
            printf '%s' "tenant String, signal String, timestamp DateTime64(9, 'UTC'), parent_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), series_id Nullable(String), ordinal Nullable(UInt32), name Nullable(String), value_type Nullable(String), scalar_integer Nullable(Int64), scalar_double_bits Nullable(UInt64), attributes Map(String, String), attribute_ids Map(String, String), attributes_json Nullable(String), labels Map(String, String), metadata Map(String, String)"
            ;;
        *) return 2 ;;
    esac
}

relation_url() {
    local separator='?'
    [[ $SHARD_TELEMETRY_URL == *\?* ]] && separator='&'
    printf '%s%srelation=%s' "$SHARD_TELEMETRY_URL" "$separator" "$1"
}

source_sql() {
    local relation=$1
    local url structure
    url=$(escape_sql "$(relation_url "$relation")")
    structure=$(escape_sql "$(relation_structure "$relation")")
    printf "url('%s&wire=rowbinary', 'RowBinary', '%s', headers('Authorization' = 'Bearer %s', 'X-Scope-OrgID' = '%s'))" \
        "$url" "$structure" \
        "$(escape_sql "$SHARD_TELEMETRY_CLICKHOUSE_TOKEN")" "$(escape_sql "$SHARD_TELEMETRY_TENANT")"
}

RESULT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/shard-telemetry-all-signals-compat.XXXXXX")
cleanup() {
    rm -rf -- "$RESULT_DIR"
}
trap cleanup EXIT INT TERM

run_case() {
    local relation=$1 name=$2 query=$3 source reference external_output reference_output
    source=$(source_sql "$relation")
    reference="reference_$relation"
    external_output="$RESULT_DIR/$name.external"
    reference_output="$RESULT_DIR/$name.reference"

    {
        printf '%s FORMAT JSONCompactEachRow\n' "${query//__TABLE__/$source}"
    } | run_clickhouse local --multiquery >"$external_output"

    {
        printf 'CREATE TABLE %s (%s) ENGINE = Memory;\n' "$reference" "$(relation_structure "$relation")"
        printf 'INSERT INTO %s SELECT * FROM %s;\n' "$reference" "$source"
        printf '%s FORMAT JSONCompactEachRow\n' "${query//__TABLE__/$reference}"
    } | run_clickhouse local --multiquery >"$reference_output"

    if ! cmp -s "$external_output" "$reference_output"; then
        echo "compatibility mismatch: $name" >&2
        diff -u "$reference_output" "$external_output" >&2 || true
        exit 1
    fi
    echo "PASS $name"
}

run_cross_case() {
    local name=$1 query=$2 external_query reference_query external_output reference_output
    external_query=${query//__LOGS__/$(source_sql logs)}
    external_query=${external_query//__SPANS__/$(source_sql spans)}
    external_query=${external_query//__METRICS__/$(source_sql metric_points)}
    external_query=${external_query//__EXEMPLARS__/$(source_sql metric_exemplars)}
    reference_query=${query//__LOGS__/reference_logs}
    reference_query=${reference_query//__SPANS__/reference_spans}
    reference_query=${reference_query//__METRICS__/reference_metric_points}
    reference_query=${reference_query//__EXEMPLARS__/reference_metric_exemplars}
    external_output="$RESULT_DIR/$name.external"
    reference_output="$RESULT_DIR/$name.reference"

    {
        printf '%s FORMAT JSONCompactEachRow\n' "$external_query"
    } | run_clickhouse local --multiquery >"$external_output"

    {
        for relation in logs spans metric_points metric_exemplars; do
            printf 'CREATE TABLE reference_%s (%s) ENGINE = Memory;\n' \
                "$relation" "$(relation_structure "$relation")"
            printf 'INSERT INTO reference_%s SELECT * FROM %s;\n' \
                "$relation" "$(source_sql "$relation")"
        done
        printf '%s FORMAT JSONCompactEachRow\n' "$reference_query"
    } | run_clickhouse local --multiquery >"$reference_output"

    if ! cmp -s "$external_output" "$reference_output"; then
        echo "compatibility mismatch: $name" >&2
        diff -u "$reference_output" "$external_output" >&2 || true
        exit 1
    fi
    echo "PASS $name"
}

if [[ $REQUIRE_NONEMPTY -eq 1 ]]; then
    for relation in spans span_events span_links metric_points metric_exemplars; do
        count=$(
            {
                printf 'SELECT count() FROM %s;\n' "$(source_sql "$relation")"
            } | run_clickhouse local --multiquery
        )
        [[ $count -gt 0 ]] || {
            echo "compatibility fixture has no rows for $relation" >&2
            exit 1
        }
    done
fi

run_case spans span-aggregates \
    "SELECT count(), uniqExact(trace_id), sum(duration_nanos), countIf(status_code = 2) FROM __TABLE__"
run_case spans span-window \
    "SELECT trace_id, span_id, row_number() OVER (PARTITION BY trace_id ORDER BY timestamp, offset) FROM __TABLE__ ORDER BY trace_id, timestamp, offset"
run_case spans span-map-filter \
    "SELECT trace_id, span_id FROM __TABLE__ WHERE resource_attributes['service.name'] != '' AND attributes['http.request.method'] != '' ORDER BY trace_id, span_id"
run_case span_events event-group \
    "SELECT name, count(), uniqExact(trace_id) FROM __TABLE__ GROUP BY name ORDER BY name"
run_case span_links link-topology \
    "SELECT trace_id, linked_trace_id, count() FROM __TABLE__ GROUP BY trace_id, linked_trace_id ORDER BY trace_id, linked_trace_id"
run_case metric_points metric-aggregates \
    "SELECT name, metric_kind, count(), uniqExact(series_id), countIf(value_type = 'integer') FROM __TABLE__ GROUP BY name, metric_kind ORDER BY name, metric_kind"
run_case metric_points metric-window \
    "SELECT series_id, timestamp, row_number() OVER (PARTITION BY series_id ORDER BY timestamp, offset) FROM __TABLE__ ORDER BY series_id, timestamp, offset"
run_case metric_points metric-map-filter \
    "SELECT series_id, timestamp FROM __TABLE__ WHERE labels['service.name'] != '' OR attributes['service.name'] != '' ORDER BY series_id, timestamp"
run_case metric_exemplars exemplar-group \
    "SELECT trace_id, span_id, count(), uniqExact(series_id) FROM __TABLE__ GROUP BY trace_id, span_id ORDER BY trace_id, span_id"
run_cross_case log-span-correlation \
    "SELECT spans.name, count() FROM __LOGS__ AS logs INNER JOIN __SPANS__ AS spans ON logs.tenant = spans.tenant AND logs.trace_id = spans.trace_id AND logs.span_id = spans.span_id GROUP BY spans.name ORDER BY spans.name"
run_cross_case exemplar-span-correlation \
    "SELECT spans.name, uniqExact(exemplars.series_id), count() FROM __EXEMPLARS__ AS exemplars INNER JOIN __SPANS__ AS spans ON exemplars.tenant = spans.tenant AND exemplars.trace_id = spans.trace_id AND exemplars.span_id = spans.span_id GROUP BY spans.name ORDER BY spans.name"
run_cross_case resource-correlation \
    "SELECT logs.resource_id, count(), uniqExact(metrics.series_id) FROM __LOGS__ AS logs INNER JOIN __METRICS__ AS metrics ON logs.tenant = metrics.tenant AND logs.resource_id = metrics.resource_id GROUP BY logs.resource_id ORDER BY logs.resource_id"

echo "All-signal ClickHouse analytical compatibility passed"
