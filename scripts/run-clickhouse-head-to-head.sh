#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SHARD_TELEMETRY_REPOSITORY=${SHARD_TELEMETRY_REPOSITORY:-$(cd -- "$SCRIPT_DIR/.." && pwd)}
SOURCE=${SOURCE:?set SOURCE to an immutable Docker json-file input}
EXPECTED_SHA256=${EXPECTED_SHA256:-}
EXPECTED_FILE_BYTES=${EXPECTED_FILE_BYTES:-}
SOURCE_LIMIT_BYTES=${SOURCE_LIMIT_BYTES:-1073741824}
CORE_COUNT=${CORE_COUNT:-16}
QUERY_ITERATIONS=${QUERY_ITERATIONS:-20}
REQUIRED_NOFILE=${REQUIRED_NOFILE:-262144}
SHARD_TELEMETRY_SERVER=${SHARD_TELEMETRY_SERVER:-$SHARD_TELEMETRY_REPOSITORY/target/release/shard-telemetry-server}
SHARD_TELEMETRY_LOAD_BIN=${SHARD_TELEMETRY_LOAD_BIN:-$SHARD_TELEMETRY_REPOSITORY/target/release/shard-telemetry-loki-load}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:-sha256:422be85ae7344058369cdd366ac0efea9daa8428b55c9cf50258e83a7d12fcb3}
RESULT_ROOT=${RESULT_ROOT:-$SHARD_TELEMETRY_REPOSITORY/benchmark-results/clickhouse-head-to-head}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
SHARD_HTTP_ADDRESS=${SHARD_HTTP_ADDRESS:-127.0.0.1:32100}
SHARD_NATIVE_ADDRESS=${SHARD_NATIVE_ADDRESS:-127.0.0.1:32101}
SHARD_OTLP_GRPC_ADDRESS=${SHARD_OTLP_GRPC_ADDRESS:-127.0.0.1:34317}
SHARD_OTLP_HTTP_ADDRESS=${SHARD_OTLP_HTTP_ADDRESS:-127.0.0.1:34318}
CLICKHOUSE_HTTP_PORT=18123
CLICKHOUSE_TCP_PORT=19000
CLICKHOUSE_INTERSERVER_PORT=19009

CLICKHOUSE_CONFIG=$SCRIPT_DIR/clickhouse-benchmark.xml
CLICKHOUSE_PORT_CONFIG=$SCRIPT_DIR/clickhouse-benchmark-ports.xml
CLICKHOUSE_INGEST=$SCRIPT_DIR/clickhouse-ingest-range.sh
TENANT=benchmark
CLICKHOUSE_TOKEN=shard-telemetry-clickhouse-head-to-head-token

if [[ $CORE_COUNT -ne 16 ]]; then
    echo "this comparison is fixed at 16 physical cores" >&2
    exit 2
fi
if [[ $QUERY_ITERATIONS -lt 1 ]]; then
    echo "QUERY_ITERATIONS must be nonzero" >&2
    exit 2
fi
if [[ $REQUIRED_NOFILE -lt 4096 ]]; then
    echo "REQUIRED_NOFILE must be at least 4096" >&2
    exit 2
fi
HARD_NOFILE=$(ulimit -Hn)
if [[ $HARD_NOFILE != unlimited && $HARD_NOFILE -lt $REQUIRED_NOFILE ]]; then
    echo "hard nofile limit $HARD_NOFILE is below required $REQUIRED_NOFILE" >&2
    exit 2
fi
if [[ $(ulimit -Sn) -lt $REQUIRED_NOFILE ]]; then
    ulimit -Sn "$REQUIRED_NOFILE"
fi
for command in awk cmp curl dd docker grep lscpu sha256sum ss stat taskset; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
for path in \
    "$SOURCE" \
    "$SHARD_TELEMETRY_SERVER" \
    "$SHARD_TELEMETRY_LOAD_BIN" \
    "$CLICKHOUSE_CONFIG" \
    "$CLICKHOUSE_PORT_CONFIG" \
    "$CLICKHOUSE_INGEST"; do
    [[ -e $path ]] || {
        echo "required path does not exist: $path" >&2
        exit 2
    }
done
for executable in "$SHARD_TELEMETRY_SERVER" "$SHARD_TELEMETRY_LOAD_BIN"; do
    [[ -x $executable ]] || {
        echo "required binary is not executable: $executable" >&2
        exit 2
    }
done

mapfile -t PHYSICAL_CPUS < <(
    lscpu -p=CPU,CORE |
        awk -F, '!/^#/ && !seen[$2]++ { print $1 }' |
        awk -v count="$CORE_COUNT" 'NR <= count'
)
[[ ${#PHYSICAL_CPUS[@]} -eq $CORE_COUNT ]] || {
    echo "could not resolve $CORE_COUNT physical CPUs" >&2
    exit 2
}
CPU_SET=$(IFS=,; echo "${PHYSICAL_CPUS[*]}")

SOURCE_FILE_BYTES=$(stat -c %s "$SOURCE")
if [[ -n $EXPECTED_FILE_BYTES && $SOURCE_FILE_BYTES -ne $EXPECTED_FILE_BYTES ]]; then
    echo "source byte length mismatch" >&2
    exit 2
fi
[[ $SOURCE_LIMIT_BYTES -gt 0 && $SOURCE_LIMIT_BYTES -le $SOURCE_FILE_BYTES ]] || {
    echo "source limit must be in 1..=$SOURCE_FILE_BYTES" >&2
    exit 2
}
SOURCE_SHA256=$(sha256sum "$SOURCE" | awk '{ print $1 }')
if [[ -n $EXPECTED_SHA256 && $SOURCE_SHA256 != "$EXPECTED_SHA256" ]]; then
    echo "source SHA-256 mismatch" >&2
    exit 2
fi
IMAGE_ID=$(docker image inspect "$CLICKHOUSE_IMAGE" --format '{{.Id}}')
[[ $IMAGE_ID == "$CLICKHOUSE_IMAGE" ]] || {
    echo "ClickHouse image mismatch" >&2
    exit 2
}

RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "result directory already exists: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR/shard-data" "$RUN_DIR/clickhouse-data" "$RUN_DIR/clickhouse-logs"
exec > >(tee "$RUN_DIR/harness.log") 2>&1

CH_CONTAINER="shard-telemetry-clickhouse-${RUN_ID//[^a-zA-Z0-9_.-]/-}"
CH_STARTED=0
SHARD_STARTED=0
cleanup() {
    if [[ $CH_STARTED -eq 1 ]]; then
        docker logs "$CH_CONTAINER" >"$RUN_DIR/clickhouse-container.log" 2>&1 || true
        docker stop --time 60 "$CH_CONTAINER" >/dev/null 2>&1 || true
        docker rm "$CH_CONTAINER" >/dev/null 2>&1 || true
    fi
    if [[ $SHARD_STARTED -eq 1 ]] && kill -0 "$SHARD_PID" 2>/dev/null; then
        kill -TERM "$SHARD_PID" 2>/dev/null || true
        wait "$SHARD_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

{
    echo "run_id=$RUN_ID"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "source=$SOURCE"
    echo "source_file_bytes=$SOURCE_FILE_BYTES"
    echo "source_limit_bytes=$SOURCE_LIMIT_BYTES"
    echo "source_sha256=$SOURCE_SHA256"
    echo "physical_cpu_set=$CPU_SET"
    echo "core_count=$CORE_COUNT"
    echo "query_iterations=$QUERY_ITERATIONS"
    echo "soft_nofile=$(ulimit -Sn)"
    echo "hard_nofile=$(ulimit -Hn)"
    echo "shard_telemetry_server=$SHARD_TELEMETRY_SERVER"
    echo "shard_telemetry_server_sha256=$(sha256sum "$SHARD_TELEMETRY_SERVER" | awk '{ print $1 }')"
    echo "shard_telemetry_load_bin=$SHARD_TELEMETRY_LOAD_BIN"
    echo "shard_telemetry_load_sha256=$(sha256sum "$SHARD_TELEMETRY_LOAD_BIN" | awk '{ print $1 }')"
    echo "clickhouse_image=$CLICKHOUSE_IMAGE"
    echo "clickhouse_image_id=$IMAGE_ID"
    echo "clickhouse_http_port=$CLICKHOUSE_HTTP_PORT"
    echo "clickhouse_tcp_port=$CLICKHOUSE_TCP_PORT"
    echo "clickhouse_interserver_port=$CLICKHOUSE_INTERSERVER_PORT"
    echo "clickhouse_ingest_format=LineAsString+isValidJSON+JSONExtractTuple"
    echo "execution_tier=native_linux_host_performance"
    echo "replay_claim=none"
    echo "kernel=$(uname -srmo)"
    lscpu
} >"$RUN_DIR/provenance.txt"

printf '%s\n' "$CLICKHOUSE_TOKEN" >"$RUN_DIR/clickhouse-token"
chmod 0600 "$RUN_DIR/clickhouse-token"

echo "ShardTelemetry: starting on CPUs $CPU_SET"
taskset -c "$CPU_SET" "$SHARD_TELEMETRY_SERVER" \
    --insecure-development-mode \
    --listen "$SHARD_HTTP_ADDRESS" \
    --native-listen "$SHARD_NATIVE_ADDRESS" \
    --otlp-grpc-listen "$SHARD_OTLP_GRPC_ADDRESS" \
    --otlp-http-listen "$SHARD_OTLP_HTTP_ADDRESS" \
    --default-tenant "$TENANT" \
    --data-directory "$RUN_DIR/shard-data" \
    --shards "$CORE_COUNT" \
    --tenant-partitions 256 \
    --append-linger-micros 0 \
    --native-durable-ack \
    --clickhouse-token-file "$RUN_DIR/clickhouse-token" \
    >"$RUN_DIR/shard-server.log" 2>&1 &
SHARD_PID=$!
SHARD_STARTED=1
for _ in $(seq 1 120); do
    if curl --fail --silent http://127.0.0.1:32100/ready >"$RUN_DIR/shard-ready.txt"; then
        break
    fi
    kill -0 "$SHARD_PID" 2>/dev/null || {
        echo "ShardTelemetry exited before readiness" >&2
        exit 1
    }
    sleep 0.25
done
curl --fail --silent http://127.0.0.1:32100/ready >/dev/null

echo "ShardTelemetry: prewarming $SOURCE_LIMIT_BYTES source bytes"
dd if="$SOURCE" of=/dev/null bs=64M iflag=count_bytes count="$SOURCE_LIMIT_BYTES" status=none
echo "ShardTelemetry: ingesting through native v1"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/shard-ingest-time.txt" \
    taskset -c "$CPU_SET" "$SHARD_TELEMETRY_LOAD_BIN" "$SOURCE" \
    --host 127.0.0.1 \
    --port 32101 \
    --protocol native \
    --workers "$CORE_COUNT" \
    --batch-bytes 8388608 \
    --pipeline-depth 8 \
    --tenant "$TENANT" \
    --limit-bytes "$SOURCE_LIMIT_BYTES" \
    >"$RUN_DIR/shard-ingest.txt"
cat "$RUN_DIR/shard-ingest.txt"

SHARD_SOURCE_BYTES=$(awk -F': ' '$1 == "source bytes" { print $2 }' "$RUN_DIR/shard-ingest.txt")
SHARD_RECORDS=$(awk -F': ' '$1 == "records" { print $2 }' "$RUN_DIR/shard-ingest.txt")
SHARD_MALFORMED=$(awk -F': ' '$1 == "malformed records skipped" { print $2 }' "$RUN_DIR/shard-ingest.txt")
[[ -n $SHARD_SOURCE_BYTES && -n $SHARD_RECORDS && -n $SHARD_MALFORMED ]] || {
    echo "could not parse ShardTelemetry ingestion report" >&2
    exit 1
}
curl --fail --silent --show-error -X POST http://127.0.0.1:32100/flush \
    >"$RUN_DIR/shard-flush.txt"
curl --fail --silent http://127.0.0.1:32100/metrics >"$RUN_DIR/shard-metrics.txt"
du -sb "$RUN_DIR/shard-data" >"$RUN_DIR/shard-data-du.txt"

echo "ClickHouse: starting stock server on CPUs $CPU_SET"
if ss -ltn | grep -Eq ":($CLICKHOUSE_HTTP_PORT|$CLICKHOUSE_TCP_PORT|$CLICKHOUSE_INTERSERVER_PORT)[[:space:]]"; then
    echo "one or more benchmark ClickHouse host-network ports are already in use" >&2
    exit 2
fi
docker run --detach \
    --name "$CH_CONTAINER" \
    --network host \
    --cpuset-cpus "$CPU_SET" \
    --ulimit nofile=262144:262144 \
    --env CLICKHOUSE_SKIP_USER_SETUP=1 \
    --volume "$SOURCE:/benchmark/input.json:ro" \
    --volume "$SCRIPT_DIR:/benchmark-scripts:ro" \
    --volume "$CLICKHOUSE_CONFIG:/etc/clickhouse-server/config.d/benchmark.xml:ro" \
    --volume "$CLICKHOUSE_PORT_CONFIG:/etc/clickhouse-server/config.d/ports.xml:ro" \
    --volume "$RUN_DIR/clickhouse-data:/var/lib/clickhouse" \
    --volume "$RUN_DIR/clickhouse-logs:/var/log/clickhouse-server" \
    "$CLICKHOUSE_IMAGE" >"$RUN_DIR/clickhouse-container-id.txt"
CH_STARTED=1
for _ in $(seq 1 120); do
    if docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
        --query 'SELECT 1' >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
    --query 'SELECT version()' \
    >"$RUN_DIR/clickhouse-version.txt"
LOG_STRUCTURE="tenant String, signal String, timestamp DateTime64(9, 'UTC'), observed_timestamp Nullable(DateTime64(9, 'UTC')), partition UInt32, offset UInt64, resource_id Nullable(String), scope_id Nullable(String), trace_id Nullable(String), span_id Nullable(String), message Nullable(String), body_json Nullable(String), severity_number Nullable(Int32), severity_text Nullable(String), event_name Nullable(String), flags Nullable(UInt32), dropped_attributes_count Nullable(UInt32), labels Map(String, String), metadata Map(String, String), attributes Map(String, String), resource_attributes Map(String, String), scope_attributes Map(String, String), attribute_ids Map(String, String), resource_attribute_ids Map(String, String), scope_attribute_ids Map(String, String), attributes_json Nullable(String), resource_attributes_json Nullable(String), scope_attributes_json Nullable(String)"
SHARD_URL="http://127.0.0.1:32100/shardtelemetry/api/v1/clickhouse/scan?relation=logs&wire=rowbinary"
docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" --multiquery --query "
CREATE DATABASE benchmark;
CREATE TABLE benchmark.logs
(
    time DateTime64(9, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    stream LowCardinality(String) CODEC(ZSTD(1)),
    log String CODEC(ZSTD(1)),
    INDEX log_text log TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = lower(log))
)
ENGINE = MergeTree
ORDER BY (stream, time)
SETTINGS index_granularity = 8192, fsync_after_insert = 1;
CREATE TABLE benchmark.shard_logs ($LOG_STRUCTURE)
ENGINE = URL('$SHARD_URL', 'RowBinary', headers(
    'Authorization' = 'Bearer $CLICKHOUSE_TOKEN',
    'X-Scope-OrgID' = '$TENANT'));
"

echo "ClickHouse: prewarming the exact $SHARD_SOURCE_BYTES-byte accepted range"
dd if="$SOURCE" of=/dev/null bs=64M iflag=count_bytes count="$SHARD_SOURCE_BYTES" status=none
echo "ClickHouse: ingesting the equal source range"
/usr/bin/time -f 'wall_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kib=%M' \
    -o "$RUN_DIR/clickhouse-ingest-time.txt" \
    docker exec \
    --env CORE_COUNT="$CORE_COUNT" \
    --env SOURCE_SKIP_BYTES=0 \
    --env SOURCE_BYTES="$SHARD_SOURCE_BYTES" \
    --env CLICKHOUSE_CLIENT_PORT="$CLICKHOUSE_TCP_PORT" \
    "$CH_CONTAINER" \
    /bin/bash /benchmark-scripts/clickhouse-ingest-range.sh

docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
    --query 'SELECT count() FROM benchmark.logs' \
    >"$RUN_DIR/clickhouse-row-count.txt"
docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
    --query 'SELECT count() FROM benchmark.shard_logs' \
    >"$RUN_DIR/shard-row-count.txt"
CLICKHOUSE_RECORDS=$(tr -d '[:space:]' <"$RUN_DIR/clickhouse-row-count.txt")
SHARD_QUERY_RECORDS=$(tr -d '[:space:]' <"$RUN_DIR/shard-row-count.txt")
[[ $CLICKHOUSE_RECORDS -eq $SHARD_RECORDS && $SHARD_QUERY_RECORDS -eq $SHARD_RECORDS ]] || {
    echo "record-count mismatch: loader=$SHARD_RECORDS URL=$SHARD_QUERY_RECORDS ClickHouse=$CLICKHOUSE_RECORDS" >&2
    exit 1
}

docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" --query "
SELECT sum(rows), sum(bytes_on_disk), sum(data_compressed_bytes),
       sum(data_uncompressed_bytes), sum(marks_bytes), count()
FROM system.parts
WHERE active AND database = 'benchmark' AND table = 'logs'
FORMAT TSVWithNames
" >"$RUN_DIR/clickhouse-parts.tsv"

run_pair() {
    local name=$1 shard_query=$2 clickhouse_query=$3
    echo "Query: $name"
    docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
        --query "$shard_query FORMAT TabSeparatedRaw" \
        >"$RUN_DIR/shard-$name-results.tsv"
    docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
        --query "$clickhouse_query FORMAT TabSeparatedRaw" \
        >"$RUN_DIR/clickhouse-$name-results.tsv"
    cmp "$RUN_DIR/shard-$name-results.tsv" "$RUN_DIR/clickhouse-$name-results.tsv"
    sha256sum "$RUN_DIR/shard-$name-results.tsv" "$RUN_DIR/clickhouse-$name-results.tsv" \
        >"$RUN_DIR/$name-result-sha256.txt"

    docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
        --query "$shard_query SETTINGS max_threads=$CORE_COUNT,use_query_cache=0 FORMAT Null" \
        >/dev/null
    docker exec "$CH_CONTAINER" clickhouse-benchmark --port "$CLICKHOUSE_TCP_PORT" \
        --concurrency 1 \
        --iterations "$QUERY_ITERATIONS" \
        --query "$shard_query SETTINGS max_threads=$CORE_COUNT,use_query_cache=0 FORMAT Null" \
        >"$RUN_DIR/shard-$name-warm.txt" 2>&1

    docker exec "$CH_CONTAINER" clickhouse-client --port "$CLICKHOUSE_TCP_PORT" \
        --query "$clickhouse_query SETTINGS max_threads=$CORE_COUNT,use_query_cache=0 FORMAT Null" \
        >/dev/null
    docker exec "$CH_CONTAINER" clickhouse-benchmark --port "$CLICKHOUSE_TCP_PORT" \
        --concurrency 1 \
        --iterations "$QUERY_ITERATIONS" \
        --query "$clickhouse_query SETTINGS max_threads=$CORE_COUNT,use_query_cache=0 FORMAT Null" \
        >"$RUN_DIR/clickhouse-$name-warm.txt" 2>&1
}

SHARD_PROJECT="toUnixTimestamp64Nano(timestamp), hex(metadata['docker_stream']), hex(ifNull(message, ''))"
CLICKHOUSE_PROJECT="toUnixTimestamp64Nano(time), hex(stream), hex(log)"
run_pair latest \
    "SELECT $SHARD_PROJECT FROM benchmark.shard_logs ORDER BY timestamp DESC LIMIT 100" \
    "SELECT $CLICKHOUSE_PROJECT FROM benchmark.logs ORDER BY time DESC LIMIT 100"
run_pair stream \
    "SELECT $SHARD_PROJECT FROM benchmark.shard_logs WHERE metadata['docker_stream'] = 'stderr' ORDER BY timestamp DESC LIMIT 100" \
    "SELECT $CLICKHOUSE_PROJECT FROM benchmark.logs WHERE stream = 'stderr' ORDER BY time DESC LIMIT 100"
run_pair cannot \
    "SELECT $SHARD_PROJECT FROM benchmark.shard_logs WHERE hasTokenCaseInsensitive(ifNull(message, ''), 'cannot') ORDER BY timestamp DESC LIMIT 100" \
    "SELECT $CLICKHOUSE_PROJECT FROM benchmark.logs WHERE hasTokenCaseInsensitive(log, 'cannot') ORDER BY time DESC LIMIT 100"

du -sb "$RUN_DIR/clickhouse-data" >"$RUN_DIR/clickhouse-data-du.txt"

{
    printf 'query\tengine\tp50_seconds\tp95_seconds\tp99_seconds\n'
    for name in latest stream cannot; do
        for engine in shard clickhouse; do
            latency_file="$RUN_DIR/$engine-$name-warm.txt"
            p50=$(awk '$1 == "50%" { value = $2 } END { print value }' "$latency_file")
            p95=$(awk '$1 == "95%" { value = $2 } END { print value }' "$latency_file")
            p99=$(awk '$1 == "99%" { value = $2 } END { print value }' "$latency_file")
            [[ -n $p50 && -n $p95 && -n $p99 ]] || {
                echo "could not parse query latency: $latency_file" >&2
                exit 1
            }
            printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$engine" "$p50" "$p95" "$p99"
        done
    done
} >"$RUN_DIR/query-latency-summary.tsv"

SHARD_STORED_BYTES=$(awk '{ print $1 }' "$RUN_DIR/shard-data-du.txt")
CLICKHOUSE_STORED_BYTES=$(awk -F'\t' 'NR == 2 { print $2 }' "$RUN_DIR/clickhouse-parts.tsv")
SHARD_ELAPSED=$(awk -F': ' '$1 == "ingest elapsed seconds" { print $2 }' "$RUN_DIR/shard-ingest.txt")
CLICKHOUSE_ELAPSED=$(awk -F= '$1 == "wall_seconds" { print $2 }' "$RUN_DIR/clickhouse-ingest-time.txt")
{
    printf 'engine\tsource_bytes\trecords\tstored_bytes\telapsed_seconds\tthroughput_mib_s\tcompression_ratio\n'
    awk -v source="$SHARD_SOURCE_BYTES" -v records="$SHARD_RECORDS" \
        -v stored="$SHARD_STORED_BYTES" -v elapsed="$SHARD_ELAPSED" \
        'BEGIN { printf "ShardTelemetry-live\t%.0f\t%.0f\t%.0f\t%.6f\t%.2f\t%.2f\n", source, records, stored, elapsed, source / 1048576 / elapsed, source / stored }'
    awk -v source="$SHARD_SOURCE_BYTES" -v records="$CLICKHOUSE_RECORDS" \
        -v stored="$CLICKHOUSE_STORED_BYTES" -v elapsed="$CLICKHOUSE_ELAPSED" \
        'BEGIN { printf "ClickHouse-MergeTree\t%.0f\t%.0f\t%.0f\t%.6f\t%.2f\t%.2f\n", source, records, stored, elapsed, source / 1048576 / elapsed, source / stored }'
} >"$RUN_DIR/summary.tsv"

{
    printf 'engine\tdirectory_bytes\tactive_part_bytes\tcompressed_column_bytes\tuncompressed_column_bytes\tmarks_bytes\tactive_parts\n'
    printf 'ShardTelemetry\t%s\t%s\tNA\tNA\tNA\tNA\n' "$SHARD_STORED_BYTES" "$SHARD_STORED_BYTES"
    awk -F'\t' -v directory="$(awk '{ print $1 }' "$RUN_DIR/clickhouse-data-du.txt")" \
        'NR == 2 { printf "ClickHouse\t%s\t%s\t%s\t%s\t%s\t%s\n", directory, $2, $3, $4, $5, $6 }' \
        "$RUN_DIR/clickhouse-parts.tsv"
} >"$RUN_DIR/storage-accounting.tsv"
cat "$RUN_DIR/summary.tsv"
printf 'passed\n' >"$RUN_DIR/status.txt"
echo "results: $RUN_DIR"
