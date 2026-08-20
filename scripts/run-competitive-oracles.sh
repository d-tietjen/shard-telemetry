#!/usr/bin/env bash
set -euo pipefail

# Runs external compatibility oracles against one repository-generated OTLP
# fixture. This is a functional qualification campaign. It intentionally does
# not report latency as a cross-engine performance result: the fixture is too
# small and the engines have materially different storage models.

REPOSITORY=${SHARD_TELEMETRY_REPOSITORY:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}
COMPETITIVE_DIR=$REPOSITORY/competitive
SERVER=${SHARD_TELEMETRY_SERVER:-$REPOSITORY/target/release/shard-telemetry-server}
FIXTURE_BIN=${SHARD_TELEMETRY_FIXTURE_BIN:-$REPOSITORY/target/release/shard-telemetry-clickhouse-fixture}
RESULT_ROOT=${RESULT_ROOT:-/var/tmp/shard-telemetry-competitive}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
TIMEOUT_SECONDS=${COMPETITIVE_TIMEOUT_SECONDS:-120}
# An explicit override makes a failed campaign reproducible. The default is
# deliberately current: Prometheus correctly rejects fixed fixture samples
# that are too far ahead of its wall clock.
FIXTURE_TIMESTAMP_NANOS=${COMPETITIVE_FIXTURE_TIMESTAMP_NANOS:-$(date -u +%s%N)}

SHARD_HTTP=${SHARD_TELEMETRY_HTTP_ADDRESS:-127.0.0.1:32100}
SHARD_NATIVE=${SHARD_TELEMETRY_NATIVE_ADDRESS:-127.0.0.1:32101}
SHARD_OTLP_GRPC=${SHARD_TELEMETRY_OTLP_GRPC_ADDRESS:-127.0.0.1:34317}
SHARD_OTLP_HTTP=${SHARD_TELEMETRY_OTLP_HTTP_ADDRESS:-127.0.0.1:34318}
PROMETHEUS_HTTP=${PROMETHEUS_HTTP_ADDRESS:-127.0.0.1:39090}
LOKI_HTTP=${LOKI_HTTP_ADDRESS:-127.0.0.1:33100}
TEMPO_HTTP=${TEMPO_HTTP_ADDRESS:-127.0.0.1:33200}
TEMPO_OTLP_HTTP=${TEMPO_OTLP_HTTP_ADDRESS:-127.0.0.1:34319}

# shellcheck source=competitive/images.env
source "$COMPETITIVE_DIR/images.env"

if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
    echo "competitive oracle campaigns require Linux/x86_64" >&2
    exit 2
fi
if [[ $TIMEOUT_SECONDS -lt 1 ]]; then
    echo "COMPETITIVE_TIMEOUT_SECONDS must be positive" >&2
    exit 2
fi
[[ $FIXTURE_TIMESTAMP_NANOS =~ ^[1-9][0-9]*$ ]] || {
    echo "COMPETITIVE_FIXTURE_TIMESTAMP_NANOS must be a positive Unix-nanosecond integer" >&2
    exit 2
}
if (( ${#FIXTURE_TIMESTAMP_NANOS} > 19 )) \
    || { (( ${#FIXTURE_TIMESTAMP_NANOS} == 19 )) && [[ $FIXTURE_TIMESTAMP_NANOS > 9223372035854775807 ]]; }; then
    echo "COMPETITIVE_FIXTURE_TIMESTAMP_NANOS exceeds the supported signed-nanosecond range" >&2
    exit 2
fi
# All fixture points occur in the first few milliseconds after the base.
# Query two whole seconds later, so a base close to a second boundary cannot
# exclude its metric sample due to integer-second query rounding.
FIXTURE_QUERY_SECONDS=$(($FIXTURE_TIMESTAMP_NANOS / 1000000000 + 2))
for command in cmp curl docker jq sha256sum uname; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
for executable in "$SERVER" "$FIXTURE_BIN"; do
    [[ -x $executable ]] || {
        echo "required executable is not executable: $executable" >&2
        exit 2
    }
done

RUN_DIR=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIR ]] || {
    echo "refusing to overwrite result directory: $RUN_DIR" >&2
    exit 2
}
mkdir -p "$RUN_DIR"/{fixture,shard-data,prometheus-data,loki-data,tempo-data}
umask 077

SHARD_STARTED=0
declare -a CONTAINERS=()

cleanup() {
    local code=$?
    trap - EXIT INT TERM
    set +e
    for container in "${CONTAINERS[@]}"; do
        docker logs "$container" >"$RUN_DIR/$container.log" 2>&1
        docker rm --force "$container" >/dev/null 2>&1
    done
    if [[ $SHARD_STARTED -eq 1 ]] && kill -0 "$SHARD_PID" 2>/dev/null; then
        kill -TERM "$SHARD_PID" 2>/dev/null
        wait "$SHARD_PID" 2>/dev/null
    fi
    exit "$code"
}
trap cleanup EXIT INT TERM

wait_for_http() {
    local label=$1
    local url=$2
    local output=$3
    for _ in $(seq 1 "$TIMEOUT_SECONDS"); do
        if curl --fail --silent --show-error "$url" >"$output" 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "$label did not become ready: $url" >&2
    return 1
}

wait_for_json() {
    local label=$1
    local output=$2
    local assertion=$3
    shift 3
    local stderr_output="${output}.stderr"
    local jq_stderr_output="${output}.jq.stderr"
    for _ in $(seq 1 "$TIMEOUT_SECONDS"); do
        if "$@" >"$output" 2>"$stderr_output" \
            && jq -e "$assertion" "$output" > /dev/null 2>"$jq_stderr_output"; then
            return 0
        fi
        sleep 1
    done
    echo "$label did not satisfy its oracle assertion" >&2
    cat "$output" >&2 || true
    cat "$stderr_output" >&2 || true
    cat "$jq_stderr_output" >&2 || true
    return 1
}

pull_oracle() {
    local image=$1
    docker pull "$image" >"$RUN_DIR/$(tr '/:@' '____' <<<"$image").pull.log"
    docker image inspect "$image" --format '{{range .RepoDigests}}{{println .}}{{end}}' \
        | grep --fixed-strings --quiet "$image" || {
        echo "pulled image does not retain the required digest: $image" >&2
        return 1
    }
}

container_name() {
    local name=$1
    printf 'shard-telemetry-%s-%s' "$name" "${RUN_ID//[^a-zA-Z0-9_.-]/-}"
}

record_image() {
    local name=$1
    local image=$2
    printf '%s=%s\n' "$name" "$image" >>"$RUN_DIR/images.env"
    docker image inspect "$image" --format '{{.Id}} {{range .RepoDigests}}{{.}} {{end}}' \
        >"$RUN_DIR/$name-image.txt"
}

for image in "$CLICKHOUSE_PULL_IMAGE" "$PROMETHEUS_IMAGE" "$LOKI_IMAGE" "$TEMPO_IMAGE" "$DUCKDB_IMAGE"; do
    pull_oracle "$image"
done
test "$(docker image inspect "$CLICKHOUSE_IMAGE_ID" --format '{{.Id}}')" = "$CLICKHOUSE_IMAGE_ID"
record_image clickhouse "$CLICKHOUSE_PULL_IMAGE"
record_image prometheus "$PROMETHEUS_IMAGE"
record_image loki "$LOKI_IMAGE"
record_image tempo "$TEMPO_IMAGE"
record_image duckdb "$DUCKDB_IMAGE"

"$FIXTURE_BIN" \
    --output-directory "$RUN_DIR/fixture" \
    --timestamp-unix-nanos "$FIXTURE_TIMESTAMP_NANOS" \
    >"$RUN_DIR/fixture-files.txt"
sha256sum "$RUN_DIR"/fixture/*.pb >"$RUN_DIR/fixture-sha256.txt"
printf '%s\n' 'shard-telemetry-competitive-analytics-token' >"$RUN_DIR/analytics-token"
chmod 0600 "$RUN_DIR/analytics-token"

{
    printf 'run_id=%s\nstarted_utc=%s\nfixture_timestamp_unix_nanos=%s\nfixture_query_seconds=%s\n' \
        "$RUN_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$FIXTURE_TIMESTAMP_NANOS" "$FIXTURE_QUERY_SECONDS"
    printf 'repository=%s\nrevision=%s\n' "$REPOSITORY" "$(git -C "$REPOSITORY" rev-parse HEAD)"
    printf 'server=%s\nserver_sha256=%s\n' "$SERVER" "$(sha256sum "$SERVER" | awk '{ print $1 }')"
    printf 'fixture_binary=%s\nfixture_binary_sha256=%s\n' "$FIXTURE_BIN" "$(sha256sum "$FIXTURE_BIN" | awk '{ print $1 }')"
    printf 'mode=linux-amd64-functional-oracle\n'
    printf 'clickhouse_version=%s\nprometheus_version=%s\nloki_version=%s\ntempo_version=%s\nduckdb_version=%s\n' \
        "$CLICKHOUSE_VERSION" "$PROMETHEUS_VERSION" "$LOKI_VERSION" "$TEMPO_VERSION" "$DUCKDB_VERSION"
    uname -a
    docker version
} >"$RUN_DIR/provenance.txt"

"$SERVER" \
    --insecure-development-mode \
    --listen "$SHARD_HTTP" \
    --native-listen "$SHARD_NATIVE" \
    --otlp-grpc-listen "$SHARD_OTLP_GRPC" \
    --otlp-http-listen "$SHARD_OTLP_HTTP" \
    --default-tenant competitive \
    --data-directory "$RUN_DIR/shard-data" \
    --shards 4 \
    --tenant-partitions 16 \
    --append-linger-micros 0 \
    --clickhouse-token-file "$RUN_DIR/analytics-token" \
    >"$RUN_DIR/shard-server.log" 2>&1 &
SHARD_PID=$!
SHARD_STARTED=1
wait_for_http "ShardTelemetry" "http://$SHARD_HTTP/ready" "$RUN_DIR/shard-ready.txt"

PROMETHEUS_CONTAINER=$(container_name prometheus)
docker run --detach --name "$PROMETHEUS_CONTAINER" --user "$(id -u):$(id -g)" \
    --publish "$PROMETHEUS_HTTP:9090" \
    --volume "$COMPETITIVE_DIR/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
    --volume "$RUN_DIR/prometheus-data:/prometheus" \
    "$PROMETHEUS_IMAGE" \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --web.enable-otlp-receiver >"$RUN_DIR/prometheus-container-id.txt"
CONTAINERS+=("$PROMETHEUS_CONTAINER")
wait_for_http "Prometheus" "http://$PROMETHEUS_HTTP/-/ready" "$RUN_DIR/prometheus-ready.txt"

LOKI_CONTAINER=$(container_name loki)
docker run --detach --name "$LOKI_CONTAINER" --user "$(id -u):$(id -g)" \
    --publish "$LOKI_HTTP:3100" \
    --volume "$COMPETITIVE_DIR/loki.yaml:/etc/loki/config.yaml:ro" \
    --volume "$RUN_DIR/loki-data:/loki" \
    "$LOKI_IMAGE" -config.file=/etc/loki/config.yaml >"$RUN_DIR/loki-container-id.txt"
CONTAINERS+=("$LOKI_CONTAINER")
wait_for_http "Loki" "http://$LOKI_HTTP/ready" "$RUN_DIR/loki-ready.txt"

TEMPO_CONTAINER=$(container_name tempo)
docker run --detach --name "$TEMPO_CONTAINER" --user "$(id -u):$(id -g)" \
    --publish "$TEMPO_HTTP:3200" \
    --publish "$TEMPO_OTLP_HTTP:4318" \
    --volume "$COMPETITIVE_DIR/tempo.yaml:/etc/tempo/config.yaml:ro" \
    --volume "$RUN_DIR/tempo-data:/tmp/tempo" \
    "$TEMPO_IMAGE" -config.file=/etc/tempo/config.yaml >"$RUN_DIR/tempo-container-id.txt"
CONTAINERS+=("$TEMPO_CONTAINER")
wait_for_http "Tempo" "http://$TEMPO_HTTP/ready" "$RUN_DIR/tempo-ready.txt"

for signal in logs traces metrics; do
    curl --fail-with-body --silent --show-error \
        --header 'Content-Type: application/x-protobuf' \
        --data-binary "@$RUN_DIR/fixture/$signal.pb" \
        "http://$SHARD_OTLP_HTTP/v1/$signal" >"$RUN_DIR/shard-$signal-ingest.pb"
done
curl --fail-with-body --silent --show-error \
    --header 'Content-Type: application/x-protobuf' \
    --data-binary "@$RUN_DIR/fixture/metrics.pb" \
    "http://$PROMETHEUS_HTTP/api/v1/otlp/v1/metrics" >"$RUN_DIR/prometheus-metrics-ingest.pb"
curl --fail-with-body --silent --show-error \
    --header 'Content-Type: application/x-protobuf' \
    --data-binary "@$RUN_DIR/fixture/logs.pb" \
    "http://$LOKI_HTTP/otlp/v1/logs" >"$RUN_DIR/loki-logs-ingest.pb"
curl --fail-with-body --silent --show-error \
    --header 'Content-Type: application/x-protobuf' \
    --data-binary "@$RUN_DIR/fixture/traces.pb" \
    "http://$TEMPO_OTLP_HTTP/v1/traces" >"$RUN_DIR/tempo-traces-ingest.pb"
curl --fail-with-body --silent --show-error -X POST "http://$SHARD_HTTP/flush" >"$RUN_DIR/shard-flush.txt"

wait_for_json "ShardTelemetry PromQL" "$RUN_DIR/shard-prometheus-query.json" \
    '.status == "success" and .data.resultType == "vector" and (.data.result | length) == 1 and .data.result[0].value[1] == "1"' \
    curl --fail --silent --show-error --get "http://$SHARD_HTTP/api/v1/query" \
    --data-urlencode 'query={__name__="checkout.requests"}' \
    --data-urlencode "time=$FIXTURE_QUERY_SECONDS"
wait_for_json "Prometheus PromQL" "$RUN_DIR/prometheus-query.json" \
    '.status == "success" and .data.resultType == "vector" and (.data.result | length) == 1 and .data.result[0].value[1] == "1"' \
    curl --fail --silent --show-error --get "http://$PROMETHEUS_HTTP/api/v1/query" \
    --data-urlencode 'query=checkout_requests' \
    --data-urlencode "time=$FIXTURE_QUERY_SECONDS"

wait_for_json "ShardTelemetry LogQL" "$RUN_DIR/shard-loki-query.json" \
    '.status == "success" and ((.data.result | map(.values | length) | add) == 3) and ([.data.result[]?.values[]? | select(.[1] | contains("checkout"))] | length) == 2' \
    curl --fail --silent --show-error --get "http://$SHARD_HTTP/loki/api/v1/query_range" \
    --data-urlencode 'query={}' \
    --data-urlencode "start=$FIXTURE_TIMESTAMP_NANOS" \
    --data-urlencode "end=$(($FIXTURE_TIMESTAMP_NANOS + 1000000000))" \
    --data-urlencode 'direction=forward'
# Stock Loki rejects an empty selector; its OTLP receiver promotes service.name
# to the service_name stream label.
wait_for_json "Loki LogQL" "$RUN_DIR/loki-query.json" \
    '.status == "success" and ((.data.result | map(.values | length) | add) == 3) and ([.data.result[]?.values[]? | select(.[1] | contains("checkout"))] | length) == 2' \
    curl --fail --silent --show-error --get "http://$LOKI_HTTP/loki/api/v1/query_range" \
    --data-urlencode 'query={service_name="checkout-api"}' \
    --data-urlencode "start=$FIXTURE_TIMESTAMP_NANOS" \
    --data-urlencode "end=$(($FIXTURE_TIMESTAMP_NANOS + 1000000000))" \
    --data-urlencode 'direction=forward'

TRACE_ID=11111111111111111111111111111111
wait_for_json "ShardTelemetry trace-by-ID" "$RUN_DIR/shard-tempo-trace.json" \
    '(.batches | length) == 1' \
    curl --fail --silent --show-error \
    --header 'Accept: application/json' \
    "http://$SHARD_HTTP/api/v2/traces/$TRACE_ID"
wait_for_json "Tempo trace-by-ID" "$RUN_DIR/tempo-trace.json" \
    '(.trace.resourceSpans | length) == 1' \
    curl --fail --silent --show-error \
    --header 'Accept: application/json' \
    "http://$TEMPO_HTTP/api/v2/traces/$TRACE_ID"

curl --fail --silent --show-error --get \
    --header 'Authorization: Bearer shard-telemetry-competitive-analytics-token' \
    --header 'X-Scope-OrgID: competitive' \
    --data-urlencode 'relation=logs' \
    --data-urlencode 'columns=timestamp,message' \
    --data-urlencode 'wire=jsonl' \
    "http://$SHARD_HTTP/shardtelemetry/api/v1/clickhouse/scan" >"$RUN_DIR/shard-logs.ndjson"
jq --slurp -e 'length == 3 and ([.[] | select(.message | contains("checkout"))] | length) == 2' \
    "$RUN_DIR/shard-logs.ndjson" >/dev/null
docker run --rm --volume "$RUN_DIR:/competitive:ro" "$DUCKDB_IMAGE" \
    duckdb -json -c "SELECT count(*) AS rows, count(*) FILTER (WHERE message LIKE '%checkout%') AS checkout_rows FROM read_ndjson_auto('/competitive/shard-logs.ndjson')" \
    >"$RUN_DIR/duckdb-query.json"
jq -e 'length == 1 and .[0].rows == 3 and .[0].checkout_rows == 2' "$RUN_DIR/duckdb-query.json" >/dev/null

RESULT_DIR="$RUN_DIR/clickhouse" \
SHARD_TELEMETRY_SERVER="$SERVER" \
SHARD_TELEMETRY_FIXTURE_BIN="$FIXTURE_BIN" \
SHARD_TELEMETRY_HTTP_ADDRESS=127.0.0.1:42100 \
SHARD_TELEMETRY_NATIVE_ADDRESS=127.0.0.1:42101 \
SHARD_TELEMETRY_OTLP_GRPC_ADDRESS=127.0.0.1:44317 \
SHARD_TELEMETRY_OTLP_HTTP_ADDRESS=127.0.0.1:44318 \
SHARD_TELEMETRY_FIXTURE_TIMESTAMP_UNIX_NANOS="$FIXTURE_TIMESTAMP_NANOS" \
CLICKHOUSE_IMAGE="$CLICKHOUSE_IMAGE_ID" \
EXPECTED_CLICKHOUSE_VERSION="$CLICKHOUSE_VERSION" \
    "$REPOSITORY/scripts/run-clickhouse-acceptance.sh"
for signal in logs metrics traces; do
    cmp --silent "$RUN_DIR/fixture/$signal.pb" "$RUN_DIR/clickhouse/fixture/$signal.pb" || {
        echo "ClickHouse acceptance did not receive the campaign's $signal fixture" >&2
        exit 1
    }
done
sha256sum "$RUN_DIR"/clickhouse/fixture/*.pb >"$RUN_DIR/clickhouse-fixture-sha256.txt"

du -sb "$RUN_DIR/shard-data" "$RUN_DIR/prometheus-data" "$RUN_DIR/loki-data" "$RUN_DIR/tempo-data" \
    >"$RUN_DIR/storage-bytes.tsv"
printf 'passed\n' >"$RUN_DIR/status.txt"
printf 'competitive oracle evidence: %s\n' "$RUN_DIR"
