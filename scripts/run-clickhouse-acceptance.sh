#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_IMAGE:?set CLICKHOUSE_IMAGE to the pinned official ClickHouse image digest}"
: "${SHARD_TELEMETRY_SERVER:?set SHARD_TELEMETRY_SERVER to shard-telemetry-server}"
: "${SHARD_TELEMETRY_FIXTURE_BIN:?set SHARD_TELEMETRY_FIXTURE_BIN to shard-telemetry-clickhouse-fixture}"
: "${RESULT_DIR:?set RESULT_DIR to a new retained evidence directory}"

EXPECTED_CLICKHOUSE_VERSION=${EXPECTED_CLICKHOUSE_VERSION:-26.3.17.56}
HTTP_ADDRESS=${SHARD_TELEMETRY_HTTP_ADDRESS:-127.0.0.1:32100}
NATIVE_ADDRESS=${SHARD_TELEMETRY_NATIVE_ADDRESS:-127.0.0.1:32101}
OTLP_GRPC_ADDRESS=${SHARD_TELEMETRY_OTLP_GRPC_ADDRESS:-127.0.0.1:34317}
OTLP_HTTP_ADDRESS=${SHARD_TELEMETRY_OTLP_HTTP_ADDRESS:-127.0.0.1:34318}
TENANT=${SHARD_TELEMETRY_TENANT:-fake}

[[ ! -e $RESULT_DIR ]] || {
    echo "refusing to overwrite result directory: $RESULT_DIR" >&2
    exit 2
}
for command in curl docker sha256sum; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done
for executable in "$SHARD_TELEMETRY_SERVER" "$SHARD_TELEMETRY_FIXTURE_BIN"; do
    [[ -x $executable ]] || {
        echo "required executable is not executable: $executable" >&2
        exit 2
    }
done

IMAGE_ID=$(docker image inspect "$CLICKHOUSE_IMAGE" --format '{{.Id}}')
[[ $IMAGE_ID == "$CLICKHOUSE_IMAGE" ]] || {
    echo "ClickHouse image is not pinned by its local image ID" >&2
    exit 2
}
OBSERVED_CLICKHOUSE_VERSION=$(
    docker run --rm --network host "$CLICKHOUSE_IMAGE" clickhouse local --query 'SELECT version()'
)
[[ $OBSERVED_CLICKHOUSE_VERSION == "$EXPECTED_CLICKHOUSE_VERSION" ]] || {
    echo "ClickHouse version mismatch: expected $EXPECTED_CLICKHOUSE_VERSION, observed $OBSERVED_CLICKHOUSE_VERSION" >&2
    exit 2
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$RESULT_DIR/fixture" "$RESULT_DIR/data"
umask 077
TOKEN_FILE=$RESULT_DIR/clickhouse-token
printf '%s\n' 'shard-telemetry-clickhouse-acceptance-token' >"$TOKEN_FILE"

"$SHARD_TELEMETRY_FIXTURE_BIN" --output-directory "$RESULT_DIR/fixture" \
    >"$RESULT_DIR/fixture-files.txt"
sha256sum "$RESULT_DIR"/fixture/*.pb >"$RESULT_DIR/fixture-sha256.txt"
printf 'image=%s\nimage_id=%s\nversion=%s\n' \
    "$CLICKHOUSE_IMAGE" "$IMAGE_ID" "$OBSERVED_CLICKHOUSE_VERSION" \
    >"$RESULT_DIR/clickhouse-image.txt"

"$SHARD_TELEMETRY_SERVER" \
    --insecure-development-mode \
    --listen "$HTTP_ADDRESS" \
    --native-listen "$NATIVE_ADDRESS" \
    --otlp-grpc-listen "$OTLP_GRPC_ADDRESS" \
    --otlp-http-listen "$OTLP_HTTP_ADDRESS" \
    --default-tenant "$TENANT" \
    --data-directory "$RESULT_DIR/data" \
    --shards 4 \
    --tenant-partitions 16 \
    --append-linger-micros 0 \
    --clickhouse-token-file "$TOKEN_FILE" \
    >"$RESULT_DIR/server.log" 2>&1 &
SERVER_PID=$!
cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

ready=0
for _ in $(seq 1 120); do
    if curl --fail --silent "http://$HTTP_ADDRESS/ready" >"$RESULT_DIR/ready.txt"; then
        ready=1
        break
    fi
    kill -0 "$SERVER_PID" 2>/dev/null || {
        echo "ShardTelemetry server exited before readiness" >&2
        exit 1
    }
    sleep 0.25
done
[[ $ready -eq 1 ]] || {
    echo "ShardTelemetry server did not become ready" >&2
    exit 1
}

for signal in logs traces metrics; do
    curl --fail-with-body --silent --show-error \
        -H 'Content-Type: application/x-protobuf' \
        --data-binary "@$RESULT_DIR/fixture/$signal.pb" \
        "http://$OTLP_HTTP_ADDRESS/v1/$signal" \
        >"$RESULT_DIR/$signal-response.pb"
done

SHARD_TELEMETRY_CLICKHOUSE_TOKEN_FILE=$TOKEN_FILE \
CLICKHOUSE_IMAGE=$CLICKHOUSE_IMAGE \
CLICKHOUSE_NETWORK=host \
SHARD_TELEMETRY_URL="http://$HTTP_ADDRESS/shardtelemetry/api/v1/clickhouse/scan" \
SHARD_TELEMETRY_TENANT=$TENANT \
SHARD_TELEMETRY_REQUIRE_NONEMPTY=1 \
EXPECTED_CLICKHOUSE_VERSION=$EXPECTED_CLICKHOUSE_VERSION \
    "$SCRIPT_DIR/run-clickhouse-telemetry-compatibility.sh" \
    2>&1 | tee "$RESULT_DIR/compatibility.txt"

curl --fail --silent "http://$HTTP_ADDRESS/metrics" >"$RESULT_DIR/server-metrics.txt"
sha256sum "$SHARD_TELEMETRY_SERVER" "$SHARD_TELEMETRY_FIXTURE_BIN" \
    >"$RESULT_DIR/binary-sha256.txt"
uname -a >"$RESULT_DIR/uname.txt"
printf 'passed\n' >"$RESULT_DIR/status.txt"
