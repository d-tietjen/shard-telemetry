#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_BIN:?set CLICKHOUSE_BIN to the pinned custom ClickHouse binary}"
: "${CLICKHOUSE_SOURCE:?set CLICKHOUSE_SOURCE to the pinned ClickHouse source checkout}"
: "${SHARD_TELEMETRY_SERVER:?set SHARD_TELEMETRY_SERVER to shard-telemetry-server}"
: "${SHARD_TELEMETRY_FIXTURE_BIN:?set SHARD_TELEMETRY_FIXTURE_BIN to shard-telemetry-clickhouse-fixture}"
: "${RESULT_DIR:?set RESULT_DIR to a new retained evidence directory}"

EXPECTED_CLICKHOUSE_TAG=${EXPECTED_CLICKHOUSE_TAG:-v26.3.17.56-lts}
EXPECTED_CLICKHOUSE_COMMIT=${EXPECTED_CLICKHOUSE_COMMIT:-c57540de480d8a501b601163471d3843674378cf}
# ClickHouse's local CI build stamps a development patch component even when
# the source checkout is an exact release tag. Prove both identities instead
# of weakening the source-revision check.
EXPECTED_CLICKHOUSE_BINARY_VERSION=${EXPECTED_CLICKHOUSE_BINARY_VERSION:-26.3.17.0}

[[ ! -e $RESULT_DIR ]] || {
    echo "refusing to overwrite result directory: $RESULT_DIR" >&2
    exit 2
}
for executable in "$CLICKHOUSE_BIN" "$SHARD_TELEMETRY_SERVER" "$SHARD_TELEMETRY_FIXTURE_BIN"; do
    [[ -x $executable ]] || {
        echo "required executable is not executable: $executable" >&2
        exit 2
    }
done
[[ -d $CLICKHOUSE_SOURCE/.git ]] || {
    echo "ClickHouse source is not a Git checkout: $CLICKHOUSE_SOURCE" >&2
    exit 2
}
OBSERVED_CLICKHOUSE_TAG=$(git -C "$CLICKHOUSE_SOURCE" describe --tags --exact-match 2>/dev/null || true)
OBSERVED_CLICKHOUSE_COMMIT=$(git -C "$CLICKHOUSE_SOURCE" rev-parse HEAD)
if [[ $OBSERVED_CLICKHOUSE_TAG != "$EXPECTED_CLICKHOUSE_TAG" \
    || $OBSERVED_CLICKHOUSE_COMMIT != "$EXPECTED_CLICKHOUSE_COMMIT" ]]; then
    echo "ClickHouse source mismatch: expected $EXPECTED_CLICKHOUSE_TAG at $EXPECTED_CLICKHOUSE_COMMIT, observed ${OBSERVED_CLICKHOUSE_TAG:-no exact tag} at $OBSERVED_CLICKHOUSE_COMMIT" >&2
    exit 2
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HTTP_ADDRESS=${SHARD_TELEMETRY_HTTP_ADDRESS:-127.0.0.1:32100}
NATIVE_ADDRESS=${SHARD_TELEMETRY_NATIVE_ADDRESS:-127.0.0.1:32101}
OTLP_GRPC_ADDRESS=${SHARD_TELEMETRY_OTLP_GRPC_ADDRESS:-127.0.0.1:34317}
OTLP_HTTP_ADDRESS=${SHARD_TELEMETRY_OTLP_HTTP_ADDRESS:-127.0.0.1:34318}
TENANT=${SHARD_TELEMETRY_TENANT:-fake}

mkdir -p "$RESULT_DIR/fixture" "$RESULT_DIR/data"
umask 077
TOKEN_FILE=$RESULT_DIR/clickhouse-token
printf '%s\n' 'shard-telemetry-clickhouse-acceptance-token' >"$TOKEN_FILE"

"$SHARD_TELEMETRY_FIXTURE_BIN" --output-directory "$RESULT_DIR/fixture" \
    >"$RESULT_DIR/fixture-files.txt"
sha256sum "$RESULT_DIR/fixture"/*.pb >"$RESULT_DIR/fixture-sha256.txt"

"$CLICKHOUSE_BIN" local --query 'SELECT version()' >"$RESULT_DIR/clickhouse-version.txt"
OBSERVED_CLICKHOUSE_BINARY_VERSION=$(<"$RESULT_DIR/clickhouse-version.txt")
if [[ $OBSERVED_CLICKHOUSE_BINARY_VERSION != "$EXPECTED_CLICKHOUSE_BINARY_VERSION" ]]; then
    echo "ClickHouse binary version mismatch: expected $EXPECTED_CLICKHOUSE_BINARY_VERSION, observed $OBSERVED_CLICKHOUSE_BINARY_VERSION" >&2
    exit 2
fi
printf 'tag=%s\ncommit=%s\n' "$OBSERVED_CLICKHOUSE_TAG" "$OBSERVED_CLICKHOUSE_COMMIT" \
    >"$RESULT_DIR/clickhouse-source.txt"
"$CLICKHOUSE_BIN" local --query \
    "SELECT name FROM system.table_engines WHERE name = 'ShardTelemetry'" \
    >"$RESULT_DIR/clickhouse-engine.txt"
[[ $(<"$RESULT_DIR/clickhouse-engine.txt") == ShardTelemetry ]] || {
    echo "custom ClickHouse binary does not advertise StorageShardTelemetry" >&2
    exit 1
}

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
CLICKHOUSE_BIN=$CLICKHOUSE_BIN \
SHARD_TELEMETRY_URL="http://$HTTP_ADDRESS/shardtelemetry/api/v1/clickhouse/scan" \
SHARD_TELEMETRY_TENANT=$TENANT \
SHARD_TELEMETRY_ADAPTER_MODE=1 \
SHARD_TELEMETRY_REQUIRE_NONEMPTY=1 \
EXPECTED_CLICKHOUSE_VERSION=$EXPECTED_CLICKHOUSE_BINARY_VERSION \
    "$SCRIPT_DIR/run-clickhouse-adapter-compatibility.sh" \
    2>&1 | tee "$RESULT_DIR/compatibility.txt"

curl --fail --silent "http://$HTTP_ADDRESS/metrics" >"$RESULT_DIR/server-metrics.txt"
sha256sum "$CLICKHOUSE_BIN" "$SHARD_TELEMETRY_SERVER" "$SHARD_TELEMETRY_FIXTURE_BIN" \
    >"$RESULT_DIR/binary-sha256.txt"
uname -a >"$RESULT_DIR/uname.txt"
printf 'passed\n' >"$RESULT_DIR/status.txt"
