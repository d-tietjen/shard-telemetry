#!/usr/bin/env bash
set -euo pipefail

ADAPTER_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SEARCHBENCH_ROOT=${SEARCHBENCH_ROOT:?set SEARCHBENCH_ROOT to the SearchBench checkout}

export ENGINE_NAME="ShardTelemetry"
export ENGINE_TAGS='["Rust","thread-per-core","analytical-scan"]'
export SEARCHBENCH_QUERIES=${SEARCHBENCH_QUERIES:-$SEARCHBENCH_ROOT/serenedb/queries.sql}
export ST_QUERY_FILE=$SEARCHBENCH_QUERIES
export ST_ADAPTER_DIR=$ADAPTER_DIR
export ST_PRODUCT_ROOT=${ST_PRODUCT_ROOT:?set ST_PRODUCT_ROOT to the ShardTelemetry checkout}
export ST_BINARY=${ST_BINARY:-$ST_PRODUCT_ROOT/target/release/shard-telemetry-server}
export ST_HTTP_PORT=${ST_HTTP_PORT:-32100}
export ST_OTLP_PORT=${ST_OTLP_PORT:-34318}
export ST_SHARDS=${ST_SHARDS:-16}
export ST_RUNTIME_WORKERS=${ST_RUNTIME_WORKERS:-16}
export ST_TENANT=${ST_TENANT:-benchmark}
export ST_TOKEN=${ST_TOKEN:-shard-telemetry-searchbench-token}
export ST_ENGINE_DATA_DIR=${ST_ENGINE_DATA_DIR:?set ST_ENGINE_DATA_DIR to an engine data directory}
export ST_TOKEN_FILE=${ST_TOKEN_FILE:-$ST_ENGINE_DATA_DIR/.clickhouse-token}
export SEARCHBENCH_QUERY_TIMEOUT=${SEARCHBENCH_QUERY_TIMEOUT:-120}

exec "$SEARCHBENCH_ROOT/lib/benchmark.sh" "$@"
