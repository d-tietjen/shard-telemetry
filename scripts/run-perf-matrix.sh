#!/usr/bin/env bash
set -euo pipefail

# Reproducible local qualification for embedded storage and the native/HTTP
# server paths. The network matrix is opt-in because it starts one server per
# combination. Set RUN_NETWORK=1 to include it.

REPOSITORY=${SHARD_TELEMETRY_REPOSITORY:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}
BENCH=${SHARD_TELEMETRY_BENCH:-$REPOSITORY/target/release/shard-telemetry-signal-bench}
SERVER=${SHARD_TELEMETRY_SERVER:-$REPOSITORY/target/release/shard-telemetry-server}
LOADER=${SHARD_TELEMETRY_LOADER:-$REPOSITORY/target/release/shard-telemetry-loki-load}
NATIVE_QUERY=${SHARD_TELEMETRY_NATIVE_QUERY:-$REPOSITORY/target/release/shard-telemetry-native-query}
RESULT_ROOT=${RESULT_ROOT:-$REPOSITORY/benchmark-results/perf-matrix}
RUN_ID=${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
RECORDS=${RECORDS:-32768}
LOOKUP_ITERATIONS=${LOOKUP_ITERATIONS:-500}
SCAN_ITERATIONS=${SCAN_ITERATIONS:-100}
PARTITIONS=${PARTITIONS:-256}
SHARDS=${SHARDS:-"1 4 16"}
WRITERS=${WRITERS:-"1 4 16"}
PIPELINE_DEPTH=${PIPELINE_DEPTH:-8}
RUN_NETWORK=${RUN_NETWORK:-0}
TENANT=${TENANT:-production-example}
BASE_PORT=${BASE_PORT:-33600}
OBJECT_STORE_DIRECTORY=${OBJECT_STORE_DIRECTORY:-}

for executable in "$BENCH" "$SERVER"; do
    [[ -x $executable ]] || {
        echo "missing executable: $executable" >&2
        exit 2
    }
done
if [[ $RUN_NETWORK == 1 ]]; then
    for executable in "$LOADER" "$NATIVE_QUERY"; do
        [[ -x $executable ]] || {
            echo "missing executable: $executable" >&2
            exit 2
        }
    done
fi

RUN_DIR=$RESULT_ROOT/$RUN_ID
mkdir -p "$RUN_DIR"
SUMMARY=$RUN_DIR/summary.csv
printf '%s\n' \
    'mode,shards,writers,protocol,recovery_journal,records,storage_bytes,bytes_per_record,append_seconds,recovery_seconds,write_mib_s,p50_us,p95_us,p99_us,max_rss_kib,user_seconds,system_seconds' \
    >"$SUMMARY"

if [[ $(uname -s) == Darwin ]]; then
    # macOS's `time -l` queries sysctl and is blocked in some managed shells;
    # keep portable wall/user/system timing here and use Linux's verbose mode
    # for RSS when the matrix runs on a Linux qualification host.
    TIME_FLAGS=(-p)
else
    TIME_FLAGS=(-v)
fi

run_timed() {
    local time_file=$1
    local output_file=$2
    shift 2
    /usr/bin/time "${TIME_FLAGS[@]}" -o "$time_file" "$@" >"$output_file" 2>&1
}

time_field() {
    local key=$1
    local file=$2
    awk -v key="$key" '
        tolower($0) ~ tolower(key) {
            for (field_index = NF; field_index > 0; field_index--) {
                if ($field_index ~ /^[0-9]+([.][0-9]+)?$/) {
                    print $field_index
                    exit
                }
            }
        }
    ' "$file"
}

line_field() {
    local prefix=$1
    local key=$2
    local file=$3
    awk -v prefix="$prefix" -v key="$key" '
        $1 == prefix {
            for (field_index = 1; field_index <= NF; field_index++) {
                split($field_index, pair, "=")
                if (pair[1] == key) {
                    print pair[2]
                    exit
                }
            }
        }
    ' "$file"
}

scan_field() {
    local signal=$1
    local key=$2
    local file=$3
    awk -v signal="$signal" -v key="$key" '
        $1 == "server_scan" && $2 == "signal=" signal {
            for (field_index = 1; field_index <= NF; field_index++) {
                split($field_index, pair, "=")
                if (pair[1] == key) {
                    print pair[2]
                    exit
                }
            }
        }
    ' "$file"
}

append_row() {
    printf '%s\n' "$1" >>"$SUMMARY"
}

for shard_count in $SHARDS; do
    for journal in false true; do
        name="embedded-s${shard_count}-j${journal}"
        data_directory=$RUN_DIR/$name/data
        mkdir -p "$RUN_DIR/$name"
        append_output=$RUN_DIR/$name/append.txt
        append_time=$RUN_DIR/$name/append.time
        arguments=(
            --records "$RECORDS"
            --iterations "$LOOKUP_ITERATIONS"
            --server-data-directory "$data_directory"
            --server-shards "$shard_count"
            --server-partitions "$PARTITIONS"
            --server-append-linger-micros 0
            --server-scan-iterations "$SCAN_ITERATIONS"
            --server-only
        )
        if [[ $journal == true ]]; then
            arguments+=(--server-recovery-journal)
        fi
        run_timed "$append_time" "$append_output" "$BENCH" "${arguments[@]}"

        open_output=$RUN_DIR/$name/open.txt
        open_time=$RUN_DIR/$name/open.time
        open_arguments=(
            --records "$RECORDS"
            --iterations "$LOOKUP_ITERATIONS"
            --server-data-directory "$data_directory"
            --server-shards "$shard_count"
            --server-partitions "$PARTITIONS"
            --server-append-linger-micros 0
            --server-scan-iterations "$SCAN_ITERATIONS"
            --server-open-only
            --server-only
        )
        if [[ $journal == true ]]; then
            open_arguments+=(--server-recovery-journal)
        fi
        run_timed "$open_time" "$open_output" "$BENCH" \
            "${open_arguments[@]}"

        storage_bytes=$(du -sk "$data_directory" | awk '{ print $1 * 1024 }')
        logs_seconds=$(line_field embedded_store logs_seconds "$append_output")
        traces_seconds=$(line_field embedded_store traces_seconds "$append_output")
        metrics_seconds=$(line_field embedded_store metrics_seconds "$append_output")
        append_seconds=$(awk -v logs="$logs_seconds" -v traces="$traces_seconds" \
            -v metrics="$metrics_seconds" 'BEGIN { print logs + traces + metrics }')
        [[ -n $append_seconds ]] || append_seconds=0
        recovery_seconds=$(line_field embedded_open_only open_seconds "$open_output")
        [[ -n $recovery_seconds ]] || recovery_seconds=0
        write_mib_s=$(awk -v bytes="$storage_bytes" -v seconds="$append_seconds" \
            'BEGIN { if (seconds > 0) print bytes / 1048576 / seconds; else print 0 }')
        bytes_per_record=$(awk -v bytes="$storage_bytes" -v records="$RECORDS" \
            'BEGIN { print bytes / (records * 3) }')
        max_rss_kib=$(time_field 'resident set size' "$append_time")
        user_seconds=$(time_field '^user' "$append_time")
        system_seconds=$(time_field 'sys' "$append_time")
        [[ -n $max_rss_kib ]] || max_rss_kib=0
        [[ -n $user_seconds ]] || user_seconds=0
        [[ -n $system_seconds ]] || system_seconds=0
        append_row "embedded,$shard_count,1,embedded,$journal,$RECORDS,$storage_bytes,$bytes_per_record,$append_seconds,$recovery_seconds,$write_mib_s,$(scan_field resource p50_us "$open_output"),$(scan_field resource p95_us "$open_output"),$(scan_field resource p99_us "$open_output"),$max_rss_kib,$user_seconds,$system_seconds"
    done
done

if [[ $RUN_NETWORK == 1 ]]; then
    source="$RUN_DIR/network-source.jsonl"
    python3 - "$source" "$RECORDS" <<'PY'
import json
import sys

path = sys.argv[1]
records = int(sys.argv[2])
with open(path, "w", encoding="utf-8") as output:
    for ordinal in range(records):
        hour = (ordinal // 3600) % 24
        minute = (ordinal // 60) % 60
        second = ordinal % 60
        payload = {
            "log": f"2026-09-20T{hour:02d}:{minute:02d}:{second:02d}.000Z request completed service=checkout route=/cart status={200 if ordinal % 10 else 500}\n",
            "stream": "stdout",
            "time": f"2026-09-20T{hour:02d}:{minute:02d}:{second:02d}.000000000Z",
        }
        output.write(json.dumps(payload, separators=(",", ":")) + "\n")
PY

    combination=0
    server_pid=''
    stop_server() {
        if [[ -n $server_pid ]]; then
            kill "$server_pid" 2>/dev/null || true
            wait "$server_pid" 2>/dev/null || true
            server_pid=''
        fi
    }
    trap stop_server EXIT INT TERM

    for shard_count in $SHARDS; do
        for writer_count in $WRITERS; do
            for protocol in native loki; do
                combination=$((combination + 1))
                http_port=$((BASE_PORT + combination * 10 + 1))
                native_port=$((BASE_PORT + combination * 10 + 2))
                grpc_port=$((BASE_PORT + combination * 10 + 3))
                otlp_port=$((BASE_PORT + combination * 10 + 4))
                name="server-s${shard_count}-w${writer_count}-${protocol}"
                data_directory=$RUN_DIR/$name/data
                mkdir -p "$RUN_DIR/$name"
                server_log=$RUN_DIR/$name/server.log
                server_args=(
                    --insecure-development-mode
                    --listen "127.0.0.1:$http_port"
                    --native-listen "127.0.0.1:$native_port"
                    --otlp-grpc-listen "127.0.0.1:$grpc_port"
                    --otlp-http-listen "127.0.0.1:$otlp_port"
                    --default-tenant "$TENANT"
                    --data-directory "$data_directory"
                    --shards "$shard_count"
                    --tenant-partitions "$PARTITIONS"
                    --append-linger-micros 0
                )
                if [[ -n $OBJECT_STORE_DIRECTORY ]]; then
                    server_args+=(--object-store-directory "$OBJECT_STORE_DIRECTORY/$name")
                fi
                "$SERVER" "${server_args[@]}" >"$server_log" 2>&1 &
                server_pid=$!
                ready=0
                for _ in $(seq 1 120); do
                    if curl --fail --silent "http://127.0.0.1:$http_port/ready" >/dev/null 2>&1; then
                        ready=1
                        break
                    fi
                    sleep 0.25
                done
                [[ $ready == 1 ]] || {
                    cat "$server_log" >&2
                    exit 1
                }

                load_output=$RUN_DIR/$name/load.txt
                load_time=$RUN_DIR/$name/load.time
                load_args=(
                    "$LOADER" "$source"
                    --host 127.0.0.1
                    --workers "$writer_count"
                    --batch-bytes 1048576
                    --partitions "$PARTITIONS"
                    --tenant "$TENANT"
                )
                if [[ $protocol == native ]]; then
                    load_args+=(--protocol native --port "$native_port" --pipeline-depth "$PIPELINE_DEPTH")
                else
                    load_args+=(--protocol loki --port "$http_port")
                fi
                run_timed "$load_time" "$load_output" "${load_args[@]}"
                query_output=$RUN_DIR/$name/query.txt
                "$NATIVE_QUERY" \
                    --host 127.0.0.1 \
                    --port "$native_port" \
                    --tenant "$TENANT" \
                    --term completed \
                    --limit 100 \
                    --warmup 5 \
                    --iterations 50 \
                    >"$query_output"
                storage_bytes=$(du -sk "$data_directory" | awk '{ print $1 * 1024 }')
                throughput=$(awk -F': ' '/source throughput MiB\/s/{ print $2; exit }' "$load_output")
                query_p50_us=$(awk -F': ' '/p50 latency ms/{ print $2 * 1000; exit }' "$query_output")
                query_p95_us=$(awk -F': ' '/p95 latency ms/{ print $2 * 1000; exit }' "$query_output")
                query_p99_us=$(awk -F': ' '/p99 latency ms/{ print $2 * 1000; exit }' "$query_output")
                bytes_per_record=$(awk -v bytes="$storage_bytes" -v records="$RECORDS" \
                    'BEGIN { print bytes / records }')
                max_rss_kib=$(time_field 'resident set size' "$load_time")
                user_seconds=$(time_field '^user' "$load_time")
                system_seconds=$(time_field '^system' "$load_time")
                [[ -n $throughput ]] || throughput=0
                [[ -n $query_p50_us ]] || query_p50_us=0
                [[ -n $query_p95_us ]] || query_p95_us=0
                [[ -n $query_p99_us ]] || query_p99_us=0
                [[ -n $max_rss_kib ]] || max_rss_kib=0
                [[ -n $user_seconds ]] || user_seconds=0
                [[ -n $system_seconds ]] || system_seconds=0
                append_row "server,$shard_count,$writer_count,$protocol,true,$RECORDS,$storage_bytes,$bytes_per_record,0,0,$throughput,$query_p50_us,$query_p95_us,$query_p99_us,$max_rss_kib,$user_seconds,$system_seconds"
                stop_server
            done
        done
    done
fi

echo "wrote $SUMMARY"
