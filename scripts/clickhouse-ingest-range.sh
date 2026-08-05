#!/usr/bin/env bash
set -euo pipefail

: "${CORE_COUNT:?CORE_COUNT must be set}"
: "${SOURCE_SKIP_BYTES:?SOURCE_SKIP_BYTES must be set}"
: "${SOURCE_BYTES:?SOURCE_BYTES must be set}"
: "${MIN_CHUNK_BYTES_FOR_PARALLEL_PARSING:=134217728}"
: "${CLICKHOUSE_CLIENT_PORT:=9000}"

dd if=/benchmark/input.json \
    bs=64M \
    iflag=skip_bytes,count_bytes \
    skip="$SOURCE_SKIP_BYTES" \
    count="$SOURCE_BYTES" \
    status=none |
    # LineAsString keeps malformed JSON local to one record. JSONEachRow's
    # parallel brace segmenter can otherwise consume gigabytes after one
    # unbalanced line before it reaches its oversized-object guard.
    clickhouse-client \
        --port="$CLICKHOUSE_CLIENT_PORT" \
        --async_insert=0 \
        --query "
            INSERT INTO benchmark.logs
            WITH JSONExtract(
                raw,
                'Tuple(log String, stream String, time String)'
            ) AS parsed
            SELECT
                parseDateTime64BestEffort(parsed.3, 9, 'UTC'),
                parsed.2,
                parsed.1
            FROM input('raw String')
            WHERE isValidJSON(raw)
            FORMAT LineAsString
        " \
        --max_threads="$CORE_COUNT" \
        --max_insert_threads="$CORE_COUNT" \
        --date_time_input_format=best_effort \
        --input_format_parallel_parsing=1 \
        --min_chunk_bytes_for_parallel_parsing="$MIN_CHUNK_BYTES_FOR_PARALLEL_PARSING"
