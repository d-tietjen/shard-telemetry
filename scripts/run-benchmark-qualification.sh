#!/usr/bin/env bash
set -euo pipefail

# Runs one retained-corpus performance suite on a dedicated Linux benchmark
# host and retains only non-sensitive, small evidence. This is deliberately a
# manual qualification path: PR CI proves functional equivalence with small
# fixtures, while the source corpora and durable stores used here are too large
# for a shared CI runner.

REPOSITORY=${SHARD_TELEMETRY_REPOSITORY:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}
REPOSITORY=$(cd "$REPOSITORY" && pwd -P)
SUITE=${BENCHMARK_SUITE:?set BENCHMARK_SUITE}
RESULT_ROOT=${BENCHMARK_RESULT_ROOT:?set BENCHMARK_RESULT_ROOT to a dedicated benchmark volume}
RUN_ID=${BENCHMARK_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}

[[ $RUN_ID =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]] || {
    echo 'BENCHMARK_RUN_ID must contain only letters, digits, dot, underscore, or hyphen' >&2
    exit 2
}
[[ $RESULT_ROOT == /* ]] || {
    echo 'BENCHMARK_RESULT_ROOT must be an absolute path on the benchmark volume' >&2
    exit 2
}
[[ $RESULT_ROOT != / && $RESULT_ROOT != "$REPOSITORY" && $RESULT_ROOT != "$REPOSITORY"/* ]] || {
    echo 'BENCHMARK_RESULT_ROOT must be a dedicated volume outside the source checkout' >&2
    exit 2
}
for command in cargo date find git install mkdir sha256sum sort stat xargs; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 2
    }
done

tracked_changes=$(git -C "$REPOSITORY" status --porcelain --untracked-files=no)
[[ -z $tracked_changes ]] || {
    echo 'refusing to benchmark a tracked dirty worktree' >&2
    exit 2
}
cd "$REPOSITORY"

mkdir -p "$RESULT_ROOT"
RESULT_ROOT=$(cd "$RESULT_ROOT" && pwd -P)
[[ $RESULT_ROOT != / && $RESULT_ROOT != "$REPOSITORY" && $RESULT_ROOT != "$REPOSITORY"/* ]] || {
    echo 'BENCHMARK_RESULT_ROOT resolves inside the source checkout' >&2
    exit 2
}
RUN_DIRECTORY=$RESULT_ROOT/$RUN_ID
[[ ! -e $RUN_DIRECTORY ]] || {
    echo "refusing to overwrite benchmark evidence: $RUN_DIRECTORY" >&2
    exit 2
}

run_suite() {
    case "$SUITE" in
        clickhouse-logs)
            cargo build --release --locked \
                --bin shard-telemetry-server \
                --bin shard-telemetry-loki-load
            RESULT_ROOT="$RESULT_ROOT" RUN_ID="$RUN_ID" \
            SHARD_TELEMETRY_SERVER="$REPOSITORY/target/release/shard-telemetry-server" \
            SHARD_TELEMETRY_LOAD_BIN="$REPOSITORY/target/release/shard-telemetry-loki-load" \
                "$REPOSITORY/scripts/run-clickhouse-head-to-head.sh"
            ;;
        loki-logs)
            cargo build --release --locked --bin shard-telemetry-loki-load
            RESULT_ROOT="$RESULT_ROOT" RUN_ID="$RUN_ID" \
            LOADER_BIN="$REPOSITORY/target/release/shard-telemetry-loki-load" \
                "$REPOSITORY/scripts/run-loki-benchmark.sh"
            ;;
        clickhouse-signals)
            cargo build --release --locked \
                --bin shard-telemetry-server \
                --bin shard-telemetry-signal-bench
            RESULT_ROOT="$RESULT_ROOT" RUN_ID="$RUN_ID" \
            SHARD_TELEMETRY_SERVER="$REPOSITORY/target/release/shard-telemetry-server" \
            SHARD_TELEMETRY_BIN="$REPOSITORY/target/release/shard-telemetry-signal-bench" \
                "$REPOSITORY/scripts/run-signal-clickhouse-head-to-head.sh"
            ;;
        clickhouse-hot-queries)
            cargo build --release --locked \
                --bin shard-telemetry-structural-bench \
                --bin shard-telemetry-pack-query-bench
            RESULT_ROOT="$RESULT_ROOT" RUN_ID="$RUN_ID" \
            SHARD_TELEMETRY_BUILD_BIN="$REPOSITORY/target/release/shard-telemetry-structural-bench" \
            SHARD_TELEMETRY_QUERY_BIN="$REPOSITORY/target/release/shard-telemetry-pack-query-bench" \
                "$REPOSITORY/scripts/run-query-head-to-head.sh"
            ;;
        clickhouse-cold-queries)
            cargo build --release --locked --bin shard-telemetry-pack-query-bench
            RESULT_ROOT="$RESULT_ROOT" RUN_ID="$RUN_ID" \
            SHARD_TELEMETRY_QUERY_BIN="$REPOSITORY/target/release/shard-telemetry-pack-query-bench" \
                "$REPOSITORY/scripts/run-cold-query-head-to-head.sh"
            ;;
        *)
            echo "unsupported BENCHMARK_SUITE: $SUITE" >&2
            echo 'supported suites: clickhouse-logs, loki-logs, clickhouse-signals, clickhouse-hot-queries, clickhouse-cold-queries' >&2
            exit 2
            ;;
    esac
}

run_suite

[[ -d $RUN_DIRECTORY ]] || {
    echo "benchmark suite did not create its evidence directory: $RUN_DIRECTORY" >&2
    exit 1
}
for required in provenance.txt harness.log; do
    [[ -s $RUN_DIRECTORY/$required ]] || {
        echo "benchmark suite did not create required evidence: $required" >&2
        exit 1
    }
done
case "$SUITE" in
    clickhouse-logs)
        required=(status.txt summary.tsv storage-accounting.tsv query-latency-summary.tsv)
        ;;
    loki-logs)
        required=(summary.tsv components.tsv)
        ;;
    clickhouse-signals)
        required=(summary.tsv query-result-sha256.txt)
        ;;
    clickhouse-hot-queries)
        required=(ingest-summary.tsv)
        ;;
    clickhouse-cold-queries)
        required=(checksums.tsv)
        ;;
esac
for artifact in "${required[@]}"; do
    [[ -s $RUN_DIRECTORY/$artifact ]] || {
        echo "benchmark suite did not create expected result: $artifact" >&2
        exit 1
    }
done

# Never upload raw corpus data, durable stores, local tokens, or full request
# logs. The selected text evidence is enough to reproduce the comparison from
# the recorded corpus hashes and command provenance.
EVIDENCE_DIRECTORY=$RUN_DIRECTORY/evidence
mkdir -p "$EVIDENCE_DIRECTORY"
{
    printf 'suite=%s\nrun_id=%s\nrepository=%s\nrevision=%s\ncompleted_utc=%s\n' \
        "$SUITE" "$RUN_ID" "$REPOSITORY" "$(git -C "$REPOSITORY" rev-parse HEAD)" \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$EVIDENCE_DIRECTORY/qualification.env"

declare -a artifact_names=(provenance.txt)
artifact_names+=("${required[@]}")
case "$SUITE" in
    clickhouse-logs)
        artifact_names+=(clickhouse-parts.tsv query-latency-summary.tsv)
        ;;
    loki-logs)
        artifact_names+=(files.tsv settlement.log)
        ;;
    clickhouse-signals)
        artifact_names+=(query-result-sha256.txt)
        ;;
    clickhouse-hot-queries|clickhouse-cold-queries)
        artifact_names+=(checksums.tsv clickhouse-query-log.tsv)
        ;;
esac

for artifact in "${artifact_names[@]}"; do
    source=$RUN_DIRECTORY/$artifact
    [[ -f $source ]] || continue
    [[ $(stat -c %s "$source") -le 16777216 ]] || {
        echo "refusing to retain oversized evidence artifact: $artifact" >&2
        exit 1
    }
    install -m 0644 "$source" "$EVIDENCE_DIRECTORY/$artifact"
done

(
    cd "$EVIDENCE_DIRECTORY"
    find . -maxdepth 1 -type f -print0 | sort -z | xargs -0 sha256sum
) >"$EVIDENCE_DIRECTORY/SHA256SUMS"
printf 'passed\n' >"$EVIDENCE_DIRECTORY/status.txt"
printf 'benchmark qualification evidence: %s\n' "$EVIDENCE_DIRECTORY"
