#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

expected=$'bincode v1.3.3\nlrpar v0.13.10\nlrlex v0.13.10\npromql-parser v0.10.0\nshard-telemetry v0.1.0'
observed="$({
  cargo tree --locked -e normal -i bincode@1.3.3 --prefix none |
    sed -e '/^promql-parser v0.10.0 (\*)$/d' \
        -e "s#shard-telemetry v0.1.0 (.*)#shard-telemetry v0.1.0#"
})"

if [[ "$observed" != "$expected" ]]; then
  echo "RUSTSEC-2025-0141 exception dependency path changed" >&2
  diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$observed") >&2 || true
  exit 1
fi

if rg -n --glob '*.rs' '\bbincode\b' src; then
  echo "bincode must not be used directly by ShardTelemetry" >&2
  exit 1
fi

echo "RUSTSEC-2025-0141 remains confined to promql-parser 0.10.0 -> lrlex/lrpar 0.13.10"
