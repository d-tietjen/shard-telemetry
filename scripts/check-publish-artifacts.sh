#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

fail() {
  echo "publish-artifact check failed: $*" >&2
  exit 1
}

metadata_file="$(mktemp "${TMPDIR:-/tmp}/shard-telemetry-release-metadata.XXXXXX")"
package_list="$(mktemp "${TMPDIR:-/tmp}/shard-telemetry-package-list.XXXXXX")"
trap 'rm -f "$metadata_file" "$package_list"' EXIT

cargo metadata --locked --no-deps --format-version 1 > "$metadata_file"

package_version="$(
  jq -r '.packages[] | select(.name == "shard-telemetry") | .version' "$metadata_file"
)"
test -n "$package_version" && test "$package_version" != "null" || fail "could not resolve the shard-telemetry package version"

publish_policy="$(
  jq -c '.packages[] | select(.name == "shard-telemetry") | .publish' "$metadata_file"
)"
test "$publish_policy" = '["crates-io"]' || fail "shard-telemetry must publish only to crates.io"

documentation="$(
  jq -r '.packages[] | select(.name == "shard-telemetry") | .documentation' "$metadata_file"
)"
test "$documentation" = "https://docs.rs/shard-telemetry" || fail "docs.rs package metadata is missing"

expected_shard_stream_rev="fef8695b1724e04e10aa26e500e68b8b535efa13"
expected_shard_stream_version="=0.2.0"
shard_stream_dependencies=0
while IFS=$'\t' read -r dependency requirement source; do
  test -n "$dependency" || continue
  case "$dependency" in
    shard-stream-core|shard-stream-engine|shard-stream-protocol) ;;
    *) fail "unexpected Git dependency: $dependency" ;;
  esac
  test "$requirement" = "$expected_shard_stream_version" || fail "$dependency must require $expected_shard_stream_version"
  case "$source" in
    *"rev=$expected_shard_stream_rev"*) ;;
    *) fail "$dependency is not pinned to shard-stream $expected_shard_stream_rev" ;;
  esac
  shard_stream_dependencies=$((shard_stream_dependencies + 1))
done < <(
  jq -r '.packages[]
    | select(.name == "shard-telemetry")
    | .dependencies[]
    | select(.source != null and (.source | startswith("git+")))
    | [.name, .req, .source]
    | @tsv' "$metadata_file"
)
test "$shard_stream_dependencies" -eq 3 || fail "expected exactly three pinned shard-stream dependencies"

package_args=(--locked)
if test "${SHARD_TELEMETRY_ALLOW_DIRTY_PACKAGE:-0}" = "1"; then
  package_args+=(--allow-dirty)
fi
cargo package "${package_args[@]}" --list > "$package_list"

required_files=(
  Cargo.lock
  Cargo.toml
  LICENSE
  NOTICE
  README.md
  THIRD_PARTY_NOTICES
  src/embedded.rs
  src/fast_telemetry.rs
  src/lib.rs
)
for required_file in "${required_files[@]}"
do
  grep -Fxq "$required_file" "$package_list" || fail "package archive does not include $required_file"
done

repository_only_prefixes=(
  .github/
  clickhouse/
  competitive/
  deploy/
  scripts/
)
for repository_only_prefix in "${repository_only_prefixes[@]}"
do
  if grep -Fq "$repository_only_prefix" "$package_list"; then
    fail "package archive includes repository-only path $repository_only_prefix"
  fi
done

printf 'Crates.io manifest and file list are ready for shard-telemetry %s.\n' "$package_version"
printf 'Run cargo publish --dry-run only after shard-stream %s is in crates.io.\n' "${expected_shard_stream_version#=}"
