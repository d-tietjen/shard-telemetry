# Contributing to ShardTelemetry

Thank you for helping improve ShardTelemetry.

## Development

ShardTelemetry uses the Rust toolchain pinned in `rust-toolchain.toml`. A change is
ready for review when these commands pass:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo build --workspace --all-targets --all-features --release --locked
scripts/release-gate.sh
```

Add regression tests for behavioral changes. Storage-format, recovery, query,
and protocol changes require malformed-input and restart-boundary coverage.
Benchmark claims must follow BENCHMARKS.md: include the command, corpus
identity, machine, CPU allocation, build revision and profile, verification
mode, and retained result artifact. Do not include private host names, paths,
or non-redistributable data in a public pull request.

## Pull requests

Keep changes focused and explain compatibility or operational consequences.
Do not commit credentials, production log content, benchmark corpora, build
artifacts, or generated secrets. By submitting a contribution, you agree that
it is licensed under the Apache License 2.0.

Use GitHub's private vulnerability-reporting flow for security issues; see
`SECURITY.md`.
