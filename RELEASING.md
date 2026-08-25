# Releasing ShardTelemetry

Releases are built from annotated `vMAJOR.MINOR.PATCH` tags by GitHub Actions.
ShardTelemetry is distributed as a crates.io package, GitHub source archive,
and attested server binary. The manifest pins source builds to the exact public
shard-stream release commit while retaining exact registry versions for the
normalized crates.io manifest.

1. Update `CHANGELOG.md`, `Cargo.toml`, and `Cargo.lock` with the release date
   and version.
2. Confirm the pinned shard-stream commit is its annotated release tag and
   publish the shard-stream crates in the dependency order documented by that
   repository. The exact `shard-stream-core`, `shard-stream-protocol`, and
   `shard-stream-engine` versions must be visible in the crates.io index
   before ShardTelemetry can be packaged.
3. Run `bash scripts/release-gate.sh` on Linux and retain its output with the
   release evidence.
4. Run `cargo publish --dry-run --locked`. Inspect the normalized manifest and
   package file list; do not proceed if Cargo selects a different shard-stream
   version or includes repository-only material.
5. Deploy the candidate on an isolated restore of a production-shaped backup.
   Require readiness, representative log/trace/metric queries, an
   administrative flush, clean shutdown, and a second successful restart. For
   S3, use a new prefix and verify the incomplete-multipart lifecycle rule;
   never run the rehearsal against the live prefix.
6. Confirm required CI and supply-chain checks pass on the release commit and
   archive the backup/restore rehearsal evidence with the gate output.
7. Create and push an annotated version tag.
8. Verify the GitHub release contains the Linux binary archive, source archive,
   Apache and third-party notices, SHA-256 checksums, SPDX SBOM, and
   build-provenance attestation. The tag workflow also repeats the crates.io
   dry run against the exact tagged source.
9. From a clean checkout of that exact tag, run `cargo publish --locked`.
   Publishing is a manual, irreversible step and is never performed by the
   validation workflow.
10. Confirm `cargo info shard-telemetry@MAJOR.MINOR.PATCH` resolves, then install
    the archive on a clean Linux host and run
    `shard-telemetry-server --help` before publishing the release notes.

Never retag or replace a published release. Issue a new patch version instead.
Pre-release storage upgrades are not in-place compatible unless the release
notes explicitly say so; preserve the prior binary and verified backup until
the new release passes its second restart.
