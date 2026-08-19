# Releasing ShardTelemetry

Releases are built from annotated `vMAJOR.MINOR.PATCH` tags by GitHub Actions.
ShardTelemetry is distributed as a GitHub source archive and attested server binary;
`publish = false` prevents an unusable crates.io package while shard-stream is
consumed from pinned Git revisions.

1. Update `CHANGELOG.md`, `Cargo.toml`, and `Cargo.lock` with the release date
   and version.
2. Run `bash scripts/release-gate.sh` on Linux and retain its output with the
   release evidence.
3. Deploy the candidate on an isolated restore of a production-shaped backup.
   Require readiness, representative log/trace/metric queries, an
   administrative flush, clean shutdown, and a second successful restart. For
   S3, use a new prefix and verify the incomplete-multipart lifecycle rule;
   never run the rehearsal against the live prefix.
4. Confirm required CI and supply-chain checks pass on the release commit and
   archive the backup/restore rehearsal evidence with the gate output.
5. Create and push an annotated version tag.
6. Verify the GitHub release contains the Linux binary archive, source archive,
   Apache and third-party notices, SHA-256 checksums, SPDX SBOM, and
   build-provenance attestation.
7. Install the archive on a clean Linux host and run `shard-telemetry-server --help`
   before publishing the release notes.

Never retag or replace a published release. Issue a new patch version instead.
Pre-release storage upgrades are not in-place compatible unless the release
notes explicitly say so; preserve the prior binary and verified backup until
the new release passes its second restart.
