# Changelog

All notable changes to ShardTelemetry will be documented here. The project follows
Semantic Versioning after `1.0.0`; pre-1.0 releases may change unstable storage
and protocol interfaces when called out in release notes.

## [Unreleased]

## [0.2.0] - 2026-09-07

### Added

- Bounded embedded recent-history retention with independent RAM, SSD, and
  time limits, oldest-first eviction, delete or S3 offload policies, and
  crash-safe lifetime metric rollups.
- An optional fast-telemetry snapshot bridge for exporting application metrics
  directly into the embedded runtime without changing its recording hot path.
- Rust-native S3/S3-compatible durable object storage with workload
  credentials, conditional catalog publication, streaming multipart uploads,
  and BLAKE3 verification.
- Physical all-signal retention, exact-key object-store metrics, and automatic
  shard-stream source-pack reclamation after compressed catalog checkpoints.
- Production backup/restore, upgrade/rollback, S3 lifecycle, and monitoring
  runbooks.

### Changed

- Object publication and retention now use bounded, crash-replayable ownership
  records, immutable catalog leases, and exact-key reclamation. No bucket
  listing or tracing garbage collector is required.
- shard-stream is pinned to the public 0.2.0 crates.io release candidate
  `fef8695b1724e04e10aa26e500e68b8b535efa13`.

## [0.1.0] - 2026-08-03

### Added

- Apache License 2.0 distribution files and public contribution policies.
- Automated formatting, lint, test, supply-chain, package, and release checks.
- Append-aligned immutable payload/query-index publication, cold range reads,
  bounded SSD caching, and catalog-checkpoint recovery.
- Loki POST form compatibility, parser/formatting pipelines, unwrapped range
  functions, vector aggregation, binary operators, and vector matching.
- Required third-party notices, SPDX SBOM generation, provenance attestation,
  and immutable GitHub Actions dependencies.

### Removed

- GPL/AGPL codec dependencies from the public build and codec benchmark.

- Initial public release of the single-tenant ShardTelemetry storage engine.
