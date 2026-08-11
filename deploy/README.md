# Single-node deployment

`shard-telemetry.service` is the hardened baseline for the open single-tenant
server. HA, replication, quorum writes, and automatic failover are licensed
distribution features and are intentionally absent here.

## Install

1. Install the release binary as `/usr/local/bin/shard-telemetry-server`.
2. Create the `shard-telemetry` system user and group.
3. Put a random token of at least 16 bytes in
   `/etc/shard-telemetry/auth-token`, owned by `root:shard-telemetry` with mode
   `0640` or stricter.
4. Select exactly one durable object backend:
   - keep the unit's local `--object-store-directory` on durable storage; or
   - replace it with the S3 bucket, prefix, and region flags documented below.
5. Edit the tenant, shard count, retention, and admission limits in the unit.
6. Terminate TLS in a local reverse proxy and expose only the proxy. Keep the
   native listener private unless the network supplies equivalent encryption
   and identity controls.
7. Run `systemctl daemon-reload && systemctl enable --now shard-telemetry`.

Use `/ready` for readiness and `/metrics` for scraping; both are intentionally
unauthenticated for local supervisors. Every data and administrative route
requires `Authorization: Bearer <token>`. Stop the unit normally so SIGTERM can
drain admission, publish compressed catalogs, reclaim covered source packs,
and synchronize the final checkpoint.

## S3

Replace `--object-store-directory` with, for example:

```text
--object-store-s3-bucket=company-telemetry
--object-store-s3-prefix=production/shard-telemetry
--object-store-s3-region=us-east-1
```

The server uses the standard AWS environment, workload-identity, ECS, and
instance-metadata credential chain. Do not put long-lived credentials in the
unit. Give the workload only object read, create, conditional update, and exact
delete access under its dedicated prefix. Deny public access and require TLS;
production mode rejects `--object-store-s3-allow-http`.

Enable bucket versioning and retain versions according to the recovery policy.
Also configure a bounded provider lifecycle action that aborts incomplete
multipart uploads left by a hard host failure. Do not configure age-based
expiration for completed catalog objects. ShardTelemetry reclaims completed
objects from exact transaction/catalog ownership records without listing the
bucket or running a tracing garbage collector.

`--object-store-writer-lease-seconds` must exceed the longest permitted flush
or upload, and `--object-store-reader-grace-seconds` must exceed the longest
query. Startup enforces the configured flush/query relationships. A fresh
`PENDING` lease fails closed; do not bypass it by deleting control objects.

## Backup and restore

The supported public-repo backup is a quiesced single-node snapshot. Do not
copy a live data directory and do not edit `FORMAT`, `LOCK`, `CURRENT`, or
`PENDING` by hand.

1. Stop ingestion at the proxy and wait for in-flight clients to finish.
2. Stop the service normally and verify it exited successfully.
3. Snapshot the complete local data directory, preserving ownership, modes,
   sparse files, and filesystem synchronization semantics.
4. For a local object backend, snapshot the complete object directory in the
   same stopped interval.
5. For S3, record the bucket, deployment prefix, object-versioning state, and a
   version-inventory/checkpoint timestamp after the process has stopped. Retain
   every version visible at that boundary.
6. Record the release version, binary SHA-256, configuration, and the
   shard-stream dependency commit from `Cargo.lock` with the backup manifest.

Restore only into empty, isolated directories and a new S3 prefix. Restore the
local data plus its matching local-object snapshot, or restore the exact S3
object versions captured at the backup boundary. Start the same release on
loopback, require `/ready`, run representative log/trace/metric lookups, invoke
an administrative flush, stop it cleanly, and verify a second restart before
moving traffic. Never point a restore rehearsal at the live S3 prefix.

Rehearse this procedure for every release candidate and at least quarterly.
An untested copy is not a backup.

## Upgrade and rollback

This pre-release product ships one storage format and no dual-format reader.
Before an upgrade, complete and verify a backup as above. Drain and stop the old
binary, install the new binary, then start it against a cloned restore first.
Promote only after readiness, exact queries, flush, and second-restart checks
pass. If startup reports a format mismatch, stop; restore the prior snapshot
and binary. Never rewrite the format marker to force an upgrade or rollback.

## Monitoring

Alert when readiness is zero, durable-sink pending bytes or checkpoint age
continues growing, dirty partitions are nonzero, object-store failures rise,
retention failures rise, or source reclaimed offsets stop advancing while
ingest continues. Capacity planning must include the local write spool, object
cache, retained compressed payload/index bytes, and the configured S3
version-retention window.
