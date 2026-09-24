# Embedded product-usage ledger

`EmbeddedUsageLedger` is the bounded offline usage-data path. It is separate
from logs, traces, metrics, and `LifetimeMetricRollup`: those observability
systems preserve arbitrary signal identity and therefore cannot provide the
same end-to-end size contract.

## Stored data

The host supplies a fixed feature-ID registry when it opens the ledger. The
ledger stores only:

- one monotonic lifetime count per registered feature;
- total active seconds;
- optionally, the same values for the latest fixed number of calendar months;
- one fixed overflow count if `UnknownFeaturePolicy::AccumulateOverflow` is
  selected.

There are no arbitrary labels, descriptions, resource attributes, histograms,
logs, or traces. The registry is sorted and fingerprinted. Reopening an
existing file with a changed registry, month count, or unknown-feature policy
fails instead of silently reinterpreting counters.

~~~rust
use shard_telemetry::{
    EmbeddedUsageLedger, EmbeddedUsageLedgerConfig, UnknownFeaturePolicy,
};

let usage = EmbeddedUsageLedger::open(
    EmbeddedUsageLedgerConfig::new(
        "/var/lib/my-app/usage.ledger",
        ["search", "export", "share"],
    )
    .with_monthly_buckets(12)
    .with_unknown_feature_policy(UnknownFeaturePolicy::Reject)
    .with_max_file_bytes(1024 * 1024),
)?;

usage.record_batch(30, [("search", 1)])?;
let snapshot = usage.snapshot()?;
let health = usage.health()?;
# Ok::<(), shard_telemetry::LokiApiError>(())
~~~

Use `record_batch_at` to commit active time and several feature increments as
one generation. Explicit timestamps make accelerated and deterministic tests
possible without changing the wall clock.

Every non-empty call synchronously writes and fsyncs one generation. Keep
recording off latency-sensitive application paths and batch related counters
with `record_batch` or `record_batch_at` when practical.

## Quota and crash contract

The complete ledger is one exclusively locked file containing two fixed-size
generation slots. Each generation has a format version, configuration
fingerprint, payload length, codec, generation number, payload CRC32C, and
header CRC32C. The MessagePack payload uses Zstandard when compression makes it
smaller.

The largest possible state is serialized before file creation. Open fails if
two slots do not fit the configured quota. The file's physical space is then
reserved up front, so later in-range writes cannot fail for lack of disk space.
There is no usage WAL, sidecar, spool, or temporary rewrite file. Both logical
file length and physical allocated bytes are checked against the quota and are
reported by `health()`.

Open rejects filesystems that do not report full physical reservation for the
fixed logical file. CRC32C detects torn writes and accidental corruption; it is
not an authentication code. Protect the ledger and its parent directory from
untrusted local writers with normal filesystem ownership and permissions.

A checkpoint writes and synchronizes the inactive payload before publishing
and synchronizing its checksummed header. Recovery validates both slots and
chooses the newest valid generation. A torn newest slot therefore falls back
to the prior complete update. A validation, counter-overflow, policy, quota, or
I/O failure leaves the prior in-memory and durable generation unchanged.
On Unix, initial creation also synchronizes the parent directory entry before
`open` returns successfully.

The quota covers ledger-owned persistent data. Parent-directory contents and
filesystem-global metadata are controlled by the host filesystem and are not
silently counted as usage data; place the ledger at a dedicated path if those
need a separate volume quota. The file quota is not a whole-process RSS limit:
serialization and compression use transient memory bounded by the fixed schema,
while allocator and process overhead remain the host's responsibility.

For backup, stop usage writers and drop the ledger handle before copying its
single file. Restore that file at a new isolated path, open it with the exact
same registry and policies, and verify `snapshot()` and `health()` before using
it. A mismatched configuration or invalid pair of generations fails closed.

## Bounds and health

Version 1 accepts at most 4,096 fixed feature IDs, 4,096 feature increments per
atomic batch, 128 UTF-8 bytes per feature ID, and 120 monthly buckets. Unknown
IDs are either rejected atomically or accumulated into one fixed overflow
counter. Updates older than a full rolling month window still affect lifetime
totals but do not recreate an expired month.

`EmbeddedUsageHealth` exposes logical and allocated file bytes, quota
headroom, current encoded generation bytes, series and month counts, durable
generation, last successful checkpoint, checkpoint/rejection counters,
unknown-feature behavior, and pending updates. Checkpoints are synchronous, so
accepted-update backlog is always zero.

For the general observability runtime,
`EmbeddedTelemetryRuntime::storage_health()` separately reports complete
logical and allocated regular-file bytes below its owned data directory.
Directory-entry and filesystem-global metadata remain host-controlled.

Regression coverage includes one-month and twelve-month accelerated workloads,
100 features over twelve months below 64 KiB, restart recovery, torn newest
generation recovery, closed-file backup/restore, quota exhaustion before file
creation, exclusive locking, configuration mismatch, and both unknown-feature
policies.
