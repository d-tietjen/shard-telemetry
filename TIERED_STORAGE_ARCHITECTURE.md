# Tiered storage architecture

ShardTelemetry's durable tier is designed so data volume can grow to at least one
pebibyte without requiring a process to load a corpus-wide block catalog,
enumerate an object-store bucket, or validate every payload at startup.

The implementation follows shard-stream's proven lifecycle:

- data objects are immutable;
- publication is ordered and idempotent;
- a small `CURRENT` object selects an immutable metadata generation;
- the final publication is conditional, fencing stale writers;
- local data is released only after object durability is visible; and
- retention publishes new metadata before deterministically relinquishing exact
  object keys.

ShardTelemetry changes the manifest shape. shard-stream's per-shard pack list is
small enough to keep in one manifest. A petabyte-scale log database needs a
partition-scoped hierarchy of roots, catalog pages, block groups, query-index
segments, dictionaries, and payload ranges.

## Storage units

| Unit | Recommended production target | Purpose |
| --- | ---: | --- |
| Structural block | Existing 8 MiB source target | Independent compression, checksum, and selective decode boundary |
| Block group | 1 GiB compressed target, 2 GiB hard limit | Amortizes object PUT/GET cost across ordered blocks |
| Blocks per group | 4,096 hard limit | Bounds group manifests even when blocks compress extremely well |
| Catalog page | 1,024 groups | Bounds metadata fetched for one range lookup |
| Catalog root | References pages only | Keeps startup independent of block count |
| SSD cache chunk | 4 MiB | Avoids downloading an entire group for one matching block |
| Control-object read | 64 MiB hard limit | Prevents corrupt metadata from causing unbounded allocation |

The defaults are represented by `ObjectTierConfig` and `SsdCacheConfig`.
Deployments may close a group before its byte target for age, durability, or
partition-idleness reasons, but may not cross the configured hard limits.

Embedded deployments additionally use `DurableTelemetryLimits` and
`EmbeddedTelemetryConfig::with_storage_budgets` to divide a node-local budget
between signal heads, control cache, payload cache, and local immutable
payloads. The payload budget is enforced per signal partition; complete groups
are retired in event-time order and the newest group remains the recovery
anchor. Embedded group sizing is reduced to keep that anchor within its
partition budget. Byte accounting covers engine-controlled payload/cache state;
filesystem metadata and one in-flight WAL/spool group remain bounded bursts
that operators must reserve headroom for.

At the worst case of one PiB of already-compressed payload and 1 GiB groups,
there are 1,048,576 groups and 1,024 catalog pages. A root therefore has about
one thousand references, not one million block entries. With highly
compressible 8 MiB source blocks, the 4,096-block cap closes groups first and
keeps every group manifest bounded. Physical shards and logical partitions
split those totals further.

These are metadata-scale guarantees, not an assumption that arbitrary logs
will match the 136.68x ratio measured on the repetitive ClickHouse corpus.
Capacity planning must use measured stored bytes for each production source.

## Namespace and artifacts

Every catalog is scoped to one physical shard and one logical partition:

```text
catalog/
  shard-<physical-shard>/
    topic-<32-hex-digit-topic-id>/
      partition-<logical-partition>/
        CURRENT
        PENDING
        transactions/<transaction-id>/
          roots/root-<generation>-<checksum>.json
          pages/page-<sequence>-<checksum>.json
          groups/<group-sequence>/
            manifest-<checksum>.json
            payload-<name>-<checksum>
            query-index-<name>-<checksum>
            dictionary-<name>-<checksum>
            dictionary-catalog-<name>-<checksum>
```

`CURRENT` and `PENDING` are the only mutable control keys. Every data key is
immutable, belongs to one transaction-specific prefix, and includes a BLAKE3
content checksum. Readers never use object listing. They start from a known
namespace and follow authenticated references.

A block-group manifest records:

- physical shard and logical partition identity;
- source cohort, final compression placement, and dictionary ID per block;
- offset, timestamp, record-count, source, structural, and stored byte
  accounting;
- compression temperature and variance diagnostics;
- exact payload offset and length per block;
- a BLAKE3 checksum for each compressed block; and
- object key, size, and BLAKE3 checksum for every group artifact.

The query index is an independent artifact per group. This is the persistent
query architecture's segmentation boundary: a cold lookup loads postings only
for candidate groups instead of expanding the measured 15.90 GiB global index.

## Publication protocol

The owning shard worker is the only normal publisher for a
`(physical shard, topic, partition)` namespace. Publication is:

1. Seal a bounded set of compressed blocks and its independent query index.
2. Compute the complete, bounded set of transaction-owned object keys and
   conditionally create `PENDING` with those exact keys, the target root, and a
   writer-lease deadline.
3. Write and synchronize a local payload pack. `write_staged_payload_pack`
   records every block's exact range and checksum without buffering the whole
   pack a second time.
4. Put payload, query index, required dictionaries, and assignment metadata
   with immutable put-if-absent semantics.
5. Put the immutable group manifest.
6. Append the group entry to an immutable catalog page.
7. Put a new immutable catalog root that references the new page generation.
8. Compare-and-swap `CURRENT` from the writer's observed object version token to the new root
   pointer.
9. Delete `PENDING`; if this idempotent cleanup is interrupted, recovery sees
   that its target is selected and removes only `PENDING`.
10. Call `mark_group_offloaded` only after step 8 succeeds. This records object
   ranges and releases staged block payloads.

Retries with identical logical content are accepted. Reusing a sequence with
different content is corruption. The fixed `PENDING` key serializes conforming
publishers for one catalog, and conditional `CURRENT` replacement fences stale
catalog generations.

A crash before `CURRENT` moves leaves an exact ownership record. Once its
writer lease expires, startup deletes only the keys named by that record and
then removes `PENDING`. A crash after `CURRENT` moves preserves all selected
objects and removes only `PENDING`. A fresh lease fails closed, so a second
process cannot reclaim objects still owned by a live publisher. This is the
storage equivalent of Rust ownership: transaction keys have one owner, catalog
roots share immutable reachability through leases, and cleanup occurs when
ownership ends. It requires no bucket listing, reachability sweep, or tracing
garbage collector.

`LocalObjectStore` implements these rules with synchronized temporary writes,
atomic rename, parent-directory synchronization, BLAKE3 verification, and a
filesystem update lock. The shipped Rust-native `S3ObjectStore` implements
immutable create, bounded GET, range GET, HEAD, exact-key delete, streaming
multipart upload, and conditional replacement using object version tokens. If
an object service cannot conditionally replace `CURRENT`, it is unsafe for this
catalog. A hard host failure can strand provider-internal, incomplete multipart
parts before an object key exists; configure the bucket's bounded
abort-incomplete-multipart lifecycle rule for those hidden parts. That rule is
not catalog garbage collection and must never expire completed catalog objects.

## Cold query path

A lookup over sealed data performs:

1. Read `CURRENT` and the selected root from the metadata SSD cache, refreshing
   only when its generation changes.
2. Prune root page references by offset and event-time bounds.
3. Read only candidate catalog pages.
4. Prune their group entries by the same coarse bounds.
5. Load each surviving group's `SLOGQIX2`/`SLOGQIZ2` query-index segment.
6. Intersect exact term and metadata postings and apply trigram rejection.
7. Range-read only selected compressed block extents through the payload SSD
   cache.
8. Verify the per-block checksum, decompress, selectively reconstruct
   candidate records, and run every exact residual predicate.

This preserves the existing hot/cold query compatibility contract. Catalog
and trigram collisions can only create extra reads; reconstruction and exact
filtering remain authoritative.

`TelemetryObjectTier::open` deliberately validates only `CURRENT` and the immutable
root. A page is verified when its bounds are touched, a group manifest when
selected, and a full artifact when read. Verifying every referenced payload on
startup would turn process recovery into a petabyte scan. A separate
background auditor should continuously sample or sweep immutable objects
without blocking availability.

## SSD tier

Object storage remains the durable authority after publication. Local NVMe has
two roles:

- an unpublished write spool for newly sealed groups; and
- a recoverable range cache for already published objects.

An S3-backed embedded writer admits each newly published payload and query
index directly from its staging file. It also warms the corresponding catalog
page and group manifest. The newest data is therefore locally queryable without
an initial range download, while cache eviction leaves the immutable S3 object
and catalog ownership unchanged.

`SsdObjectCache` is byte bounded and uses fixed-size chunks. Its cache identity
is the BLAKE3 hash of object key, immutable version token, and chunk index. Each local
chunk has its own length and BLAKE3 integrity header. A corrupt chunk is
discarded and fetched again. Startup reconstructs the cache directory and
evicts least-recently-used entries until it fits the configured budget.

Production should create at least two cache instances:

| Cache | Suggested policy |
| --- | --- |
| Metadata/index | Smaller chunks, protected capacity, long residency |
| Payload | 4 MiB chunks, large capacity, scan-resistant admission |

`read_range_with_metadata` accepts object size, version token, and BLAKE3
content digest already authenticated
by a group manifest, avoiding one remote HEAD request per block lookup.
`read_range` remains available when the caller has only an object key.

The current cache uses exact LRU for its bounded local directory. A later
high-concurrency implementation may replace only the admission/eviction data
structure with stripe-local TinyLFU; it must retain the same immutable cache
identity and integrity framing.

## Recovery and durability

There are three explicit durability states:

| State | Meaning |
| --- | --- |
| Stream durable | shard-stream has synchronized the source append |
| SSD staged | ShardTelemetry block, query index, and group files can be retried locally |
| Object durable | `CURRENT` selects a root that reaches every required immutable artifact |

An object-durable acknowledgement, when requested, must wait through the
`CURRENT` compare-and-swap. A local-durable acknowledgement may return after
the write spool is synchronized, with offload continuing in the owning worker.
The indexed watermark must never advance beyond the selected durability mode's
data and query index.

On restart:

1. Resolve `PENDING`: preserve a selected transaction, reject a fresh active
   writer, or exact-delete an expired uncommitted transaction.
2. Recover shard-stream and replay any source offsets beyond ShardTelemetry's durable
   index checkpoint.
3. Open each known catalog directly; do not list the bucket.
4. Reconcile synchronized local spool groups against the selected root.
5. Retry unpublished groups idempotently.
6. Remove a local spool group and advance shard-stream's batch-aligned log start
   only after its catalog generation is selected.

Historical physical-shard ownership is part of the query coordinator's routing
metadata. A logical partition that moved between physical shards may have
catalogs in more than one shard namespace; the coordinator merges them by the
same durable offset and timestamp order used by hot queries.

## Retention and deterministic reclamation

Retention is metadata first:

1. Build new immutable pages excluding groups wholly below the retention
   boundary.
2. Publish a new root and conditionally advance `CURRENT`.
3. Record every superseded exact key in the selected root with a reclamation
   deadline.
4. Wait for all in-process `CatalogLease` references and the configured
   cross-process reader grace period.
5. Delete the recorded group artifacts, manifests, superseded pages, and roots
   by exact key during bounded maintenance passes.
6. Evict matching SSD cache chunks opportunistically; correctness does not
   depend on immediate eviction.

For embedded local-delete mode, the same catalog transaction also runs when a
partition exceeds its configured payload capacity, selecting the oldest
complete groups first. For embedded S3-archive mode, time retention is logical
on the device: old raw objects remain in S3, while the byte-bounded local cache
expels chunks as newer publications arrive.

Before either source WAL reclamation path advances, the embedded store folds
all metric points through the durable WAL into a local lifetime-rollup catalog
and atomically synchronizes it. Cumulative sums and histograms derive deltas
across snapshots and detect resets; gauges retain latest/min/max outcomes. A
decode, cardinality-limit, or persistence failure aborts reclamation without
advancing the rollup checkpoint, preventing both data loss and double counting
on retry.

Boundary groups remain intact until every block in them expires. Optional
compaction may rewrite a partially expired group under a new sequence, but it
must publish the replacement before removing the original. Legal hold is a
root-selection policy: held groups remain reachable regardless of the normal
time cutoff.

The newest group remains as a bounded checkpoint anchor even when every record
in it is older than the cutoff. Reclamation state is capped by
`max_retired_objects`; a writer fails closed rather than create an unbounded
delete backlog. `TelemetryObjectStore::delete` accepts only validated exact
keys, is idempotent, and is used by the ownership protocol itself. It never
lists a namespace and never infers liveness from object reachability.

## Worker integration

The worker-level integration mirrors shard-stream's pack offloader:

- one mutable group builder belongs to each shard worker;
- block compression, query-index construction, and local spool writes remain
  worker local;
- a group closes on target bytes, block count, explicit flush, or shutdown;
- publication runs in sequence order for each shard/partition namespace;
- backpressure is based on unpublished SSD spool bytes, never total retained
  object bytes; and
- local spool retention advances only from authoritative catalog generations.

`LogStripe::offload_indexed_groups` constructs append-aligned payload and query
artifacts, publishes them through `TelemetryObjectTier`, and releases resident frames
only after the new `CURRENT` generation is selected. On restart, catalog
checkpoints skip already-published recovery transactions. Queries load a group
index before payload and verify each selected frame checksum after range read.

The standalone binary ships both `LocalObjectStore` and the Rust-native
`S3ObjectStore`. The public `TelemetryObjectStore` trait remains the integration
point for other backends; adapters must preserve immutable create, bounded
reads, exact idempotent deletion, and conditional `CURRENT` replacement.

## Required operational metrics

At minimum, report these per shard and partition:

- staged and object-durable group sequence;
- unpublished SSD spool bytes and oldest spool age;
- group payload bytes, block count, and close reason;
- `CURRENT` generation and conditional-publication failures;
- catalog root/page/group cache hit rates;
- payload-cache hit bytes, miss bytes, evictions, and integrity failures;
- object PUT, GET, range-GET, HEAD, compare-and-swap, exact-delete, transferred
  bytes, and failure counts;
- query pages and groups pruned before index fetch;
- index bytes fetched and blocks range-read per query;
- checksum, decompression, and reconstruction failures; and
- retention runs, retired groups/bytes/exact keys, and source offsets reclaimed.

Alerts should fire on a non-advancing object-durable sequence, spool growth
approaching its budget, repeated stale-writer failures, or any immutable object
checksum mismatch.
