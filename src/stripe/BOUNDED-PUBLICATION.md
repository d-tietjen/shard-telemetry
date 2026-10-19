# Consuming log query-index publication

EDEN-2505 adds the internal `LogStripe::publish_durable_log_until` producer.
It consumes the actual stripe and `DurableAppend` before checking limits or
the supplied `NativeResumeWork`. The complete original work owner, input,
existing query state and partial results remain in the returned owner. An
error has no stripe or native-owner extractor. Only success can return its
stripe through `PublishedLogIndex::into_stripe`.

This producer validates the actual physical shard, Publish delivery, absence
of an atomic group, reservation extent and supplied contiguous checkpoint
edge. It verifies STEL using the ordinary checksum domain and borrowed parser,
then invokes the real retained SLW1/SLT1 preparation and embedded-index
producers. With `IndexedFramePreparationLimits::decompression: None`, missing
actual process-local index context retains the previous explicit refusal.
With `Some(policy)`, the real bounded streaming zstd child reconstructs the
structural section and embedded index from durable compressed bytes. It
retains its native context, output prefixes and actual results on the same
work/E; no transient SLT1 sidecar is invented and the ordinary unbounded
decompressor is not entered. Ordinary ingest and query behavior is unchanged.

The common internal preparation path also accepts a closed journal-row input
from an actual anchored source. It validates the real stored shard/extent and
native expected/next instead of fabricating absent live reservation fields.
Its metadata/payload getters borrow that original row, and its result retains
the exact anchor. The existing `DurableAppend` entry remains the default type
and retains every original live eligibility check. The replay variants,
imports and source methods are Unix-only; the existing live publication
module and `DurableAppend` path remain available on other platforms.
Replay still uses the same
STEL/frame/decompression/index staging and final publication producer.

Completed child indexes move into paid `Arc` allocations and the normal
`IndexedIngestFrame`/`IndexedFrameAppend` representation. The original child
attempts, encoded input, shared work and complete child Results remain owned
after this value transfer. Compressed `Bytes` ranges refer to the actual
retained payload. Each explicit slice/clone pays the selected bytes 1.12.1
Shared promotion header before entry, independently of earlier child fees.

All parsing, checksum chunks, output reservations, measured capacity excess,
movement and hash-table probes spend the SAME original work and absolute
deadline. `LogIndexPublicationLimits` also bounds outer envelope/context bytes,
partitions, appends, declared values, element slots and work. Child ceilings
are additional and share the original native cumulative ledger. These are
source-visible values and requested element capacities; allocator internals,
RSS and a hard bound on allocator duration are outside this accounting.

Map migration reserves an empty same-hasher table before moving the actual
old map into a retained iterator. Each real iteration and insertion prepays
the relevant actual capacity. The current entry, target partition, staged
table and existing active-partition cache remain owned on refusal. This
avoids a deep clone or an opaque rehash of live query owners. The old cache
is retained when the empty replacement is installed.

`prepare_durable_log_until` now returns a consuming prepared owner with all
commit fees already paid. It owns the actual stripe, input, completed children,
staged state and original work; it publishes no new append or watermark.
`PreparedLogIndexPublication::commit_until` rechecks the same original E and
installs that retained staging. The existing `publish_durable_log_until`
method remains their exact composition. The consuming
[log-apply transaction](../sink/BOUNDED-APPLY.md) can therefore settle actual
journal durability between preparation and index commit without extracting
or cloning child indexes. An unexpected/late commit error keeps its whole owner.

The final exclusive commit uses admitted capacity and fixed integer-key
hashing. It installs the complete append before its watermark, then saves
frame IDs, the staged map, cache retirement and actual outcome without a
callback, allocation or deadline check between those movements. A later
deadline or observer panic returns a terminal error retaining the actual
publication. No rollback or no-mutation assertion follows from an error.
Covered ranges retain the ordinary watermark behavior and report
`AlreadyCovered`; this is not content-identity or durable-ACK evidence.

Seven fixture groups use an actual recovered engine/WAL/strict callback to
obtain real `NativeResumeWork`. The callback deliberately refuses after the
component assertions, so no successful telemetry factory or full native
resume is fabricated. Actual ordinary compression, STEL, SLW1/SLT1 indexes and
unchanged queries establish component parity. Sources cover identity and
capacity refusals, duplicate/partial-overlap ordering, actual old-owner/cache
movement, staged and published late/panic outcomes, and exact/one-less
cumulative shared work. Real resident index Weak references also prove that
interrupted occupied-map migration retains its one original index owner until
the terminal error drops, including randomized iterator/target/table placement.
The work-boundary fixture measures a real publication and prepays
other composing work on the SAME ledger before a second equivalent attempt;
it creates no replacement budget or native worker. The captured transient
context is the actual ordinary producer's process-local sidecar, explicitly
supplied to the fixture rather than inferred from WAL recovery. Subsequent
synthetic placement examples use real sequencer results as component inputs;
only the first append in each fixture is the recovered WAL append.

This is query-index publication. The separate consuming log transaction joins
it to actual anchored journal durability and application checkpoint publication;
the publisher alone cannot return Applied. Neither unit installs a
`DurableAppendSinkFactory`, object-store producer, leader grant or serving
readiness. Existing stripe
setup/destruction and ordinary later queries are outside its bounded scope.
The concrete original graphs still require coherent API-bearing public
releases, unchanged-lock reconciliation where applicable and genuine missing
dependency archives before compilation and runtime qualification. No shipping
manifest, lock or pin changes are part of this unit.
