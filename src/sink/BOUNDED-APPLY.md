# Consuming log apply transaction

EDEN-2505 adds the internal synchronous
`LogStripe::apply_durable_log_until` producer. It consumes the actual stripe,
owned native `DurableAppend`, original application expected/next coordinates,
opaque anchored journal, and the same application-checkpoint and active-cache
owners used by its caller. All enter retained custody before admission. This
component alone does not install a factory callback, queue, worker, or serving
host. The separate [owned log factory](BOUNDED-OWNED-LOG.md) now calls it from
the actual strict trait callback on its real registered worker.

Only the real dispatcher's single Publish/non-atomic log mode is supported.
Actual attached object-tier mode, other signals, atomic or Stage delivery,
incorrect physical shard/extent/coordinates, and unadopted journal replay
refuse before journal I/O. Original application coordinates and the ordinary
lane-gap rule stay intact. Only the separate data-index edge is derived from
the actual supplied reservation. No reservation, event identity or application
checkpoint is reconstructed from a journal row.

`SinkJournal::open_anchored_until` wraps the unchanged descriptor-anchored
`open_until`. It consumes the supplied directory descriptor, prepays its
wrapper/error/Arc requests on the same original `TelemetryRecoveryBudget`,
and retains every actual replay row and native Result. Its opaque success has
no ordinary-journal converter, raw journal accessor, mutable owner extractor,
or reset. A late wrapper refusal retains even a fully completed native open,
including its actual advisory lock. This carrier records provenance only;
the caller must supply genuine namespace ownership and live control. Setup E
is retained as history and never reused or renewed as command permission.

The transaction uses one original live `NativeResumeWork` and its minimum E
through every phase. It observes the same opaque anchor's sticky negative
state before entry and after completed phases. Same-owner Busy/refusal closes
that dependency, so an in-flight transaction observes denial before its next
phase or acceptance check. Denial is not a rollback: an entered physical call
settles synchronously, its complete result is saved first, and a later check
can return a terminal owner with genuine durable or published effects.

The actual application checkpoint mutex spans checkpoint observation, index
preparation, journal durability, index commit and application publication.
The actual active-cache mutex spans physical work and final cache retirement.
`try_lock` refuses contention. Poison and every error/panic path close the
negative dependency before a captured guard releases. User code cannot enter
between the final admitted cache/checkpoint/effect movements. Observer panic
payloads and actual child Results are retained before guard release.

Preparation invokes the real bounded STEL/frame/index producers. Optional
`IndexedFramePreparationLimits::decompression` performs actual streaming
contextless durable recovery; `None` preserves the previous explicit refusal
when the genuine live context is absent. The complete prepared index remains
owned while the opaque anchor enters the actual bounded journal bridge. Only
`JournalAppendKind::Committed` at the exact original next checkpoint permits
index commit. `AlreadyCovered` proves only a journal watermark; it cannot
prove this input's content and is refused. Likewise a covered index watermark
cannot substitute for an application checkpoint or exact-content retry proof.

After actual complete file write and sync, the consuming prepared publisher
installs normal query frames/watermark. The application checkpoint is then
inserted in the original shared map, the original active cache is retained and
replaced, and `DurableSinkApply::Applied` is saved. A later expiry/panic keeps
that true effect in the terminal error. A genuine application checkpoint
conflict returns the ordinary `CheckpointConflict` scalar without journal or
index work. Only success can return stripe custody; an error has no retry or
native-owner extractor.

The opt-in factory uses a sealed input variant for actual anchored journal
rows. The journal row retains its exact anchor and stored coordinates/payload;
no missing reservation, producer/event identity, placement epoch or delivery
field is invented. Replay uses the real preparation/index path, skips journal
append, and returns a distinct checkpoint receipt after actual application
publication, with no live Applied ACK. A closed completion receipt tied to the
same anchor and application-map owners admits later genuine native suffixes.

For `STELAPP2`, a separate native append entry borrows the held global map and
original work through synchronous physical settlement. Its private seal stores
the exact observed global checkpoint, native expected/next and physical anchor.
Only that entry permits a nonregressing sparse per-anchor sequence; ordinary
`STELSNK1` retains its previous exact placement rule. The distinct header is
checked by the real descriptor-based recovery producer, and complete global
chain validation still precedes any replay index mutation. New seal/option/mode
storage is prepaid on the same native ledger; expanded source/error objects
remain covered by their actual size-based fees. No additional per-row codec
copy, hash, or physical I/O is introduced by format selection.

Outer limits bound checkpoint/cache capacities, observed cache name bytes and
partition-vector slots, declared retained values, entries and work. Point
operations prepay actual spare capacity. Growth prepays a finite scalar-table
ceiling and repeated fixed-key probe/movement work, then admits measured
capacity excess before further use. Cache traversal is charged before entry.
The frozen frame, journal and publication producers spend additional fees on
the SAME nonresettable native ledger. These are declared source values and
requested/measured element capacities, not allocator metadata, RSS, an atomic
distributed fence or a bound on physical filesystem call duration.

Twelve fixture groups use actual files, journal write/sync/lock and a real
recovered WAL callback to obtain `NativeResumeWork`. Positive contextless input
comes from that actual callback; no transient context or journal reservation
is invented. Ordinary compression/query decoding supplies a parity reference.
Sources cover Applied publication, genuine checkpoint conflict, pre-covered
different journal content, real mutex contention/poison/spare capacity, sticky
denial after native effects, partial writes and retained advisory lock, late
durable/index/application effects and panic, unadopted replay refusal,
cumulative original-work exhaustion, unsupported real object mode and native
identity/codec policy, and anchored setup admission/late/native-error custody.
The callback deliberately refuses the outer continuation after its component
assertions; it does not pretend that the factory or full resume succeeded.

The actual owned log factory now supplies consuming bounded-source adoption
and registered worker/queue/error-carrier lifetime for this log mode.
Exact-content retry, object publication,
trace/metric/atomic modes, current control integration and installed host
qualification remain open. Existing ordinary constructors, callbacks and
handlers retain their behavior. Original graph compilation/runtime validation
still requires authentic dependencies and coherent API-bearing releases.
This unit changes no shipping manifest, lock or dependency pin.
