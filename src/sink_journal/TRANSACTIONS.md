# Bounded journal transactions

The additive Unix `SinkJournal::append_until` consumes a genuine owned
`JournalTransaction` and the actual `NativeResumeWork` supplied by the engine's
bounded callback. It retains that same work owner and its original absolute
expiry. It does not construct work, refresh counters, call the ordinary append
method, start a task, or infer write authority from an `Arc<SinkJournal>`.
The journal retains its constructor's actual physical shard identity.

The finite input census precedes any encoded frame allocation. It validates
the real owned vector length and capacity, cumulative visible payload/context
bytes, partition/shard identity, record ranges, placement order and checkpoint
boundaries. It checks the complete wire length before reserving a frame, uses a
fallible exact reservation, and admits measured capacity excess before filling
it. Input `Bytes` owners are preserved rather than cloned by this producer.
Their visible lengths are charged; private backing capacity, allocator metadata
and process RSS are not measured. The caller must already own and admit its
borrowed-to-owned input copy before entering this consuming API.

All cumulative value, entry and work fees use the supplied original work.
Local positive limits independently cap input slots, appends, bytes, frame
bytes, entered filesystem attempts, requested write bytes and per-attempt
write size (at most 16 KiB). Checkpoint point operations prepay work from actual
table capacity; growth separately pays movement and measured new capacity.
The actual journal mutex uses `try_lock`, so contention refuses immediately.
This source-visible accounting does not measure private hash-table layout.

The producer operates on the exact retained `File` inside that journal. It
checks its actual regular-file type, link count and physical length, then
prepays eventual checkpoint insertion and a real file sync before entering the
seek/write mutation. Each explicit write attempt has its own admission and
original-clock check, including short and interrupted attempts. Confirmed
progress and the original native error/panic are retained before cleanup.
A prepaid file sync physically runs once after entry despite expiry, finite
refusal or unwind. Its independent native result and panic remain retained.
No clock check or exhausted counter cancels that settlement. A syscall may
outlast the expiry; there is no wall-time or kernel cancellation guarantee.

The terminal write latch is set before physical entry. A partial or uncertain
frame keeps the same journal terminal for both the bounded and ordinary append
APIs. Dropping a refusal never grants permission to append to that object.
Reopening requires genuine recovery of the actual file. Only a complete frame,
successful sync and published in-memory byte/checkpoint cache clear the latch.
That truthful cache publication still happens for a complete synced frame
whose following clock observation is late; the call returns a retained
refusal, with the actual durable effect, rather than post-expiry success.
`Committed` means that journal frame and local cache were completed;
`AlreadyCovered(actual)` reports the actual durable checkpoint observed.
Neither result establishes telemetry index application or serving readiness.

Both result variants retain the whole original transaction, actual journal
`Arc`/file owner, shared work, frame allocation and effect observations. The
error is returned **by value**. An initial exhausted-budget refusal allocates
no emergency error box. A later strict callback adapter must prepay and own
its actual error carrier and outcome queue before calling this producer, and
bridge through those retained carriers. It cannot allocate a fresh box after
this API refuses or turn that refusal into a retry cycle.

The existing ordinary append behavior is preserved for ordinary-only objects,
whose terminal latch starts clear. This transaction API always settles a file
sync, independent of ordinary batching configuration. For an anchored journal
opened by `open_until`, all transaction operations keep its exact inode even
after ancestor renames. Ordinary writers and namespace changes must still be
coordinated by the real consuming directory owner; this unit is neither an
atomic namespace lease nor a distributed writer fence. It does not finish the
bounded factory, object/dictionary recovery, index application or owned host.

Regression sources obtain genuine work from an actual recovered engine's
bounded callback and deliberately refuse the rest of that continuation. They
cover two physical commits/restart and covered checkpoints, real one-byte
partial write plus mandatory sync and terminal refusal of both APIs, original
expiry after a full write, independent post-write/post-sync panics, cumulative
work exhaustion without frame allocation, actual mutex contention and retained
input spare capacity, wrong shard/offset census, and actual same-inode
read-only/O_PATH descriptor errors. Descriptor-mode fixtures retain the
original exclusive file owner and provoke genuine kernel errors; they do not
change a production constructor or fabricate I/O results. A separate lifetime
fixture proves the retained error owns the real journal/advisory lock until
drop, followed by genuine recovery. These are component regressions and do
not claim a successful full callback adapter or factory. Compilation and test
acceptance still require the unchanged original dependency graph.
