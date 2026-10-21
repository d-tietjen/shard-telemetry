# Bounded callback prerequisites

`TelemetrySinkFactory::load_checkpoint_until` reads exactly the same shared
checkpoint map as its ordinary callback. That map represents successful index
application and required object publication. The ordinary constructor advances
it from recovered journal transactions only after indexing those transactions;
live indexing advances it only after indexing and offload succeed. A durable
`SinkJournal` frame has a separate checkpoint and can be ahead after an index
or publication failure. These two meanings are preserved.

The bounded read prepays its actual retained error carrier before allocation,
retains the same work/map owners, uses `try_lock`, and charges a point-operation
fee from the map's actual capacity, including spare slots, before `get`. Actual
absence remains `None`. The native scalar result is saved before its following
original-expiry check. A late/refused/panicking read returns the already-owned
carrier with the observed scalar, capacity and actual map owner. Contention
refuses immediately. For mutex poison the borrowed guard cannot enter a
`Send + 'static` callback error; it is released deliberately while the same map
and its actual poison state remain owned with the native error discriminant.
An initial work-admission refusal uses the engine's inline typed refusal and
does not allocate an emergency box. Positive read results remain ordinary
checkpoint scalars; the engine retains their actual callback observations.

The synchronous internal `append_journal_until` is a necessary journal bridge
for a future bounded apply producer. Its actual error/outcome box exists before
copy or journal entry. There is no channel, worker or detached task. It first
censuses the borrowed descriptors and visible payload/context lengths. It pays
the actual requested vector slots, complete copy work, retained visible bytes
and each possible `Bytes` promotion header before cloning anything. A fallible
exact vector reservation and measured capacity-excess admission precede its
explicit per-append clone loop. The same original `NativeResumeWork` and its
original minimum expiry are used throughout.

The selected `bytes 1.12.1` clone implementation may promote a Vec-backed value
by allocating one `Shared` before its compare/exchange; even a losing promoter
first allocates. That source type contains one pointer, one `usize` and one
`AtomicUsize`, all word-aligned. The bridge conservatively prepays that
source-derived header request per payload and present transient context,
including static/shared values whose actual clone needs no promotion. The
payload data stays shared with the real original `Bytes`. Visible retention,
source-derived requests and measured vector capacity are declared accounting;
private backing capacity, allocator metadata and RSS are not measured. A
dependency implementation change requires review of this clone fee.

Every completed clone is put into the retained vector before a following
clock check. Partial-copy error, expiry or unwind preserves that actual prefix
and the prepaid carrier. The original borrowed source must remain owned by its
caller through the synchronous call. The real native resume caller additionally
keeps `current_append` until callback acceptance; refusal retains it in the
outer engine error. This helper cannot independently own unprocessed borrowed
elements without copying them, and makes no such promise. Once journal entry
occurs, the complete copied input moves into the actual consuming producer.
Its entire success/error result is saved in the already-admitted carrier
before any later clock check or unwind. Error conversion reuses that same box;
there is no post-refusal emergency allocation or retry extractor.

`JournalCallbackReceipt` can borrow the actual journal outcome and its exact
owned transaction for a future index producer. It cannot produce
`DurableSinkApply::Applied`, update an application checkpoint, or grant current
writer authority. A complete durable frame remains an observed physical effect
even if the bridge subsequently refuses. The new internal
[consuming log transaction](BOUNDED-APPLY.md) joins genuine anchored journal
commit, prepared query-index commit and publication in the same application
checkpoint/cache owners. It requires exact `Committed(next)`; journal watermark
coverage is refused because it does not prove this input's content. This
synchronous component does not install a factory or worker callback. The
legacy factory's bounded apply callback continues to refuse explicitly, which
enters no ordinary indexing, object operation, channel or worker. Ordinary
callbacks retain their current behavior. The separate actual
[owned log factory](BOUNDED-OWNED-LOG.md) installs the real strict trait callback
and registered worker for its declared single-log mode; it uses no ordinary
fallback and does not qualify other signals or serving.

The remaining full adapter needs genuine producers for index materialization,
trace/metric decoding and correlation, object reads/publication, complete
factory setup and registered index-worker lifetime. Existing local resident
limits do not by themselves carry the same cumulative work or original expiry
through those operations. This unit does not wrap those ordinary operations or
claim full attachment, factory or serving acceptance.

Regression sources use work obtained from a real recovered engine's bounded
callback, then deliberately refuse the rest of that continuation. They cover
actual ordinary index checkpoints and absence; journal durability staying
separate until genuine existing recovery indexes it; actual map contention,
poison and spare-capacity refusal; late scalar/owner retention; real partial
copy expiry/unwind with caller-owned originals; real partial journal writes,
terminal refusal and retained advisory lock; and complete native outcomes
surviving later bridge expiry/unwind. Exhausted-work refusal precedes carrier
allocation and leaves the real file/input untouched. The harness is component
coverage, not a successful full callback/factory path. Source-defined tests do
not establish compilation or runtime acceptance on the original graph.
