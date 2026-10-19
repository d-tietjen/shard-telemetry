# Bounded grouped-frame preparation and recovery

`prepare_indexed_ingest_frames_until` implements the actual SLW1/SLT1 branch of
the ordinary `decode_indexed_ingest_frames` path. The normal chain is sink
apply, durable-append indexing, STEL dispatch, log-stripe append, then grouped
frame preparation. This additive producer stops before stripe mutation. The
normal constructors, decoders, routes and workers keep their existing behavior.

The original complete `Bytes` payload and optional context move into the
outside attempt before the first resource/deadline check. Success and by-value
failure both retain those actual owners, the same genuine `NativeResumeWork`,
saved cursors/group facts, partial BLAKE3 state and progress, the frame vector,
current slice, completed prefix and actual child decoder results. There is no
post-refusal emergency box, synthetic sidecar or work constructor. A late
completed child or full preparation stays retained with the refusal.

SLW1 group checksums are the first four bytes of BLAKE3, exactly as in the
ordinary encoder/decoder. The producer admits finite metadata/count/byte
limits before the affected production, hashes in at most 16 KiB input chunks,
and checks the original expiry between chunks. Actual hash state/progress and
the final checksum are saved before following checks or observer unwinds.
The selected `blake3 1.8.7` hasher keeps a fixed inline stack of 55 chaining
values; ordinary update/finalize neither creates a worker nor allocates an
output collection. These are explicit source-work and value bounds, with no
CPU instruction or hard wall-time guarantee.

The producer checks both header schemas, reserved bits, per-group count and
structural-size/timestamp invariants, durable/transient cohort/count/size joins,
complete group totals, input consumption and the actual compressed checksum.
The record ceiling covers the expected total and actual cumulative group
counts before each child decode, including a malformed pack whose individually
admitted groups cumulatively exceed its declared total.
It uses the existing consuming bounded embedded-index decoder for each real
sidecar range, using the SAME native work and original E throughout. Each
returned child Result is stored before any following check or unwind. No
ordinary opaque embedded decoder or bulk decompressor runs in this path. The frozen
embedded-index implementation and its tests remain unchanged.

Frame-slot allocation requests and movement are admitted before
`try_reserve_exact`; measured excess capacity is admitted before population or
another allocation. One possible selected `bytes 1.12.1` promotion header is
prepaid before each actual context slice. That header contains one pointer,
one `usize` and one `AtomicUsize`, including a promoter that loses the CAS.
Borrowed compressed ranges require no additional `Bytes` clones. Original
private backing capacity, allocator internals and process RSS are not measured.

Outer limits and reported outer usage cover framing inputs, fixed values,
frame-vector capacity, promotion requests and parser/hash/movement work. The
existing child policy separately bounds each decoded index's nested counts,
allocations and work. ALL outer and child fees also debit the same cumulative
native counters, which bound their combined repeated work; no parent allowance
is reset or replaced by the per-child policy. Work exhaustion retains the same
inputs without entering an ordinary fallback.

For a nonempty pack, absent or unrecognized SLT1 context requires an explicit
`IndexedFramePreparationLimits.decompression` policy. `None` preserves the
previous refusal. `Some(FrameDecompressionLimits)` uses the actual compressed
durable group and never invents a transient sidecar. Zero-group packs retain
the ordinary no-decompression behavior, including ignoring unrecognized
context. With recognized context and fitting finite limits, ordinary wire
acceptance, typed index values and codec errors are preserved. Like the
ordinary live sidecar branch, this checks joins/checksums and decodes its index;
it does not independently compare that index with decompressed structural data.

The recovery child consumes the compressed `Bytes` before its first check,
uses a fresh native zstd context, sets `WindowLogMax` before compressed input,
and streams through fixed input/output slices. Input, expected output, chunk
sizes and stream-call count are finite. Its initialized output allocation has
one extra byte to detect an understated structural length without growing the
native destination. Native results, consumed/written counts, decoder context
and actual output prefixes and native `sizeof` observations remain in the
complete child Result before any following deadline check or test observer.
Modern and skippable concatenated frames use the same input/output extent and
call ceiling. Every frame boundary checks its actual magic before native entry.
Legacy versions refuse under this explicit recovery policy: the selected
library's legacy decoder bypasses the modern window and size accounting.
Truncation, trailing invalid data,
incorrect output length and no-progress decoding refuse with actual state
retained.

The caller prepays a finite native-context reservation. The selected zstd
context's `sizeof` is captured after native steps; excess admission is attempted
before any further step. An elapsed deadline can refuse that admission while
the actual context and observed size remain retained. This is a reservation and retained
post-operation observation, not a hard allocator/RSS cap. Opaque native scratch,
allocator internals and duration of one native call remain outside the logical
work accounting. The native buffered-window policy, input/output extents and
call count are enforced. The selected native whole-frame shortcut can decode
directly into a sufficiently large fixed destination before checking the
buffered-window threshold. That path needs no separate native window buffer;
`max_window_log` is therefore not a maximum accepted frame-header window value.
All explicit declarations and source-work fees use the SAME original native
counters and absolute expiry, including repeated groups and refused attempts.

After actual decompression, the ordinary structural-section parser validates
complete framing and returns the real final embedded-index range. The child
checks the actual structural record count; the parent slices that actual
buffer into the existing bounded embedded-index decoder. Complete compressed
input, decompressed bytes, native outcomes and both child Results remain
retained through index transfer, success or terminal refusal. Recovery does
not reconstruct record bodies, write a journal or advance application progress.

Seven regression source groups use actual ordinary-generated compressed SLW1
and SLT1 data and work obtained from a real recovered engine callback. The
component harness deliberately refuses the remaining continuation. Coverage
includes multi-group/empty/single exact differential results, malformed headers,
checksums and child wire, local finite ceilings, same-ledger cumulative
exhaustion, actual finite hash prefixes, original input identity and real
child/completed prefix owners across expiry/unwind. None establishes successful
full sink/factory/host behavior. Original-graph compilation and execution remain
required before type/runtime acceptance.

Five additional source groups exercise real zstd frames with the same genuine
recovered callback work: contextless ordinary-index parity, native errors and
exact/one-less stream-call ceilings, actual context/output/result custody across
expiry/unwind, exact/one-less source-work admission, and actual modern
concatenation/skippable decoding with legacy-tail refusal. These fixtures do not
create a replacement native budget or claim the full callback completed.

Still required: composed strict sink/journal/application-checkpoint recovery,
trace/metric and correlation producers, object publication, bounded factory
construction, registered index-worker lifetime and original graph validation.
The separate consuming log-index publisher can use this recovery policy, but
a prepared group is neither `Applied` nor an
indexed/application checkpoint, durable journal receipt or serving permission.
