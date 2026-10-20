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


## Index after the recovery timestamp parent

`PreparedRecoveredStructuralTimestamps::prepare_index_until` consumes the actual
successful outer-zstd/section/offset/Pco chain into bounded embedded-index
preparation. It runs the existing two-pass parser against the original section
view using the original non-clone recovery ledger and absolute expiry. All actual
index state drops before the complete timestamp/native parent. The completed
index is an immutable result; bodies, fields and typed metadata remain encoded.
It is neither a fully reconstructed row nor query/serving permission.

`RecoveredStructuralIndexLimits` bounds the same parser input/counts, output
requests, allocation slots and explicit work as the existing native path. Fixed
continuation/Bytes-sharing metadata and parser-state/input framing are separately
charged to the original recovery ledger, outside local output counters. Unlike
native work, recovery entries do not add an implicit work surcharge or become
JSON values/records. Real reserve requests, old-vector copies and observed
capacity excess retain their existing admission order. A refusal retains actual
grown vectors, source, partial/completed index and original ledger error; no
rollback, retry or refund is provided. Actual native-work framing still charges
one complete native wrapper including its original work holder.

Success can consume all completed children into `decompress_next_until` on the
same already-spent ledger/E. Terminal error exposes only immutable observations,
including an index completed before late expiry/unwind; its presence alone does
not mean acceptance. Catching unwind does not make a process-abort build return
from panic. These APIs do not establish whole replay/factory or host completion.


## Internal scoped stream state

The native streaming loop borrows an existing `FrameWork` at its admission
boundaries. The owned structural wrapper retains that work between its data
state and its actual native context, preserving their destruction order. Its
STLG conversion, section validation, result custody and public entrypoints
remain in that wrapper. Fixed fees use the actual owned wrapper/error sizes.

The scoped composition owner retains a separate context and its complete
native results/output while borrowing the complete parent's original recovery
work. Its production consumer reconstructs the finite typed-metadata byte
extent below. Its continuation fee covers the actual parent/child/result
carriers. It does not validate STLG, MessagePack or typed rows. An entered
attempt cannot reset or replace its retained context. The real retained-row
consumer remains unwired; there is no retry or work/context extractor.

Observed native context excess remains admitted before further work, and a
refusal retains the actual context and produced bytes. The production
`FrameStreamState` refactor still requires normal native layout and frame
qualification. The metadata-byte producer requires its own normal layout, native and retained
regression gates; source-derived fees do not bound allocator RSS or a
process-abort stack.


## Retained typed-metadata byte extent

An accepted recovered structural index can be consumed into an absent-metadata
owner or an exact native metadata-byte owner on its original spent recovery
ledger and expiry. The complete index/Pco/section/outer-context parent remains
owned after the metadata context, result and output state. This is an opaque
byte prerequisite: MessagePack, packed metadata row counts, typed rows and query
or HA ownership are not accepted by it.

An empty encoded metadata lane enters no second decoder and intentionally does
not validate unused native decoder ceilings. It still requires the actual
accepted parent, original expiry and full continuation framing admission. A
nonempty lane captures its original four-byte little-endian raw length, checks
the separate64MiB safety ceiling and uses its entire compressed remainder with
the unchanged bounded stream limits. The caller's limits are never clipped or
normalized. A nonempty declared-zero raw length has an explicit unsupported
outcome; it is distinct from the ordinary absent lane. Modern/skippable and
concatenated input, exact output extent and compressed trailing-garbage refusal
follow the existing bounded native loop. Decompressed trailing MessagePack
bytes remain retained; no MessagePack EOF rule is introduced.

Actual complete continuation/prepared/error/closure framing is paid before
metadata effects. Native fixed framing is then zero because those actual
carriers are already covered; the original stream's three local framing entries
and all input/output/sentinel/step/context/excess/per-call fees remain. Original
cumulative work is never reset or extracted. Success can consume the child and
continue another frame on that same ledger/E; terminal errors retain their
actual partial output, results, initialized destination and context without a
retry. Process abort and unreturned library transitions cannot be reconstructed.

The scoped stream now has this production consumer under the existing Unix
recovery boundary. Native decoder constructors, ordinary STLG finalization,
legacy/window restrictions and ordinary bulk/Serde decoding remain unchanged.
Normal canonical formatting, strict Clippy, native/retained regression, release,
actual carrier/layout and production-frame gates apply. This byte stage supplies
no std String-format allocation proof, typed-row visitor, service/host/HA or
release-gate acceptance.


## Retained typed packed metadata

`PreparedRecoveredStructuralMetadataBytes::prepare_packed_metadata_until` consumes
that complete parent into one schema-typed packed-metadata arena. The original
recovery ledger and expiry remain shared across setup, both native streams,
sections, offsets, timestamps, index and this typed parse. Successful owners can
consume the same spent ledger into another actual frame. Refused owners retain
partial arena drafts, actual callback values/ranges captured before admission,
actual MessagePack/UTF8/reservation outcomes, the original
refusal, caught unwind and all complete native parents; they expose no retry or
mutable work/context extractor.

The arena contains scalar values, ordered links and ranges into the retained
raw backing. It covers the six packed tables, fourteen row fields and all nine
`TelemetryValue` variants without constructing additional Arc-backed models.
Bytes encoded as scalar arrays retain each decoded byte; they are not mistaken
for a contiguous source payload. Named/map structs, unsigned numeric identifiers,
missing optional map fields, ignored fields and the ordinary fixed-ID array/BIN
distinction follow the same borrowed RMP parser. Fixed IDs read exactly sixteen
or eight elements: the parser rejects excess actual-array elements but permits
excess BIN bytes as its ordinary derived path does. The complete raw first-value
tail remains retained; there is no invented consumed-byte cursor or parser retry.
Dictionary references, conflicting ID-source validation, template/body/field
reconstruction and final typed log rows remain later operations.

Traversal limits bound input, authoritative records, nodes, recursion, callbacks,
allocation requests/slots and explicit work. Every active draft is owned before
a fallible child. A distinct delivered-input observation is stored in its already
admitted node before callback fees, expiry checks or shape/range validation; it
is never labeled an accepted typed value. Actual scalar/range/capture slots are
prepaid before the deserializer invocation. The source frame tables include
their own actual storage, and actual bridge closure values are measured before
the child's complete framing admission. Actual `try_reserve_exact` results and returned-capacity excess
remain retained on refusal. A single preadmitted static `&str` error conversion
stops a visitor refusal; no default Serde formatting, derived leaf decoder or
native enum adapter runs inside this path. UTF8 work is admitted before the
borrowed parser can scan strings. The closed private adapter graph distinguishes
the binary RMP deserializer from its BIN-element scalar adapter on each actual
seed entry; this is not a general claim about arbitrary Serde deserializers.
The original parser's default depth ceiling is preserved in addition to the
child's finite seed-depth limit.

An absent metadata lane enters no MessagePack parser and does not validate
unused parser ceilings; original-expiry checks and complete continuation framing
still apply. A present malformed or zero-length lane is not treated as absent.
These source/configuration-specific request and copy fees are not allocator RSS,
portable ABI, total CPU-time or process-stack limits. Unwind retention does not
apply to process abort. Canonical CONTRIBUTING gates and actual normal profile
qualification remain required for source changes. This arena grants no query,
HA, serving, atomic-append or release authority, and does not complete the owned
service/product attachment or a whole configured partition traversal.

PreparedRecoveredPackedMetadata::prepare_messages_until consumes that arena
and the complete native lineage into full-scope validated message bytes.
Template and body traversal shares the ordinary wire core. Its ordinary
wrappers retain their existing allocation order, raw/exact/dynamic Arc caches,
nonminimal-varint acceptance and malformed-input precedence. Any positive
checkpoint interval, including an interval larger than the row count, and equal
checkpoints retain their existing reader meaning; the producer's interval is
not an additional admission rule. Unused template literals remain byte strings,
so invalid UTF8 in an unused literal is not rejected. A reconstructed row is
validated after all its fragments have been scanned and copied; UTF8 can span
fragment boundaries.

The bounded owner retains template literal ranges, actual directory checkpoints,
actual helper Results and consumed cursors, completed ordered row ranges,
initialized reconstruction bytes and actual UTF8/reservation outcomes. Helpers
store their actual Result before the observation hook. Allocation attempts
record requested capacity, actual old capacity, actual Result and actual
returned capacity before the hook or excess-capacity admission. Growth charges
the old allocation capacity as well as the new request, including growth while
initialized length is below capacity. Observed excess remains owned on refusal.
The attempted row ordinal is distinct from the ordinal associated with the last
delivered source and UTF8 result; a later failed scan does not relabel an earlier
result as the current row.

Every helper/carrier/control table includes its actual storage; native borrowed
UTF8 results, mapped results and actual bridge/operation closures are included.
The scoped internal bridge borrows disjoint accepted index/structural input and
the same nonClone recovery work without returning an owner or work reference.
All original setup and previous frame usage remain spent, the same absolute
expiry applies, actual child results precede post-checks and the complete
packed/index/Pco/native parent is destroyed last. Errors are terminal and keep
the real partial child, first original-work refusal, helper error, UTF8 error or
unwind. The bounded path retains std::str::Utf8Error; the ordinary decoder
continues to report its existing static codec category for invalid text.

These ranges are message-byte results, not final log models. Field/attribute
dictionary references, typed-row cross-table validation, index publication,
query ownership, full configured traversal, serving/HA/product attachment and
release gates remain separate. The request/copy accounting is not allocator
RSS, a portable ABI or a total-stack bound; panic retention applies only when
the process unwinds.
