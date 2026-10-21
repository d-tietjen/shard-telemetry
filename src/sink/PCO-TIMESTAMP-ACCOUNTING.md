# Bounded timestamp source accounting

The consuming timestamp producer retains its complete successful offset,
section and native-frame parent and uses that SAME original recovery ledger
and absolute E. It is position substrate. Body/field reconstruction, bounded
native typed metadata/MessagePack, complete typed rows, query/service, private
product, installed host/HA ownership, shipping graph and release gate remain
separate acceptance requirements. The existing ordinary decoder is unchanged.

The exact dependency domain is Pco1.0.3 and better_io0.2.0 (Apache-2.0, no
package features). The direct reader dependency is already present transitively.
The root package adds that dependency edge through normal Cargo resolution;
locks must be generated and verified normally, never handwritten. Unknown
compiler/target/effective flags refuse only this bounded operation before its
reader/native entry, retaining the complete parent.

## Configuration and retention

The build selector requires Rust1.93.0 commit
`254b59607d4417e9dffbc307138ae5c86280fe4c`,
`x86_64-unknown-linux-gnu`, little endian, 64-bit pointers and ordinary
`fxsr,sse,sse2` target features. It rejects bootstrap, wrapper, effective Rust
flag and environment profile overrides. Both normal dev/release unwind and
canonical owning-host release abort have explicit geometry paths. SDK has no
release panic override. A host may set panic=abort, LTO, codegen and stripping;
standalone qualification cannot certify that host's effective artifact. Normal
standalone release and each owning host's canonical release layout/allocation
artifact need their own checks, with no profile/flag/stack workaround.
Caught-panic retention applies only under unwind; abort retains process-abort
semantics. Native Err, finite limits and expiry remain ordinary Result paths.

The admitted wire is standalone3/wrapped4.1, u64 Classic and NoOp or first-order
Consecutive with no secondary delta. This includes the actual configured
producer's incompressible fallback. An allocation-free, paid LSB preflight
checks complete header, chunk count, bins/weights/offset widths, delta/ANS
page framing and padding before native constructors. The outer record count
and decoded offsets, never n_hint, determine output storage. Unsupported
formats/modes are explicit full-traversal gates, with no ordinary fallback.

A nonClone reader wrapper is consumed through real File/Chunk/EndOfData owners.
Its immutable, admitted finite input Vec lives in an acyclic Arc escrow. Only
the active reader writes the cursor and counters; load/checked-store counters
have no lock/CAS retry loop. Constructor Err may destroy its consumed wrapper;
the escrow retains the input and last committed consume observations. Pco
with_reader returns before update on error, so those observations do not claim
the exact inspected/error bit position or unreturned native locals.

Actual allocator, File::new, chunk constructor and read Results are captured
before observers/original-E checks. Successful returned owners move into live
state only after those checks. Every read has a positive destination of at
most256 entries and performs one batch; a later-batch Err cannot hide earlier
returned progress. The whole destination is initialized and retained. A native
Err can follow writes before final padding validation: that window stays
untrusted, beside the live chunk, actual Err and confirmed earlier prefix.
No retry, mutable budget/native extractor, self reference or renewed E exists.
Only genuinely finished read progress checked against original E permits the
real chunk.into_src transition. Actual EndOfData is required after the expected
count (a uniform-type peek is insufficient). Unread tail is retained/accepted
as in the ordinary decoder's first-file semantics.

## Requested storage and work

These declarations bound source requests and logical byte/element work under
the reviewed source/configuration. They are not allocator metadata, RSS,
portable private ABI, CPU cycles, wall time or panic-machinery bounds. Checked
arithmetic rejects overflow/isize excess. Child-local input/records/chunks/calls,
bins/ANS, declared bytes, entries and work ceilings are additional to the same
cumulative original retained/work budget; entries do not masquerade as JSON or
filesystem records. There is no refund/reset. Each operation pays before entry.

Concrete continuation/capture/move storage is
`3*sizeof(actual FrameContinuation)+sizeof(actual closure)` from the existing
executor. Each native call additionally pays three actual return/capture/move
Result carriers. Vec requests, actual reserve Results and observed capacity
excess are retained/admitted before copy/initialization. Output uses the true
record count, and read receipts reserve at most one slot per true record.
Rust1.93 ArcInner is repr(C,align(2)) with two AtomicUsize fields followed by
the actual ReaderState; Layout::extend/pad_to_align computes that source layout.
Two actual ReaderState constructor/move carriers are additional to that heap.

Let b be actual bins, s<=14, S=2^s, d=0/1 and k=ceil(b/128). The preflight
requires b<=S, b=1 implies s=0, and exact summed weights. Source components use
`U(fields,A)=align_up(sum(fields)+(field_count-1)*(A-1),A)`.
Private delta payload is max of public usize/DeltaLookbackConfig/DeltaConv1Config
plus an isize tag and worst inter-field padding. Public Conv1Config is40/align8,
so the corrected delta bound is64. Three align64 scratch arrays
[u32;256],[u32;256],[u64;256] give4224. The six private box fields (delta, usize,
Vec<u64>, usize, Vec<Node> header, scratch) give Q=4672. Source Node(u16,u8,u8)
is bounded by6. Measured development private sizes4224/4096/40/4 are comparisons,
not substituted fee constants or release evidence. Ordinary private layout
still requires actual supported configuration/artifact qualification.

| Heap request in one real chunk constructor | Declared bytes | Pco1.0.3 source |
|---|---:|---|
| Retained bins | b*sizeof(Bin<u64>) | metadata/chunk_latent_var.rs137 |
| Offset-bit vector | 4b | chunk_latent_decompressor.rs36 |
| Weights plus empty fallback | 4b + (b==0 ?4:0) | metadata/bins.rs21; ans/spec.rs66 |
| State symbols | 4S | ans/spec.rs49 |
| State lowers | 8S | chunk_latent_decompressor.rs39 |
| Decoder nodes | 6S | ans/decoding.rs34 |
| Symbol-weight clone | 4*max(b,1) | ans/decoding.rs36 |
| Aligned latent/scratch box | Q=4672 on selected configuration | chunk_latent_decompressor.rs64; scratch_array.rs10 |
| Stored delta and actual working clone | 16d | metadata/dyn_latents.rs48; wrapped/page_decompressor.rs51 |
| Possible preamble EOF buffers | 68 | standalone/decompressor.rs195-225 |
| All possible metadata EOF buffers | 404+138b+18k | metadata/mode.rs21; chunk.rs137; chunk_latent_var.rs20-33 |
| Possible page-metadata EOF buffer | 2*(ceil((64d+4s)/8)+9) | wrapped/page_decompressor.rs81; metadata/page_latent_var.rs39 |

Pco bit_reader.rs274-295 allocates vec![0;2*requested] when its reusable EOF
window is short and copies fewer than requested bytes. Every possible request
is prepaid even when actual reuse avoids it. Metadata constants are twice
mode26+delta147+bin-header28+final1 =404, twice69 bytes per bin =138b,
and twice9 overshoot per bin batch =18k. Preamble requests1 and24+9 give68.
File::new separately pays its actual four-byte header Vec and EOF buffers
2*(4+31+2)=74. One read pays2*(256*(16+2)+9+1)=9236 possible EOF bytes from
MAX_BATCH_LATENT_VAR_SIZE and final padding. Termination pays its real request1.
The constructor heap subtotal excluding error/capture/input/output is5364 for
b1/s0/d0,52342 for b256/s8/d1,3022168 for b16384/s14/d1.

Allocation initialization/copy writes pay that complete heap sum. Additional
native work rows in recovery_timestamps/cost.rs enumerate actual source loops:

| Source pass beyond admitted allocation writes | Additional logical bytes |
|---|---:|
| EOF input copies | half the sum of possible EOF request bytes |
| Repeated native metadata inspection | actual preflighted chunk header/page extent |
| Max offsets, mapped offsets, mapped weights | 3*b*sizeof(Bin<u64>) |
| Weight sum, spread outer loop, clone reads | 3*max(b,1)*sizeof(u32) |
| Spec spread inner | S*sizeof(u32) |
| State-lower source lookup | S*(sizeof(u32)+sizeof(Bin<u64>)) |
| Decoder node source/update loop | S*(symbol u32+weight read/write u32+offset lookup u32) |
| Scratch zeroing and possible singleton refill | 2*256*(2*sizeof(u32)+sizeof(u64)) |
| Page working-state clone input | d*sizeof(u64) |

Sources are metadata/bins.rs12-21, chunk_latent_decompressor.rs35-62,
ans/spec.rs39-60, ans/decoding.rs34-54 and wrapped/page_decompressor.rs44-58.
A constructor has7+k with_reader calls: preamble2 plus mode, delta, bin header,
final metadata and page1 each, and k bin batches. A file has3; each read at
most2; EOF1. Each reader request adds seven usize source accesses: fill reads
cursor and reads/writes its counter, consume reads/writes cursor and its counter.

One batch additionally pays per value the source upper envelope: ANS packed
input8, node6, lower8, scratch csum/bits/latent writes4+4+8, state read/write8;
offset csum/bits/input/latent read/write4+4+15+8+8; singleton fill/read16;
delta centering16 and first-order moment/latent reads/writes16+16; classic
join16. Sources: page_latent_decompressor.rs24-40,100-187,207-254,
delta/mod.rs and delta/consecutive.rs40-66, mode/classic.rs20. Charging the
whole envelope when a branch is absent deliberately overpays source shortcuts.
Each real ReadCall capture/move also pays three actual carrier sizes.
Local preflight's one-bit loop pays one inspected byte/entry before each bit,
including malformed/truncated attempts; no corpus scan is outside the ledger.

## Reachable native error catalogue

PcoError::new always copies the message. corruption(format!(...)) can hold a
growing temporary String and its copied message simultaneously. Rust1.93
alloc::fmt/core::fmt/raw_vec sources give cumulative format requests below4M
and the owned copy M;5M plus two concrete String headers is prepaid per call.
The table bounds accepted-domain numeric arguments by20 rendered bytes and
fixed current header/type/version arguments. No arbitrary IO payload is
formatted by the in-memory reader. Maximum M is198 and5M is990. In addition to requested allocation writes, error work prepays a possible read
per cumulative requested byte (growth/owned-copy input) and at most M logical
rendered literal/numeric bytes. This is not a formatter CPU bound. The actual
returned message and any capacity excess are retained/admitted after capture.
A constructor/read can return only its first Err, so one complete catalogue
maximum per call suffices. Preflight excludes Conv1's dynamic formatting and
all dictionary/lookback constructors before native entry. Some rows below are
therefore conservative unreachable coverage, never used to admit their modes.

| Pco1.0.3 source | Form | Maximum message bytes |
|---|---|---:|
| standalone/decompressor.rs:30 | format | 46 |
| standalone/decompressor.rs:93 | format | 84 |
| standalone/decompressor.rs:121 | format | 117 |
| standalone/decompressor.rs:178 | format | 53 |
| standalone/decompressor.rs:205 | format | 109 |
| standalone/decompressor.rs:214 | format | 114 |
| standalone/decompressor.rs:18 | static | 20 |
| standalone/decompressor.rs:183 | static | 43 |
| wrapped/chunk_decompressor.rs:48 | format | 71 |
| wrapped/page_decompressor.rs:53 | format | 72 |
| wrapped/page_decompressor.rs:201 | format | 158 |
| wrapped/page_decompressor.rs:186 | static | 49 |
| metadata/format_version.rs:75 | format | 198 |
| metadata/chunk.rs:52 | format | 107 |
| metadata/chunk.rs:62 | static | 68 |
| metadata/chunk.rs:169 | static | 51 |
| metadata/chunk_latent_var.rs:36 | format | 77 |
| metadata/chunk_latent_var.rs:116 | format | 90 |
| metadata/chunk_latent_var.rs:122 | format | 65 |
| metadata/chunk_latent_var.rs:128 | format | 83 |
| metadata/mode.rs:151 | format | 41 |
| metadata/mode.rs:121 | static | 79 |
| metadata/mode.rs:141 | static | 50 |
| metadata/delta_encoding.rs:160 | format | 93 |
| metadata/delta_encoding.rs:166 | format | 103 |
| metadata/delta_encoding.rs:195 | format | 50 |
| metadata/delta_encoding.rs:144 | static | 46 |
| ans/spec.rs:40 | format | 95 |
| bit_reader.rs:141 | format | 110 |
| bit_reader.rs:221 | format | 76 |

Rust1.93 source references: alloc/src/sync.rs360-405 (ArcInner),
alloc/src/fmt.rs639-648 and core/src/fmt/mod.rs724-774 (format capacity),
alloc/src/raw_vec/mod.rs621-672 (growth). Upstream source changes, unknown
compiler/target/effective flags or unsupported mode require a new review;
ordinary decoding remains available throughout.
