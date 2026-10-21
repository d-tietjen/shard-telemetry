# Bounded embedded log-index preparation

`EmbeddedFrameIndex::decode_until` is an additive consuming producer for the
existing embedded index wire. It receives the original encoded `Bytes`, the
actual expected record count, explicit positive limits and the same genuine
`NativeResumeWork` supplied by the engine callback. Both result variants retain
that exact input owner and shared work. It neither constructs work nor extends
the original minimum expiry. Errors are returned by value, including initial
expiry/capacity refusal; there is no emergency error-box allocation.

The first parser pass validates the original schema with fixed state and no
output heap allocations. It checks both dictionary counts, bitpacked and RLE
columns, record coverage, dictionary IDs, position flags, bit padding, residual
IDs, membership bytes, term/field fingerprints and their ordered locator IDs,
varint limits and complete consumption. The second pass directly produces the
actual index. It never calls the ordinary opaque decoder. Accepted wire and
typed result remain the same when the finite allowances fit; malformed wire
retains the original codec error variant/message. Resource refusal is a
separate outcome. Noncanonical varints accepted by the original decoder remain
accepted, and ordered wire is checked rather than silently sorted.

Local limits cover input bytes and records, cumulative layout/field dictionary
counts, term and field locator counts, all nested locator IDs, both columns'
RLE runs, actual output allocation bytes/slots and cumulative declared work.
Wire-count ceilings apply independently in each pass. Work and allocations
are cumulative across both passes and also debit the unchanged shared native
allowance. Every read, comparison, packed-ID validation and RLE expansion has
an explicit source-work admission before production. Packed ID checks account
for their fixed byte window and bit width; runs pay for each expanded record
and produced bit before changing their destination. There is no sort or hash
producer in this decoding path.

Entered packed-byte copies recheck the original clock between at most 16 KiB
chunks; RLE production rechecks it between at most 256 expanded records. Each
actual produced prefix/progress is retained before those checks. These finite
source phases do not provide a CPU instruction or wall-time guarantee.

Every output vector uses `try_reserve_exact`. The request and movement are paid
before entry; the vector remains in the outside attempt while its measured
capacity excess is checked and paid before use. Output allocation fees cover
the two packed columns, current and completed nested ID vectors, term/field
locator vectors and both membership filters. Membership filters have a fixed
compile-time word count: each uses an exact prepaid `Box<[u64; WORDS]>`
allocation, coerced to a slice without collect, resize or hidden shrink. Those
fixed `Box` allocations use the normal Rust allocator/OOM behavior; vector
reservation failures retain their actual `TryReserveError`. Allocator metadata,
private `Bytes` backing capacity and process RSS are not measured.

RLE writes directly into an already-reserved packed destination. It preserves
the original result while avoiding the legacy temporary full `Vec<u32>` of
expanded IDs. No supplied record count is used for allocation or expansion
until its explicit ceiling and source-work fees fit. Fixed and variable wire
counts still use the original acceptance checks, so a minimal malformed wire
does not acquire a new successful interpretation.

The actual input/cursor, observed scalar/range, partial columns, nested IDs,
completed term/field locators, membership allocations and final index live
outside the unwind boundary. Each actual produced value is stored before a
following original-clock check or test interruption. Late/error/panic results
keep those actual values and the original input. A fully decoded index refused
by its following clock observation remains retained, without becoming a
post-expiry success. `PreparedEmbeddedFrameIndex` supplies read-only borrows of
the actual completed index and original input; it is neither an application
checkpoint nor a consuming error-to-retry extractor.

Regression sources obtain real work from a recovered engine's bounded callback
and deliberately refuse the remaining continuation. They compare real
ordinary-generated STLG and SLT1 sections and typed outputs, bitpacked/RLE,
zero/single-dictionary inputs, truncations and byte mutations against the
legacy decoder's actual wire acceptance and error messages. Actual output
allocation fees are compared to the produced index's measured dynamic
capacity. Other groups cover nested/run/count ceilings before output
allocation, one-less output/slot/work ceilings, shared cumulative exhaustion,
and actual column/ID/membership/term/field/completed owners surviving real
original-expiry pauses and unwind. The fixtures do not fabricate a work owner
or claim a successful full sink/factory path.

This is pure index preparation. Bounded STEL/SLW1/SLT1 framing, original native
decompression context/window/output producers, stripe mutation and query/index
publication, signals/correlation, object publication and registered factory
workers remain separate requirements. No ordinary decompression, Polars,
index mutation, journal append or object operation is clock-wrapped here. No
`Applied`, authority, readiness or full owned attachment is established.
Compilation and runtime acceptance still require the original dependency graph.
