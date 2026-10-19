//! Requested source storage/work for exactly pco1.0.3 Classic/u64.
//! No allocator metadata, RSS, wall-time or portable private-layout claim.
use super::{Failure, Result, add};
use pco::metadata::{Bin, DeltaConv1Config, DeltaLookbackConfig};
use std::mem::{align_of, size_of};

pub(super) fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(Failure::Limits)
}
fn sum(values: &[usize]) -> Result<usize> {
    values.iter().try_fold(0, |a, b| add(a, *b))
}
fn align(value: usize, alignment: usize) -> Result<usize> {
    mul(add(value, alignment - 1)? / alignment, alignment)
}
fn fields(values: &[usize], alignment: usize) -> Result<usize> {
    align(
        add(sum(values)?, mul(values.len() - 1, alignment - 1)?)?,
        alignment,
    )
}

/// Source fields from chunk_latent_decompressor.rs18-26/scratch_array.rs10.
/// Ordinary Rust private layout still requires the reviewed build selector and
/// actual normal dev/release artifact qualification; this is not a Rust ABI law.
pub(super) fn latent_box() -> Result<usize> {
    let a = align_of::<DeltaConv1Config>()
        .max(align_of::<DeltaLookbackConfig>())
        .max(align_of::<usize>());
    let payload = size_of::<DeltaConv1Config>()
        .max(size_of::<DeltaLookbackConfig>())
        .max(size_of::<usize>());
    let delta = align(sum(&[payload, size_of::<isize>(), mul(2, a - 1)?])?, a)?;
    // Three repr(align(64)) ScratchArray fields, each exactly FULL_BATCH_N=256.
    let scratch = fields(
        &[
            mul(256, size_of::<u32>())?,
            mul(256, size_of::<u32>())?,
            mul(256, size_of::<u64>())?,
        ],
        64,
    )?;
    fields(
        &[
            delta,
            size_of::<usize>(),
            size_of::<Vec<u64>>(),
            size_of::<usize>(),
            size_of::<Vec<u8>>(),
            scratch,
        ],
        64,
    )
}

// errors.rs44-48 copies the message even when corruption(format!(...)) has
// already allocated it. Reachable accepted-domain catalogue has M<=198 bytes:
// 20-digit usize/u64 renderings and fixed current format/header/Classic strings.
// Rust1.93 fmt/raw_vec growth: cumulative requests <4*M, then owned copy M.
// See the source catalogue in PCO-TIMESTAMP-ACCOUNTING.md; no arbitrary IO
// payload can enter this in-memory reader. Returned excess is observed/charged.
pub(super) const MAX_MESSAGE: usize = 198;
pub(super) fn error_storage() -> Result<usize> {
    add(mul(5, MAX_MESSAGE)?, mul(2, size_of::<String>())?)
}

pub(super) fn error_extra_work() -> Result<usize> {
    // Heap-write declarations already include5*M. Add one possible read per
    // cumulative requested byte (growth/owned-copy inputs), plus at most M
    // logical rendered literal/numeric bytes. This is not std formatter CPU.
    add(error_storage()?, MAX_MESSAGE)
}

pub(super) fn reader_work(requests: usize) -> Result<usize> {
    // Each with_reader invokes one fill (cursor read + fill counter read/write)
    // and at most one successful consume (cursor read/write + counter read/write).
    // Our sole-writer counters have no CAS/retry loop. bit_reader.rs274,316.
    mul(requests, mul(7, size_of::<usize>())?)
}

pub(super) fn file() -> Result<(usize, usize)> {
    // standalone/decompressor.rs89 header.to_vec(), then with_reader requests
    // 4,31,2. bit_reader.rs285 may allocate 2*request, copy <=request each.
    let padding = mul(2, 4 + 31 + 2)?;
    let bytes = sum(&[4, padding, error_storage()?])?;
    Ok((
        bytes,
        sum(&[
            bytes,
            padding / 2,
            4 + 31 + 2,
            reader_work(3)?,
            error_extra_work()?,
        ])?,
    ))
}

pub(super) fn chunk(b: usize, s: usize, d: usize, scanned: usize) -> Result<(usize, usize)> {
    let states = 1usize
        .checked_shl(u32::try_from(s).map_err(|_| Failure::Limits)?)
        .ok_or(Failure::Limits)?;
    let k = add(b, 127)? / 128; // metadata/chunk_latent_var.rs20: batches128.
    // Node{u16,u8,u8}: sum4 + two one-byte padding gaps =6, never assumed4.
    let node = fields(&[size_of::<u16>(), size_of::<u8>(), size_of::<u8>()], 2)?;
    let metadata_padding = sum(&[404, mul(138, b)?, mul(18, k)?])?;
    let page_bits = add(mul(64, d)?, mul(4, s)?)?;
    let page_padding = mul(2, add(add(page_bits, 7)? / 8, 9)?)?;
    let padding = sum(&[68, metadata_padding, page_padding])?;
    let bytes = sum(&[
        mul(b, size_of::<Bin<u64>>())?, // metadata/chunk_latent_var.rs137.
        mul(4, b)?,                     // chunk_latent_decompressor.rs36 offsets.
        mul(4, b)?,
        if b == 0 { 4 } else { 0 }, // metadata/bins.rs21, ans/spec.rs66.
        mul(4, states)?,
        mul(8, states)?, // ans/spec.rs49, latent.rs39 state lowers.
        mul(node, states)?,
        mul(4, b.max(1))?, // ans/decoding.rs34,36 nodes/clone.
        latent_box()?,
        mul(16, d)?, // stored page state and actual working clone.
        padding,
        error_storage()?,
    ])?;
    // Allocation initialization/writes are already charged by bytes. Remaining
    // logical source reads/updates are individually enumerated (overpayment
    // for source shortcuts is intentional, never an inferred CPU bound).
    let work = sum(&[
        bytes,
        error_extra_work()?,
        padding / 2,
        scanned, // EOF copies + repeated native metadata read.
        mul(mul(3, b)?, size_of::<Bin<u64>>())?, // max/map offsets/map weights.
        mul(mul(3, b.max(1))?, size_of::<u32>())?, // sum/spread outer/clone reads.
        mul(states, size_of::<u32>())?, // spread inner: one state-symbol write.
        mul(states, size_of::<u32>() + size_of::<Bin<u64>>())?, // state-lower lookup.
        mul(
            states,
            size_of::<u32>() + 2 * size_of::<u32>() + size_of::<u32>(),
        )?,
        // Decoder node loop: symbol, weight read+write, offset-bits lookup.
        mul(2, 256 * (2 * size_of::<u32>() + size_of::<u64>()))?,
        // Three arrays zeroed; possible singleton refill. Box transfer is bytes.
        mul(d, size_of::<u64>())?, // working page clone input reads.
        reader_work(add(7, k)?)?,  // preamble2, mode/delta/header/final/page5, k bin batches.
    ])?;
    Ok((bytes, work))
}

pub(super) fn read(n: usize) -> Result<(usize, usize)> {
    // constants.rs: MAX_BATCH_LATENT_VAR_SIZE=256*(16+2)+9=4617;
    // wrapped/page_decompressor.rs105,184: one body request and final pad1.
    let padding = mul(2, 256 * (16 + 2) + 9 + 1)?;
    let bytes = add(padding, error_storage()?)?;
    let per_value = sum(&[
        8, 6, 8, 4, 4, 8, 8, // page_latent_decompressor.rs100-187 ANS input,
        // node/lower reads, scratch writes, state-index read/write.
        4, 4, 15, 8, 8,  // read_offsets.rs24-40 csum/bits/input/latent read+write.
        16, // singleton latent fill/read allowance.
        16, 16, 16, // delta center, first-order moment+latent reads/writes.
        16, // mode/classic.rs20: source latent and destination value.
    ])?;
    Ok((
        bytes,
        sum(&[
            bytes,
            padding / 2,
            mul(n, per_value)?,
            reader_work(2)?,
            error_extra_work()?,
        ])?,
    ))
}
