//! `locality/` owns collation, message fingerprints, placement scoring, and tests.
//! Model-free compression-locality collation.
//!
//! Records receive a cheap integer temperature on ingress, but placement is a
//! block decision. A full block is scored against stripe-local compression
//! shards, split with farthest-point seeds when its internal variance is high,
//! and handed to the nearest shard in bounded owned byte prefixes.

mod collator;
mod fingerprint;
use fingerprint::splitmix64;
pub use fingerprint::{analyze_message, fingerprint_message, scan_message_terms};
mod placement;
use placement::*;
#[cfg(test)]
mod tests;

use std::collections::VecDeque;
use std::mem::size_of;
use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};
use bytes_handoff::{HandoffBuffer, HandoffBufferConfig, HandoffBufferPolicy};

use crate::{CompressionCohortId, MetadataField, TelemetryError, TelemetryResult};

const MAX_ROUTER_STATE_BYTES: usize = 512 * 1024;
const MAX_COMPRESSION_SHARDS: usize = 16;
const MAX_SPLIT_DEPTH: u8 = 2;
const SPLIT_EXPLORATION_SLOTS: usize = 64;
const SPLIT_FAILURES_BEFORE_BACKOFF: u8 = 2;
const SPLIT_BACKOFF_BLOCKS: u8 = 63;
const INDEX_BYTES: usize = size_of::<u32>();
const SHAPE_MISMATCH_DISTANCE: u8 = 4;
const MAX_LOCALITY_DISTANCE: u8 = 16 + SHAPE_MISMATCH_DISTANCE;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const BASE_PLACEMENT_DOMAIN: u64 = 0x3a72_3e4b_b78f_19d5;
const COLLATED_PLACEMENT_DOMAIN: u64 = 0xd6e8_feb8_6659_fd93;

/// Stable identifier for a final compression placement.
///
/// Placement IDs select active blocks and immutable compression dictionaries.
/// They are independent of the producer-derived source cohort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressionPlacementId(u64);

impl CompressionPlacementId {
    /// Creates a placement identifier from a stable raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw placement identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the deterministic fail-open placement for a source cohort.
    #[must_use]
    pub fn from_source_cohort(source: CompressionCohortId) -> Self {
        Self(splitmix64(source.get() ^ BASE_PLACEMENT_DOMAIN))
    }

    fn from_temperature(
        source: CompressionCohortId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> Self {
        Self(splitmix64(
            source.get()
                ^ COLLATED_PLACEMENT_DOMAIN
                ^ u64::from(temperature.get()).rotate_left(19)
                ^ shape_hash.rotate_right(11),
        ))
    }
}

/// Integer compression-locality temperature.
///
/// The bits are a SimHash locality signature, so distance is measured with XOR
/// Hamming distance rather than numeric subtraction. It affects placement only
/// and is not needed to decode a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompressionTemperature(u16);

impl CompressionTemperature {
    /// Creates a temperature from its integer value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the raw integer temperature.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Returns the Hamming distance between two compression temperatures.
    #[must_use]
    pub const fn distance(self, other: Self) -> u8 {
        (self.0 ^ other.0).count_ones() as u8
    }
}

/// Locality class selected for a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LocalityGranularity {
    /// Producer-derived OTLP service/scope cohort.
    Base = 0,
    /// Block was assigned to an active stripe-local compression shard.
    Collated = 1,
}

/// Final owner-local placement decision for one collated block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompressionPlacement {
    /// Stable block and dictionary placement identifier.
    pub placement_id: CompressionPlacementId,
    /// Byte-weighted centroid of record temperatures.
    pub temperature: CompressionTemperature,
    /// Whether the block uses the base cohort or a compression shard.
    pub granularity: LocalityGranularity,
    /// Locality distance from the block centroid to the selected shard.
    pub distance_to_shard: u8,
    /// Byte-weighted mean squared locality deviation, Q8.
    pub internal_variance_q8: u16,
}

impl CompressionPlacement {
    /// Returns the fail-open placement for a producer-derived source cohort.
    #[must_use]
    pub fn base(source: CompressionCohortId, temperature: CompressionTemperature) -> Self {
        Self {
            placement_id: CompressionPlacementId::from_source_cohort(source),
            temperature,
            granularity: LocalityGranularity::Base,
            distance_to_shard: 0,
            internal_variance_q8: 0,
        }
    }
}

/// Bounded settings for one stripe-local block collator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressionLocalityConfig {
    /// Enables block scoring, splitting, and compression-shard placement.
    pub enabled: bool,
    /// Maximum active compression shards owned by one stripe.
    pub max_compression_shards: usize,
    /// Maximum recursive farthest-seed split depth.
    pub max_split_depth: u8,
    /// Minimum records required in each child of a split.
    pub min_split_records: usize,
    /// Minimum source bytes required in each child of a split.
    pub min_split_bytes: u64,
    /// Split blocks above this byte-weighted Q8 variance.
    pub split_variance_q8: u16,
    /// Do not train a specialized shard with a worse leaf variance.
    pub max_shard_variance_q8: u16,
    /// Maximum locality distance for assigning a block to an existing shard.
    pub max_assignment_distance: u8,
    /// Minimum leaf bytes needed to create a new compression shard.
    pub min_admission_bytes: u64,
    /// Maximum memory reserved for persistent collation profiles.
    pub state_budget_bytes: usize,
}

impl Default for CompressionLocalityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_compression_shards: MAX_COMPRESSION_SHARDS,
            max_split_depth: MAX_SPLIT_DEPTH,
            min_split_records: 8,
            min_split_bytes: 64 * 1024,
            split_variance_q8: 768,
            max_shard_variance_q8: 3_072,
            max_assignment_distance: 6,
            min_admission_bytes: 4 * 1024 * 1024,
            state_budget_bytes: MAX_ROUTER_STATE_BYTES,
        }
    }
}

impl CompressionLocalityConfig {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.max_compression_shards == 0 || self.max_compression_shards > MAX_COMPRESSION_SHARDS
        {
            return Err("locality max_compression_shards must be between 1 and 16");
        }
        if self.max_split_depth > MAX_SPLIT_DEPTH {
            return Err("locality max_split_depth must be no larger than 2");
        }
        if self.min_split_records < 2 {
            return Err("locality min_split_records must be at least 2");
        }
        if self.min_split_bytes == 0 || self.min_admission_bytes == 0 {
            return Err("locality byte thresholds must be nonzero");
        }
        if self.split_variance_q8 == 0 || self.max_shard_variance_q8 < self.split_variance_q8 {
            return Err("locality variance thresholds are inconsistent");
        }
        if self.max_assignment_distance > MAX_LOCALITY_DISTANCE {
            return Err("locality max_assignment_distance must be no larger than 20");
        }
        if self.state_budget_bytes == 0 || self.state_budget_bytes > MAX_ROUTER_STATE_BYTES {
            return Err("locality state_budget_bytes must be between 1 and 512 KiB");
        }
        if estimated_state_bytes(self) > self.state_budget_bytes {
            return Err("locality tables exceed locality state_budget_bytes");
        }
        Ok(())
    }
}

/// Allocation-free message analysis used by routing and term indexing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageFingerprint {
    /// Stable template-shape hash with dynamic values removed.
    pub shape_hash: u64,
    /// Integer SimHash over static and type-class features.
    pub locality_signature: u16,
}

/// One record presented to block collation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionLocalityRecord {
    /// Allocation-free message fingerprint.
    pub fingerprint: MessageFingerprint,
    /// Logical source bytes contributed by this record.
    pub source_bytes: u64,
}

/// Integer score for a complete block or sub-block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionBlockScore {
    /// Byte-weighted majority-bit centroid.
    pub temperature: CompressionTemperature,
    /// Representative exact template-shape hash.
    pub shape_hash: u64,
    /// Byte-weighted mean squared locality distance from the centroid, Q8.
    pub internal_variance_q8: u16,
    /// Largest record-to-centroid locality distance.
    pub max_deviation: u8,
    /// Logical source bytes represented by the score.
    pub source_bytes: u64,
    /// Records represented by the score.
    pub record_count: usize,
}

/// Final assignment of one complete sub-block.
#[derive(Debug, Clone)]
pub struct CompressionBlockAssignment {
    /// Selected compression shard and block diagnostics.
    pub placement: CompressionPlacement,
    /// Score calculated before assignment.
    pub score: CompressionBlockScore,
    membership: BlockMembership,
}

impl CompressionBlockAssignment {
    /// Iterates original input indices belonging to this sub-block.
    pub fn record_indices(&self) -> impl ExactSizeIterator<Item = usize> + '_ {
        self.membership.indices()
    }
}

/// Read-only diagnostics for one active compression shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompressionShardProfile {
    /// Stable placement and dictionary identifier.
    pub placement_id: CompressionPlacementId,
    /// Producer-derived source cohort isolated by this shard.
    pub source_cohort: CompressionCohortId,
    /// Current byte-weighted shard temperature.
    pub temperature: CompressionTemperature,
    /// Current representative exact template-shape hash.
    pub shape_hash: u64,
    /// Integer EWMA of assigned block variance, Q8.
    pub variance_q8: u16,
    /// Blocks assigned to this shard.
    pub blocks: u64,
    /// Source bytes assigned to this shard.
    pub source_bytes: u64,
}

/// Cumulative diagnostics for one stripe-local collator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompressionLocalityStats {
    /// Records scored in complete blocks.
    pub observations: u64,
    /// Complete blocks and recursive sub-blocks scored.
    pub blocks_scored: u64,
    /// Parent blocks divided by farthest-seed partitioning.
    pub blocks_split: u64,
    /// Child blocks produced by splitting.
    pub subblocks_created: u64,
    /// High-variance blocks kept whole after repeated unproductive splits.
    pub split_explorations_suppressed: u64,
    /// Records ultimately kept in the base source cohort.
    pub base_placements: u64,
    /// Records assigned to active compression shards.
    pub collated_placements: u64,
    /// Records moved away from the block's tentative home shard.
    pub records_reassigned: u64,
    /// Source bytes moved away from the tentative home shard.
    pub bytes_reassigned: u64,
    /// Current number of active compression shards.
    pub active_compression_shards: usize,
    /// Largest scored internal variance, Q8.
    pub max_internal_variance_q8: u16,
    /// Packed membership bytes transferred through `bytes-handoff`.
    pub handoff_membership_bytes: u64,
    /// Preallocated persistent profile memory in bytes.
    pub allocated_state_bytes: usize,
}

#[derive(Debug, Clone)]
struct CompressionShard {
    snapshot: CompressionShardProfile,
    one_weights: [u64; 16],
    total_weight: u64,
    shape_vote_weight: u64,
}

#[derive(Debug)]
struct WorkBlock {
    membership: BlockMembership,
    depth: u8,
}

#[derive(Debug, Clone)]
enum BlockMembership {
    Contiguous(Range<usize>),
    Packed(Bytes),
}

impl BlockMembership {
    fn indices(&self) -> MembershipIndices<'_> {
        match self {
            Self::Contiguous(range) => MembershipIndices::Contiguous(range.clone()),
            Self::Packed(bytes) => MembershipIndices::Packed(bytes.chunks_exact(INDEX_BYTES)),
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Contiguous(_) => 0,
            Self::Packed(bytes) => bytes.len(),
        }
    }
}

enum MembershipIndices<'a> {
    Contiguous(Range<usize>),
    Packed(std::slice::ChunksExact<'a, u8>),
}

impl Iterator for MembershipIndices<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Contiguous(range) => range.next(),
            Self::Packed(chunks) => chunks.next().map(|chunk| {
                usize::try_from(u32::from_le_bytes(
                    chunk.try_into().expect("membership entry is four bytes"),
                ))
                .expect("u32 membership index fits usize")
            }),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.len();
        (length, Some(length))
    }
}

impl ExactSizeIterator for MembershipIndices<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Contiguous(range) => range.len(),
            Self::Packed(chunks) => chunks.len(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct SplitExploration {
    source_tag: u64,
    occupied: bool,
    failed_explorations: u8,
    blocks_since_exploration: u8,
}

#[derive(Debug, Clone, Copy)]
struct ScoredMembership {
    score: CompressionBlockScore,
    seed_a: usize,
    seed_b: usize,
}

/// Fixed-capacity, owner-local block collator.
#[derive(Debug)]
pub struct CompressionBlockCollator {
    config: CompressionLocalityConfig,
    target_block_bytes: u64,
    shards: Vec<CompressionShard>,
    split_explorations: [SplitExploration; SPLIT_EXPLORATION_SLOTS],
    stats: CompressionLocalityStats,
}
