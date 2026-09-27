use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};

use crate::query::{RequiredIndexConstraints, text_matches};
use crate::{
    LogQuery, NumericComparison, QueryOrder, StructuralRecordView, TelemetryError, TelemetryResult,
    analyze_message,
};

const QUERY_INDEX_MAGIC_V1: &[u8; 8] = b"STLGQIX1";
const QUERY_INDEX_MAGIC: &[u8; 8] = b"STLGQIX2";
const COMPRESSED_QUERY_INDEX_MAGIC_V1: &[u8; 8] = b"STLGQIZ1";
const COMPRESSED_QUERY_INDEX_MAGIC: &[u8; 8] = b"STLGQIZ2";
const DELTA_POSTING: u8 = 0;
const RUN_POSTING: u8 = 1;
const MESSAGE_TERM_CACHE_ENTRIES: usize = 1_024;
const TERM_CACHE_ENTRIES: usize = 4_096;
const MESSAGE_TRIGRAM_FILTER_BITS: usize = 65_536;
const MESSAGE_TRIGRAM_FILTER_WORDS: usize = MESSAGE_TRIGRAM_FILTER_BITS / u64::BITS as usize;
const CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_BITS: usize = 4_096;
const CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS: usize =
    CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_BITS / u64::BITS as usize;
const MESSAGE_TRIGRAM_FILTER_BYTES: usize = MESSAGE_TRIGRAM_FILTER_WORDS * size_of::<u64>();
const CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_BYTES: usize =
    CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS * size_of::<u64>();

struct CachedMessageTerms<'a> {
    message: &'a str,
    term_ids: Vec<usize>,
}

#[derive(Clone, Copy)]
struct CachedTerm {
    term_id: usize,
}

/// Lossless block-level rejection filter for literal message predicates.
///
/// Every bit represents a deterministic hash of one UTF-8 byte trigram after
/// Unicode lowercase normalization. A missing bit proves that the literal is
/// absent. A present bit remains only a candidate because hashes can collide.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MessageTrigramFilter {
    words: Box<[u64]>,
}

impl MessageTrigramFilter {
    fn new() -> Self {
        Self::with_words(MESSAGE_TRIGRAM_FILTER_WORDS)
    }

    fn new_case_sensitive() -> Self {
        Self::with_words(CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS)
    }

    fn with_words(words: usize) -> Self {
        Self {
            words: vec![0; words].into_boxed_slice(),
        }
    }

    fn from_bytes(encoded: &[u8]) -> TelemetryResult<Self> {
        Self::from_bytes_with_words(encoded, MESSAGE_TRIGRAM_FILTER_WORDS)
    }

    fn from_bytes_case_sensitive(encoded: &[u8]) -> TelemetryResult<Self> {
        Self::from_bytes_with_words(encoded, CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_WORDS)
    }

    fn from_bytes_with_words(encoded: &[u8], words: usize) -> TelemetryResult<Self> {
        if encoded.len() != words.saturating_mul(size_of::<u64>()) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "invalid message trigram filter length",
            ));
        }
        let mut filter = Self::with_words(words);
        for (word, bytes) in filter.words.iter_mut().zip(encoded.chunks_exact(8)) {
            *word = u64::from_le_bytes(bytes.try_into().map_err(|_| {
                TelemetryError::InvalidBlockEncoding("invalid trigram filter word")
            })?);
        }
        Ok(filter)
    }

    fn insert_message(&mut self, message: &str) {
        self.insert_message_with_case(message, false);
    }

    fn insert_case_sensitive_message(&mut self, message: &str) {
        self.insert_message_with_case(message, true);
    }

    fn insert_message_with_case(&mut self, message: &str, case_sensitive: bool) {
        let filter_bits = self.words.len().saturating_mul(u64::BITS as usize);
        visit_message_trigrams(message, case_sensitive, |trigram| {
            let slot = message_trigram_slot_for_bits(trigram, filter_bits);
            self.words[slot / u64::BITS as usize] |= 1u64 << (slot % u64::BITS as usize);
        });
    }

    fn might_contain_all(&self, slots: &[usize]) -> bool {
        slots.iter().all(|slot| {
            self.words[*slot / u64::BITS as usize] & (1u64 << (*slot % u64::BITS as usize)) != 0
        })
    }
}

fn visit_message_trigrams(message: &str, case_sensitive: bool, mut observe: impl FnMut([u8; 3])) {
    if case_sensitive {
        for trigram in message.as_bytes().windows(3) {
            observe([trigram[0], trigram[1], trigram[2]]);
        }
    } else if message.is_ascii() {
        for trigram in message.as_bytes().windows(3) {
            observe([
                trigram[0].to_ascii_lowercase(),
                trigram[1].to_ascii_lowercase(),
                trigram[2].to_ascii_lowercase(),
            ]);
        }
    } else {
        for trigram in message.to_lowercase().as_bytes().windows(3) {
            observe([trigram[0], trigram[1], trigram[2]]);
        }
    }
}

#[inline]
fn message_trigram_slot_for_bits(trigram: [u8; 3], filter_bits: usize) -> usize {
    let mut hash =
        u32::from(trigram[0]) | (u32::from(trigram[1]) << 8) | (u32::from(trigram[2]) << 16);
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x7feb_352d);
    hash ^= hash >> 15;
    hash = hash.wrapping_mul(0x846c_a68b);
    hash ^= hash >> 16;
    hash as usize & filter_bits.saturating_sub(1)
}

fn required_message_trigram_slots(literals: &[&str]) -> Vec<usize> {
    let mut slots = Vec::new();
    for literal in literals {
        visit_message_trigrams(literal, false, |trigram| {
            slots.push(message_trigram_slot_for_bits(
                trigram,
                MESSAGE_TRIGRAM_FILTER_BITS,
            ));
        });
    }
    slots.sort_unstable();
    slots.dedup();
    slots
}

fn required_case_sensitive_message_trigram_slots(literals: &[&str]) -> Vec<usize> {
    let mut slots = Vec::new();
    for literal in literals {
        visit_message_trigrams(literal, true, |trigram| {
            slots.push(message_trigram_slot_for_bits(
                trigram,
                CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_BITS,
            ));
        });
    }
    slots.sort_unstable();
    slots.dedup();
    slots
}

/// Offset and timestamp bounds for one independently compressed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryBlockMetadata {
    /// Stable ordinal used by the pack manifest.
    pub block_ordinal: u32,
    /// Logical partition represented by the block.
    pub topic_partition: TopicPartition,
    /// Lowest durable offset in the block.
    pub first_offset: LogicalOffset,
    /// Highest durable offset in the block.
    pub last_offset: LogicalOffset,
    /// Lowest event timestamp in the block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp in the block.
    pub max_timestamp_unix_nanos: u64,
    /// Number of records in the block.
    pub record_count: u32,
}

/// Exact location of a candidate record inside a sealed block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryHit {
    /// Manifest block ordinal.
    pub block_ordinal: u32,
    /// Zero-based record ordinal inside the decoded structural block.
    pub record_ordinal: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockPosting {
    block_ordinal: u32,
    record_ordinals: PostingList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrdinalRun {
    start: u32,
    length: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PostingCheckpoint {
    index: u32,
    previous: u32,
    byte_offset: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PostingList {
    Ordinals(Vec<u32>),
    Runs {
        runs: Vec<OrdinalRun>,
        cardinality: usize,
    },
    Encoded {
        bytes: Arc<[u8]>,
        start: usize,
        end: usize,
        kind: u8,
        cardinality: usize,
        checkpoints: Arc<[PostingCheckpoint]>,
    },
}

impl PostingList {
    fn from_ordinals(ordinals: Vec<u32>) -> TelemetryResult<Self> {
        if ordinals.is_empty() {
            return Err(TelemetryError::InvalidBlockEncoding("empty query posting"));
        }
        if ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "query posting is not ordered",
            ));
        }
        let runs = posting_runs(&ordinals);
        if runs.len().saturating_mul(2) < ordinals.len() {
            Ok(Self::Runs {
                runs,
                cardinality: ordinals.len(),
            })
        } else {
            Ok(Self::Ordinals(ordinals))
        }
    }

    const fn cardinality(&self) -> usize {
        match self {
            Self::Ordinals(ordinals) => ordinals.len(),
            Self::Runs { cardinality, .. } => *cardinality,
            Self::Encoded { cardinality, .. } => *cardinality,
        }
    }

    fn storage_bytes(&self) -> usize {
        match self {
            Self::Ordinals(ordinals) => ordinals.capacity() * size_of::<u32>(),
            Self::Runs { runs, .. } => runs.capacity() * size_of::<OrdinalRun>(),
            Self::Encoded { start, end, .. } => end.saturating_sub(*start),
        }
    }

    fn to_vec(&self) -> Vec<u32> {
        match self {
            Self::Ordinals(ordinals) => ordinals.clone(),
            Self::Runs { runs, cardinality } => {
                let mut ordinals = Vec::with_capacity(*cardinality);
                for run in runs {
                    ordinals.extend(run.start..run.start + run.length);
                }
                ordinals
            }
            Self::Encoded {
                bytes, start, end, ..
            } => {
                let mut cursor = 0;
                decode_posting(&bytes[*start..*end], &mut cursor, u32::MAX)
                    .expect("validated encoded query posting")
                    .to_vec()
            }
        }
    }

    fn take_ordered(&self, newest_first: bool, limit: usize) -> Vec<u32> {
        match self {
            Self::Ordinals(ordinals) => {
                let take = limit.min(ordinals.len());
                if newest_first {
                    ordinals[ordinals.len() - take..]
                        .iter()
                        .rev()
                        .copied()
                        .collect()
                } else {
                    ordinals[..take].to_vec()
                }
            }
            Self::Runs { runs, .. } => {
                let mut ordinals = Vec::with_capacity(limit.min(self.cardinality()));
                if newest_first {
                    for run in runs.iter().rev() {
                        for ordinal in (run.start..run.start + run.length).rev() {
                            ordinals.push(ordinal);
                            if ordinals.len() == limit {
                                return ordinals;
                            }
                        }
                    }
                } else {
                    for run in runs {
                        for ordinal in run.start..run.start + run.length {
                            ordinals.push(ordinal);
                            if ordinals.len() == limit {
                                return ordinals;
                            }
                        }
                    }
                }
                ordinals
            }
            Self::Encoded {
                bytes,
                start,
                end,
                kind,
                checkpoints,
                ..
            } => take_encoded_ordered(
                &bytes[*start..*end],
                *kind,
                checkpoints,
                newest_first,
                limit,
            ),
        }
    }

    fn contains(&self, ordinal: u32) -> bool {
        match self {
            Self::Ordinals(ordinals) => ordinals.binary_search(&ordinal).is_ok(),
            Self::Runs { runs, .. } => {
                let position = runs.partition_point(|run| run.start <= ordinal);
                position > 0
                    && ordinal
                        < runs[position - 1]
                            .start
                            .saturating_add(runs[position - 1].length)
            }
            Self::Encoded {
                bytes,
                start,
                end,
                kind,
                checkpoints,
                ..
            } => encoded_posting_contains(&bytes[*start..*end], *kind, checkpoints, ordinal),
        }
    }
}

type PartitionTermBlockPostings = HashMap<TopicPartition, HashMap<Arc<str>, Vec<BlockPosting>>>;
type PartitionFieldBlockPostings =
    HashMap<TopicPartition, HashMap<Arc<str>, HashMap<Arc<str>, Vec<BlockPosting>>>>;

/// Exact term and metadata postings for one structural block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockQueryIndex {
    record_count: u32,
    message_trigrams: MessageTrigramFilter,
    case_sensitive_message_trigrams: MessageTrigramFilter,
    term_postings: HashMap<Arc<str>, PostingList>,
    field_postings: HashMap<Arc<str>, HashMap<Arc<str>, PostingList>>,
}

mod block;
/// Immutable query directory for a set of sealed structural blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistentQueryIndex {
    blocks: Vec<QueryBlockMetadata>,
    message_trigram_words: Box<[u64]>,
    case_sensitive_message_trigram_words: Box<[u64]>,
    message_trigram_union: MessageTrigramFilter,
    message_trigram_intersection: MessageTrigramFilter,
    case_sensitive_message_trigram_union: MessageTrigramFilter,
    case_sensitive_message_trigram_intersection: MessageTrigramFilter,
    partition_blocks: HashMap<TopicPartition, Vec<u32>>,
    term_postings: PartitionTermBlockPostings,
    field_postings: PartitionFieldBlockPostings,
}

mod persistent;
mod postings;
use postings::*;
fn normalize_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

fn compare_numeric(observed: i128, comparison: NumericComparison, expected: i128) -> bool {
    match comparison {
        NumericComparison::Equal => observed == expected,
        NumericComparison::NotEqual => observed != expected,
        NumericComparison::LessThan => observed < expected,
        NumericComparison::LessThanOrEqual => observed <= expected,
        NumericComparison::GreaterThan => observed > expected,
        NumericComparison::GreaterThanOrEqual => observed >= expected,
    }
}

mod codec;
use codec::*;
#[cfg(test)]
mod tests;
