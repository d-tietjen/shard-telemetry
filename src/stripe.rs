use std::borrow::Cow;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::fs;
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};
use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};
use shard_stream_engine::DurableSinkCheckpoint;

use crate::ingest_pack::{
    IndexedIngestFrame, decode_indexed_ingest_frames,
    decode_indexed_ingest_frames_after_validation, decompress_indexed_ingest_frame,
};
use crate::query::{bounded_levenshtein, scan_clickhouse_tokens, text_matches};
use crate::tier_ingest::{
    DecodedTierIngestAppend, TierIngestAppendSource, TierIngestFrameSource,
    decode_tier_ingest_group, write_tier_ingest_group,
};
use crate::{
    AnalyticsGroupKey, BlockCatalog, BlockDescriptor, BlockId, CaseSensitivity,
    CompressionBlockCollator, CompressionBlockScore, CompressionCodec, CompressionCohortId,
    CompressionLocalityConfig, CompressionLocalityRecord, CompressionLocalityStats,
    CompressionPlacement, CompressionPlacementId, CompressionTemperature, DictionaryCache,
    DictionaryCatalog, DictionaryCatalogSnapshot, DictionaryId, DictionaryInsert, DurableLog,
    EmbeddedFrameIndex, LogMatch, LogPredicate, LogQuery, MessageFingerprint, NumericComparison,
    ObjectMetadata, ObjectTierConfig, OtlpLogDecoder, OtlpLogEvent, QueryOrder,
    RealtimeDictionaryObserver, RealtimeDictionaryTrainer, SharedTelemetryObjectStore,
    SsdObjectCache, TelemetryError, TelemetryObjectTier, TelemetryRecordRef, TelemetryResult,
    TierArtifact, TierArtifactKind, TierArtifactSource, TierCheckpoint, TierGroupSource,
    TierQueryRange, TierRetentionReport, TraceId, fingerprint_message, scan_message_terms,
    structural::{
        DecodedAttributeTables, PackedLogMetadata, StructuralRecordView,
        decode_structural_attribute_tables, decode_structural_fields,
        decode_structural_messages_with_embedded_index_and_templates, decode_structural_positions,
        decode_structural_records_with_cached_frame_data,
        decode_structural_records_with_cached_frame_data_and_fields, decode_structural_templates,
        decode_structural_trace_ids, decode_structural_typed_metadata, encode_structural_records,
        row_source_bytes,
    },
};

const MAX_REBALANCE_PASSES: u8 = 3;
const MESSAGE_TERM_CACHE_ENTRIES: usize = 1_024;
const FIELD_CACHE_ENTRIES: usize = 1_024;
const MAX_HOT_MESSAGE_TRIGRAM_KEYS: usize = 65_536;
const MAX_TIER_QUERY_INDEX_READ_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_INDEXED_FRAME_QUERY_CACHE_BYTES: usize = 768 * 1024 * 1024;
const MAX_CACHED_FRAME_MESSAGES: usize = 1_024;
const MAX_CACHED_FRAME_MESSAGE_BYTES: usize = 256 * 1024;
const MAX_CACHED_FRAME_FIELDS: usize = 1_024;
const MAX_CACHED_FRAME_FIELD_BYTES: usize = 512 * 1024;
const MAX_EXACT_FRAME_QUERY_TERMS: usize = 32;
const MAX_EXACT_FRAME_QUERY_FIELDS: usize = 8;
const MAX_INDEXED_FRAME_FIELD_KEYS: usize = 16;
const MAX_INDEXED_FRAME_FIELD_VALUES: usize = 4_096;
const MAX_EXACT_FRAME_POSTING_CACHE_BYTES: usize = 64 * 1024 * 1024;

type PartitionTermIds = HashMap<Arc<str>, usize>;
type PartitionFieldIds = HashMap<Arc<str>, HashMap<Arc<str>, usize>>;
type PartitionNumericFieldValues = HashMap<Arc<str>, Vec<(i128, usize)>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OrdinalRun {
    first: u32,
    last: u32,
}

#[derive(Debug, Default)]
struct HotPostingList {
    runs: Vec<OrdinalRun>,
    cardinality: usize,
}

impl HotPostingList {
    fn push(&mut self, ordinal: u32) {
        self.push_range(ordinal, ordinal);
    }

    fn push_range(&mut self, first: u32, final_ordinal: u32) {
        debug_assert!(first <= final_ordinal);
        let added = (final_ordinal - first) as usize + 1;
        if let Some(last_run) = self.runs.last_mut()
            && last_run.last.checked_add(1) == Some(first)
        {
            last_run.last = final_ordinal;
            self.cardinality += added;
            return;
        }
        debug_assert!(
            self.runs
                .last()
                .is_none_or(|last_run| last_run.last < first),
            "hot postings must be appended in ordinal order"
        );
        self.runs.push(OrdinalRun {
            first,
            last: final_ordinal,
        });
        self.cardinality += added;
    }

    fn is_empty_in(&self, start: u32, end: u32) -> bool {
        self.runs
            .binary_search_by(|run| {
                if run.last < start {
                    std::cmp::Ordering::Less
                } else if run.first >= end {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_err()
    }

    fn cardinality_in(&self, start: u32, end: u32) -> usize {
        if start == 0 && self.runs.last().is_none_or(|run| run.last < end) {
            return self.cardinality;
        }
        let (first_run, end_run) = self.run_range(start, end);
        self.runs[first_run..end_run]
            .iter()
            .map(|run| {
                let first = run.first.max(start);
                let last = run.last.min(end.saturating_sub(1));
                (last - first) as usize + 1
            })
            .try_fold(0usize, |total, count| total.checked_add(count))
            .unwrap_or(usize::MAX)
    }

    fn collect_in(
        &self,
        start: u32,
        end: u32,
        order: QueryOrder,
        limit: Option<usize>,
    ) -> Vec<u32> {
        if start >= end {
            return Vec::new();
        }
        let take = limit.unwrap_or(usize::MAX);
        let (first_run, end_run) = self.run_range(start, end);
        let mut ordinals = Vec::new();
        match order {
            QueryOrder::OldestFirst => {
                for run in &self.runs[first_run..end_run] {
                    if ordinals.len() == take {
                        break;
                    }
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    ordinals.extend((first..=last).take(take - ordinals.len()));
                }
            }
            QueryOrder::NewestFirst => {
                for run in self.runs[first_run..end_run].iter().rev() {
                    if ordinals.len() == take {
                        break;
                    }
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    ordinals.extend((first..=last).rev().take(take - ordinals.len()));
                }
            }
        }
        ordinals
    }

    fn contains(&self, ordinal: u32) -> bool {
        self.runs
            .binary_search_by(|run| {
                if run.last < ordinal {
                    std::cmp::Ordering::Less
                } else if run.first > ordinal {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    fn visit_in(
        &self,
        start: u32,
        end: u32,
        order: QueryOrder,
        mut visit: impl FnMut(u32) -> bool,
    ) {
        if start >= end {
            return;
        }
        let (first_run, end_run) = self.run_range(start, end);
        match order {
            QueryOrder::OldestFirst => {
                for run in &self.runs[first_run..end_run] {
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    for ordinal in first..=last {
                        if !visit(ordinal) {
                            return;
                        }
                    }
                }
            }
            QueryOrder::NewestFirst => {
                for run in self.runs[first_run..end_run].iter().rev() {
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    for ordinal in (first..=last).rev() {
                        if !visit(ordinal) {
                            return;
                        }
                    }
                }
            }
        }
    }

    fn run_range(&self, start: u32, end: u32) -> (usize, usize) {
        let first = self.runs.partition_point(|run| run.last < start);
        let end = self.runs.partition_point(|run| run.first < end);
        (first.min(end), end)
    }
}

fn collect_hot_posting_intersection(
    postings: &[&HotPostingList],
    start: u32,
    end: u32,
    order: QueryOrder,
    limit: Option<usize>,
) -> Vec<u32> {
    let Some(first) = postings.first() else {
        return Vec::new();
    };
    let take = limit.unwrap_or(usize::MAX);
    if take == 0 {
        return Vec::new();
    }
    let mut ordinals = Vec::with_capacity(take.min(first.cardinality));
    first.visit_in(start, end, order, |ordinal| {
        if postings[1..]
            .iter()
            .all(|postings| postings.contains(ordinal))
        {
            ordinals.push(ordinal);
        }
        ordinals.len() < take
    });
    ordinals
}

fn union_sorted_ordinals(existing: &mut Vec<u32>, incoming: Vec<u32>) {
    if incoming.is_empty() {
        return;
    }
    if existing.is_empty() {
        *existing = incoming;
        return;
    }
    let mut merged = Vec::with_capacity(existing.len().saturating_add(incoming.len()));
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => {
                merged.push(existing[existing_index]);
                existing_index += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(incoming[incoming_index]);
                incoming_index += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(existing[existing_index]);
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    merged.extend_from_slice(&existing[existing_index..]);
    merged.extend_from_slice(&incoming[incoming_index..]);
    *existing = merged;
}

fn collect_hot_posting_union(
    postings: &[&HotPostingList],
    start: u32,
    end: u32,
    limit: Option<usize>,
) -> Vec<u32> {
    if postings.is_empty() || start >= end || limit == Some(0) {
        return Vec::new();
    }
    if postings.len() == 1 {
        return postings[0].collect_in(start, end, QueryOrder::OldestFirst, limit);
    }
    if postings.len() == 2 {
        return collect_two_hot_posting_union(postings[0], postings[1], start, end, limit);
    }

    let take = limit.unwrap_or(usize::MAX);
    let capacity = postings
        .iter()
        .map(|posting| hot_posting_cardinality_in(posting, start, end))
        .fold(0usize, usize::saturating_add)
        .min(take);
    let mut ordinals = Vec::with_capacity(capacity);
    let mut cursors = vec![(0usize, 0u32, 0u32); postings.len()];
    let mut heap = BinaryHeap::<Reverse<(u32, usize)>>::new();

    for (posting_index, posting) in postings.iter().enumerate() {
        let run_index = posting.runs.partition_point(|run| run.last < start);
        let Some(run) = posting.runs.get(run_index) else {
            continue;
        };
        if run.first >= end {
            continue;
        }
        let current = run.first.max(start);
        let last = run.last.min(end - 1);
        cursors[posting_index] = (run_index, current, last);
        heap.push(Reverse((current, posting_index)));
    }

    let mut previous = None;
    while let Some(Reverse((ordinal, posting_index))) = heap.pop() {
        if previous != Some(ordinal) {
            ordinals.push(ordinal);
            previous = Some(ordinal);
            if ordinals.len() == take {
                break;
            }
        }

        let (mut run_index, mut current, mut last) = cursors[posting_index];
        if current < last {
            current += 1;
            cursors[posting_index] = (run_index, current, last);
            heap.push(Reverse((current, posting_index)));
            continue;
        }

        run_index += 1;
        let posting = postings[posting_index];
        while let Some(run) = posting.runs.get(run_index) {
            if run.first >= end {
                break;
            }
            if run.last >= start {
                current = run.first.max(start);
                last = run.last.min(end - 1);
                cursors[posting_index] = (run_index, current, last);
                heap.push(Reverse((current, posting_index)));
                break;
            }
            run_index += 1;
        }
    }
    ordinals
}

fn visit_hot_posting_union(
    postings: &[&HotPostingList],
    start: u32,
    end: u32,
    mut visit: impl FnMut(u32) -> bool,
) -> bool {
    if postings.is_empty() || start >= end {
        return true;
    }
    if postings.len() == 1 {
        let mut keep_going = true;
        postings[0].visit_in(start, end, QueryOrder::OldestFirst, |ordinal| {
            keep_going = visit(ordinal);
            keep_going
        });
        return keep_going;
    }
    if postings.len() == 2 {
        let mut left = HotPostingCursor::new(postings[0], start, end);
        let mut right = HotPostingCursor::new(postings[1], start, end);
        while left.is_some() || right.is_some() {
            let ordinal = match (left.as_ref(), right.as_ref()) {
                (Some(left), Some(right)) => left.current.min(right.current),
                (Some(left), None) => left.current,
                (None, Some(right)) => right.current,
                (None, None) => break,
            };
            if !visit(ordinal) {
                return false;
            }
            if left
                .as_ref()
                .is_some_and(|cursor| cursor.current == ordinal)
                && !left.as_mut().expect("left posting cursor exists").advance()
            {
                left = None;
            }
            if right
                .as_ref()
                .is_some_and(|cursor| cursor.current == ordinal)
                && !right
                    .as_mut()
                    .expect("right posting cursor exists")
                    .advance()
            {
                right = None;
            }
        }
        return true;
    }

    let mut cursors = vec![(0usize, 0u32, 0u32); postings.len()];
    let mut heap = BinaryHeap::<Reverse<(u32, usize)>>::new();
    for (posting_index, posting) in postings.iter().enumerate() {
        let run_index = posting.runs.partition_point(|run| run.last < start);
        let Some(run) = posting.runs.get(run_index) else {
            continue;
        };
        if run.first >= end {
            continue;
        }
        let current = run.first.max(start);
        let last = run.last.min(end - 1);
        cursors[posting_index] = (run_index, current, last);
        heap.push(Reverse((current, posting_index)));
    }

    let mut previous = None;
    while let Some(Reverse((ordinal, posting_index))) = heap.pop() {
        if previous != Some(ordinal) {
            if !visit(ordinal) {
                return false;
            }
            previous = Some(ordinal);
        }

        let (mut run_index, mut current, mut last) = cursors[posting_index];
        if current < last {
            current += 1;
            cursors[posting_index] = (run_index, current, last);
            heap.push(Reverse((current, posting_index)));
            continue;
        }

        run_index += 1;
        let posting = postings[posting_index];
        while let Some(run) = posting.runs.get(run_index) {
            if run.first >= end {
                break;
            }
            if run.last >= start {
                current = run.first.max(start);
                last = run.last.min(end - 1);
                cursors[posting_index] = (run_index, current, last);
                heap.push(Reverse((current, posting_index)));
                break;
            }
            run_index += 1;
        }
    }
    true
}

struct HotPostingCursor<'a> {
    posting: &'a HotPostingList,
    start: u32,
    end: u32,
    run_index: usize,
    current: u32,
    last: u32,
}

impl<'a> HotPostingCursor<'a> {
    fn new(posting: &'a HotPostingList, start: u32, end: u32) -> Option<Self> {
        if start >= end {
            return None;
        }
        let run_index = posting.runs.partition_point(|run| run.last < start);
        let run = posting.runs.get(run_index)?;
        if run.first >= end {
            return None;
        }
        Some(Self {
            posting,
            start,
            end,
            run_index,
            current: run.first.max(start),
            last: run.last.min(end - 1),
        })
    }

    fn advance(&mut self) -> bool {
        if self.current < self.last {
            self.current += 1;
            return true;
        }
        self.run_index += 1;
        while let Some(run) = self.posting.runs.get(self.run_index) {
            if run.first >= self.end {
                return false;
            }
            if run.last >= self.start {
                self.current = run.first.max(self.start);
                self.last = run.last.min(self.end - 1);
                return true;
            }
            self.run_index += 1;
        }
        false
    }
}

fn collect_two_hot_posting_union(
    left: &HotPostingList,
    right: &HotPostingList,
    start: u32,
    end: u32,
    limit: Option<usize>,
) -> Vec<u32> {
    let take = limit.unwrap_or(usize::MAX);
    let capacity = hot_posting_cardinality_in(left, start, end)
        .saturating_add(hot_posting_cardinality_in(right, start, end))
        .min(take);
    let mut ordinals = Vec::with_capacity(capacity);
    let mut left = HotPostingCursor::new(left, start, end);
    let mut right = HotPostingCursor::new(right, start, end);
    while left.is_some() || right.is_some() {
        let ordinal = match (left.as_ref(), right.as_ref()) {
            (Some(left), Some(right)) => left.current.min(right.current),
            (Some(left), None) => left.current,
            (None, Some(right)) => right.current,
            (None, None) => break,
        };
        ordinals.push(ordinal);
        if ordinals.len() == take {
            break;
        }
        if left
            .as_ref()
            .is_some_and(|cursor| cursor.current == ordinal)
            && !left.as_mut().expect("left posting cursor exists").advance()
        {
            left = None;
        }
        if right
            .as_ref()
            .is_some_and(|cursor| cursor.current == ordinal)
            && !right
                .as_mut()
                .expect("right posting cursor exists")
                .advance()
        {
            right = None;
        }
    }
    ordinals
}

fn hot_posting_cardinality_in(posting: &HotPostingList, start: u32, end: u32) -> usize {
    if start == 0 && posting.runs.last().is_none_or(|run| run.last < end) {
        posting.cardinality
    } else {
        posting.cardinality_in(start, end)
    }
}

/// Resource limits for one shard-aligned log stripe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeConfig {
    /// Approximate uncompressed byte threshold at which an active block seals.
    pub target_block_bytes: u64,
    /// Byte capacity of the stripe-local compression-dictionary LRU.
    pub dictionary_cache_bytes: usize,
    /// Zstandard level used by this stripe's owner-local encoder context.
    pub compression_level: i32,
    /// Fixed-capacity algorithmic compression-locality routing settings.
    pub compression_locality: CompressionLocalityConfig,
}

impl Default for StripeConfig {
    fn default() -> Self {
        Self {
            target_block_bytes: 8 * 1024 * 1024,
            dictionary_cache_bytes: 16 * 1024 * 1024,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig::default(),
        }
    }
}

impl StripeConfig {
    fn validate(&self) -> TelemetryResult<()> {
        if self.target_block_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "target_block_bytes must be nonzero",
            ));
        }
        if self.dictionary_cache_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "dictionary_cache_bytes must be nonzero",
            ));
        }
        if !zstd::compression_level_range().contains(&self.compression_level) {
            return Err(TelemetryError::InvalidConfig(
                "compression_level is outside zstd's supported range",
            ));
        }
        self.compression_locality
            .validate()
            .map_err(TelemetryError::InvalidConfig)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ActiveBlockKey {
    topic_partition: TopicPartition,
    source_compression_cohort: CompressionCohortId,
    placement_id: CompressionPlacementId,
    dictionary_id: Option<DictionaryId>,
}

#[derive(Debug)]
struct DictionarySelection {
    dictionary_id: Option<DictionaryId>,
    payload: Option<Arc<[u8]>>,
}

#[derive(Debug, Clone)]
struct IndexedRecord {
    record: DurableLog,
    tentative_placement: CompressionPlacement,
    temperature: CompressionTemperature,
    final_placement: Option<CompressionPlacement>,
}

#[derive(Debug)]
struct CachedMessageTerms {
    topic_partition: TopicPartition,
    message: Arc<str>,
    term_ids: Arc<[usize]>,
}

#[derive(Debug)]
struct CachedFields {
    topic_partition: TopicPartition,
    fields: Arc<Vec<crate::MetadataField>>,
    field_ids: Arc<[usize]>,
}

#[derive(Debug)]
struct PartitionIndex {
    records: Vec<IndexedRecord>,
    term_ids: PartitionTermIds,
    term_postings: Vec<HotPostingList>,
    message_trigram_postings: HashMap<u32, HotPostingList>,
    message_trigram_index_complete: bool,
    message_trigram_ascii_only: bool,
    field_ids: PartitionFieldIds,
    numeric_field_values: PartitionNumericFieldValues,
    field_postings: Vec<HotPostingList>,
    field_presence_postings: HashMap<Arc<str>, HotPostingList>,
    timestamp_order: TimestampOrder,
    indexed_through: Option<LogicalOffset>,
}

impl Default for PartitionIndex {
    fn default() -> Self {
        Self {
            records: Vec::new(),
            term_ids: HashMap::default(),
            term_postings: Vec::new(),
            message_trigram_postings: HashMap::default(),
            message_trigram_index_complete: true,
            message_trigram_ascii_only: true,
            field_ids: HashMap::default(),
            numeric_field_values: HashMap::default(),
            field_postings: Vec::new(),
            field_presence_postings: HashMap::default(),
            timestamp_order: TimestampOrder::default(),
            indexed_through: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum TimestampOrder {
    #[default]
    NonDecreasing,
    Unordered,
}

#[derive(Debug)]
struct IndexedFrameAppend {
    tenant: Arc<str>,
    first_offset: LogicalOffset,
    last_offset: LogicalOffset,
    record_count: u32,
    frames: Vec<IndexedIngestFrame>,
    next_checkpoint: Option<DurableSinkCheckpoint>,
}

#[derive(Debug, Default)]
struct IndexedFramePartition {
    appends: Vec<IndexedFrameAppend>,
    indexed_through: Option<LogicalOffset>,
}

#[derive(Debug)]
struct StripeTierState {
    tiers: HashMap<TopicPartition, TelemetryObjectTier<SharedTelemetryObjectStore>>,
    spool_directory: PathBuf,
    control_cache: Arc<SsdObjectCache>,
    payload_cache: Arc<SsdObjectCache>,
    warm_local_cache_on_publish: bool,
    config: ObjectTierConfig,
}

#[derive(Clone, Copy)]
struct IndexedFrameQuery<'a> {
    query: &'a LogQuery,
    append: &'a IndexedFrameAppend,
    frame: &'a IndexedIngestFrame,
}

/// Narrow log match used by relevance scans that only need positions and the
/// message body. It avoids reconstructing fields, attributes, and typed
/// metadata for every candidate before the top-k heap discards most of them.
#[derive(Debug, Clone)]
pub(crate) struct LogMessageMatch {
    pub(crate) record_ref: TelemetryRecordRef,
    pub(crate) timestamp_unix_nanos: u64,
    pub(crate) message: Option<Arc<str>>,
    relevance: Option<IndexedMessageRelevance>,
}

#[derive(Debug, Clone)]
struct MessageRelevanceTopKItem {
    score: f64,
    timestamp_unix_nanos: u64,
    offset: u64,
    matched: LogMessageMatch,
}

impl PartialEq for MessageRelevanceTopKItem {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == std::cmp::Ordering::Equal
            && self.timestamp_unix_nanos == other.timestamp_unix_nanos
            && self.offset == other.offset
    }
}

impl Eq for MessageRelevanceTopKItem {}

impl PartialOrd for MessageRelevanceTopKItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessageRelevanceTopKItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.timestamp_unix_nanos.cmp(&other.timestamp_unix_nanos))
            .then_with(|| self.offset.cmp(&other.offset))
    }
}

#[derive(Debug, Clone)]
struct IndexedMessageRelevance {
    stats: Arc<CachedMessageTokenStats>,
    ordinal: u32,
}

impl LogMessageMatch {
    pub(crate) fn message_arc(&self) -> Arc<str> {
        if let Some(message) = &self.message {
            return Arc::clone(message);
        }
        let relevance = self
            .relevance
            .as_ref()
            .expect("indexed message matches retain relevance metadata");
        relevance
            .stats
            .messages
            .get(relevance.ordinal as usize)
            .cloned()
            .expect("indexed message ordinal has a cached message")
    }
}

struct AbsoluteDecodedRecordView<'a> {
    record: &'a crate::DecodedStructuralRecord,
    absolute_offset: LogicalOffset,
}

impl StructuralRecordView for AbsoluteDecodedRecordView<'_> {
    fn structural_offset(&self) -> LogicalOffset {
        self.absolute_offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.record.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.record.message
    }

    fn structural_field_count(&self) -> usize {
        self.record.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.record
            .fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }
}

type ExactMessageTermPostings = HashMap<(Arc<str>, CaseSensitivity), Arc<[u32]>>;
type ExactFieldPostings = HashMap<(Arc<str>, Arc<str>), Arc<[u32]>>;
#[derive(Debug)]
struct MessageTokenPosting {
    ordinals: Arc<[u32]>,
    frequencies: Arc<[u32]>,
}

type MessageTokenPostings = HashMap<Arc<str>, Arc<MessageTokenPosting>>;

#[derive(Debug)]
struct CachedMessageTokenStats {
    postings: MessageTokenPostings,
    document_lengths: Arc<[u32]>,
    messages: Arc<[Arc<str>]>,
    token_ids_by_term: HashMap<Arc<str>, u32>,
    token_sequence: Arc<[u32]>,
    token_offsets: Arc<[u32]>,
}

impl CachedMessageTokenStats {
    fn candidate_ordinals_for_predicate(&self, predicate: &LogPredicate) -> Option<Vec<u32>> {
        if !self.messages.iter().all(|message| message.is_ascii()) {
            return None;
        }
        match predicate {
            LogPredicate::MessageTokenRegex(regex)
                if regex.case_sensitivity() == CaseSensitivity::Insensitive
                    && regex.pattern().is_ascii() =>
            {
                let mut candidates = Vec::new();
                for (token, posting) in &self.postings {
                    if regex.is_match(token) {
                        union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                    }
                }
                Some(candidates)
            }
            LogPredicate::MessageTokenPrefix {
                value,
                case_sensitivity: CaseSensitivity::Insensitive,
            } if value.is_ascii() => {
                let prefix = normalize_term(value);
                let mut candidates = Vec::new();
                for (token, posting) in &self.postings {
                    if token.starts_with(prefix.as_ref()) {
                        union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                    }
                }
                Some(candidates)
            }
            LogPredicate::MessageFuzzy {
                value,
                max_distance,
            } if value.is_ascii() => {
                let value = normalize_term(value);
                let mut candidates = Vec::new();
                for (token, posting) in &self.postings {
                    if bounded_levenshtein(token, value.as_ref(), usize::from(*max_distance)) {
                        union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                    }
                }
                Some(candidates)
            }
            LogPredicate::And(predicates) if !predicates.is_empty() => {
                let mut candidates = None;
                for predicate in predicates {
                    let posting_candidates = self.candidate_ordinals_for_predicate(predicate)?;
                    intersect_frame_candidate_slice(&mut candidates, &posting_candidates);
                    if candidates.as_ref().is_some_and(Vec::is_empty) {
                        break;
                    }
                }
                Some(candidates.unwrap_or_default())
            }
            _ => None,
        }
    }

    fn phrase_candidate_ordinals(
        &self,
        candidates: &[u32],
        terms: &[Arc<str>],
        max_gap: usize,
        case_sensitivity: CaseSensitivity,
    ) -> Option<Vec<u32>> {
        if case_sensitivity != CaseSensitivity::Insensitive
            || !terms.iter().all(|term| term.is_ascii())
            || !self.messages.iter().all(|message| message.is_ascii())
        {
            return None;
        }
        let term_ids = terms
            .iter()
            .map(|term| {
                self.token_ids_by_term
                    .get(normalize_term(term).as_ref())
                    .copied()
            })
            .collect::<Option<Vec<_>>>();
        let Some(term_ids) = term_ids else {
            return Some(Vec::new());
        };
        Some(
            candidates
                .iter()
                .copied()
                .filter(|ordinal| {
                    let Some(&start) = self.token_offsets.get(*ordinal as usize) else {
                        return false;
                    };
                    let Some(&end) = self.token_offsets.get(*ordinal as usize + 1) else {
                        return false;
                    };
                    let Ok(start) = usize::try_from(start) else {
                        return false;
                    };
                    let Ok(end) = usize::try_from(end) else {
                        return false;
                    };
                    let Some(tokens) = self.token_sequence.get(start..end) else {
                        return false;
                    };
                    message_token_ids_have_phrase(tokens, &term_ids, max_gap)
                })
                .collect(),
        )
    }

    fn cache_bytes(&self) -> usize {
        self.document_lengths
            .len()
            .saturating_mul(size_of::<u32>())
            .saturating_add(self.token_sequence.len().saturating_mul(size_of::<u32>()))
            .saturating_add(self.token_offsets.len().saturating_mul(size_of::<u32>()))
            .saturating_add(
                self.messages
                    .iter()
                    .map(|message| size_of::<Arc<str>>().saturating_add(message.len()))
                    .sum::<usize>(),
            )
            .saturating_add(
                self.token_ids_by_term
                    .keys()
                    .map(|token| token.len().saturating_add(size_of::<u32>()))
                    .sum::<usize>(),
            )
            .saturating_add(
                self.postings
                    .iter()
                    .map(|(token, posting)| {
                        token
                            .len()
                            .saturating_add(posting.ordinals.len().saturating_mul(size_of::<u32>()))
                            .saturating_add(
                                posting.frequencies.len().saturating_mul(size_of::<u32>()),
                            )
                    })
                    .sum::<usize>(),
            )
    }

    fn score_batch<E>(
        &self,
        scorer: &crate::analytics::RelevanceScorer,
        ordinals: impl Iterator<Item = u32>,
        mut emit: impl FnMut(f64) -> Result<(), E>,
    ) -> Result<(), E> {
        let postings = scorer
            .terms()
            .iter()
            .map(|term| self.postings.get(term).map(Arc::as_ref))
            .collect::<Vec<_>>();
        let mut posting_positions = vec![0usize; postings.len()];
        for ordinal in ordinals {
            let document_length = self
                .document_lengths
                .get(ordinal as usize)
                .copied()
                .unwrap_or_default();
            let score = scorer.score_indexed_by_index(document_length, |index| {
                let Some(Some(posting)) = postings.get(index) else {
                    return 0;
                };
                let position = &mut posting_positions[index];
                while *position < posting.ordinals.len() && posting.ordinals[*position] < ordinal {
                    *position += 1;
                }
                if *position >= posting.ordinals.len() || posting.ordinals[*position] != ordinal {
                    return 0;
                }
                posting
                    .frequencies
                    .get(*position)
                    .copied()
                    .unwrap_or_default()
            });
            emit(score)?;
        }
        Ok(())
    }
}

fn message_token_ids_have_phrase(tokens: &[u32], terms: &[u32], max_gap: usize) -> bool {
    let Some(&first_term) = terms.first() else {
        return true;
    };
    if max_gap == 0 {
        let mut next = 0usize;
        for &token in tokens {
            if token == terms[next] {
                next += 1;
                if next == terms.len() {
                    return true;
                }
            } else {
                next = usize::from(token == first_term);
            }
        }
        return false;
    }
    for start in 0..tokens.len() {
        if tokens[start] != first_term {
            continue;
        }
        let mut cursor = start + 1;
        let mut matched = true;
        for &term in &terms[1..] {
            let search_end = cursor.saturating_add(max_gap + 1).min(tokens.len());
            let Some(relative) = tokens
                .get(cursor..search_end)
                .and_then(|window| window.iter().position(|token| *token == term))
            else {
                matched = false;
                break;
            };
            cursor = cursor.saturating_add(relative + 1);
        }
        if matched {
            return true;
        }
    }
    false
}

pub(crate) fn for_each_message_match_score<E>(
    matches: &[LogMessageMatch],
    scorer: &crate::analytics::RelevanceScorer,
    emit: &mut dyn FnMut(&LogMessageMatch, f64) -> Result<(), E>,
) -> Result<(), E> {
    let mut start = 0usize;
    while start < matches.len() {
        let Some(relevance) = matches[start].relevance.as_ref() else {
            let matched = &matches[start];
            let message = matched
                .message
                .as_deref()
                .expect("hot message matches retain their message body");
            emit(matched, scorer.score(message))?;
            start += 1;
            continue;
        };
        let stats = Arc::clone(&relevance.stats);
        let mut end = start + 1;
        while end < matches.len()
            && matches[end]
                .relevance
                .as_ref()
                .is_some_and(|next| Arc::ptr_eq(&stats, &next.stats))
        {
            end += 1;
        }
        let run = &matches[start..end];
        let mut match_iter = run.iter();
        stats.score_batch(
            scorer,
            run.iter().map(|matched| {
                matched
                    .relevance
                    .as_ref()
                    .expect("indexed relevance run has indexed matches")
                    .ordinal
            }),
            |score| {
                let matched = match_iter
                    .next()
                    .expect("indexed relevance score has a matching row");
                emit(matched, score)
            },
        )?;
        start = end;
    }
    Ok(())
}

#[derive(Debug)]
struct CachedIndexedFrame {
    structural: Arc<[u8]>,
    embedded_index: Arc<EmbeddedFrameIndex>,
    templates: Arc<[Vec<Vec<u8>>]>,
    attribute_tables: Arc<DecodedAttributeTables>,
    offsets: Arc<[LogicalOffset]>,
    timestamps: Arc<[u64]>,
    message_bodies: Mutex<CachedFrameMessages>,
    metadata_fields: Mutex<CachedFrameFields>,
    trace_ids: Mutex<Option<Arc<[Option<TraceId>]>>>,
    typed_metadata: Mutex<Option<Arc<CachedTypedMetadata>>>,
    exact_message_terms: Mutex<ExactMessageTermPostings>,
    message_token_stats: Mutex<Option<Arc<CachedMessageTokenStats>>>,
    message_predicate_candidates: Mutex<HashMap<Arc<str>, Arc<[u32]>>>,
    exact_fields: Mutex<ExactFieldPostings>,
    field_postings: Mutex<HashMap<Arc<str>, Arc<CachedFieldPostings>>>,
}

#[derive(Debug, Default)]
struct CachedFrameMessages {
    values: HashMap<u32, Arc<str>>,
    bytes: usize,
    last: Option<(Vec<u32>, CachedMessageEntries)>,
}

#[derive(Debug, Default)]
struct CachedFrameFields {
    values: HashMap<u32, Arc<Vec<crate::MetadataField>>>,
    bytes: usize,
    last: Option<(Vec<u32>, CachedFieldEntries)>,
}

type CachedMessageEntries = Arc<[Arc<str>]>;
type CachedFieldEntries = Arc<[Arc<Vec<crate::MetadataField>>]>;

fn cached_field_bytes(fields: &Arc<Vec<crate::MetadataField>>) -> usize {
    size_of::<Arc<Vec<crate::MetadataField>>>()
        .saturating_add(
            fields
                .len()
                .saturating_mul(size_of::<crate::MetadataField>()),
        )
        .saturating_add(
            fields
                .iter()
                .map(|field| field.key.len().saturating_add(field.value.len()))
                .sum::<usize>(),
        )
}

#[derive(Debug)]
struct CachedTypedMetadata {
    packed: Arc<PackedLogMetadata>,
    cache_bytes: usize,
}

#[derive(Debug)]
struct CachedFieldPostings {
    values: HashMap<Arc<str>, Arc<[u32]>>,
    presence: Arc<[u32]>,
    ordinal_value_ids: Arc<[u32]>,
    value_table: Arc<[Arc<str>]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IndexedGroupValue {
    Missing,
    Field(u32),
    Minute(u64),
}

fn indexed_group_value(
    group: AnalyticsGroupKey,
    ordinal: u32,
    cached: &CachedIndexedFrame,
    field_posting: Option<&Arc<CachedFieldPostings>>,
) -> IndexedGroupValue {
    match group {
        AnalyticsGroupKey::Minute => usize::try_from(ordinal)
            .ok()
            .and_then(|index| cached.timestamps.get(index))
            .map_or(IndexedGroupValue::Missing, |timestamp| {
                IndexedGroupValue::Minute(timestamp / 60_000_000_000)
            }),
        AnalyticsGroupKey::SeverityText | AnalyticsGroupKey::ScopeName => field_posting
            .and_then(|posting| posting.ordinal_value_ids.get(ordinal as usize))
            .filter(|id| **id != u32::MAX)
            .map_or(IndexedGroupValue::Missing, |id| {
                IndexedGroupValue::Field(*id)
            }),
    }
}

fn materialize_indexed_group_value(
    value: IndexedGroupValue,
    field_posting: Option<&Arc<CachedFieldPostings>>,
) -> Option<Arc<str>> {
    match value {
        IndexedGroupValue::Missing => None,
        IndexedGroupValue::Field(id) => field_posting
            .and_then(|posting| posting.value_table.get(id as usize))
            .cloned(),
        IndexedGroupValue::Minute(minute) => Some(Arc::from(minute.to_string())),
    }
}

impl CachedFieldPostings {
    fn cache_bytes(&self) -> usize {
        self.presence
            .len()
            .saturating_mul(size_of::<u32>())
            .saturating_add(
                self.ordinal_value_ids
                    .len()
                    .saturating_mul(size_of::<u32>()),
            )
            .saturating_add(self.value_table.len().saturating_mul(size_of::<Arc<str>>()))
            .saturating_add(
                self.values
                    .iter()
                    .map(|(value, posting)| {
                        value
                            .len()
                            .saturating_add(posting.len().saturating_mul(size_of::<u32>()))
                    })
                    .sum::<usize>(),
            )
    }
}

impl CachedIndexedFrame {
    fn cache_bytes(&self) -> usize {
        let field_posting_bytes = self
            .field_postings
            .lock()
            .expect("indexed frame field postings lock is not poisoned")
            .values()
            .map(|postings| postings.cache_bytes())
            .sum::<usize>();
        let message_token_bytes = self
            .message_token_stats
            .lock()
            .expect("indexed frame message token cache lock is not poisoned")
            .as_ref()
            .map_or(0, |stats| stats.cache_bytes());
        let typed_metadata_bytes = self
            .typed_metadata
            .lock()
            .expect("indexed frame typed metadata cache lock is not poisoned")
            .as_ref()
            .map_or(0, |metadata| metadata.cache_bytes);
        let message_predicate_bytes = self
            .message_predicate_candidates
            .lock()
            .expect("indexed frame message predicate cache lock is not poisoned")
            .iter()
            .map(|(key, posting)| {
                key.len()
                    .saturating_add(posting.len().saturating_mul(size_of::<u32>()))
            })
            .sum::<usize>();
        let message_body_bytes = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned")
            .values
            .values()
            .map(|message| {
                size_of::<u32>()
                    .saturating_add(size_of::<Arc<str>>())
                    .saturating_add(message.len())
            })
            .sum::<usize>();
        let message_body_last_bytes = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned")
            .last
            .as_ref()
            .map_or(0, |(ordinals, messages)| {
                ordinals
                    .capacity()
                    .saturating_mul(size_of::<u32>())
                    .saturating_add(messages.len().saturating_mul(size_of::<Arc<str>>()))
            });
        let metadata_field_cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        let metadata_field_bytes = metadata_field_cache
            .values
            .values()
            .map(cached_field_bytes)
            .sum::<usize>();
        let metadata_field_last_bytes =
            metadata_field_cache
                .last
                .as_ref()
                .map_or(0, |(ordinals, fields)| {
                    ordinals
                        .capacity()
                        .saturating_mul(size_of::<u32>())
                        .saturating_add(
                            fields
                                .len()
                                .saturating_mul(size_of::<Arc<Vec<crate::MetadataField>>>()),
                        )
                });
        self.structural
            .len()
            .saturating_add(self.embedded_index.cache_bytes())
            .saturating_add(
                self.attribute_tables
                    .0
                    .iter()
                    .map(|key| key.len().saturating_add(size_of::<Arc<str>>()))
                    .sum::<usize>(),
            )
            .saturating_add(
                self.attribute_tables
                    .1
                    .iter()
                    .map(|values| {
                        values
                            .iter()
                            .map(|value| value.len().saturating_add(size_of::<Arc<str>>()))
                            .sum::<usize>()
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                self.templates
                    .iter()
                    .map(|literals| {
                        size_of::<Vec<Vec<u8>>>().saturating_add(
                            literals
                                .iter()
                                .map(|literal| {
                                    size_of::<Vec<u8>>().saturating_add(literal.capacity())
                                })
                                .sum::<usize>(),
                        )
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                self.offsets
                    .len()
                    .saturating_mul(size_of::<LogicalOffset>()),
            )
            .saturating_add(self.timestamps.len().saturating_mul(size_of::<u64>()))
            .saturating_add(
                self.trace_ids
                    .lock()
                    .expect("indexed frame trace ID cache lock is not poisoned")
                    .as_ref()
                    .map(|trace_ids| trace_ids.len().saturating_mul(size_of::<Option<TraceId>>()))
                    .unwrap_or_default(),
            )
            .saturating_add(typed_metadata_bytes)
            .saturating_add(message_token_bytes)
            .saturating_add(message_predicate_bytes)
            .saturating_add(message_body_bytes)
            .saturating_add(message_body_last_bytes)
            .saturating_add(metadata_field_bytes)
            .saturating_add(metadata_field_last_bytes)
            .saturating_add(field_posting_bytes)
    }

    fn cached_messages(&self, ordinals: &[u32]) -> Option<Arc<[Arc<str>]>> {
        if ordinals.len() > MAX_CACHED_FRAME_MESSAGES {
            return None;
        }
        let cache = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned");
        if let Some((cached_ordinals, messages)) = &cache.last
            && cached_ordinals.as_slice() == ordinals
        {
            return Some(Arc::clone(messages));
        }
        let messages = ordinals
            .iter()
            .map(|ordinal| cache.values.get(ordinal).cloned())
            .collect::<Option<Vec<_>>>()?;
        if messages.iter().map(|message| message.len()).sum::<usize>()
            > MAX_CACHED_FRAME_MESSAGE_BYTES
        {
            return None;
        }
        let messages = Arc::<[Arc<str>]>::from(messages);
        drop(cache);
        let mut cache = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned");
        cache.last = Some((ordinals.to_vec(), Arc::clone(&messages)));
        Some(messages)
    }

    fn cache_messages(&self, ordinals: &[u32], messages: &[crate::DecodedStructuralRecord]) {
        if ordinals.len() != messages.len() {
            return;
        }
        let mut cache = self
            .message_bodies
            .lock()
            .expect("indexed frame message body cache lock is not poisoned");
        for (ordinal, record) in ordinals.iter().zip(messages) {
            if cache.values.contains_key(ordinal) {
                continue;
            }
            let bytes = record.message.len();
            if cache.values.len() >= MAX_CACHED_FRAME_MESSAGES
                || cache.bytes.saturating_add(bytes) > MAX_CACHED_FRAME_MESSAGE_BYTES
            {
                break;
            }
            cache.bytes = cache.bytes.saturating_add(bytes);
            cache.values.insert(*ordinal, Arc::clone(&record.message));
        }
        let batch_bytes = messages
            .iter()
            .map(|record| record.message.len())
            .sum::<usize>();
        cache.last = (ordinals.len() <= MAX_CACHED_FRAME_MESSAGES
            && batch_bytes <= MAX_CACHED_FRAME_MESSAGE_BYTES)
            .then(|| {
                (
                    ordinals.to_vec(),
                    Arc::from(
                        messages
                            .iter()
                            .map(|record| Arc::clone(&record.message))
                            .collect::<Vec<_>>(),
                    ),
                )
            });
    }

    fn cached_fields(&self, ordinals: &[u32]) -> Option<Arc<[Arc<Vec<crate::MetadataField>>]>> {
        if ordinals.len() > MAX_CACHED_FRAME_FIELDS {
            return None;
        }
        let cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        if let Some((cached_ordinals, fields)) = &cache.last
            && cached_ordinals.as_slice() == ordinals
        {
            return Some(Arc::clone(fields));
        }
        let fields = ordinals
            .iter()
            .map(|ordinal| cache.values.get(ordinal).cloned())
            .collect::<Option<Vec<_>>>()?;
        if fields.iter().map(cached_field_bytes).sum::<usize>() > MAX_CACHED_FRAME_FIELD_BYTES {
            return None;
        }
        let fields = Arc::<[Arc<Vec<crate::MetadataField>>]>::from(fields);
        drop(cache);
        let mut cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        cache.last = Some((ordinals.to_vec(), Arc::clone(&fields)));
        Some(fields)
    }

    fn cache_fields(&self, ordinals: &[u32], records: &[crate::DecodedStructuralRecord]) {
        if ordinals.len() != records.len() {
            return;
        }
        let mut cache = self
            .metadata_fields
            .lock()
            .expect("indexed frame metadata field cache lock is not poisoned");
        for (ordinal, record) in ordinals.iter().zip(records) {
            if cache.values.contains_key(ordinal) {
                continue;
            }
            let bytes = cached_field_bytes(&record.fields);
            if cache.values.len() >= MAX_CACHED_FRAME_FIELDS
                || cache.bytes.saturating_add(bytes) > MAX_CACHED_FRAME_FIELD_BYTES
            {
                break;
            }
            cache.bytes = cache.bytes.saturating_add(bytes);
            cache.values.insert(*ordinal, Arc::clone(&record.fields));
        }
        let batch_bytes = records
            .iter()
            .map(|record| cached_field_bytes(&record.fields))
            .sum::<usize>();
        cache.last = (ordinals.len() <= MAX_CACHED_FRAME_FIELDS
            && batch_bytes <= MAX_CACHED_FRAME_FIELD_BYTES)
            .then(|| {
                (
                    ordinals.to_vec(),
                    Arc::from(
                        records
                            .iter()
                            .map(|record| Arc::clone(&record.fields))
                            .collect::<Vec<_>>(),
                    ),
                )
            });
    }
}

fn retain_cached_timestamp_candidates(
    query: &LogQuery,
    cached: &CachedIndexedFrame,
    candidates: &mut Vec<u32>,
) {
    if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
        return;
    }
    if cached.embedded_index.timestamp_offset_ordinal_ordered() {
        // Posting intersections preserve ordinal order, so an ordered frame's
        // timestamp window can trim the candidate vector without probing each
        // timestamp individually.
        let start = query.start_timestamp_unix_nanos.map_or(0, |timestamp| {
            cached
                .timestamps
                .partition_point(|candidate| *candidate < timestamp)
        });
        let end = query
            .end_timestamp_unix_nanos
            .map_or(cached.timestamps.len(), |timestamp| {
                cached
                    .timestamps
                    .partition_point(|candidate| *candidate < timestamp)
            });
        if start >= end {
            candidates.clear();
            return;
        }
        let first = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < start));
        let last = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < end));
        candidates.truncate(last);
        candidates.drain(..first);
    } else {
        candidates.retain(|ordinal| {
            usize::try_from(*ordinal)
                .ok()
                .and_then(|index| cached.timestamps.get(index))
                .is_some_and(|timestamp| query.timestamp_matches(*timestamp))
        });
    }
}

fn count_cached_timestamp_candidates(
    query: &LogQuery,
    cached: &CachedIndexedFrame,
    candidates: &[u32],
) -> usize {
    if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
        return candidates.len();
    }
    if cached.embedded_index.timestamp_offset_ordinal_ordered() {
        let start = query.start_timestamp_unix_nanos.map_or(0, |timestamp| {
            cached
                .timestamps
                .partition_point(|candidate| *candidate < timestamp)
        });
        let end = query
            .end_timestamp_unix_nanos
            .map_or(cached.timestamps.len(), |timestamp| {
                cached
                    .timestamps
                    .partition_point(|candidate| *candidate < timestamp)
            });
        if start >= end {
            return 0;
        }
        let first = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < start));
        let last = candidates
            .partition_point(|ordinal| usize::try_from(*ordinal).is_ok_and(|index| index < end));
        return last.saturating_sub(first);
    }
    candidates
        .iter()
        .filter_map(|ordinal| {
            usize::try_from(*ordinal)
                .ok()
                .and_then(|index| cached.timestamps.get(index))
                .copied()
        })
        .filter(|timestamp| query.timestamp_matches(*timestamp))
        .count()
}

#[derive(Debug, Default)]
struct IndexedFrameQueryCache {
    entries: HashMap<u64, Arc<CachedIndexedFrame>>,
    eviction_order: VecDeque<u64>,
    bytes: usize,
}

impl IndexedFrameQueryCache {
    fn get(&mut self, frame_id: u64) -> Option<Arc<CachedIndexedFrame>> {
        // Hits do not update the eviction order. Maintaining an exact LRU here
        // would make every frame hit scan the order queue; insertion-order
        // eviction keeps the bounded cache out of the query hot path.
        self.entries.get(&frame_id).cloned()
    }

    fn insert(&mut self, frame_id: u64, cached: Arc<CachedIndexedFrame>) {
        if cached.cache_bytes() > MAX_INDEXED_FRAME_QUERY_CACHE_BYTES {
            return;
        }
        self.entries.remove(&frame_id);
        self.remove_from_eviction_order(frame_id);
        self.entries.insert(frame_id, cached);
        self.eviction_order.push_back(frame_id);
        self.enforce_budget();
    }

    fn remove_from_eviction_order(&mut self, frame_id: u64) {
        if let Some(position) = self
            .eviction_order
            .iter()
            .position(|cached| *cached == frame_id)
        {
            self.eviction_order.remove(position);
        }
    }

    fn enforce_budget(&mut self) {
        self.bytes = self
            .entries
            .values()
            .map(|cached| cached.cache_bytes())
            .fold(0usize, usize::saturating_add);
        while self.bytes > MAX_INDEXED_FRAME_QUERY_CACHE_BYTES {
            let Some(evicted_id) = self.eviction_order.pop_front() else {
                break;
            };
            self.entries.remove(&evicted_id);
            self.bytes = self
                .entries
                .values()
                .map(|cached| cached.cache_bytes())
                .fold(0usize, usize::saturating_add);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum ExactPostingKey {
    Message(u64, Arc<str>, CaseSensitivity),
    Field(u64, Arc<str>, Arc<str>),
}

#[derive(Debug, Default)]
struct ExactPostingCache {
    entries: HashMap<ExactPostingKey, Arc<[u32]>>,
    eviction_order: VecDeque<ExactPostingKey>,
    bytes: usize,
}

impl ExactPostingCache {
    fn get(&self, key: &ExactPostingKey) -> Option<Arc<[u32]>> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: ExactPostingKey, posting: Arc<[u32]>) {
        if self.entries.contains_key(&key) {
            return;
        }
        self.bytes = self
            .bytes
            .saturating_add(posting.len().saturating_mul(size_of::<u32>()));
        self.eviction_order.push_back(key.clone());
        self.entries.insert(key, posting);
        while self.bytes > MAX_EXACT_FRAME_POSTING_CACHE_BYTES {
            let Some(evicted) = self.eviction_order.pop_front() else {
                break;
            };
            if let Some(posting) = self.entries.remove(&evicted) {
                self.bytes = self
                    .bytes
                    .saturating_sub(posting.len().saturating_mul(size_of::<u32>()));
            }
        }
    }
}

impl PartitionIndex {
    fn record(&self, offset: LogicalOffset) -> Option<&IndexedRecord> {
        self.records
            .binary_search_by_key(&offset, |record| record.record.record_ref.offset)
            .ok()
            .and_then(|index| self.records.get(index))
    }

    fn record_mut(&mut self, offset: LogicalOffset) -> Option<&mut IndexedRecord> {
        self.records
            .binary_search_by_key(&offset, |record| record.record.record_ref.offset)
            .ok()
            .and_then(|index| self.records.get_mut(index))
    }

    fn last_offset(&self) -> Option<LogicalOffset> {
        self.records
            .last()
            .map(|record| record.record.record_ref.offset)
    }
}

#[derive(Debug, Clone)]
struct PendingRecord {
    record: DurableLog,
    source_bytes: u64,
    fingerprint: MessageFingerprint,
}

impl PendingRecord {
    fn locality(&self) -> CompressionLocalityRecord {
        CompressionLocalityRecord {
            fingerprint: self.fingerprint,
            source_bytes: self.source_bytes,
        }
    }
}

impl StructuralRecordView for PendingRecord {
    fn structural_offset(&self) -> LogicalOffset {
        self.record.structural_offset()
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.record.structural_timestamp_unix_nanos()
    }

    fn structural_message(&self) -> &str {
        self.record.structural_message()
    }

    fn structural_field_count(&self) -> usize {
        self.record.structural_field_count()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.record.structural_field(index)
    }

    fn structural_log_metadata(&self) -> Option<crate::structural::StructuralLogMetadataRef<'_>> {
        self.record.structural_log_metadata()
    }
}

#[derive(Debug, Clone)]
struct ActiveBlock {
    first_offset: LogicalOffset,
    last_offset: LogicalOffset,
    record_count: u32,
    source_bytes: u64,
    min_timestamp_unix_nanos: u64,
    max_timestamp_unix_nanos: u64,
    dictionary_payload: Option<Arc<[u8]>>,
    rebalance_passes: u8,
    records: Vec<PendingRecord>,
}

impl ActiveBlock {
    fn new(record: PendingRecord, dictionary_payload: Option<Arc<[u8]>>) -> Self {
        let durable = &record.record;
        Self {
            first_offset: durable.record_ref.offset,
            last_offset: durable.record_ref.offset,
            record_count: 1,
            source_bytes: record.source_bytes,
            min_timestamp_unix_nanos: durable.timestamp_unix_nanos,
            max_timestamp_unix_nanos: durable.timestamp_unix_nanos,
            dictionary_payload,
            rebalance_passes: 0,
            records: vec![record],
        }
    }

    fn from_records(
        mut records: Vec<PendingRecord>,
        dictionary_payload: Option<Arc<[u8]>>,
        rebalance_passes: u8,
    ) -> Self {
        if records.windows(2).any(|adjacent| {
            adjacent[0].record.record_ref.offset > adjacent[1].record.record_ref.offset
        }) {
            records.sort_unstable_by_key(|record| record.record.record_ref.offset);
        }
        let mut records = records.into_iter();
        let first = records
            .next()
            .expect("a collation assignment always contains records");
        let mut active = Self::new(first, dictionary_payload);
        active.rebalance_passes = rebalance_passes;
        for record in records {
            active.append(record);
        }
        active
    }

    fn append(&mut self, record: PendingRecord) {
        self.first_offset = self.first_offset.min(record.record.record_ref.offset);
        self.last_offset = self.last_offset.max(record.record.record_ref.offset);
        self.record_count = self
            .record_count
            .checked_add(1)
            .expect("a bounded active block cannot contain more than u32 records");
        self.source_bytes = self.source_bytes.saturating_add(record.source_bytes);
        self.min_timestamp_unix_nanos = self
            .min_timestamp_unix_nanos
            .min(record.record.timestamp_unix_nanos);
        self.max_timestamp_unix_nanos = self
            .max_timestamp_unix_nanos
            .max(record.record.timestamp_unix_nanos);
        self.records.push(record);
    }

    fn append_block(&mut self, other: Self) {
        self.first_offset = self.first_offset.min(other.first_offset);
        self.last_offset = self.last_offset.max(other.last_offset);
        self.record_count = self
            .record_count
            .checked_add(other.record_count)
            .expect("a bounded active block cannot contain more than u32 records");
        self.source_bytes = self.source_bytes.saturating_add(other.source_bytes);
        self.min_timestamp_unix_nanos = self
            .min_timestamp_unix_nanos
            .min(other.min_timestamp_unix_nanos);
        self.max_timestamp_unix_nanos = self
            .max_timestamp_unix_nanos
            .max(other.max_timestamp_unix_nanos);
        self.rebalance_passes = self.rebalance_passes.max(other.rebalance_passes);
        let total_records = self.records.len().saturating_add(other.records.len());
        let mut left = std::mem::take(&mut self.records).into_iter().peekable();
        let mut right = other.records.into_iter().peekable();
        let mut merged = Vec::with_capacity(total_records);
        while let (Some(left_record), Some(right_record)) = (left.peek(), right.peek()) {
            if left_record.record.record_ref.offset <= right_record.record.record_ref.offset {
                merged.push(left.next().expect("left record was present"));
            } else {
                merged.push(right.next().expect("right record was present"));
            }
        }
        merged.extend(left);
        merged.extend(right);
        self.records = merged;
    }
}

/// Reusable compression context owned exclusively by one log stripe.
///
/// Dictionary changes occur only when a block seals. The context is never
/// shared with another shard, so the ingest path avoids both locks and cache
/// line contention from a global compressor pool.
struct StripeCompressor {
    zstd_level: i32,
    active_dictionary: Option<DictionaryId>,
    zstd: zstd::bulk::Compressor<'static>,
}

impl std::fmt::Debug for StripeCompressor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StripeCompressor")
            .field("zstd_level", &self.zstd_level)
            .field("active_dictionary", &self.active_dictionary)
            .finish_non_exhaustive()
    }
}

impl StripeCompressor {
    fn new(zstd_level: i32) -> TelemetryResult<Self> {
        Ok(Self {
            zstd_level,
            active_dictionary: None,
            zstd: zstd::bulk::Compressor::new(zstd_level)
                .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?,
        })
    }

    fn compress(
        &mut self,
        source: &[u8],
        dictionary_id: Option<DictionaryId>,
        dictionary_payload: Option<&[u8]>,
    ) -> TelemetryResult<Vec<u8>> {
        if self.active_dictionary != dictionary_id {
            let dictionary = match (dictionary_id, dictionary_payload) {
                (Some(_), Some(payload)) => payload,
                (Some(dictionary_id), None) => {
                    return Err(TelemetryError::MissingDictionary(dictionary_id));
                }
                (None, _) => &[],
            };
            self.zstd
                .set_dictionary(self.zstd_level, dictionary)
                .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))?;
            self.active_dictionary = dictionary_id;
        }
        self.zstd
            .compress(source)
            .map_err(|error| TelemetryError::CompressionFailed(error.to_string()))
    }
}

/// Result of publishing one durable append into the hot index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReceipt {
    /// Record made visible to term and metadata queries.
    pub record_ref: TelemetryRecordRef,
    /// Indexed watermark after the publication.
    pub indexed_through: LogicalOffset,
    /// Per-record temperature used for block scoring.
    pub compression_temperature: CompressionTemperature,
    /// Tentative collection lane; final placement is a block decision.
    pub tentative_compression_placement: CompressionPlacement,
    /// Blocks sealed as a result of this append and bounded redistribution.
    pub sealed_blocks: Vec<BlockDescriptor>,
}

struct AppliedRecord {
    receipt: IndexReceipt,
    ordinal: u32,
    term_ids: Option<Arc<[usize]>>,
    message_trigram_keys: Option<Arc<[u32]>>,
    field_ids: Option<Arc<[usize]>>,
}

/// Integration point called by the shard-stream worker after durable append.
///
/// This deliberately uses a post-durability callback. The append log remains
/// authoritative; index loss after a process failure can be repaired by
/// replaying records from the last sealed index-block watermark.
pub trait ShardStreamDurableSink {
    /// Publishes a durable log event into the shard-local hot index.
    fn on_durable_append(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt>;
}

/// A lock-free-by-ownership hot index for one shard-stream physical shard.
///
/// The type intentionally exposes mutation only through `&mut self`. The
/// shard-stream worker that owns the corresponding physical shard is therefore
/// the sole writer; readers consume immutable snapshots at a higher query
/// layer or are scheduled on that same stripe. This avoids a global concurrent
/// map on the ingestion path.
#[derive(Debug)]
pub struct LogStripe {
    stream_shard_id: ShardId,
    config: StripeConfig,
    partitions: HashMap<TopicPartition, PartitionIndex>,
    indexed_frame_partitions: HashMap<TopicPartition, IndexedFramePartition>,
    indexed_frame_query_cache: Mutex<IndexedFrameQueryCache>,
    exact_posting_cache: Mutex<ExactPostingCache>,
    active_partition_cache: HashMap<Arc<str>, Vec<TopicPartition>>,
    message_term_cache: Vec<Option<CachedMessageTerms>>,
    field_cache: Vec<Option<CachedFields>>,
    active_blocks: HashMap<ActiveBlockKey, ActiveBlock>,
    catalog: BlockCatalog,
    placement_dictionaries: HashMap<CompressionPlacementId, DictionaryId>,
    dictionary_cache: DictionaryCache,
    dictionary_catalog: Option<Arc<DictionaryCatalog>>,
    dictionary_snapshot: Option<Arc<DictionaryCatalogSnapshot>>,
    dictionary_generation: u64,
    realtime_dictionary: Option<RealtimeDictionaryObserver>,
    block_collator: CompressionBlockCollator,
    compressor: StripeCompressor,
    tier: Option<StripeTierState>,
    next_frame_id: u64,
}

impl LogStripe {
    /// Creates a stripe owned by one physical shard-stream shard.
    pub fn new(stream_shard_id: ShardId, config: StripeConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let compression_level = config.compression_level;
        let block_collator = CompressionBlockCollator::new(
            config.compression_locality.clone(),
            config.target_block_bytes,
        )?;
        Ok(Self {
            stream_shard_id,
            dictionary_cache: DictionaryCache::new(config.dictionary_cache_bytes)?,
            config,
            partitions: HashMap::new(),
            indexed_frame_partitions: HashMap::new(),
            indexed_frame_query_cache: Mutex::new(IndexedFrameQueryCache::default()),
            exact_posting_cache: Mutex::new(ExactPostingCache::default()),
            active_partition_cache: HashMap::new(),
            message_term_cache: std::iter::repeat_with(|| None)
                .take(MESSAGE_TERM_CACHE_ENTRIES)
                .collect(),
            field_cache: std::iter::repeat_with(|| None)
                .take(FIELD_CACHE_ENTRIES)
                .collect(),
            active_blocks: HashMap::new(),
            catalog: BlockCatalog::default(),
            placement_dictionaries: HashMap::new(),
            dictionary_catalog: None,
            dictionary_snapshot: None,
            dictionary_generation: 0,
            realtime_dictionary: None,
            block_collator,
            compressor: StripeCompressor::new(compression_level)?,
            tier: None,
            next_frame_id: 0,
        })
    }

    /// Creates a stripe that receives immutable dictionary publications from a
    /// shared control-plane catalog.
    pub fn with_dictionary_catalog(
        stream_shard_id: ShardId,
        config: StripeConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> TelemetryResult<Self> {
        let mut stripe = Self::new(stream_shard_id, config)?;
        stripe.dictionary_catalog = Some(dictionary_catalog);
        stripe.refresh_dictionary_catalog()?;
        Ok(stripe)
    }

    /// Creates a stripe that contributes sealed blocks to a bounded real-time
    /// dictionary learner and adopts accepted immutable generations.
    pub fn with_realtime_dictionary(
        stream_shard_id: ShardId,
        config: StripeConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> TelemetryResult<Self> {
        let mut stripe = Self::with_dictionary_catalog(stream_shard_id, config, trainer.catalog())?;
        stripe.realtime_dictionary = Some(trainer.observer());
        Ok(stripe)
    }

    /// Attaches a non-blocking real-time dictionary observer.
    ///
    /// The observer must publish into the same catalog configured for this
    /// stripe. A full learner queue drops only the observation, never the block.
    pub fn attach_realtime_dictionary(&mut self, observer: RealtimeDictionaryObserver) {
        self.realtime_dictionary = Some(observer);
    }

    /// Returns the shard-stream physical shard that owns this stripe.
    #[must_use]
    pub const fn stream_shard_id(&self) -> ShardId {
        self.stream_shard_id
    }

    /// Returns the visible indexed watermark for a partition.
    #[must_use]
    pub fn indexed_through(&self, topic_partition: TopicPartition) -> Option<LogicalOffset> {
        let record_watermark = self
            .partitions
            .get(&topic_partition)
            .and_then(|partition| partition.indexed_through);
        let frame_watermark = self
            .indexed_frame_partitions
            .get(&topic_partition)
            .and_then(|partition| partition.indexed_through);
        record_watermark.max(frame_watermark)
    }

    /// Returns the local catalog of sealed data blocks.
    #[must_use]
    pub const fn catalog(&self) -> &BlockCatalog {
        &self.catalog
    }

    /// Attaches partition-scoped immutable object catalogs and returns their
    /// durable recovery watermarks.
    pub(crate) fn attach_object_tier(
        &mut self,
        store: SharedTelemetryObjectStore,
        spool_directory: PathBuf,
        caches: (Arc<SsdObjectCache>, Arc<SsdObjectCache>),
        partitions: impl IntoIterator<Item = TopicPartition>,
        config: ObjectTierConfig,
        warm_local_cache_on_publish: bool,
    ) -> TelemetryResult<Vec<DurableSinkCheckpoint>> {
        if self.tier.is_some() {
            return Err(TelemetryError::InvalidConfig(
                "an object tier is already attached to this stripe",
            ));
        }
        let spool_directory = spool_directory.join(format!("shard-{}", self.stream_shard_id.get()));
        fs::create_dir_all(&spool_directory)
            .map_err(|error| TelemetryError::StorageIo(format!("create tier spool: {error}")))?;
        let mut tiers = HashMap::new();
        let mut checkpoints = Vec::new();
        let mut next_frame_id = self.next_frame_id;
        for partition in partitions {
            let object_tier =
                TelemetryObjectTier::open(store.clone(), self.stream_shard_id, partition, config)?;
            next_frame_id = next_frame_id.max(object_tier.root().next_block_id);
            if let Some(checkpoint) = object_tier.root().latest_checkpoint {
                checkpoints.push(DurableSinkCheckpoint {
                    topic_partition: partition,
                    next_placement_sequence: shard_stream_core::PlacementSequence::new(
                        checkpoint.next_placement_sequence,
                    ),
                    next_offset: LogicalOffset::new(checkpoint.next_offset),
                });
            }
            if tiers.insert(partition, object_tier).is_some() {
                return Err(TelemetryError::InvalidConfig(
                    "object tier contains a duplicate partition",
                ));
            }
        }
        if tiers.is_empty() {
            return Err(TelemetryError::InvalidConfig(
                "object tier requires at least one partition",
            ));
        }
        self.next_frame_id = next_frame_id;
        let (control_cache, payload_cache) = caches;
        self.tier = Some(StripeTierState {
            tiers,
            spool_directory,
            control_cache,
            payload_cache,
            warm_local_cache_on_publish,
            config,
        });
        Ok(checkpoints)
    }

    /// Returns compressed payload bytes that have not reached immutable object storage.
    #[must_use]
    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.indexed_frame_partitions
            .values()
            .flat_map(|partition| &partition.appends)
            .flat_map(|append| &append.frames)
            .map(|frame| u64::try_from(frame.compressed.len()).unwrap_or(u64::MAX))
            .sum()
    }

    /// Returns the local catalog of sealed data blocks for offload bookkeeping.
    pub fn catalog_mut(&mut self) -> &mut BlockCatalog {
        &mut self.catalog
    }

    /// Returns the stripe-local cache of immutable compression dictionaries.
    #[must_use]
    pub const fn dictionary_cache(&self) -> &DictionaryCache {
        &self.dictionary_cache
    }

    /// Returns the stripe-local cache of immutable compression dictionaries.
    pub fn dictionary_cache_mut(&mut self) -> &mut DictionaryCache {
        &mut self.dictionary_cache
    }

    /// Returns the last immutable catalog generation observed by this stripe.
    #[must_use]
    pub const fn dictionary_generation(&self) -> u64 {
        self.dictionary_generation
    }

    /// Returns cumulative diagnostics from this stripe's block collator.
    #[must_use]
    pub fn compression_collation_stats(&self) -> CompressionLocalityStats {
        self.block_collator.stats()
    }

    /// Returns the final block placement once the record's block has sealed.
    #[must_use]
    pub fn final_compression_placement(
        &self,
        record_ref: TelemetryRecordRef,
    ) -> Option<CompressionPlacement> {
        self.partitions
            .get(&record_ref.topic_partition)
            .and_then(|partition| partition.record(record_ref.offset))
            .and_then(|record| record.final_placement)
    }

    /// Adopts control-plane state at an append boundary.
    pub fn begin_append_batch(&mut self) -> TelemetryResult<bool> {
        self.refresh_dictionary_catalog()
    }

    /// Adopts the latest immutable dictionary snapshot at a batch boundary.
    ///
    /// This is deliberately explicit: individual records only inspect the
    /// stripe-owned assignment map and LRU. The durable sink invokes it once
    /// before each append batch, while embedded callers can choose their own
    /// safe batch boundary.
    pub fn refresh_dictionary_catalog(&mut self) -> TelemetryResult<bool> {
        let Some(dictionary_catalog) = &self.dictionary_catalog else {
            return Ok(false);
        };
        let snapshot = dictionary_catalog.snapshot()?;
        if snapshot.generation() == self.dictionary_generation {
            return Ok(false);
        }

        self.placement_dictionaries.clear();
        for (placement_id, dictionary_id) in snapshot.assignments() {
            self.placement_dictionaries
                .insert(placement_id, dictionary_id);
        }
        self.dictionary_generation = snapshot.generation();
        self.dictionary_snapshot = Some(snapshot);
        Ok(true)
    }

    /// Installs an immutable dictionary for future blocks in a placement.
    ///
    /// Existing active blocks retain their previous dictionary identifier, so a
    /// dictionary rotation never makes already accepted log records ambiguous.
    pub fn install_dictionary(
        &mut self,
        placement_id: CompressionPlacementId,
        dictionary_id: DictionaryId,
        payload: Arc<[u8]>,
    ) -> TelemetryResult<DictionaryInsert> {
        if let Some(dictionary_catalog) = &self.dictionary_catalog {
            dictionary_catalog.publish(placement_id, dictionary_id, Arc::clone(&payload))?;
            self.refresh_dictionary_catalog()?;
        } else {
            self.placement_dictionaries
                .insert(placement_id, dictionary_id);
        }
        let insert = self.dictionary_cache.insert(dictionary_id, payload)?;
        Ok(insert)
    }

    /// Applies a record only after the corresponding shard-stream append is durable.
    ///
    /// Index postings are written before the visible watermark advances. A
    /// query constrained to [`Self::indexed_through`] consequently cannot see
    /// an incomplete posting update.
    pub fn apply_durable(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        if record.stream_shard_id != self.stream_shard_id {
            return Err(TelemetryError::WrongStripe {
                expected: self.stream_shard_id,
                observed: record.stream_shard_id,
            });
        }
        if self
            .partitions
            .get(&record.record_ref.topic_partition)
            .and_then(|partition| partition.record(record.record_ref.offset))
            .is_some()
        {
            return Err(TelemetryError::DuplicateRecord {
                partition: record.record_ref.topic_partition,
                offset: record.record_ref.offset,
            });
        }
        self.apply_durable_new(record)
    }

    fn apply_durable_new(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        self.apply_durable_new_inner(record, true)
            .map(|applied| applied.receipt)
    }

    fn apply_durable_new_inner(
        &mut self,
        record: DurableLog,
        index_record: bool,
    ) -> TelemetryResult<AppliedRecord> {
        self.active_partition_cache.clear();
        self.validate_offset(&record)?;

        let record_source_bytes = row_source_bytes(&record)?;
        let fingerprint = if self.block_collator.is_enabled() {
            fingerprint_message(&record.message, &record.fields)
        } else {
            MessageFingerprint {
                shape_hash: 0,
                locality_signature: 0,
            }
        };
        let compression_temperature = CompressionTemperature::new(fingerprint.locality_signature);
        let tentative_compression_placement = self
            .block_collator
            .tentative_placement(record.compression_cohort, fingerprint);
        let dictionary = self.resolve_dictionary(tentative_compression_placement.placement_id)?;
        let active_key = ActiveBlockKey {
            topic_partition: record.record_ref.topic_partition,
            source_compression_cohort: record.compression_cohort,
            placement_id: tentative_compression_placement.placement_id,
            dictionary_id: dictionary.dictionary_id,
        };
        let reference = record.record_ref;
        let pending = PendingRecord {
            record: record.clone(),
            source_bytes: record_source_bytes,
            fingerprint,
        };
        let record_ordinal = {
            let partition = self
                .partitions
                .entry(reference.topic_partition)
                .or_default();
            let record_ordinal = u32::try_from(partition.records.len())
                .map_err(|_| TelemetryError::RecordTooLarge)?;
            partition.records.push(IndexedRecord {
                record: record.clone(),
                tentative_placement: tentative_compression_placement,
                temperature: compression_temperature,
                final_placement: None,
            });
            record_ordinal
        };

        let next_source_bytes = self.active_blocks.get(&active_key).map_or_else(
            || record_source_bytes,
            |active| active.source_bytes.saturating_add(record_source_bytes),
        );
        let sealed_result = if next_source_bytes >= self.config.target_block_bytes {
            let active = match self.active_blocks.remove(&active_key) {
                Some(mut active) => {
                    active.append(pending);
                    active
                }
                None => ActiveBlock::new(pending, dictionary.payload),
            };
            self.rebalance_block(active_key, active, false)
        } else {
            match self.active_blocks.get_mut(&active_key) {
                Some(active) => {
                    active.append(pending);
                }
                None => {
                    self.active_blocks
                        .insert(active_key, ActiveBlock::new(pending, dictionary.payload));
                }
            }
            Ok(Vec::new())
        };
        let sealed_blocks = match sealed_result {
            Ok(sealed_blocks) => sealed_blocks,
            Err(error) => {
                let removed = self
                    .partitions
                    .get_mut(&reference.topic_partition)
                    .and_then(|partition| partition.records.pop());
                debug_assert!(
                    removed.is_some_and(|removed| removed.record.record_ref == reference)
                );
                return Err(error);
            }
        };

        if let Some(partition) = self.partitions.get_mut(&reference.topic_partition)
            && partition.timestamp_order == TimestampOrder::NonDecreasing
            && partition
                .records
                .get(partition.records.len().saturating_sub(2))
                .is_some_and(|previous| {
                    previous.record.timestamp_unix_nanos > record.timestamp_unix_nanos
                })
        {
            partition.timestamp_order = TimestampOrder::Unordered;
        }

        let (term_ids, message_trigram_keys, field_ids) = if index_record {
            let term_ids = self.index_terms(&record, record_ordinal);
            let message_trigram_keys = self.index_message_trigrams(&record, record_ordinal);
            let field_ids = self.index_fields(&record, record_ordinal);
            // This assignment is deliberately last: it is the publication
            // barrier for readers sharing this stripe's ordering domain.
            self.partitions
                .get_mut(&reference.topic_partition)
                .expect("record partition was inserted")
                .indexed_through = Some(reference.offset);
            (Some(term_ids), message_trigram_keys, Some(field_ids))
        } else {
            (None, None, None)
        };

        Ok(AppliedRecord {
            receipt: IndexReceipt {
                record_ref: reference,
                indexed_through: reference.offset,
                compression_temperature,
                tentative_compression_placement,
                sealed_blocks,
            },
            ordinal: record_ordinal,
            term_ids,
            message_trigram_keys,
            field_ids,
        })
    }

    /// Publishes OTLP events after shard-stream has made their append durable.
    ///
    /// The live ingestion path should decode the export once before appending,
    /// set shard-stream's `record_count` to `events.len()`, then pass the same
    /// events here after it receives `first_offset` in the append response.
    pub fn apply_otlp_events(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: impl IntoIterator<Item = OtlpLogEvent>,
    ) -> TelemetryResult<Vec<IndexReceipt>> {
        self.begin_append_batch()?;
        let events = events.into_iter().collect::<Vec<_>>();
        validate_batch_offset_range(topic_partition, first_offset, events.len())?;
        if self.can_index_as_homogeneous_range(topic_partition, first_offset, &events) {
            self.apply_homogeneous_events(topic_partition, first_offset, events)
        } else {
            events
                .into_iter()
                .enumerate()
                .map(|(index, event)| {
                    let offset = batch_offset(topic_partition, first_offset, index)?;
                    self.apply_durable_idempotent(event.into_durable(
                        self.stream_shard_id,
                        topic_partition,
                        offset,
                    ))
                })
                .collect()
        }
    }

    /// Publishes one durable compressed ingest pack without rebuilding a
    /// second per-record posting index.
    ///
    /// The authoritative compressed cohort frames remain resident and the
    /// compressor-derived indexes select candidates. Exact bodies and fields
    /// are reconstructed only for candidate records during lookup.
    #[cfg(test)]
    pub(crate) fn apply_indexed_ingest_pack(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
    ) -> TelemetryResult<()> {
        self.apply_indexed_ingest_pack_inner(
            Arc::from("test-tenant"),
            topic_partition,
            first_offset,
            record_count,
            payload,
            None,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_checkpointed_ingest_pack(
        &mut self,
        tenant: Arc<str>,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
        transient_context: Option<&[u8]>,
        payload_already_validated: bool,
        checkpoints: (DurableSinkCheckpoint, DurableSinkCheckpoint),
    ) -> TelemetryResult<()> {
        let (expected_checkpoint, next_checkpoint) = checkpoints;
        if expected_checkpoint.topic_partition != topic_partition
            || next_checkpoint.topic_partition != topic_partition
        {
            return Err(TelemetryError::CorruptSinkJournal(
                "indexed append checkpoints refer to another partition".into(),
            ));
        }
        self.apply_indexed_ingest_pack_inner(
            tenant,
            topic_partition,
            first_offset,
            record_count,
            payload,
            transient_context,
            payload_already_validated,
            Some(next_checkpoint),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_indexed_ingest_pack_inner(
        &mut self,
        tenant: Arc<str>,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        record_count: u32,
        payload: Bytes,
        transient_context: Option<&[u8]>,
        payload_already_validated: bool,
        next_checkpoint: Option<DurableSinkCheckpoint>,
    ) -> TelemetryResult<()> {
        self.active_partition_cache.clear();
        if tenant.is_empty() || record_count == 0 {
            return Err(TelemetryError::InvalidConfig(
                "compressed ingest append must have a tenant and contain records",
            ));
        }
        let last_offset = batch_offset(
            topic_partition,
            first_offset,
            usize::try_from(record_count - 1)
                .map_err(|_| TelemetryError::OffsetExhausted(topic_partition))?,
        )?;
        if let Some(previous) = self.indexed_through(topic_partition)
            && first_offset <= previous
        {
            if last_offset <= previous {
                return Ok(());
            }
            let expected = previous
                .get()
                .checked_add(1)
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(topic_partition))?;
            return Err(TelemetryError::OffsetOutOfOrder {
                partition: topic_partition,
                expected,
                observed: first_offset,
            });
        }
        let mut frames = if payload_already_validated {
            decode_indexed_ingest_frames_after_validation(payload, transient_context, record_count)?
        } else {
            decode_indexed_ingest_frames(payload, transient_context, record_count)?
        };
        for frame in &mut frames {
            frame.frame_id = self.next_frame_id;
            self.next_frame_id = self
                .next_frame_id
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
        let partition = self
            .indexed_frame_partitions
            .entry(topic_partition)
            .or_default();
        partition.appends.push(IndexedFrameAppend {
            tenant,
            first_offset,
            last_offset,
            record_count,
            frames,
            next_checkpoint,
        });
        // Publication barrier: readers never observe a watermark before all
        // frame metadata and embedded index views are installed.
        partition.indexed_through = Some(last_offset);
        Ok(())
    }

    /// Decodes and publishes one OTLP `ExportLogsServiceRequest`.
    ///
    /// This convenience method is suited to replay and tests. A live OTLP
    /// receiver should instead decode before the shard-stream append, use the
    /// decoded event count for reservation, then call [`Self::apply_otlp_events`]
    /// after the durable append response.
    pub fn apply_otlp_export(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        payload: &[u8],
    ) -> TelemetryResult<Vec<IndexReceipt>> {
        self.apply_otlp_events(
            topic_partition,
            first_offset,
            OtlpLogDecoder.decode(payload)?,
        )
    }

    /// Seals every active block and returns their immutable descriptors.
    pub fn seal_active_blocks(&mut self) -> TelemetryResult<Vec<BlockDescriptor>> {
        let active_blocks = std::mem::take(&mut self.active_blocks);
        let mut sealed = Vec::new();
        for (key, active) in active_blocks {
            sealed.extend(self.rebalance_block(key, active, true)?);
        }
        Ok(sealed)
    }

    /// Publishes complete indexed append boundaries to immutable object storage.
    ///
    /// When `force` is false, only groups at the configured target size are
    /// published. A forced pass seals every remaining checkpointed append.
    pub(crate) fn offload_indexed_groups(&mut self, force: bool) -> TelemetryResult<usize> {
        let Some(state) = self.tier.as_ref() else {
            return Ok(0);
        };
        let partitions = state.tiers.keys().copied().collect::<Vec<_>>();
        let mut published = 0usize;
        for partition in partitions {
            loop {
                if !self.offload_one_indexed_group(partition, force)? {
                    break;
                }
                published = published.saturating_add(1);
            }
        }
        Ok(published)
    }

    pub(crate) fn reclaim_retired_object_generations(&mut self) -> TelemetryResult<()> {
        let Some(state) = self.tier.as_mut() else {
            return Ok(());
        };
        for tier in state.tiers.values_mut() {
            tier.reclaim_retired_objects()?;
        }
        Ok(())
    }

    pub(crate) fn retain_object_tier_since(
        &mut self,
        cutoff_timestamp_unix_nanos: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        let Some(state) = self.tier.as_mut() else {
            return Ok(TierRetentionReport::default());
        };
        let mut total = TierRetentionReport::default();
        for tier in state.tiers.values_mut() {
            let report = tier.retain_since_timestamp(cutoff_timestamp_unix_nanos)?;
            total.retired_groups = total.retired_groups.saturating_add(report.retired_groups);
            total.retired_payload_bytes = total
                .retired_payload_bytes
                .saturating_add(report.retired_payload_bytes);
            total.retired_objects = total.retired_objects.saturating_add(report.retired_objects);
        }
        Ok(total)
    }

    pub(crate) fn retain_object_tier_to_payload_bytes(
        &mut self,
        max_payload_bytes_per_partition: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        let Some(state) = self.tier.as_mut() else {
            return Ok(TierRetentionReport::default());
        };
        let mut total = TierRetentionReport::default();
        for tier in state.tiers.values_mut() {
            let report = tier.retain_to_payload_bytes(max_payload_bytes_per_partition)?;
            total.retired_groups = total.retired_groups.saturating_add(report.retired_groups);
            total.retired_payload_bytes = total
                .retired_payload_bytes
                .saturating_add(report.retired_payload_bytes);
            total.retired_objects = total.retired_objects.saturating_add(report.retired_objects);
        }
        Ok(total)
    }

    fn offload_one_indexed_group(
        &mut self,
        partition: TopicPartition,
        force: bool,
    ) -> TelemetryResult<bool> {
        let state = self
            .tier
            .as_ref()
            .ok_or(TelemetryError::InvalidConfig("object tier is not attached"))?;
        let config = state.config;
        let Some(resident) = self.indexed_frame_partitions.get(&partition) else {
            return Ok(false);
        };
        let mut selected_appends = 0usize;
        let mut selected_payload_bytes = 0u64;
        let mut selected_frames = 0usize;
        for append in &resident.appends {
            if append.next_checkpoint.is_none() {
                break;
            }
            let append_payload_bytes = append.frames.iter().try_fold(0u64, |total, frame| {
                total
                    .checked_add(
                        u64::try_from(frame.compressed.len())
                            .map_err(|_| TelemetryError::RecordTooLarge)?,
                    )
                    .ok_or(TelemetryError::RecordTooLarge)
            })?;
            let append_frames = append.frames.len();
            if append_payload_bytes > config.max_group_payload_bytes
                || append_frames > config.max_blocks_per_group
            {
                return Err(TelemetryError::ObjectStore(
                    "one durable append exceeds the object-tier group limit".into(),
                ));
            }
            if selected_appends > 0
                && (selected_payload_bytes.saturating_add(append_payload_bytes)
                    > config.max_group_payload_bytes
                    || selected_frames.saturating_add(append_frames) > config.max_blocks_per_group)
            {
                break;
            }
            selected_appends += 1;
            selected_payload_bytes = selected_payload_bytes
                .checked_add(append_payload_bytes)
                .ok_or(TelemetryError::RecordTooLarge)?;
            selected_frames = selected_frames
                .checked_add(append_frames)
                .ok_or(TelemetryError::RecordTooLarge)?;
            if selected_payload_bytes >= config.target_group_payload_bytes {
                break;
            }
        }
        if selected_appends == 0
            || (!force && selected_payload_bytes < config.target_group_payload_bytes)
        {
            return Ok(false);
        }

        let sources = resident.appends[..selected_appends]
            .iter()
            .map(|append| TierIngestAppendSource {
                tenant: append.tenant.to_string(),
                first_offset: append.first_offset,
                last_offset: append.last_offset,
                record_count: append.record_count,
                frames: append
                    .frames
                    .iter()
                    .map(|frame| TierIngestFrameSource {
                        frame_id: frame.frame_id,
                        cohort: frame.cohort,
                        record_count: frame.record_count,
                        structural_bytes: frame.structural_bytes,
                        min_timestamp_unix_nanos: frame.min_timestamp_unix_nanos,
                        max_timestamp_unix_nanos: frame.max_timestamp_unix_nanos,
                        compressed: frame.compressed.clone(),
                        index: frame.index.clone(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let checkpoint = resident.appends[selected_appends - 1]
            .next_checkpoint
            .expect("selected checkpointed append has a next checkpoint");
        let state = self
            .tier
            .as_mut()
            .expect("object tier was checked before group selection");
        let tier = state
            .tiers
            .get_mut(&partition)
            .expect("selected partition has an object tier");
        let group_sequence = tier
            .root()
            .pages
            .last()
            .map_or(0, |page| page.last_group_sequence.saturating_add(1));
        let group_directory = state.spool_directory.join(format!(
            "topic-{}-partition-{}/group-{group_sequence:020}",
            partition.topic_id.get(),
            partition.partition_id.get()
        ));
        let payload_path = group_directory.join("payload.pack");
        let query_index_path = group_directory.join("query-index.sltqix");
        let blocks = write_tier_ingest_group(&sources, &payload_path, &query_index_path)?;
        let manifest = tier.publish_group(TierGroupSource {
            group_sequence,
            checkpoint: TierCheckpoint {
                next_placement_sequence: checkpoint.next_placement_sequence.get(),
                next_offset: checkpoint.next_offset.get(),
            },
            blocks,
            artifacts: vec![
                TierArtifactSource {
                    kind: TierArtifactKind::PayloadPack,
                    name: "payload.pack".into(),
                    path: payload_path.clone(),
                },
                TierArtifactSource {
                    kind: TierArtifactKind::QueryIndex,
                    name: "query-index.sltqix".into(),
                    path: query_index_path.clone(),
                },
            ],
        })?;
        if state.warm_local_cache_on_publish {
            let entry = tier
                .latest_group_cached(&state.control_cache)?
                .ok_or_else(|| {
                    TelemetryError::CorruptTier(
                        "published log group is missing from its catalog".into(),
                    )
                })?;
            let _ = tier.load_group_cached(&entry, &state.control_cache)?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("log group has no payload".into()))?;
            state
                .payload_cache
                .admit_file(payload_artifact, &payload_path)?;
            let query_index_artifact =
                manifest
                    .artifact(TierArtifactKind::QueryIndex)
                    .ok_or_else(|| {
                        TelemetryError::CorruptTier("log group has no query index".into())
                    })?;
            state
                .control_cache
                .admit_file(query_index_artifact, &query_index_path)?;
        }
        self.indexed_frame_partitions
            .get_mut(&partition)
            .expect("resident partition remains present")
            .appends
            .drain(..selected_appends);
        remove_published_spool_file(&payload_path);
        remove_published_spool_file(&query_index_path);
        let _ = fs::remove_dir(&group_directory);
        if let Some(parent) = group_directory.parent() {
            let _ = fs::remove_dir(parent);
        }
        Ok(true)
    }

    /// Marks a sealed block as durably written to the object tier.
    pub fn mark_block_offloaded(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
    ) -> TelemetryResult<()> {
        self.catalog.mark_offloaded(block_id, object_key)
    }

    /// Marks a sealed block as a byte range inside a durable object-tier pack.
    pub fn mark_block_offloaded_range(
        &mut self,
        block_id: BlockId,
        object_key: impl Into<Arc<str>>,
        object_offset: u64,
    ) -> TelemetryResult<()> {
        self.catalog
            .mark_offloaded_range(block_id, object_key, object_offset)
    }

    /// Performs one exact Boolean lookup over normalized log records.
    ///
    /// This hot-index implementation is intentionally partition-local. A
    /// coordinator may fan out across selected time or tenant partitions, but
    /// that expensive choice is never implicit on a stripe.
    #[must_use]
    pub fn query(&self, query: &LogQuery) -> Vec<LogMatch> {
        self.query_checked(query).unwrap_or_default()
    }

    pub(crate) fn query_checked(&self, query: &LogQuery) -> TelemetryResult<Vec<LogMatch>> {
        self.query_checked_with_typed_metadata(query, true, true)
    }

    fn query_checked_with_typed_metadata(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        let mut matches = self.query_hot_matches(query, include_typed_metadata, include_fields);
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            matches.extend(self.query_indexed_frames(
                query,
                partition,
                include_typed_metadata,
                include_fields,
            )?);
        }
        matches.extend(self.query_tiered_groups(query, include_typed_metadata, include_fields)?);
        let has_indexed_frames = self
            .indexed_frame_partitions
            .get(&query.topic_partition)
            .is_some_and(|partition| !partition.appends.is_empty());
        if !has_indexed_frames && self.tier.is_none() {
            if let Some(limit) = query.limit {
                matches.truncate(limit);
            }
            return Ok(matches);
        }
        matches.sort_unstable_by(|left, right| query.compare(&left.record, &right.record));
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }
        Ok(matches)
    }

    pub(crate) fn query_partitions_checked(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partitions_checked_projected(queries, true)
    }

    pub(crate) fn query_partitions_checked_projected(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partitions_checked_projected_with_fields(queries, include_typed_metadata, true)
    }

    pub(crate) fn query_partitions_checked_projected_with_fields(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let Some(ordering_query) = queries.first() else {
            return Ok(Vec::new());
        };
        if self.tier.is_some() {
            let mut matches = queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )?);
                Ok::<_, TelemetryError>(matches)
            })?;
            matches.sort_unstable_by(|left, right| {
                ordering_query.compare(&left.record, &right.record)
            });
            if let Some(limit) = ordering_query.limit {
                matches.truncate(limit);
            }
            return Ok(matches);
        }
        let Some(limit) = ordering_query.limit else {
            return queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )?);
                Ok(matches)
            });
        };
        if ordering_query.sort != crate::QuerySort::Timestamp
            || !queries
                .iter()
                .all(|query| same_query_across_partition(ordering_query, query))
        {
            return queries.iter().try_fold(Vec::new(), |mut matches, query| {
                matches.extend(self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )?);
                Ok(matches)
            });
        }

        let mut matches: Vec<LogMatch> = Vec::new();
        let mut frames = Vec::new();
        for query in queries {
            if query.limit == Some(0) || query.has_invalid_range() {
                continue;
            }
            matches.extend(self.query_hot_matches(query, include_typed_metadata, include_fields));
            let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
                continue;
            };
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                frames.extend(
                    append
                        .frames
                        .iter()
                        .filter(|frame| frame_matches_query_bounds(query, frame))
                        .map(|frame| IndexedFrameQuery {
                            query,
                            append,
                            frame,
                        }),
                );
            }
        }
        match ordering_query.order {
            QueryOrder::NewestFirst => frames.sort_unstable_by(|left, right| {
                right
                    .frame
                    .max_timestamp_unix_nanos
                    .cmp(&left.frame.max_timestamp_unix_nanos)
            }),
            QueryOrder::OldestFirst => frames.sort_unstable_by(|left, right| {
                left.frame
                    .min_timestamp_unix_nanos
                    .cmp(&right.frame.min_timestamp_unix_nanos)
            }),
        }
        sort_and_limit_matches(&mut matches, ordering_query, limit);
        for pending in frames {
            if matches.len() == limit {
                let boundary = matches
                    .last()
                    .expect("a full result page has a boundary")
                    .record
                    .timestamp_unix_nanos;
                let cannot_improve = match ordering_query.order {
                    QueryOrder::NewestFirst => pending.frame.max_timestamp_unix_nanos < boundary,
                    QueryOrder::OldestFirst => pending.frame.min_timestamp_unix_nanos > boundary,
                };
                if cannot_improve {
                    break;
                }
            }
            matches.extend(self.query_indexed_frame(
                pending.query,
                pending.append,
                pending.frame,
                include_typed_metadata,
                include_fields,
            )?);
            sort_and_limit_matches(&mut matches, ordering_query, limit);
        }
        Ok(matches)
    }

    pub(crate) fn query_partition_refs_checked_projected_each_with_fields(
        &self,
        queries: &[&LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<Vec<LogMatch>>> {
        queries
            .iter()
            .map(|query| {
                self.query_checked_with_typed_metadata(
                    query,
                    include_typed_metadata,
                    include_fields,
                )
            })
            .collect()
    }

    pub(crate) fn query_partitions_checked_messages_top_k(
        &self,
        queries: &[LogQuery],
        scorer: &crate::analytics::RelevanceScorer,
        limit: usize,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut top = BinaryHeap::with_capacity(limit);
        for query in queries {
            self.for_each_checked_message_batch(query, &mut |matches| {
                for_each_message_match_score(&matches, scorer, &mut |matched, score| {
                    let item = MessageRelevanceTopKItem {
                        score,
                        timestamp_unix_nanos: matched.timestamp_unix_nanos,
                        offset: matched.record_ref.offset.get(),
                        matched: matched.clone(),
                    };
                    let should_keep =
                        top.len() < limit || top.peek().is_some_and(|Reverse(worst)| item > *worst);
                    if should_keep {
                        if top.len() == limit {
                            top.pop();
                        }
                        top.push(Reverse(item));
                    }
                    Ok(())
                })?;
                Ok(())
            })?;
        }
        Ok(top.into_iter().map(|Reverse(item)| item.matched).collect())
    }

    pub(crate) fn query_partitions_checked_trace_ids(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<Vec<TraceId>> {
        queries.iter().try_fold(Vec::new(), |mut trace_ids, query| {
            trace_ids.extend(self.query_checked_trace_ids(query)?);
            Ok(trace_ids)
        })
    }

    fn for_each_checked_message_batch(
        &self,
        query: &LogQuery,
        emit: &mut dyn FnMut(Vec<LogMessageMatch>) -> TelemetryResult<()>,
    ) -> TelemetryResult<()> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(());
        }
        let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
        let hot_matches = self
            .query_hot_matches(query, false, true)
            .into_iter()
            .map(|matched| LogMessageMatch {
                record_ref: matched.record.record_ref,
                timestamp_unix_nanos: matched.record.timestamp_unix_nanos,
                message: Some(matched.record.message),
                relevance: None,
            })
            .collect::<Vec<_>>();
        emit(hot_matches)?;
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        let frame_matches = self.query_indexed_frame_messages(
                            query,
                            append,
                            frame,
                            message_predicate_key.as_ref(),
                        )?;
                        emit(frame_matches)?;
                    }
                }
            }
        }
        if self.tier.is_some() {
            emit(self.query_tiered_groups_messages(query, message_predicate_key.as_ref())?)?;
        }
        Ok(())
    }

    fn query_checked_trace_ids(&self, query: &LogQuery) -> TelemetryResult<Vec<TraceId>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
        let mut trace_ids = self
            .query_hot_matches(query, true, true)
            .into_iter()
            .filter_map(|matched| matched.record.trace_id)
            .collect::<Vec<_>>();
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        trace_ids.extend(self.query_indexed_frame_trace_ids(
                            query,
                            append,
                            frame,
                            message_predicate_key.as_ref(),
                        )?);
                    }
                }
            }
        }
        if self.tier.is_some() {
            trace_ids
                .extend(self.query_tiered_groups_trace_ids(query, message_predicate_key.as_ref())?);
        }
        Ok(trace_ids)
    }

    fn query_indexed_frame_messages(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let message_cache_can_supply_the_base =
            message_cache_can_supply_frame_base(query, append.tenant.as_ref());
        let mut used_message_cache_base = false;
        let mut candidates: Vec<u32>;
        let mut cached_base_candidates: Option<Arc<[u32]>> = None;
        if message_cache_can_supply_the_base && exact_tokens.is_none() {
            if let Some(message_candidates) = message_predicate_key.and_then(|_| {
                self.cached_message_predicate_candidates_arc_if_present(
                    frame.frame_id,
                    message_predicate_key,
                )
            }) {
                used_message_cache_base = true;
                cached_base_candidates = Some(message_candidates);
                candidates = Vec::new();
            } else {
                candidates = indexed_frame_candidates_for_append(
                    query,
                    &frame.index,
                    frame.record_count,
                    append.tenant.as_ref(),
                );
            }
        } else if let Some(tokens) = exact_tokens
            .as_deref()
            .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
        {
            candidates = self
                .exact_indexed_frame_candidates(query, append, frame, tokens, &exact_fields)?
                .unwrap_or_else(|| {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                });
        } else if message_cache_can_supply_the_base {
            candidates = (0..frame.record_count).collect();
        } else {
            candidates = indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            );
        }
        if !used_message_cache_base {
            candidates =
                self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
            if !matches!(query.predicate, LogPredicate::MatchAll) {
                let Some(message_candidates) = self
                    .cached_message_predicate_candidates_for_relevance(
                        query,
                        frame,
                        message_predicate_key,
                    )?
                else {
                    return Ok(Vec::new());
                };
                let mut selected = Some(candidates);
                intersect_frame_candidate_slice(&mut selected, &message_candidates);
                candidates = selected.unwrap_or_default();
            }
        }
        let candidates = cached_base_candidates.as_deref().unwrap_or(&candidates);
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let relevance_stats = self.cached_message_token_stats(&cached, frame.record_count)?;
        let mut matches = Vec::with_capacity(candidates.len());
        let needs_message_filter =
            used_message_cache_base && !cached_message_predicate_is_exact(&query.predicate);
        for ordinal in candidates.iter().copied() {
            if needs_message_filter {
                let message = relevance_stats.messages.get(ordinal as usize).ok_or(
                    TelemetryError::InvalidBlockEncoding(
                        "message candidate ordinal is out of range",
                    ),
                )?;
                if !query.message_candidate_matches(message).unwrap_or(false) {
                    continue;
                }
            }
            let index = usize::try_from(ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("record ordinal overflow"))?;
            let timestamp_unix_nanos =
                *cached
                    .timestamps
                    .get(index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate timestamp missing",
                    ))?;
            if !query.timestamp_matches(timestamp_unix_nanos) {
                continue;
            }
            let relative_offset =
                cached
                    .offsets
                    .get(index)
                    .copied()
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate offset missing",
                    ))?;
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(relative_offset.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            matches.push(LogMessageMatch {
                record_ref: TelemetryRecordRef::new(query.topic_partition, absolute_offset),
                timestamp_unix_nanos,
                message: None,
                relevance: Some(IndexedMessageRelevance {
                    stats: Arc::clone(&relevance_stats),
                    ordinal,
                }),
            });
        }
        Ok(matches)
    }

    fn query_indexed_frame_trace_ids(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<TraceId>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let candidates = if let Some(tokens) = exact_tokens
            .as_deref()
            .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
        {
            self.exact_indexed_frame_candidates(query, append, frame, tokens, &exact_fields)?
                .unwrap_or_else(|| {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                })
        } else {
            indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            )
        };
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.decode_indexed_frame_trace_ids(
            query,
            append,
            frame,
            &candidates,
            message_predicate_key,
        )
    }

    fn decode_indexed_frame_trace_ids(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        candidates: &[u32],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<TraceId>> {
        let mut candidates = self
            .indexed_frame_field_predicate_candidates(query, frame, candidates)?
            .unwrap_or_else(|| candidates.to_vec());
        if query.exact_message_token_conjunction().is_none()
            && !matches!(query.predicate, LogPredicate::MatchAll)
        {
            let Some(message_candidates) = self.cached_message_predicate_candidates_with_key(
                query,
                frame,
                message_predicate_key,
            )?
            else {
                let matches = self
                    .query_indexed_frame(query, append, frame, true, true)?
                    .into_iter()
                    .filter_map(|matched| matched.record.trace_id)
                    .collect::<Vec<_>>();
                return Ok(matches);
            };
            let mut selected = Some(candidates);
            intersect_frame_candidate_slice(&mut selected, &message_candidates);
            candidates = selected.unwrap_or_default();
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let mut verified = Vec::with_capacity(candidates.len());
        for ordinal in candidates {
            let index = usize::try_from(ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("trace ID ordinal overflow"))?;
            let timestamp =
                *cached
                    .timestamps
                    .get(index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "trace ID candidate timestamp missing",
                    ))?;
            if !query.timestamp_matches(timestamp) {
                continue;
            }
            let offset = append
                .first_offset
                .get()
                .checked_add(
                    cached
                        .offsets
                        .get(index)
                        .ok_or(TelemetryError::InvalidBlockEncoding(
                            "trace ID candidate offset missing",
                        ))?
                        .get(),
                )
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            let offset = LogicalOffset::new(offset);
            if query.start_offset.is_some_and(|start| offset < start)
                || query.end_offset.is_some_and(|end| offset >= end)
                || query.after.is_some_and(|cursor| match query.order {
                    QueryOrder::OldestFirst => offset <= cursor.offset,
                    QueryOrder::NewestFirst => offset >= cursor.offset,
                })
            {
                continue;
            }
            verified.push(ordinal);
        }
        if verified.is_empty() {
            return Ok(Vec::new());
        }
        if !trace_predicate_candidates_are_exact(&query.predicate) || !query.terms.is_empty() {
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &verified,
                &cached.embedded_index,
                &cached.templates,
            )?;
            verified = verified
                .into_iter()
                .zip(messages)
                .filter_map(|(ordinal, message)| {
                    query
                        .message_candidate_matches(&message)
                        .unwrap_or(true)
                        .then_some(ordinal)
                })
                .collect();
            if verified.is_empty() {
                return Ok(Vec::new());
            }
        }
        let trace_ids = self.cached_trace_ids(&cached, frame.record_count)?;
        Ok(verified
            .into_iter()
            .filter_map(|ordinal| {
                usize::try_from(ordinal)
                    .ok()
                    .and_then(|index| trace_ids.get(index).copied().flatten())
            })
            .collect())
    }

    fn decode_indexed_frame_messages(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        candidates: &[u32],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        let message_cache_can_supply_the_base =
            message_cache_can_supply_frame_base(query, append.tenant.as_ref());
        let cached_base_candidates = (message_cache_can_supply_the_base
            && query.exact_message_token_conjunction().is_none())
        .then(|| {
            message_predicate_key.and_then(|_| {
                self.cached_message_predicate_candidates_arc_if_present(
                    frame.frame_id,
                    message_predicate_key,
                )
            })
        })
        .flatten();
        let used_message_cache_base = cached_base_candidates.is_some();
        let mut owned_candidates = cached_base_candidates
            .is_none()
            .then(|| candidates.to_vec());
        if !used_message_cache_base {
            let candidates = owned_candidates
                .as_mut()
                .expect("non-cached message candidates are owned");
            if let Some(filtered) =
                self.indexed_frame_field_predicate_candidates(query, frame, candidates.as_slice())?
            {
                *candidates = filtered;
            }
            if !matches!(query.predicate, LogPredicate::MatchAll) {
                let Some(message_candidates) = self.cached_message_predicate_candidates_with_key(
                    query,
                    frame,
                    message_predicate_key,
                )?
                else {
                    return Ok(Vec::new());
                };
                let mut selected = Some(std::mem::take(candidates));
                intersect_frame_candidate_slice(&mut selected, &message_candidates);
                *candidates = selected.unwrap_or_default();
            }
        }
        let candidates = cached_base_candidates
            .as_deref()
            .or(owned_candidates.as_deref())
            .unwrap_or(&[]);
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let relevance_stats = self.cached_message_token_stats(&cached, frame.record_count)?;
        let mut matches = Vec::with_capacity(candidates.len());
        let needs_message_filter =
            used_message_cache_base && !cached_message_predicate_is_exact(&query.predicate);
        for ordinal in candidates.iter().copied() {
            if needs_message_filter {
                let message = relevance_stats.messages.get(ordinal as usize).ok_or(
                    TelemetryError::InvalidBlockEncoding(
                        "message candidate ordinal is out of range",
                    ),
                )?;
                if !query.message_candidate_matches(message).unwrap_or(false) {
                    continue;
                }
            }
            let index = usize::try_from(ordinal)
                .map_err(|_| TelemetryError::InvalidBlockEncoding("message ordinal overflow"))?;
            let timestamp_unix_nanos =
                *cached
                    .timestamps
                    .get(index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate timestamp missing",
                    ))?;
            if !query.timestamp_matches(timestamp_unix_nanos) {
                continue;
            }
            let relative_offset =
                cached
                    .offsets
                    .get(index)
                    .copied()
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "message candidate offset missing",
                    ))?;
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(relative_offset.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            matches.push(LogMessageMatch {
                record_ref: TelemetryRecordRef::new(query.topic_partition, absolute_offset),
                timestamp_unix_nanos,
                message: None,
                relevance: Some(IndexedMessageRelevance {
                    stats: Arc::clone(&relevance_stats),
                    ordinal,
                }),
            });
        }
        Ok(matches)
    }

    /// Counts matching records without constructing `LogMatch` values.
    ///
    /// The durable-frame path still verifies candidate ordinals against the
    /// decoded structural records, so embedded-index collisions cannot change
    /// the result. It avoids building the larger `DurableLog` representation
    /// and is used by cardinality-only analytics scans.
    pub(crate) fn count_query_partitions_checked(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<u64> {
        queries.iter().try_fold(0_u64, |total, query| {
            let count = self.count_query_checked(query)?;
            total
                .checked_add(count)
                .ok_or(TelemetryError::RecordTooLarge)
        })
    }

    pub(crate) fn group_query_partitions_checked(
        &self,
        queries: &[LogQuery],
        keys: &[AnalyticsGroupKey],
    ) -> TelemetryResult<BTreeMap<Vec<Option<Arc<str>>>, u64>> {
        let mut groups = BTreeMap::new();
        for query in queries {
            if query.limit == Some(0) || query.has_invalid_range() {
                continue;
            }
            let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
            if self.tier.is_some() {
                self.group_tiered_query(query, keys, &mut groups, message_predicate_key.as_ref())?;
                continue;
            }
            if let Some(partition) = self.partitions.get(&query.topic_partition) {
                for ordinal in self.query_ordinals(query, partition) {
                    if let Some(record) = partition.records.get(ordinal as usize) {
                        let key = keys
                            .iter()
                            .map(|group| {
                                crate::analytics::durable_group_value(&record.record, *group)
                            })
                            .collect::<Vec<_>>();
                        *groups.entry(key).or_default() += 1;
                    }
                }
            }
            let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
                continue;
            };
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if !frame_matches_query_bounds(query, frame) {
                        continue;
                    }
                    let candidates = self
                        .cached_message_predicate_candidates_if_present(
                            frame.frame_id,
                            &query.predicate,
                        )
                        .map(|candidates| candidates.to_vec())
                        .unwrap_or_else(|| {
                            indexed_frame_candidates_for_append_with_phrase_mode(
                                query,
                                &frame.index,
                                frame.record_count,
                                append.tenant.as_ref(),
                                true,
                            )
                        });
                    self.group_indexed_frame_candidates(
                        query,
                        append,
                        frame,
                        candidates,
                        keys,
                        message_predicate_key.as_ref(),
                        &mut groups,
                    )?;
                }
            }
        }
        Ok(groups)
    }

    fn group_tiered_query(
        &self,
        query: &LogQuery,
        keys: &[AnalyticsGroupKey],
        groups: &mut BTreeMap<Vec<Option<Arc<str>>>, u64>,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<()> {
        let Some(state) = &self.tier else {
            return Ok(());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(());
        };
        let mut predicate_query = query.clone();
        predicate_query
            .exact_fields
            .retain(|field| field.key.as_ref() != "resource.loki.tenant");
        let tier_groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        for group in tier_groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds)
                    || query.exact_fields.iter().any(|field| {
                        field.key.as_ref() == "resource.loki.tenant"
                            && field.value.as_ref() != bounds.tenant.as_ref()
                    })
                {
                    continue;
                }
                for cold_frame in &append.frames {
                    if !timestamp_bounds_overlap(
                        query,
                        cold_frame.min_timestamp_unix_nanos,
                        cold_frame.max_timestamp_unix_nanos,
                    ) {
                        continue;
                    }
                    let candidates = self
                        .cached_message_predicate_candidates_if_present(
                            cold_frame.frame_id,
                            &predicate_query.predicate,
                        )
                        .map(|candidates| candidates.to_vec())
                        .unwrap_or_else(|| {
                            indexed_frame_candidates_for_append_with_phrase_mode(
                                &predicate_query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                bounds.tenant.as_ref(),
                                true,
                            )
                        });
                    if candidates.is_empty() {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame.clone(),
                        candidates,
                        range_index,
                    ));
                }
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                candidates,
                range_index,
            ) in selected
            {
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index,
                };
                let append = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                self.group_indexed_frame_candidates(
                    &predicate_query,
                    &append,
                    &frame,
                    candidates,
                    keys,
                    message_predicate_key,
                    groups,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn group_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        mut candidates: Vec<u32>,
        keys: &[AnalyticsGroupKey],
        message_predicate_key: Option<&Arc<str>>,
        groups: &mut BTreeMap<Vec<Option<Arc<str>>>, u64>,
    ) -> TelemetryResult<()> {
        candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        let cached_message_candidates =
            self.cached_message_predicate_candidates_with_key(query, frame, message_predicate_key)?;
        if let Some(message_candidates) = cached_message_candidates.as_ref() {
            let mut current = Some(candidates);
            intersect_frame_candidate_slice(&mut current, message_candidates);
            candidates = current.unwrap_or_default();
        }
        if candidates.is_empty() {
            return Ok(());
        }
        let cached = self.cached_indexed_frame(frame)?;
        retain_cached_timestamp_candidates(query, &cached, &mut candidates);
        if candidates.is_empty() {
            return Ok(());
        }
        if !cached_message_predicate_is_exact(&query.predicate) {
            for decoded in decode_structural_records_with_cached_frame_data(
                &cached.structural,
                &candidates,
                &cached.embedded_index,
                &cached.templates,
                &cached.offsets,
                &cached.timestamps,
                true,
                true,
                None,
                None,
                Some(&cached.attribute_tables),
            )? {
                let absolute_offset = append
                    .first_offset
                    .get()
                    .checked_add(decoded.offset.get())
                    .map(LogicalOffset::new)
                    .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
                let view = AbsoluteDecodedRecordView {
                    record: &decoded,
                    absolute_offset,
                };
                if query.matches(&view) {
                    let key = keys
                        .iter()
                        .map(|group| crate::analytics::decoded_group_value(&decoded, *group))
                        .collect::<Vec<_>>();
                    *groups.entry(key).or_default() += 1;
                }
            }
            return Ok(());
        }
        let mut field_postings = Vec::with_capacity(keys.len());
        let mut needs_decoded_fields = false;
        for group in keys {
            let posting = match group {
                AnalyticsGroupKey::SeverityText => self.cached_field_postings(
                    &cached,
                    frame.record_count,
                    "attr.loki.metadata.severity_text",
                )?,
                AnalyticsGroupKey::ScopeName => self.cached_field_postings(
                    &cached,
                    frame.record_count,
                    "attr.loki.metadata.scope_name",
                )?,
                AnalyticsGroupKey::Minute => None,
            };
            needs_decoded_fields |=
                posting.is_none() && !matches!(group, AnalyticsGroupKey::Minute);
            field_postings.push(posting);
        }
        let decoded_fields = needs_decoded_fields
            .then(|| decode_structural_fields(&cached.structural, &candidates))
            .transpose()?;
        let compact_grouping = !keys.is_empty()
            && keys.len() <= 2
            && field_postings.iter().enumerate().all(|(index, posting)| {
                posting.is_some() || matches!(keys[index], AnalyticsGroupKey::Minute)
            });
        if compact_grouping {
            if keys.len() == 1 {
                let mut compact_groups = HashMap::<IndexedGroupValue, u64>::new();
                for ordinal in &candidates {
                    let value =
                        indexed_group_value(keys[0], *ordinal, &cached, field_postings[0].as_ref());
                    *compact_groups.entry(value).or_default() += 1;
                }
                for (value, count) in compact_groups {
                    let key = vec![materialize_indexed_group_value(
                        value,
                        field_postings[0].as_ref(),
                    )];
                    *groups.entry(key).or_default() += count;
                }
            } else if keys.len() == 2 {
                let mut compact_groups =
                    HashMap::<(IndexedGroupValue, IndexedGroupValue), u64>::new();
                for ordinal in &candidates {
                    let first =
                        indexed_group_value(keys[0], *ordinal, &cached, field_postings[0].as_ref());
                    let second =
                        indexed_group_value(keys[1], *ordinal, &cached, field_postings[1].as_ref());
                    *compact_groups.entry((first, second)).or_default() += 1;
                }
                for ((first, second), count) in compact_groups {
                    let key = vec![
                        materialize_indexed_group_value(first, field_postings[0].as_ref()),
                        materialize_indexed_group_value(second, field_postings[1].as_ref()),
                    ];
                    *groups.entry(key).or_default() += count;
                }
            }
            return Ok(());
        }
        for (position, ordinal) in candidates.iter().enumerate() {
            let key = keys
                .iter()
                .enumerate()
                .map(|(key_index, group)| match group {
                    AnalyticsGroupKey::Minute => usize::try_from(*ordinal)
                        .ok()
                        .and_then(|index| cached.timestamps.get(index))
                        .map(|timestamp| {
                            Arc::<str>::from((timestamp / 60_000_000_000).to_string())
                        }),
                    AnalyticsGroupKey::SeverityText | AnalyticsGroupKey::ScopeName => {
                        field_postings[key_index]
                            .as_ref()
                            .and_then(|postings| {
                                postings
                                    .ordinal_value_ids
                                    .get(*ordinal as usize)
                                    .and_then(|id| postings.value_table.get(*id as usize))
                                    .cloned()
                            })
                            .or_else(|| {
                                decoded_fields.as_ref().and_then(|fields| {
                                    fields[position].iter().find_map(|field| {
                                        let wanted = match group {
                                            AnalyticsGroupKey::SeverityText => {
                                                "attr.loki.metadata.severity_text"
                                            }
                                            AnalyticsGroupKey::ScopeName => {
                                                "attr.loki.metadata.scope_name"
                                            }
                                            AnalyticsGroupKey::Minute => unreachable!(),
                                        };
                                        (field.key.as_ref() == wanted)
                                            .then(|| Arc::clone(&field.value))
                                    })
                                })
                            })
                    }
                })
                .collect::<Vec<_>>();
            *groups.entry(key).or_default() += 1;
        }
        Ok(())
    }

    fn count_query_checked(&self, query: &LogQuery) -> TelemetryResult<u64> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(0);
        }
        let message_predicate_key = Self::cached_message_predicate_key(&query.predicate);
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let mut count = match self.partitions.get(&query.topic_partition) {
            Some(partition) => match self.count_hot_query_matches(query, partition) {
                Some(count) => count,
                None => u64::try_from(self.query_ordinals(query, partition).len())
                    .map_err(|_| TelemetryError::RecordTooLarge)?,
            },
            None => 0,
        };
        if let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) {
            for append in &partition.appends {
                if !append_matches_query_bounds(query, append) {
                    continue;
                }
                for frame in &append.frames {
                    if frame_matches_query_bounds(query, frame) {
                        let frame_count = self.count_indexed_frame_matches(
                            query,
                            append,
                            frame,
                            exact_tokens.as_deref(),
                            &exact_fields,
                            message_predicate_key.as_ref(),
                        )?;
                        count = count
                            .checked_add(frame_count)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                    }
                }
            }
        }
        count = count
            .checked_add(self.count_tiered_groups(
                query,
                exact_tokens.as_deref(),
                &exact_fields,
                message_predicate_key.as_ref(),
            )?)
            .ok_or(TelemetryError::RecordTooLarge)?;
        if let Some(limit) = query.limit {
            count = count.min(u64::try_from(limit).unwrap_or(u64::MAX));
        }
        Ok(count)
    }

    /// Counts hot records from an index-backed candidate source without
    /// allocating the ordinal vector that the materializing query path needs.
    /// Returning `None` keeps the general path for predicates whose only safe
    /// candidate source requires residual decoding.
    fn count_hot_query_matches(&self, query: &LogQuery, partition: &PartitionIndex) -> Option<u64> {
        let mut record_range =
            ordinal_record_window(&partition.records, query.start_offset, query.end_offset);
        if query.sort == crate::QuerySort::Offset
            && let Some(cursor) = query.after
        {
            match query.order {
                QueryOrder::OldestFirst => {
                    let first_after = partition
                        .records
                        .partition_point(|record| record.record.record_ref.offset <= cursor.offset);
                    record_range.start = record_range.start.max(first_after);
                }
                QueryOrder::NewestFirst => {
                    let first_at_or_after = partition
                        .records
                        .partition_point(|record| record.record.record_ref.offset < cursor.offset);
                    record_range.end = record_range.end.min(first_at_or_after);
                }
            }
        }
        if record_range.start >= record_range.end {
            return Some(0);
        }

        let posting_start = u32::try_from(record_range.start).ok()?;
        let posting_end = u32::try_from(record_range.end).ok()?;
        let mut direct_postings =
            Vec::with_capacity(query.terms.len().saturating_add(query.exact_fields.len()));
        for term in &query.terms {
            let Some(term_id) = partition.term_ids.get(normalize_term(term).as_ref()) else {
                return Some(0);
            };
            let Some(posting) = partition.term_postings.get(*term_id) else {
                return Some(0);
            };
            direct_postings.push(posting);
        }
        for field in &query.exact_fields {
            let Some(field_id) = partition
                .field_ids
                .get(field.key.as_ref())
                .and_then(|values| values.get(field.value.as_ref()))
            else {
                return Some(0);
            };
            let Some(posting) = partition.field_postings.get(*field_id) else {
                return Some(0);
            };
            direct_postings.push(posting);
        }
        if direct_postings
            .iter()
            .any(|posting| posting.is_empty_in(posting_start, posting_end))
        {
            return Some(0);
        }

        let predicate_driver =
            hot_predicate_driver_posting(&query.predicate, partition, posting_start, posting_end);
        let predicate_union = hot_predicate_postings(&query.predicate, partition);
        if predicate_driver.is_none()
            && direct_postings.is_empty()
            && predicate_union.is_none()
            && !matches!(query.predicate, LogPredicate::MatchAll)
        {
            return None;
        }
        if predicate_driver.is_none()
            && direct_postings.is_empty()
            && predicate_union.as_ref().is_some_and(Vec::is_empty)
        {
            return Some(0);
        }

        let mut source = predicate_driver;
        let mut source_covers_predicate = predicate_union.is_some() && source.is_some();
        if let Some(candidate) = direct_postings
            .iter()
            .copied()
            .min_by_key(|posting| posting.cardinality_in(posting_start, posting_end))
            && source.is_none_or(|current| {
                candidate.cardinality_in(posting_start, posting_end)
                    < current.cardinality_in(posting_start, posting_end)
            })
        {
            source = Some(candidate);
            source_covers_predicate = matches!(query.predicate, LogPredicate::MatchAll);
        }
        let use_predicate_union = source.is_none() && predicate_union.is_some();
        let source_covers_predicate = source_covers_predicate || use_predicate_union;

        let predicate_is_exact = hot_predicate_candidates_are_exact(&query.predicate);
        let needs_bounds_check = query.start_timestamp_unix_nanos.is_some()
            || query.end_timestamp_unix_nanos.is_some()
            || (query.after.is_some() && query.sort == crate::QuerySort::Timestamp);
        let mut count = 0_u64;
        let mut accept = |ordinal: u32| {
            let Some(record) = partition.records.get(ordinal as usize) else {
                return true;
            };
            if !direct_postings
                .iter()
                .all(|posting| posting.contains(ordinal))
            {
                return true;
            }
            let matches = if predicate_is_exact {
                (source_covers_predicate
                    || hot_predicate_matches_ordinal(&query.predicate, partition, ordinal))
                    && (!needs_bounds_check || query.matches_index_bounds(&record.record))
            } else {
                query.matches(&record.record)
            };
            if !matches {
                return true;
            }
            count = count.saturating_add(1);
            true
        };

        if let Some(source) = source {
            source.visit_in(
                posting_start,
                posting_end,
                QueryOrder::OldestFirst,
                &mut accept,
            );
        } else if let Some(postings) = predicate_union.as_ref() {
            visit_hot_posting_union(postings, posting_start, posting_end, &mut accept);
        } else {
            for ordinal in posting_start..posting_end {
                if !accept(ordinal) {
                    break;
                }
            }
        }
        Some(count)
    }

    fn count_indexed_frame_matches(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        exact_tokens: Option<&[(&str, CaseSensitivity)]>,
        exact_fields: &[(Arc<str>, Arc<str>)],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<u64> {
        if let Some(tokens) =
            exact_tokens.filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
            && !query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            })
            && let Some(candidates) =
                self.cached_exact_frame_candidates(frame.frame_id, tokens, exact_fields)
            && let Some(count) =
                self.count_cached_exact_candidates(query, frame.frame_id, &candidates)
        {
            return Ok(count);
        }
        if let Some(tokens) = exact_tokens.filter(|tokens| !tokens.is_empty()) {
            let cached = self.cached_indexed_frame(frame)?;
            let postings = self.cached_exact_message_terms(&cached, tokens)?;
            for ((token, case_sensitivity), posting) in tokens.iter().zip(&postings) {
                if let Some(posting) = posting {
                    self.cache_exact_posting(
                        exact_message_posting_key(frame.frame_id, token, *case_sensitivity),
                        Arc::clone(posting),
                    );
                }
            }
            if query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            }) {
                return Ok(0);
            }
            let field_postings = if exact_fields.is_empty() {
                Vec::new()
            } else {
                let postings = self.cached_exact_fields(&cached, exact_fields)?;
                for ((key, value), posting) in exact_fields.iter().zip(&postings) {
                    if let Some(posting) = posting {
                        self.cache_exact_posting(
                            ExactPostingKey::Field(
                                frame.frame_id,
                                Arc::clone(key),
                                Arc::clone(value),
                            ),
                            Arc::clone(posting),
                        );
                    }
                }
                postings
            };
            if exact_fields.is_empty() && postings.len() == 1 {
                let Some(posting) = postings.into_iter().next().flatten() else {
                    return Ok(0);
                };
                let count = count_cached_timestamp_candidates(query, &cached, &posting);
                return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
            }
            let mut exact_candidates = None;
            for posting in postings.into_iter().chain(field_postings) {
                let Some(posting) = posting else {
                    return Ok(0);
                };
                intersect_frame_candidate_slice(&mut exact_candidates, &posting);
                if exact_candidates.as_ref().is_some_and(Vec::is_empty) {
                    return Ok(0);
                }
            }
            let mut exact_candidates = exact_candidates.unwrap_or_default();
            retain_cached_timestamp_candidates(query, &cached, &mut exact_candidates);
            let count = exact_candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        if query.start_timestamp_unix_nanos.is_none()
            && query.end_timestamp_unix_nanos.is_none()
            && exact_fields.is_empty()
            && cached_message_predicate_is_exact(&query.predicate)
            && let Some(count) =
                self.exact_boolean_message_candidate_count(query, frame, message_predicate_key)?
        {
            return Ok(count);
        }
        if cached_message_predicate_is_exact(&query.predicate)
            && let Some(candidates) =
                self.exact_boolean_message_candidates(query, frame, message_predicate_key)?
        {
            if query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            }) {
                return Ok(0);
            }
            let cached = self.cached_indexed_frame(frame)?;
            let mut candidates = candidates;
            for posting in self.cached_exact_fields(&cached, exact_fields)? {
                let Some(posting) = posting else {
                    return Ok(0);
                };
                let mut current = Some(candidates);
                intersect_frame_candidate_slice(&mut current, &posting);
                candidates = current.unwrap_or_default();
                if candidates.is_empty() {
                    return Ok(0);
                }
            }
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        if let Some(tokens) = query
            .exact_message_token_disjunction()
            .filter(|tokens| !tokens.is_empty())
        {
            if query.exact_fields.iter().any(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() != append.tenant.as_ref()
            }) {
                return Ok(0);
            }
            let cached = self.cached_indexed_frame(frame)?;
            let postings = self.cached_exact_message_terms(&cached, &tokens)?;
            let mut candidates = Vec::new();
            for posting in postings.into_iter().flatten() {
                union_sorted_ordinals(&mut candidates, posting.to_vec());
            }
            if candidates.is_empty() {
                return Ok(0);
            }
            for posting in self.cached_exact_fields(&cached, exact_fields)? {
                let Some(posting) = posting else {
                    return Ok(0);
                };
                let mut current = Some(candidates);
                intersect_frame_candidate_slice(&mut current, &posting);
                candidates = current.unwrap_or_default();
                if candidates.is_empty() {
                    return Ok(0);
                }
            }
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        let exact_fields_are_authoritative_tenant = query.exact_fields.iter().all(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() == append.tenant.as_ref()
        });
        if query.terms.is_empty()
            && (query.exact_fields.is_empty() || exact_fields_are_authoritative_tenant)
            && cached_message_predicate_is_exact(&query.predicate)
            && let Some(candidates) = self.cached_message_predicate_candidates_with_key(
                query,
                frame,
                message_predicate_key,
            )?
        {
            let cached = self.cached_indexed_frame(frame)?;
            let mut candidates = candidates.to_vec();
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        let cached_message_candidates =
            self.cached_message_predicate_candidates_with_key(query, frame, message_predicate_key)?;
        let exact_fields_are_authoritative_tenant = query.exact_fields.iter().all(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() == append.tenant.as_ref()
        });
        let required = query.required_index_constraints();
        let has_indexed_field_constraints = !required.field_exists.is_empty()
            || !required.field_in.is_empty()
            || !required.field_text.is_empty()
            || !required.field_regex.is_empty()
            || !required.field_numeric.is_empty();
        let message_candidates_are_base = exact_fields_are_authoritative_tenant
            && predicate_is_indexed_conjunction(&query.predicate)
            && has_indexed_field_constraints
            && cached_message_candidates.is_some();
        let candidates = if message_candidates_are_base {
            cached_message_candidates.as_ref().map_or_else(
                || {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                },
                |candidates| candidates.clone(),
            )
        } else {
            indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            )
        };
        let mut candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        if !message_candidates_are_base
            && let Some(message_candidates) = cached_message_candidates.as_ref()
        {
            let mut current = Some(candidates);
            intersect_frame_candidate_slice(&mut current, message_candidates);
            candidates = current.unwrap_or_default();
        }
        if candidates.is_empty() {
            return Ok(0);
        }
        if query.terms.is_empty()
            && query.exact_fields.iter().all(|field| {
                field.key.as_ref() == "resource.loki.tenant"
                    && field.value.as_ref() == append.tenant.as_ref()
            })
            && hot_predicate_candidates_are_exact(&query.predicate)
        {
            let cached = self.cached_indexed_frame(frame)?;
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            let count = candidates.len();
            return u64::try_from(count).map_err(|_| TelemetryError::RecordTooLarge);
        }
        if query.terms.is_empty() && cached_message_candidates.is_some() {
            let cached = self.cached_indexed_frame(frame)?;
            retain_cached_timestamp_candidates(query, &cached, &mut candidates);
            if candidates.is_empty() {
                return Ok(0);
            }
            let fields = decode_structural_fields(&cached.structural, &candidates)?;
            let matches = candidates
                .iter()
                .zip(fields.iter())
                .filter(|(ordinal, fields)| {
                    let timestamp_matches = usize::try_from(**ordinal)
                        .ok()
                        .and_then(|index| cached.timestamps.get(index))
                        .is_some_and(|timestamp| query.timestamp_matches(*timestamp));
                    let exact_fields_match = query.exact_fields.iter().all(|expected| {
                        if expected.key.as_ref() == "resource.loki.tenant" {
                            expected.value.as_ref() == append.tenant.as_ref()
                        } else {
                            fields.iter().any(|field| {
                                field.key == expected.key && field.value == expected.value
                            })
                        }
                    });
                    timestamp_matches
                        && exact_fields_match
                        && crate::query::predicate_fields_match(&query.predicate, fields.as_ref())
                })
                .count();
            return u64::try_from(matches).map_err(|_| TelemetryError::RecordTooLarge);
        }
        // The embedded index is deliberately a candidate superset. For a
        // cardinality-only message query, decode only message bodies and
        // verify the residual predicate instead of materializing every typed
        // field and attribute. The append tenant is authoritative for its
        // tenant field, so it can be checked without record decoding.
        if query.can_use_indexed_message_filter() {
            let cached = self.cached_indexed_frame(frame)?;
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &candidates,
                &cached.embedded_index,
                &cached.templates,
            )?;
            let fields = (!query.exact_fields.is_empty())
                .then(|| decode_structural_fields(&cached.structural, &candidates))
                .transpose()?;
            let matches = candidates
                .iter()
                .zip(messages.iter())
                .enumerate()
                .filter(|(position, (ordinal, message))| {
                    let index = usize::try_from(**ordinal).ok();
                    let timestamp_matches = index
                        .and_then(|index| cached.timestamps.get(index))
                        .is_some_and(|timestamp| query.timestamp_matches(*timestamp));
                    let fields_match = fields.as_ref().is_none_or(|fields| {
                        fields.get(*position).is_some_and(|decoded_fields| {
                            query.exact_fields.iter().all(|expected| {
                                if expected.key.as_ref() == "resource.loki.tenant" {
                                    expected.value.as_ref() == append.tenant.as_ref()
                                } else {
                                    decoded_fields.iter().any(|field| {
                                        field.key == expected.key && field.value == expected.value
                                    })
                                }
                            })
                        })
                    });
                    timestamp_matches
                        && fields_match
                        && query.message_candidate_matches(message).unwrap_or(false)
                })
                .count();
            return u64::try_from(matches).map_err(|_| TelemetryError::RecordTooLarge);
        }
        let cached = self.cached_indexed_frame(frame)?;
        let matches = decode_structural_records_with_cached_frame_data(
            &cached.structural,
            &candidates,
            &cached.embedded_index,
            &cached.templates,
            &cached.offsets,
            &cached.timestamps,
            false,
            true,
            None,
            None,
            Some(&cached.attribute_tables),
        )?
        .into_iter()
        .filter(|record| {
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(record.offset.get())
                .map(LogicalOffset::new);
            absolute_offset.is_some_and(|absolute_offset| {
                query.matches_index_candidate(&AbsoluteDecodedRecordView {
                    record,
                    absolute_offset,
                })
            })
        })
        .count();
        u64::try_from(matches).map_err(|_| TelemetryError::RecordTooLarge)
    }

    fn exact_boolean_message_candidate_count(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<u64>> {
        if !query.terms.is_empty()
            || query.start_offset.is_some()
            || query.end_offset.is_some()
            || query.after.is_some()
        {
            return Ok(None);
        }
        let Some(cache_key) = message_predicate_key else {
            return Ok(None);
        };
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(candidates) = cached
            .message_predicate_candidates
            .lock()
            .expect("indexed frame message predicate cache lock is not poisoned")
            .get(cache_key)
            .cloned()
        {
            return Ok(Some(u64::try_from(candidates.len()).unwrap_or(u64::MAX)));
        }
        let candidates =
            self.exact_boolean_message_candidates(query, frame, message_predicate_key)?;
        Ok(candidates.map(|candidates| u64::try_from(candidates.len()).unwrap_or(u64::MAX)))
    }

    fn exact_boolean_message_candidates(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        if !query.terms.is_empty()
            || query.start_offset.is_some()
            || query.end_offset.is_some()
            || query.after.is_some()
        {
            return Ok(None);
        }
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(key) = message_predicate_key
            && let Some(candidates) = cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .get(key)
                .cloned()
        {
            return Ok(Some(candidates.to_vec()));
        }
        if let Some((tokens, minimum)) = message_token_min_match_shape(&query.predicate) {
            let requested = tokens
                .iter()
                .map(|(value, sensitivity)| (value.as_ref(), *sensitivity))
                .collect::<Vec<_>>();
            let postings = self.cached_exact_message_terms(&cached, &requested)?;
            let candidates =
                exact_message_token_min_match_candidates(&postings, frame.record_count, minimum);
            if let Some(key) = message_predicate_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        fn collect_tokens(
            predicate: &LogPredicate,
            tokens: &mut Vec<(Arc<str>, CaseSensitivity)>,
        ) -> bool {
            match predicate {
                LogPredicate::MatchAll | LogPredicate::MatchNone => true,
                LogPredicate::MessageToken {
                    value,
                    case_sensitivity,
                } if !value.is_empty()
                    && value.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
                {
                    if !tokens.iter().any(|(known, sensitivity)| {
                        known == value && sensitivity == case_sensitivity
                    }) {
                        tokens.push((Arc::clone(value), *case_sensitivity));
                    }
                    true
                }
                LogPredicate::And(predicates) | LogPredicate::Or(predicates) => predicates
                    .iter()
                    .all(|predicate| collect_tokens(predicate, tokens)),
                LogPredicate::Not(predicate) => collect_tokens(predicate, tokens),
                _ => false,
            }
        }
        fn evaluate(
            predicate: &LogPredicate,
            postings: &HashMap<(Arc<str>, CaseSensitivity), Vec<u32>>,
            record_count: u32,
        ) -> Option<Vec<u32>> {
            match predicate {
                LogPredicate::MatchAll => Some((0..record_count).collect()),
                LogPredicate::MatchNone => Some(Vec::new()),
                LogPredicate::MessageToken {
                    value,
                    case_sensitivity,
                } => Some(
                    postings
                        .get(&(Arc::clone(value), *case_sensitivity))
                        .cloned()
                        .unwrap_or_default(),
                ),
                LogPredicate::And(predicates) => {
                    let mut current = None;
                    for predicate in predicates {
                        let child = evaluate(predicate, postings, record_count)?;
                        intersect_frame_candidate_slice(&mut current, &child);
                    }
                    Some(current.unwrap_or_else(|| (0..record_count).collect()))
                }
                LogPredicate::Or(predicates) => {
                    let mut current = Vec::new();
                    for predicate in predicates {
                        union_sorted_ordinals(
                            &mut current,
                            evaluate(predicate, postings, record_count)?,
                        );
                    }
                    Some(current)
                }
                LogPredicate::Not(predicate) => {
                    let excluded = evaluate(predicate, postings, record_count)?;
                    let mut selected = Vec::new();
                    let mut excluded_index = 0;
                    for ordinal in 0..record_count {
                        if excluded.get(excluded_index).copied() == Some(ordinal) {
                            excluded_index += 1;
                        } else {
                            selected.push(ordinal);
                        }
                    }
                    Some(selected)
                }
                _ => None,
            }
        }

        let mut tokens = Vec::new();
        if !collect_tokens(&query.predicate, &mut tokens) || tokens.is_empty() {
            return Ok(None);
        }
        let requested = tokens
            .iter()
            .map(|(value, sensitivity)| (value.as_ref(), *sensitivity))
            .collect::<Vec<_>>();
        let postings = self.cached_exact_message_terms(&cached, &requested)?;
        let postings = tokens
            .into_iter()
            .zip(postings)
            .map(|((value, sensitivity), posting)| {
                (
                    (value, sensitivity),
                    posting.map(|posting| posting.to_vec()).unwrap_or_default(),
                )
            })
            .collect::<HashMap<_, _>>();
        let candidates = evaluate(&query.predicate, &postings, frame.record_count);
        if let (Some(key), Some(candidates)) = (message_predicate_key, candidates.as_ref()) {
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(Arc::clone(key), Arc::from(candidates.clone()));
        }
        Ok(candidates)
    }

    fn count_cached_exact_candidates(
        &self,
        query: &LogQuery,
        frame_id: u64,
        candidates: &[u32],
    ) -> Option<u64> {
        if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
            return Some(u64::try_from(candidates.len()).unwrap_or(u64::MAX));
        }
        let cached = self.cached_indexed_frame_if_present(frame_id)?;
        let count = count_cached_timestamp_candidates(query, &cached, candidates);
        Some(u64::try_from(count).unwrap_or(u64::MAX))
    }

    fn count_cached_message_predicate_candidates(
        &self,
        query: &LogQuery,
        frame_id: u64,
        candidates: &[u32],
    ) -> Option<u64> {
        if query.start_timestamp_unix_nanos.is_none() && query.end_timestamp_unix_nanos.is_none() {
            return Some(u64::try_from(candidates.len()).unwrap_or(u64::MAX));
        }
        let cached = self.cached_indexed_frame_if_present(frame_id)?;
        let count = count_cached_timestamp_candidates(query, &cached, candidates);
        Some(u64::try_from(count).unwrap_or(u64::MAX))
    }

    fn cached_indexed_frame(
        &self,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Arc<CachedIndexedFrame>> {
        {
            let mut cache = self
                .indexed_frame_query_cache
                .lock()
                .expect("indexed frame query cache lock is not poisoned");
            if let Some(cached) = cache.get(frame.frame_id) {
                return Ok(cached);
            }
        }

        let structural = Arc::<[u8]>::from(decompress_indexed_ingest_frame(frame)?);
        let embedded_index = Arc::new(crate::structural::decode_embedded_frame_index(&structural)?);
        let templates = Arc::<[Vec<Vec<u8>>]>::from(decode_structural_templates(&structural)?);
        let attribute_tables = Arc::new(decode_structural_attribute_tables(&structural)?);
        let (offsets, timestamps) = decode_structural_positions(&structural)?;
        let cached = Arc::new(CachedIndexedFrame {
            structural,
            embedded_index,
            templates,
            attribute_tables,
            offsets: Arc::from(offsets),
            timestamps: Arc::from(timestamps),
            message_bodies: Mutex::new(CachedFrameMessages::default()),
            metadata_fields: Mutex::new(CachedFrameFields::default()),
            trace_ids: Mutex::new(None),
            typed_metadata: Mutex::new(None),
            exact_message_terms: Mutex::new(HashMap::new()),
            message_token_stats: Mutex::new(None),
            message_predicate_candidates: Mutex::new(HashMap::new()),
            exact_fields: Mutex::new(HashMap::new()),
            field_postings: Mutex::new(HashMap::new()),
        });
        let mut cache = self
            .indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned");
        if let Some(existing) = cache.get(frame.frame_id) {
            return Ok(existing);
        }
        cache.insert(frame.frame_id, Arc::clone(&cached));
        Ok(cached)
    }

    fn cached_typed_metadata(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
    ) -> TelemetryResult<Arc<CachedTypedMetadata>> {
        {
            let typed_metadata = cached
                .typed_metadata
                .lock()
                .expect("indexed frame typed metadata cache lock is not poisoned");
            if let Some(typed_metadata) = typed_metadata.as_ref() {
                return Ok(Arc::clone(typed_metadata));
            }
        }
        let record_count =
            usize::try_from(record_count).map_err(|_| TelemetryError::RecordTooLarge)?;
        let (packed, raw_bytes) =
            decode_structural_typed_metadata(&cached.structural, record_count)?;
        let computed = Arc::new(CachedTypedMetadata {
            packed: Arc::new(packed),
            // The decompressed representation bounds the serialized values;
            // include a small allowance for Vec/Arc bookkeeping in the cache
            // budget so a typed lane cannot consume the whole query cache.
            cache_bytes: raw_bytes.saturating_mul(2),
        });
        let mut typed_metadata = cached
            .typed_metadata
            .lock()
            .expect("indexed frame typed metadata cache lock is not poisoned");
        if typed_metadata.is_none() {
            *typed_metadata = Some(Arc::clone(&computed));
        }
        let result = typed_metadata
            .as_ref()
            .expect("typed metadata was inserted")
            .clone();
        drop(typed_metadata);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(result)
    }

    fn cached_trace_ids(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
    ) -> TelemetryResult<Arc<[Option<TraceId>]>> {
        {
            let trace_ids = cached
                .trace_ids
                .lock()
                .expect("indexed frame trace ID cache lock is not poisoned");
            if let Some(trace_ids) = trace_ids.as_ref() {
                return Ok(Arc::clone(trace_ids));
            }
        }
        let ordinals = (0..record_count).collect::<Vec<_>>();
        let decoded = decode_structural_trace_ids(&cached.structural, &ordinals)?;
        let decoded = Arc::<[Option<TraceId>]>::from(decoded);
        let mut trace_ids = cached
            .trace_ids
            .lock()
            .expect("indexed frame trace ID cache lock is not poisoned");
        if trace_ids.is_none() {
            *trace_ids = Some(Arc::clone(&decoded));
        }
        drop(trace_ids);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(cached
            .trace_ids
            .lock()
            .expect("indexed frame trace ID cache lock is not poisoned")
            .as_ref()
            .cloned()
            .unwrap_or(decoded))
    }

    fn cached_indexed_frame_if_present(&self, frame_id: u64) -> Option<Arc<CachedIndexedFrame>> {
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .get(frame_id)
    }

    fn cached_message_predicate_candidates_if_present(
        &self,
        frame_id: u64,
        predicate: &LogPredicate,
    ) -> Option<Vec<u32>> {
        let key = Self::cached_message_predicate_key(predicate)?;
        self.cached_message_predicate_candidates_arc_if_present(frame_id, Some(&key))
            .map(|candidates| candidates.to_vec())
    }

    fn cached_message_predicate_candidates_arc_if_present(
        &self,
        frame_id: u64,
        cache_key: Option<&Arc<str>>,
    ) -> Option<Arc<[u32]>> {
        let key = cache_key?;
        let cached = self.cached_indexed_frame_if_present(frame_id)?;
        cached
            .message_predicate_candidates
            .lock()
            .expect("indexed frame message predicate cache lock is not poisoned")
            .get(key)
            .cloned()
    }

    fn cached_exact_posting(&self, key: &ExactPostingKey) -> Option<Arc<[u32]>> {
        self.exact_posting_cache
            .lock()
            .expect("exact posting cache lock is not poisoned")
            .get(key)
    }

    fn cache_exact_posting(&self, key: ExactPostingKey, posting: Arc<[u32]>) {
        self.exact_posting_cache
            .lock()
            .expect("exact posting cache lock is not poisoned")
            .insert(key, posting);
    }

    fn cached_exact_frame_candidates(
        &self,
        frame_id: u64,
        exact_tokens: &[(&str, CaseSensitivity)],
        exact_fields: &[(Arc<str>, Arc<str>)],
    ) -> Option<Vec<u32>> {
        if exact_tokens.is_empty() && exact_fields.is_empty() {
            return None;
        }
        let mut candidates = None;
        for (token, case_sensitivity) in exact_tokens {
            let key = exact_message_posting_key(frame_id, token, *case_sensitivity);
            let posting = self.cached_exact_posting(&key)?;
            intersect_frame_candidate_slice(&mut candidates, &posting);
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Some(Vec::new());
            }
        }
        for (key, value) in exact_fields {
            let posting = self.cached_exact_posting(&ExactPostingKey::Field(
                frame_id,
                Arc::clone(key),
                Arc::clone(value),
            ))?;
            intersect_frame_candidate_slice(&mut candidates, &posting);
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Some(Vec::new());
            }
        }
        candidates
    }

    fn cached_exact_message_terms(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        requested: &[(&str, CaseSensitivity)],
    ) -> TelemetryResult<Vec<Option<Arc<[u32]>>>> {
        let requested = requested
            .iter()
            .map(|(token, case_sensitivity)| {
                let normalized = match case_sensitivity {
                    CaseSensitivity::Sensitive => Arc::<str>::from(*token),
                    CaseSensitivity::Insensitive => Arc::<str>::from(token.to_ascii_lowercase()),
                };
                (normalized, *case_sensitivity)
            })
            .collect::<Vec<_>>();
        let missing = {
            let cached_terms = cached
                .exact_message_terms
                .lock()
                .expect("exact frame term cache lock is not poisoned");
            let missing = requested
                .iter()
                .filter(|term| !cached_terms.contains_key(*term))
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(requested
                    .iter()
                    .map(|term| cached_terms.get(term).cloned())
                    .collect());
            }
            missing
        };
        if !missing.is_empty() {
            let mut postings = missing
                .iter()
                .map(|term| (term.clone(), Vec::new()))
                .collect::<Vec<_>>();
            let mut verify_ordinals = Vec::new();
            for (missing_index, (term, case_sensitivity)) in missing.iter().enumerate() {
                if !cached.embedded_index.term_might_contain(term) {
                    continue;
                }
                let static_layouts = cached.embedded_index.term_layout_ids(term);
                let guaranteed_layouts = static_layouts
                    .iter()
                    .copied()
                    .filter(|layout_id| {
                        cached
                            .templates
                            .get(*layout_id as usize)
                            .is_some_and(|literals| {
                                Self::template_literals_contain_term(
                                    literals,
                                    term,
                                    *case_sensitivity,
                                )
                            })
                    })
                    .collect::<Vec<_>>();
                let mut guaranteed_layouts = guaranteed_layouts;
                guaranteed_layouts.sort_unstable();
                guaranteed_layouts.dedup();
                postings[missing_index].1.extend(
                    cached
                        .embedded_index
                        .record_ordinals_for_layout_ids(&guaranteed_layouts),
                );
                let verify_layouts = static_layouts
                    .into_iter()
                    .filter(|layout_id| guaranteed_layouts.binary_search(layout_id).is_err())
                    .chain(
                        cached
                            .embedded_index
                            .residual_layout_ids()
                            .iter()
                            .copied()
                            .filter(|layout_id| {
                                guaranteed_layouts.binary_search(layout_id).is_err()
                            }),
                    )
                    .collect::<Vec<_>>();
                let mut verify_layouts = verify_layouts;
                verify_layouts.sort_unstable();
                verify_layouts.dedup();
                verify_ordinals.extend(
                    cached
                        .embedded_index
                        .record_ordinals_for_layout_ids(&verify_layouts),
                );
            }
            verify_ordinals.sort_unstable();
            verify_ordinals.dedup();
            // Exact cardinality queries only need postings for the requested
            // terms. Building the full relevance statistics table here also
            // allocates a posting list for every token in the frame, which
            // made the first SearchBench token query pay the cost of an
            // unrelated relevance index. The embedded token index narrows the
            // verification decode before those requested terms are cached.
            // Keep the broader relevance cache lazy for top-k and phrase
            // queries.
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &verify_ordinals,
                &cached.embedded_index,
                &cached.templates,
            )?;
            for (ordinal, message) in verify_ordinals.into_iter().zip(messages) {
                let mut seen = Vec::<usize>::new();
                scan_clickhouse_tokens(&message, |token| {
                    let Some(index) = missing.iter().position(|expected| match expected.1 {
                        CaseSensitivity::Sensitive => token == expected.0.as_ref(),
                        CaseSensitivity::Insensitive => {
                            token.eq_ignore_ascii_case(expected.0.as_ref())
                        }
                    }) else {
                        return;
                    };
                    if seen.contains(&index) {
                        return;
                    }
                    seen.push(index);
                    postings[index].1.push(ordinal);
                });
            }
            let computed = postings
                .into_iter()
                .map(|(term, ordinals)| (term, Arc::<[u32]>::from(ordinals)))
                .collect::<HashMap<_, _>>();
            let mut cached_terms = cached
                .exact_message_terms
                .lock()
                .expect("exact frame term cache lock is not poisoned");
            for (term, posting) in &computed {
                if cached_terms.contains_key(term)
                    || cached_terms.len() < MAX_EXACT_FRAME_QUERY_TERMS
                {
                    cached_terms.insert(term.clone(), Arc::clone(posting));
                }
            }
            return Ok(requested
                .iter()
                .map(|term| {
                    cached_terms
                        .get(term)
                        .cloned()
                        .or_else(|| computed.get(term).cloned())
                })
                .collect());
        }
        let cached_terms = cached
            .exact_message_terms
            .lock()
            .expect("exact frame term cache lock is not poisoned");
        Ok(requested
            .iter()
            .map(|term| cached_terms.get(term).cloned())
            .collect())
    }

    fn cached_message_token_stats(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
    ) -> TelemetryResult<Arc<CachedMessageTokenStats>> {
        {
            let postings = cached
                .message_token_stats
                .lock()
                .expect("indexed frame message token cache lock is not poisoned");
            if let Some(postings) = postings.as_ref() {
                return Ok(Arc::clone(postings));
            }
        }
        let ordinals = (0..record_count).collect::<Vec<_>>();
        let messages = decode_structural_messages_with_embedded_index_and_templates(
            &cached.structural,
            &ordinals,
            &cached.embedded_index,
            &cached.templates,
        )?;
        let messages = Arc::<[Arc<str>]>::from(messages);
        let mut postings = HashMap::<Arc<str>, Vec<(u32, u32)>>::new();
        let mut document_lengths = Vec::with_capacity(messages.len());
        let mut token_ids_by_term = HashMap::<Arc<str>, u32>::new();
        let mut token_sequence = Vec::new();
        let mut token_offsets = Vec::with_capacity(messages.len().saturating_add(1));
        token_offsets.push(0_u32);
        for (ordinal, message) in ordinals.into_iter().zip(messages.iter()) {
            let mut counts = HashMap::<Arc<str>, u32>::new();
            let mut document_length = 0_u32;
            scan_clickhouse_tokens(message, |token| {
                document_length = document_length.saturating_add(1);
                let normalized = Arc::<str>::from(normalize_term(token).as_ref());
                let token_id = match token_ids_by_term.get(&normalized) {
                    Some(token_id) => *token_id,
                    None => {
                        let token_id = u32::try_from(token_ids_by_term.len()).unwrap_or(u32::MAX);
                        token_ids_by_term.insert(Arc::clone(&normalized), token_id);
                        token_id
                    }
                };
                token_sequence.push(token_id);
                let frequency = counts.entry(normalized).or_default();
                *frequency = frequency.saturating_add(1);
            });
            document_lengths.push(document_length);
            token_offsets.push(
                u32::try_from(token_sequence.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            );
            for (token, frequency) in counts {
                postings
                    .entry(token)
                    .or_default()
                    .push((ordinal, frequency));
            }
        }
        let postings = postings
            .into_iter()
            .map(|(token, entries)| {
                let (ordinals, frequencies): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
                (
                    token,
                    Arc::new(MessageTokenPosting {
                        ordinals: Arc::from(ordinals),
                        frequencies: Arc::from(frequencies),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let computed = Arc::new(CachedMessageTokenStats {
            postings,
            document_lengths: Arc::from(document_lengths),
            messages,
            token_ids_by_term,
            token_sequence: Arc::from(token_sequence),
            token_offsets: Arc::from(token_offsets),
        });
        let mut cached_postings = cached
            .message_token_stats
            .lock()
            .expect("indexed frame message token cache lock is not poisoned");
        if cached_postings.is_none() {
            *cached_postings = Some(Arc::clone(&computed));
        }
        let result = cached_postings
            .as_ref()
            .expect("message token postings were inserted")
            .clone();
        drop(cached_postings);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(result)
    }

    fn template_literals_contain_term(
        literals: &[Vec<u8>],
        term: &str,
        case_sensitivity: CaseSensitivity,
    ) -> bool {
        literals.iter().any(|literal| {
            let Ok(literal) = std::str::from_utf8(literal) else {
                return false;
            };
            let mut found = false;
            scan_clickhouse_tokens(literal, |token| {
                found |= match case_sensitivity {
                    CaseSensitivity::Sensitive => token == term,
                    CaseSensitivity::Insensitive => token.eq_ignore_ascii_case(term),
                };
            });
            found
        })
    }

    fn template_literals_contain_phrase(
        literals: &[Vec<u8>],
        terms: &[Arc<str>],
        max_gap: usize,
        case_sensitivity: CaseSensitivity,
    ) -> bool {
        literals.iter().any(|literal| {
            let Ok(literal) = std::str::from_utf8(literal) else {
                return false;
            };
            crate::query::message_has_phrase(literal, terms, max_gap, case_sensitivity)
        })
    }

    fn cached_message_predicate_key(predicate: &LogPredicate) -> Option<Arc<str>> {
        let sensitivity = |case_sensitivity: CaseSensitivity| match case_sensitivity {
            CaseSensitivity::Sensitive => 's',
            CaseSensitivity::Insensitive => 'i',
        };
        if let Some((tokens, minimum)) = message_token_min_match_shape(predicate) {
            return Some(Arc::from(format!(
                "min-match:{}:{}",
                minimum,
                tokens
                    .iter()
                    .map(|(value, case_sensitivity)| {
                        format!("{}:{}", sensitivity(*case_sensitivity), value)
                    })
                    .collect::<Vec<_>>()
                    .join("\u{1f}")
            )));
        }
        match predicate {
            LogPredicate::Term(term) => Some(Arc::from(format!("term:{term}"))),
            LogPredicate::MessageToken {
                value,
                case_sensitivity,
            } => Some(Arc::from(format!(
                "token:{}:{}",
                sensitivity(*case_sensitivity),
                value
            ))),
            LogPredicate::MessageTokenRegex(regex) => Some(Arc::from(format!(
                "token-regex:{}:{}",
                sensitivity(regex.case_sensitivity()),
                regex.pattern()
            ))),
            LogPredicate::MessageTokenPrefix {
                value,
                case_sensitivity,
            } => Some(Arc::from(format!(
                "token-prefix:{}:{}",
                sensitivity(*case_sensitivity),
                value
            ))),
            LogPredicate::MessageFuzzy {
                value,
                max_distance,
            } => Some(Arc::from(format!("token-fuzzy:{max_distance}:{value}"))),
            LogPredicate::MessagePhrase {
                terms,
                max_gap,
                case_sensitivity,
            } => Some(Arc::from(format!(
                "phrase:{}:{}:{}",
                sensitivity(*case_sensitivity),
                max_gap,
                terms
                    .iter()
                    .map(AsRef::as_ref)
                    .collect::<Vec<&str>>()
                    .join("\u{1f}")
            ))),
            LogPredicate::MessageRegex(regex) => Some(Arc::from(format!(
                "message-regex:{}:{}",
                sensitivity(regex.case_sensitivity()),
                regex.pattern()
            ))),
            LogPredicate::Message(matcher)
                if matcher.kind != crate::TextMatchKind::Exact
                    && !matcher.value.is_empty()
                    && matcher
                        .value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric()) =>
            {
                let kind = match matcher.kind {
                    crate::TextMatchKind::Contains => 'c',
                    crate::TextMatchKind::Prefix => 'p',
                    crate::TextMatchKind::Suffix => 's',
                    crate::TextMatchKind::Exact => unreachable!(),
                };
                Some(Arc::from(format!(
                    "message-literal:{kind}:{}:{}",
                    sensitivity(matcher.case_sensitivity),
                    matcher.value
                )))
            }
            LogPredicate::And(predicates) => {
                let parts = predicates
                    .iter()
                    .filter_map(Self::cached_message_predicate_key)
                    .collect::<Vec<_>>();
                (!parts.is_empty()).then(|| {
                    Arc::from(format!(
                        "and:{}",
                        parts
                            .iter()
                            .map(AsRef::as_ref)
                            .collect::<Vec<&str>>()
                            .join("\u{1f}")
                    ))
                })
            }
            LogPredicate::Or(predicates) => {
                let parts = predicates
                    .iter()
                    .map(Self::cached_message_predicate_key)
                    .collect::<Option<Vec<_>>>()?;
                Some(Arc::from(format!(
                    "or:{}",
                    parts
                        .iter()
                        .map(AsRef::as_ref)
                        .collect::<Vec<&str>>()
                        .join("\u{1f}")
                )))
            }
            LogPredicate::Not(predicate) => Self::cached_message_predicate_key(predicate)
                .map(|part| Arc::from(format!("not:{part}"))),
            _ => None,
        }
    }

    fn cached_message_predicate_candidates(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let cache_key = Self::cached_message_predicate_key(&query.predicate);
        self.cached_message_predicate_candidates_with_key(query, frame, cache_key.as_ref())
    }

    fn cached_message_predicate_candidates_with_key(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        cache_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        self.cached_message_predicate_candidates_with_key_mode(query, frame, cache_key, true)
    }

    fn cached_message_predicate_candidates_for_relevance(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        cache_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        self.cached_message_predicate_candidates_with_key_mode(query, frame, cache_key, false)
    }

    fn cached_message_predicate_candidates_with_key_mode(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        cache_key: Option<&Arc<str>>,
        allow_structural_message_fast_path: bool,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(key) = cache_key
            && let Some(candidates) = cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .get(key)
                .cloned()
        {
            return Ok(Some(candidates.to_vec()));
        }
        if let Some(tokens) = query
            .exact_message_token_conjunction()
            .filter(|tokens| !tokens.is_empty())
        {
            let postings = self.cached_exact_message_terms(&cached, &tokens)?;
            let mut candidates = None;
            for posting in postings {
                let Some(posting) = posting else {
                    candidates = Some(Vec::new());
                    break;
                };
                intersect_frame_candidate_slice(&mut candidates, &posting);
                if candidates.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            let candidates = candidates.unwrap_or_default();
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        if let Some(tokens) = query
            .exact_message_token_disjunction()
            .filter(|tokens| !tokens.is_empty())
        {
            let postings = self.cached_exact_message_terms(&cached, &tokens)?;
            let mut candidates = Vec::new();
            for posting in postings.into_iter().flatten() {
                union_sorted_ordinals(&mut candidates, posting.to_vec());
            }
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        // A field-only predicate has no message candidate source. Returning
        // early avoids constructing the full token-statistics cache just to
        // discover that it cannot narrow the frame. For a mixed AND, the
        // embedded index can still provide a safe token superset while the
        // normal residual field matcher verifies the complete predicate.
        if cache_key.is_none() {
            return Ok(None);
        }
        if let LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } = &query.predicate
            && allow_structural_message_fast_path
            && query.limit.is_none()
            && !terms.is_empty()
        {
            let requested = terms
                .iter()
                .map(|term| (term.as_ref(), *case_sensitivity))
                .collect::<Vec<_>>();
            let postings = self.cached_exact_message_terms(&cached, &requested)?;
            let mut ordinals = None;
            for posting in postings {
                let Some(posting) = posting else {
                    ordinals = Some(Vec::new());
                    break;
                };
                intersect_frame_candidate_slice(&mut ordinals, &posting);
                if ordinals.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            let ordinals = ordinals.unwrap_or_default();
            let mut phrase_layouts = None;
            for term in terms {
                let mut term_layouts = cached.embedded_index.term_layout_ids(term);
                term_layouts.sort_unstable();
                term_layouts.dedup();
                intersect_frame_candidate_slice(&mut phrase_layouts, &term_layouts);
            }
            let static_layouts = phrase_layouts
                .unwrap_or_default()
                .into_iter()
                .filter(|layout_id| {
                    cached
                        .templates
                        .get(*layout_id as usize)
                        .is_some_and(|literals| {
                            Self::template_literals_contain_phrase(
                                literals,
                                terms,
                                *max_gap,
                                *case_sensitivity,
                            )
                        })
                })
                .collect::<Vec<_>>();
            let static_matches = cached
                .embedded_index
                .record_ordinals_for_layout_ids(&static_layouts);
            let mut verify_ordinals =
                Vec::with_capacity(ordinals.len().saturating_sub(static_matches.len()));
            let mut static_index = 0;
            for ordinal in ordinals {
                while static_index < static_matches.len() && static_matches[static_index] < ordinal
                {
                    static_index += 1;
                }
                if static_index >= static_matches.len() || static_matches[static_index] != ordinal {
                    verify_ordinals.push(ordinal);
                }
            }
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &verify_ordinals,
                &cached.embedded_index,
                &cached.templates,
            )?;
            let mut candidates = static_matches;
            candidates.extend(verify_ordinals.into_iter().zip(messages).filter_map(
                |(ordinal, message)| {
                    crate::query::message_has_phrase(&message, terms, *max_gap, *case_sensitivity)
                        .then_some(ordinal)
                },
            ));
            candidates.sort_unstable();
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(
                    Arc::clone(cache_key.expect("message predicate cache key is present")),
                    Arc::from(candidates.clone()),
                );
            return Ok(Some(candidates));
        }
        if let Some(candidates) =
            embedded_message_predicate_candidates(&query.predicate, &cached.embedded_index)
        {
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(
                    Arc::clone(cache_key.expect("message predicate cache key is present")),
                    Arc::from(candidates.clone()),
                );
            return Ok(Some(candidates));
        }
        if let LogPredicate::MessagePhrase {
            terms,
            max_gap,
            case_sensitivity,
        } = &query.predicate
        {
            let stats = self.cached_message_token_stats(&cached, frame.record_count)?;
            let postings = &stats.postings;
            let mut ordinals = None;
            for term in terms {
                let candidates = postings
                    .get(normalize_term(term).as_ref())
                    .map(|posting| posting.ordinals.as_ref())
                    .unwrap_or(&[]);
                intersect_frame_candidate_slice(&mut ordinals, candidates);
                if ordinals.as_ref().is_some_and(Vec::is_empty) {
                    break;
                }
            }
            let ordinals = ordinals.unwrap_or_default();
            let candidates = if let Some(candidates) =
                stats.phrase_candidate_ordinals(&ordinals, terms, *max_gap, *case_sensitivity)
            {
                candidates
            } else {
                ordinals
                    .into_iter()
                    .filter_map(|ordinal| {
                        let message = stats.messages.get(ordinal as usize)?;
                        crate::query::message_has_phrase(
                            message,
                            terms,
                            *max_gap,
                            *case_sensitivity,
                        )
                        .then_some(ordinal)
                    })
                    .collect::<Vec<_>>()
            };
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        let token_stats_candidate = match &query.predicate {
            LogPredicate::MessageTokenRegex(_) | LogPredicate::MessageTokenPrefix { .. } => {
                let stats = self.cached_message_token_stats(&cached, frame.record_count)?;
                stats.candidate_ordinals_for_predicate(&query.predicate)
            }
            _ => None,
        };
        if let Some(candidates) = token_stats_candidate {
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        if allow_structural_message_fast_path
            && query.limit.is_none()
            && query.terms.is_empty()
            && message_predicate_is_message_only(&query.predicate)
        {
            let ordinals = (0..frame.record_count).collect::<Vec<_>>();
            let messages = decode_structural_messages_with_embedded_index_and_templates(
                &cached.structural,
                &ordinals,
                &cached.embedded_index,
                &cached.templates,
            )?;
            let candidates = ordinals
                .into_iter()
                .zip(messages)
                .filter_map(|(ordinal, message)| {
                    query
                        .message_candidate_matches(&message)
                        .unwrap_or(false)
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(
                    Arc::clone(cache_key.expect("message predicate cache key is present")),
                    Arc::from(candidates.clone()),
                );
            return Ok(Some(candidates));
        }
        let stats = self.cached_message_token_stats(&cached, frame.record_count)?;
        let postings = &stats.postings;
        if let Some((tokens, minimum)) = message_token_min_match_shape(&query.predicate) {
            let candidates = cached_message_token_min_match_candidates(
                postings,
                frame.record_count,
                &tokens,
                minimum,
            );
            if let Some(key) = cache_key {
                cached
                    .message_predicate_candidates
                    .lock()
                    .expect("indexed frame message predicate cache lock is not poisoned")
                    .insert(Arc::clone(key), Arc::from(candidates.clone()));
            }
            return Ok(Some(candidates));
        }
        fn token_posting_candidates(
            postings: &MessageTokenPostings,
            mut matches_token: impl FnMut(&str) -> bool,
        ) -> Vec<u32> {
            let mut candidates = Vec::new();
            for (token, posting) in postings {
                if matches_token(token) {
                    union_sorted_ordinals(&mut candidates, posting.ordinals.to_vec());
                }
            }
            candidates
        }
        fn exact_token_candidates(postings: &MessageTokenPostings, token: &str) -> Vec<u32> {
            postings
                .get(normalize_term(token).as_ref())
                .map(|posting| posting.ordinals.to_vec())
                .unwrap_or_default()
        }
        fn phrase_candidates(
            postings: &MessageTokenPostings,
            terms: &[Arc<str>],
            record_count: u32,
        ) -> Vec<u32> {
            if terms.is_empty() {
                return (0..record_count).collect();
            }
            let mut current = None;
            for term in terms {
                let candidates = exact_token_candidates(postings, term);
                intersect_frame_candidate_slice(&mut current, &candidates);
                if current.as_ref().is_some_and(Vec::is_empty) {
                    return Vec::new();
                }
            }
            current.unwrap_or_default()
        }
        fn candidates_for(
            predicate: &LogPredicate,
            postings: &MessageTokenPostings,
            record_count: u32,
        ) -> Option<Vec<u32>> {
            match predicate {
                LogPredicate::MatchAll => Some((0..record_count).collect()),
                LogPredicate::MatchNone => Some(Vec::new()),
                LogPredicate::Term(term) | LogPredicate::MessageToken { value: term, .. } => {
                    Some(exact_token_candidates(postings, term))
                }
                LogPredicate::MessageTokenPrefix { value, .. } => {
                    let prefix = normalize_term(value);
                    Some(token_posting_candidates(postings, |token| {
                        token.starts_with(prefix.as_ref())
                    }))
                }
                LogPredicate::MessageTokenRegex(regex) => {
                    if regex.case_sensitivity() == CaseSensitivity::Sensitive
                        && regex
                            .pattern()
                            .bytes()
                            .any(|byte| byte.is_ascii_uppercase())
                    {
                        return Some((0..record_count).collect());
                    }
                    Some(token_posting_candidates(postings, |token| {
                        regex.is_match(token)
                    }))
                }
                LogPredicate::MessageFuzzy {
                    value,
                    max_distance,
                } => {
                    let value = normalize_term(value);
                    Some(token_posting_candidates(postings, |token| {
                        bounded_levenshtein(token, value.as_ref(), usize::from(*max_distance))
                    }))
                }
                LogPredicate::MessagePhrase { terms, .. } => {
                    Some(phrase_candidates(postings, terms, record_count))
                }
                LogPredicate::MessageRegex(regex) => {
                    let literals = crate::query::regex_required_literals(regex.pattern())?;
                    if let Some(literal) = regex_boundary_safe_literal(regex.pattern()) {
                        return Some(exact_token_candidates(postings, literal));
                    }
                    Some(token_posting_candidates(postings, |token| {
                        literals.iter().any(|literal| {
                            normalize_term(token).contains(normalize_term(literal).as_ref())
                        })
                    }))
                }
                LogPredicate::Message(matcher)
                    if matcher.kind != crate::TextMatchKind::Exact
                        && !matcher.value.is_empty()
                        && matcher
                            .value
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric()) =>
                {
                    let literal = normalize_term(&matcher.value);
                    Some(token_posting_candidates(postings, |token| {
                        token.contains(literal.as_ref())
                    }))
                }
                LogPredicate::Message(_) => None,
                LogPredicate::And(predicates) => {
                    let mut current = None;
                    for predicate in predicates {
                        let Some(candidates) = candidates_for(predicate, postings, record_count)
                        else {
                            continue;
                        };
                        intersect_frame_candidate_slice(&mut current, &candidates);
                        if current.as_ref().is_some_and(Vec::is_empty) {
                            return Some(Vec::new());
                        }
                    }
                    current
                }
                LogPredicate::Or(predicates) => {
                    let mut current = Vec::new();
                    for predicate in predicates {
                        let candidates = candidates_for(predicate, postings, record_count)?;
                        union_sorted_ordinals(&mut current, candidates);
                    }
                    Some(current)
                }
                LogPredicate::Not(predicate) => {
                    let excluded = candidates_for(predicate, postings, record_count)?;
                    let mut candidates =
                        Vec::with_capacity((record_count as usize).saturating_sub(excluded.len()));
                    let mut next = 0_u32;
                    for ordinal in excluded {
                        if ordinal < next || ordinal >= record_count {
                            continue;
                        }
                        candidates.extend(next..ordinal);
                        next = ordinal.saturating_add(1);
                    }
                    if next < record_count {
                        candidates.extend(next..record_count);
                    }
                    Some(candidates)
                }
                LogPredicate::FieldExists(_)
                | LogPredicate::Field { .. }
                | LogPredicate::FieldIn { .. }
                | LogPredicate::FieldRegex { .. }
                | LogPredicate::FieldNumeric { .. } => None,
            }
        }
        let candidates = candidates_for(&query.predicate, postings, frame.record_count);
        if let (Some(key), Some(candidates)) = (cache_key, candidates.as_ref()) {
            cached
                .message_predicate_candidates
                .lock()
                .expect("indexed frame message predicate cache lock is not poisoned")
                .insert(Arc::clone(key), Arc::from(candidates.clone()));
        }
        Ok(candidates)
    }

    fn cached_exact_fields(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        requested: &[(Arc<str>, Arc<str>)],
    ) -> TelemetryResult<Vec<Option<Arc<[u32]>>>> {
        let missing = {
            let cached_fields = cached
                .exact_fields
                .lock()
                .expect("exact frame field cache lock is not poisoned");
            let missing = requested
                .iter()
                .filter(|field| !cached_fields.contains_key(*field))
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(requested
                    .iter()
                    .map(|field| cached_fields.get(field).cloned())
                    .collect());
            }
            missing
        };
        if !missing.is_empty() {
            let mut ordinals = Vec::new();
            for (key, value) in &missing {
                union_sorted_ordinals(
                    &mut ordinals,
                    cached.embedded_index.field_candidate_ordinals(key, value),
                );
            }
            let mut wanted_keys = missing
                .iter()
                .map(|(key, _)| key.as_ref())
                .collect::<Vec<_>>();
            wanted_keys.sort_unstable();
            wanted_keys.dedup();
            let fields = crate::structural::decode_structural_fields_for_keys(
                &cached.structural,
                &ordinals,
                &wanted_keys,
            )?;
            let mut postings = missing
                .iter()
                .map(|field| (field.clone(), Vec::new()))
                .collect::<Vec<_>>();
            for (ordinal, fields) in ordinals.into_iter().zip(fields) {
                let mut seen = Vec::<usize>::new();
                for field in fields.iter() {
                    let Some(index) = missing
                        .iter()
                        .position(|expected| expected.0 == field.key && expected.1 == field.value)
                    else {
                        continue;
                    };
                    if seen.contains(&index) {
                        continue;
                    }
                    seen.push(index);
                    postings[index].1.push(ordinal);
                }
            }
            let computed = postings
                .into_iter()
                .map(|(field, ordinals)| (field, Arc::<[u32]>::from(ordinals)))
                .collect::<HashMap<_, _>>();
            let mut cached_fields = cached
                .exact_fields
                .lock()
                .expect("exact frame field cache lock is not poisoned");
            for (field, posting) in &computed {
                if cached_fields.contains_key(field)
                    || cached_fields.len() < MAX_EXACT_FRAME_QUERY_FIELDS
                {
                    cached_fields.insert(field.clone(), Arc::clone(posting));
                }
            }
            return Ok(requested
                .iter()
                .map(|field| {
                    cached_fields
                        .get(field)
                        .cloned()
                        .or_else(|| computed.get(field).cloned())
                })
                .collect());
        }
        let cached_fields = cached
            .exact_fields
            .lock()
            .expect("exact frame field cache lock is not poisoned");
        Ok(requested
            .iter()
            .map(|field| cached_fields.get(field).cloned())
            .collect())
    }

    fn cached_field_postings(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
        key: &str,
    ) -> TelemetryResult<Option<Arc<CachedFieldPostings>>> {
        let key = Arc::<str>::from(key);
        {
            let field_postings = cached
                .field_postings
                .lock()
                .expect("indexed frame field postings lock is not poisoned");
            if let Some(postings) = field_postings.get(&key) {
                return Ok(Some(Arc::clone(postings)));
            }
        }

        let ordinals = (0..record_count).collect::<Vec<_>>();
        let fields = crate::structural::decode_structural_fields_for_keys(
            &cached.structural,
            &ordinals,
            &[key.as_ref()],
        )?;
        let mut values = HashMap::<Arc<str>, Vec<u32>>::new();
        let mut presence = Vec::new();
        let mut value_ids = HashMap::<Arc<str>, u32>::new();
        let mut value_table = Vec::<Arc<str>>::new();
        let mut ordinal_value_ids =
            vec![u32::MAX; usize::try_from(record_count).unwrap_or_default()];
        for (ordinal, fields) in ordinals.into_iter().zip(fields) {
            for field in fields.iter().filter(|field| field.key == key) {
                let value_id = if let Some(value_id) = value_ids.get(&field.value) {
                    *value_id
                } else {
                    if values.len() >= MAX_INDEXED_FRAME_FIELD_VALUES {
                        return Ok(None);
                    }
                    let value_id =
                        u32::try_from(value_table.len()).expect("indexed field values fit in u32");
                    value_ids.insert(Arc::clone(&field.value), value_id);
                    value_table.push(Arc::clone(&field.value));
                    value_id
                };
                if let Some(slot) = ordinal_value_ids.get_mut(ordinal as usize)
                    && *slot == u32::MAX
                {
                    *slot = value_id;
                }
                let posting = values.entry(Arc::clone(&field.value)).or_default();
                if posting.last().copied() != Some(ordinal) {
                    posting.push(ordinal);
                }
                if presence.last().copied() != Some(ordinal) {
                    presence.push(ordinal);
                }
            }
        }
        let postings = Arc::new(CachedFieldPostings {
            values: values
                .into_iter()
                .map(|(value, ordinals)| (value, Arc::<[u32]>::from(ordinals)))
                .collect(),
            presence: Arc::from(presence),
            ordinal_value_ids: Arc::from(ordinal_value_ids),
            value_table: Arc::from(value_table),
        });
        let mut field_postings = cached
            .field_postings
            .lock()
            .expect("indexed frame field postings lock is not poisoned");
        if let Some(existing) = field_postings.get(&key) {
            return Ok(Some(Arc::clone(existing)));
        }
        if field_postings.len() < MAX_INDEXED_FRAME_FIELD_KEYS {
            field_postings.insert(Arc::clone(&key), Arc::clone(&postings));
        }
        drop(field_postings);
        self.indexed_frame_query_cache
            .lock()
            .expect("indexed frame query cache lock is not poisoned")
            .enforce_budget();
        Ok(Some(postings))
    }

    fn cached_field_value_candidates(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        record_count: u32,
        key: &str,
        mut matches_value: impl FnMut(&str) -> bool,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let Some(postings) = self.cached_field_postings(cached, record_count, key)? else {
            return Ok(None);
        };
        let mut candidates = Vec::new();
        for (value, posting) in &postings.values {
            if matches_value(value) {
                candidates.extend(posting.iter().copied());
            }
        }
        candidates.sort_unstable();
        candidates.dedup();
        Ok(Some(candidates))
    }

    fn indexed_frame_field_predicate_candidates(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        candidates: &[u32],
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let required = query.required_index_constraints();
        let has_field_constraints = !required.field_exists.is_empty()
            || !required.field_in.is_empty()
            || !required.field_text.is_empty()
            || !required.field_regex.is_empty()
            || !required.field_numeric.is_empty();
        if !has_field_constraints {
            return Ok(None);
        }
        Ok(Some(self.indexed_frame_field_predicate_candidates_owned(
            query,
            frame,
            candidates.to_vec(),
        )?))
    }

    fn indexed_frame_field_predicate_candidates_owned(
        &self,
        query: &LogQuery,
        frame: &IndexedIngestFrame,
        candidates: Vec<u32>,
    ) -> TelemetryResult<Vec<u32>> {
        let required = query.required_index_constraints();
        let has_field_constraints = !required.field_exists.is_empty()
            || !required.field_in.is_empty()
            || !required.field_text.is_empty()
            || !required.field_regex.is_empty()
            || !required.field_numeric.is_empty();
        if !has_field_constraints {
            return Ok(candidates);
        }
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let mut current = Some(candidates);
        for key in &required.field_exists {
            let Some(field_postings) =
                self.cached_field_postings(&cached, frame.record_count, key)?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            let postings = field_postings.presence.as_ref();
            intersect_frame_candidate_slice(&mut current, postings);
        }
        for (key, values) in &required.field_in {
            let mut postings = Vec::new();
            for value in values {
                union_sorted_ordinals(
                    &mut postings,
                    cached.embedded_index.field_candidate_ordinals(key, value),
                );
            }
            intersect_frame_candidate_slice(&mut current, &postings);
            let current_candidates = current.as_deref().unwrap_or(&[]);
            if current_candidates.is_empty() {
                continue;
            }
            let fields = crate::structural::decode_structural_fields_for_keys(
                &cached.structural,
                current_candidates,
                &[key],
            )?;
            let verified = current_candidates
                .iter()
                .copied()
                .zip(fields)
                .filter_map(|(ordinal, fields)| {
                    fields
                        .iter()
                        .any(|field| {
                            field.key.as_ref() == *key
                                && values
                                    .iter()
                                    .any(|expected| field.value.as_ref() == *expected)
                        })
                        .then_some(ordinal)
                })
                .collect::<Vec<_>>();
            current = Some(verified);
        }
        for (key, matcher) in &required.field_text {
            let Some(postings) =
                self.cached_field_value_candidates(&cached, frame.record_count, key, |value| {
                    text_matches(value, matcher)
                })?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            intersect_frame_candidate_slice(&mut current, &postings);
        }
        for (key, regex) in &required.field_regex {
            let Some(postings) =
                self.cached_field_value_candidates(&cached, frame.record_count, key, |value| {
                    regex.is_match(value)
                })?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            intersect_frame_candidate_slice(&mut current, &postings);
        }
        for (key, comparison, target) in &required.field_numeric {
            let Some(postings) =
                self.cached_field_value_candidates(&cached, frame.record_count, key, |value| {
                    value
                        .parse::<i128>()
                        .is_ok_and(|observed| match *comparison {
                            NumericComparison::Equal => observed == *target,
                            NumericComparison::NotEqual => observed != *target,
                            NumericComparison::LessThan => observed < *target,
                            NumericComparison::LessThanOrEqual => observed <= *target,
                            NumericComparison::GreaterThan => observed > *target,
                            NumericComparison::GreaterThanOrEqual => observed >= *target,
                        })
                })?
            else {
                let current_candidates = current.as_deref().unwrap_or(&[]);
                return self.scan_indexed_frame_field_predicates(
                    &cached,
                    current_candidates,
                    &required,
                );
            };
            intersect_frame_candidate_slice(&mut current, &postings);
        }
        Ok(current.unwrap_or_default())
    }

    fn scan_indexed_frame_field_predicates(
        &self,
        cached: &Arc<CachedIndexedFrame>,
        candidates: &[u32],
        required: &crate::query::RequiredIndexConstraints<'_>,
    ) -> TelemetryResult<Vec<u32>> {
        let fields = decode_structural_fields(&cached.structural, candidates)?;
        let mut matches = Vec::with_capacity(candidates.len());
        for (ordinal, fields) in candidates.iter().copied().zip(fields) {
            let exists = required
                .field_exists
                .iter()
                .all(|key| fields.iter().any(|field| field.key.as_ref() == *key));
            let in_values = required.field_in.iter().all(|(key, values)| {
                fields.iter().any(|field| {
                    field.key.as_ref() == *key
                        && values
                            .iter()
                            .any(|expected| field.value.as_ref() == *expected)
                })
            });
            let text = required.field_text.iter().all(|(key, matcher)| {
                fields
                    .iter()
                    .any(|field| field.key.as_ref() == *key && text_matches(&field.value, matcher))
            });
            let regex = required.field_regex.iter().all(|(key, regex)| {
                fields
                    .iter()
                    .any(|field| field.key.as_ref() == *key && regex.is_match(&field.value))
            });
            let numeric = required
                .field_numeric
                .iter()
                .all(|(key, comparison, target)| {
                    fields.iter().any(|field| {
                        field.key.as_ref() == *key
                            && field
                                .value
                                .parse::<i128>()
                                .is_ok_and(|observed| match *comparison {
                                    NumericComparison::Equal => observed == *target,
                                    NumericComparison::NotEqual => observed != *target,
                                    NumericComparison::LessThan => observed < *target,
                                    NumericComparison::LessThanOrEqual => observed <= *target,
                                    NumericComparison::GreaterThan => observed > *target,
                                    NumericComparison::GreaterThanOrEqual => observed >= *target,
                                })
                    })
                });
            if exists && in_values && text && regex && numeric {
                matches.push(ordinal);
            }
        }
        Ok(matches)
    }

    /// Counts one tenant's records without reading or reconstructing payloads.
    ///
    /// Tenant identity is exact append metadata for compressed and tiered
    /// frames. The legacy hot-record path uses its exact (non-hashed) posting
    /// table. Consequently fingerprint collisions can never change this count.
    pub(crate) fn count_tenant_records(
        &self,
        tenant: &str,
        partitions: &[TopicPartition],
    ) -> TelemetryResult<u64> {
        let mut total = 0_u64;
        for topic_partition in partitions {
            if let Some(partition) = self.partitions.get(topic_partition)
                && let Some(posting) = partition
                    .field_ids
                    .get("resource.loki.tenant")
                    .and_then(|values| values.get(tenant))
                    .and_then(|field_id| partition.field_postings.get(*field_id))
            {
                total = total
                    .checked_add(
                        u64::try_from(posting.cardinality)
                            .map_err(|_| TelemetryError::RecordTooLarge)?,
                    )
                    .ok_or(TelemetryError::RecordTooLarge)?;
            }
            if let Some(partition) = self.indexed_frame_partitions.get(topic_partition) {
                for append in &partition.appends {
                    if append.tenant.as_ref() == tenant {
                        total = total
                            .checked_add(u64::from(append.record_count))
                            .ok_or(TelemetryError::RecordTooLarge)?;
                    }
                }
            }
            total = total
                .checked_add(self.count_tiered_tenant_records(*topic_partition, tenant)?)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
        Ok(total)
    }

    /// Returns only logical partitions that currently contain this tenant.
    ///
    /// The directory is reconstructed from hot postings, compressed-frame
    /// append metadata, and object-tier catalogs, so callers do not need to
    /// enumerate every configured logical partition after restart or offload.
    pub(crate) fn tenant_partitions(
        &mut self,
        tenant: &str,
    ) -> TelemetryResult<Vec<TopicPartition>> {
        if let Some(partitions) = self.active_partition_cache.get(tenant) {
            return Ok(partitions.clone());
        }
        let mut candidates = self.partitions.keys().copied().collect::<Vec<_>>();
        candidates.extend(self.indexed_frame_partitions.keys().copied());
        if let Some(state) = &self.tier {
            candidates.extend(state.tiers.keys().copied());
        }
        candidates.sort_unstable();
        candidates.dedup();

        let mut matches = Vec::with_capacity(candidates.len());
        for partition in candidates {
            if self.count_tenant_records(tenant, std::slice::from_ref(&partition))? > 0 {
                matches.push(partition);
            }
        }
        self.active_partition_cache
            .insert(Arc::from(tenant), matches.clone());
        Ok(matches)
    }

    fn read_tier_ingest_group_cached(
        &self,
        tier: &TelemetryObjectTier<SharedTelemetryObjectStore>,
        query_artifact: &TierArtifact,
        blocks: &[crate::TierBlockEntry],
        control_cache: &SsdObjectCache,
    ) -> TelemetryResult<Arc<[DecodedTierIngestAppend]>> {
        if let Some(appends) = control_cache.parsed_tier_ingest_hit(&query_artifact.object_key)? {
            return Ok(appends);
        }
        let query_index = tier.read_artifact_cached(
            query_artifact,
            MAX_TIER_QUERY_INDEX_READ_BYTES,
            control_cache,
        )?;
        let appends =
            Arc::<[DecodedTierIngestAppend]>::from(decode_tier_ingest_group(&query_index, blocks)?);
        control_cache.admit_parsed_tier_ingest(
            query_artifact.object_key.clone(),
            Arc::clone(&appends),
            query_artifact.bytes,
        )?;
        Ok(appends)
    }

    fn count_tiered_tenant_records(
        &self,
        topic_partition: TopicPartition,
        tenant: &str,
    ) -> TelemetryResult<u64> {
        let Some(state) = &self.tier else {
            return Ok(0);
        };
        let Some(tier) = state.tiers.get(&topic_partition) else {
            return Ok(0);
        };
        let groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: None,
                last_offset: None,
                min_timestamp_unix_nanos: None,
                max_timestamp_unix_nanos: None,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        let mut total = 0_u64;
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            for append in appends.iter() {
                if append.tenant == tenant {
                    total = total
                        .checked_add(u64::from(append.record_count))
                        .ok_or(TelemetryError::RecordTooLarge)?;
                }
            }
        }
        Ok(total)
    }

    fn query_hot_matches(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> Vec<LogMatch> {
        if let Some(matches) =
            self.query_hot_single_posting_matches(query, include_typed_metadata, include_fields)
        {
            return matches;
        }
        self.partitions
            .get(&query.topic_partition)
            .map(|partition| {
                let ordinals = self.query_ordinals(query, partition);
                let mut matches = Vec::with_capacity(ordinals.len());
                for ordinal in ordinals {
                    if let Some(record) = partition.records.get(ordinal as usize) {
                        let record = if include_typed_metadata {
                            record.record.clone()
                        } else {
                            let mut projected = DurableLog::new_projected(
                                record.record.stream_shard_id,
                                record.record.record_ref.topic_partition,
                                record.record.record_ref.offset,
                                record.record.timestamp_unix_nanos,
                                Arc::clone(&record.record.message),
                                record.record.compression_cohort,
                            );
                            projected.severity_text = Arc::clone(&record.record.severity_text);
                            if include_fields {
                                projected.fields = Arc::clone(&record.record.fields);
                            }
                            projected
                        };
                        matches.push(LogMatch { record });
                    }
                }
                matches
            })
            .unwrap_or_default()
    }

    fn query_hot_single_posting_matches(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> Option<Vec<LogMatch>> {
        if query.sort != crate::QuerySort::Offset
            || !query.terms.is_empty()
            || !query.exact_fields.is_empty()
            || query.start_offset.is_some()
            || query.end_offset.is_some()
            || query.start_timestamp_unix_nanos.is_some()
            || query.end_timestamp_unix_nanos.is_some()
            || query.after.is_some()
            || !hot_single_posting_predicate(&query.predicate)
        {
            return None;
        }
        let partition = self.partitions.get(&query.topic_partition)?;
        let posting = hot_predicate_driver_posting(
            &query.predicate,
            partition,
            0,
            u32::try_from(partition.records.len()).expect("record count was bounded"),
        )?;
        let take = query.limit.unwrap_or(usize::MAX);
        let record_end = u32::try_from(partition.records.len()).expect("record count was bounded");
        let mut matches = Vec::with_capacity(take.min(posting.cardinality_in(0, record_end)));
        posting.visit_in(0, record_end, query.order, |ordinal| {
            if let Some(record) = partition.records.get(ordinal as usize) {
                let record = if include_typed_metadata {
                    record.record.clone()
                } else {
                    let mut projected = DurableLog::new_projected(
                        record.record.stream_shard_id,
                        record.record.record_ref.topic_partition,
                        record.record.record_ref.offset,
                        record.record.timestamp_unix_nanos,
                        Arc::clone(&record.record.message),
                        record.record.compression_cohort,
                    );
                    projected.severity_text = Arc::clone(&record.record.severity_text);
                    if include_fields {
                        projected.fields = Arc::clone(&record.record.fields);
                    }
                    projected
                };
                matches.push(LogMatch { record });
            }
            matches.len() < take
        });
        Some(matches)
    }

    /// Returns matching durable record references without cloning record data.
    ///
    /// Posting lists are offset ordered. The query starts with the shortest
    /// list and intersects each remaining list with a linear merge, making
    /// constraint order irrelevant to the asymptotic cost.
    #[must_use]
    pub fn query_refs(&self, query: &LogQuery) -> Vec<TelemetryRecordRef> {
        self.query_refs_checked(query).unwrap_or_default()
    }

    fn query_refs_checked(&self, query: &LogQuery) -> TelemetryResult<Vec<TelemetryRecordRef>> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Ok(Vec::new());
        }
        if self
            .indexed_frame_partitions
            .get(&query.topic_partition)
            .is_none_or(|partition| partition.appends.is_empty())
            && self.tier.is_none()
        {
            let Some(partition) = self.partitions.get(&query.topic_partition) else {
                return Ok(Vec::new());
            };
            let ordinals = self.query_ordinals(query, partition);
            let mut refs = Vec::with_capacity(ordinals.len());
            for ordinal in ordinals {
                if let Some(record) = partition.records.get(ordinal as usize) {
                    refs.push(record.record.record_ref);
                }
            }
            if let Some(limit) = query.limit {
                refs.truncate(limit);
            }
            return Ok(refs);
        }
        if query.sort != crate::QuerySort::Offset || self.tier.is_some() {
            return Ok(self
                .query(query)
                .into_iter()
                .map(|matched| matched.record.record_ref)
                .collect());
        }
        let Some(partition) = self.indexed_frame_partitions.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let hot_partition = self
            .partitions
            .get(&query.topic_partition)
            .filter(|partition| !partition.records.is_empty());
        let mut refs = if let Some(hot_partition) = hot_partition {
            let mut hot_query = query.clone();
            hot_query.limit = None;
            self.query_ordinals(&hot_query, hot_partition)
                .into_iter()
                .filter_map(|ordinal| hot_partition.records.get(ordinal as usize))
                .map(|record| record.record.record_ref)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        for append in &partition.appends {
            if !append_matches_query_bounds(query, append) {
                continue;
            }
            for frame in &append.frames {
                if frame_matches_query_bounds(query, frame) {
                    refs.extend(self.query_indexed_frame_refs(query, append, frame)?);
                }
            }
        }
        refs.sort_unstable_by_key(|record_ref| record_ref.offset);
        if query.order == QueryOrder::NewestFirst {
            refs.reverse();
        }
        if let Some(limit) = query.limit {
            refs.truncate(limit);
        }
        Ok(refs)
    }

    fn query_indexed_frame_refs(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Vec<TelemetryRecordRef>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let exact_candidates_are_exact = exact_tokens
            .as_deref()
            .is_some_and(|tokens| !tokens.is_empty() || !exact_fields.is_empty());
        let candidates = if let Some(tokens) = exact_tokens
            .as_deref()
            .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
        {
            self.exact_indexed_frame_candidates(query, append, frame, tokens, &exact_fields)?
                .unwrap_or_else(|| {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                })
        } else {
            indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            )
        };
        let candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let decoded = decode_structural_records_with_cached_frame_data(
            &cached.structural,
            &candidates,
            &cached.embedded_index,
            &cached.templates,
            &cached.offsets,
            &cached.timestamps,
            false,
            true,
            None,
            None,
            Some(&cached.attribute_tables),
        )?;
        let mut refs = Vec::with_capacity(decoded.len());
        for record in &decoded {
            let absolute_offset = append
                .first_offset
                .get()
                .checked_add(record.offset.get())
                .map(LogicalOffset::new)
                .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
            let view = AbsoluteDecodedRecordView {
                record,
                absolute_offset,
            };
            if exact_candidates_are_exact || query.matches_index_candidate(&view) {
                refs.push(TelemetryRecordRef::new(
                    query.topic_partition,
                    absolute_offset,
                ));
            }
        }
        Ok(refs)
    }

    fn query_indexed_frames(
        &self,
        query: &LogQuery,
        partition: &IndexedFramePartition,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Ok(Vec::new());
        }
        let mut matches = Vec::new();
        for append in &partition.appends {
            if !append_matches_query_bounds(query, append) {
                continue;
            }
            for frame in &append.frames {
                if !frame_matches_query_bounds(query, frame) {
                    continue;
                }
                matches.extend(self.query_indexed_frame(
                    query,
                    append,
                    frame,
                    include_typed_metadata,
                    include_fields,
                )?);
            }
        }
        Ok(matches)
    }

    fn query_tiered_groups(
        &self,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let Some(state) = &self.tier else {
            return Ok(Vec::new());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let exact_candidates_are_exact = exact_tokens
            .as_deref()
            .is_some_and(|tokens| !tokens.is_empty() || !exact_fields.is_empty());
        let mut groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        match query.order {
            QueryOrder::NewestFirst => groups.sort_unstable_by(|left, right| {
                right
                    .max_timestamp_unix_nanos
                    .cmp(&left.max_timestamp_unix_nanos)
            }),
            QueryOrder::OldestFirst => groups.sort_unstable_by(|left, right| {
                left.min_timestamp_unix_nanos
                    .cmp(&right.min_timestamp_unix_nanos)
            }),
        }
        let mut matches: Vec<LogMatch> = Vec::new();
        for group in groups {
            if query.sort == crate::QuerySort::Timestamp
                && let Some(limit) = query.limit
                && matches.len() >= limit
            {
                let boundary = matches
                    .last()
                    .expect("a full tier result page has a boundary")
                    .record
                    .timestamp_unix_nanos;
                let cannot_improve = match query.order {
                    QueryOrder::NewestFirst => group.max_timestamp_unix_nanos < boundary,
                    QueryOrder::OldestFirst => group.min_timestamp_unix_nanos > boundary,
                };
                if cannot_improve {
                    break;
                }
            }
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds) {
                    continue;
                }
                if query.exact_fields.iter().any(|field| {
                    field.key.as_ref() == "resource.loki.tenant"
                        && field.value.as_ref() != bounds.tenant.as_ref()
                }) {
                    continue;
                }
                for cold_frame in &append.frames {
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    let exact_candidates_cached = cached_exact_candidates.is_some();
                    let cached_message_candidates = (query.terms.is_empty()
                        && exact_fields.is_empty())
                    .then(|| {
                        self.cached_message_predicate_candidates_if_present(
                            cold_frame.frame_id,
                            &query.predicate,
                        )
                    })
                    .flatten();
                    let candidates = cached_exact_candidates
                        .or(cached_message_candidates)
                        .unwrap_or_else(|| {
                            indexed_frame_candidates_for_append(
                                query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                bounds.tenant.as_ref(),
                            )
                        });
                    if candidates.is_empty()
                        || !timestamp_bounds_overlap(
                            query,
                            cold_frame.min_timestamp_unix_nanos,
                            cold_frame.max_timestamp_unix_nanos,
                        )
                    {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame,
                        candidates,
                        range_index,
                        exact_candidates_cached,
                    ));
                }
            }
            if query.sort == crate::QuerySort::Timestamp && query.limit.is_some() {
                selected.sort_unstable_by(|left, right| match query.order {
                    QueryOrder::NewestFirst => right
                        .4
                        .max_timestamp_unix_nanos
                        .cmp(&left.4.max_timestamp_unix_nanos),
                    QueryOrder::OldestFirst => left
                        .4
                        .min_timestamp_unix_nanos
                        .cmp(&right.4.min_timestamp_unix_nanos),
                });
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                candidates,
                range_index,
                exact_candidates_cached,
            ) in selected
            {
                if query.sort == crate::QuerySort::Timestamp
                    && let Some(limit) = query.limit
                    && matches.len() >= limit
                {
                    let boundary = matches
                        .last()
                        .expect("a full tier result page has a boundary")
                        .record
                        .timestamp_unix_nanos;
                    let cannot_improve = match query.order {
                        QueryOrder::NewestFirst => cold_frame.max_timestamp_unix_nanos < boundary,
                        QueryOrder::OldestFirst => cold_frame.min_timestamp_unix_nanos > boundary,
                    };
                    if cannot_improve {
                        break;
                    }
                }
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index.clone(),
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                let candidates = if exact_candidates_cached {
                    candidates
                } else if let Some(tokens) = exact_tokens
                    .as_deref()
                    .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                {
                    self.exact_indexed_frame_candidates(
                        query,
                        &bounds,
                        &frame,
                        tokens,
                        &exact_fields,
                    )?
                    .unwrap_or(candidates)
                } else {
                    candidates
                };
                matches.extend(self.decode_indexed_frame_candidates(
                    query,
                    &bounds,
                    &frame,
                    candidates,
                    include_typed_metadata,
                    include_fields,
                    exact_candidates_are_exact,
                )?);
                if query.sort == crate::QuerySort::Timestamp
                    && let Some(limit) = query.limit
                {
                    sort_and_limit_matches(&mut matches, query, limit);
                }
            }
        }
        Ok(matches)
    }

    fn query_tiered_groups_messages(
        &self,
        query: &LogQuery,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        let Some(state) = &self.tier else {
            return Ok(Vec::new());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let mut matches = Vec::new();
        let groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges: Vec<std::ops::Range<u64>> = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds)
                    || query.exact_fields.iter().any(|field| {
                        field.key.as_ref() == "resource.loki.tenant"
                            && field.value.as_ref() != bounds.tenant.as_ref()
                    })
                {
                    continue;
                }
                for cold_frame in &append.frames {
                    let message_cache_can_supply_the_base =
                        query.exact_message_token_conjunction().is_none()
                            && message_cache_can_supply_frame_base(query, bounds.tenant.as_ref());
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    let exact_candidates_cached = cached_exact_candidates.is_some();
                    let candidates = cached_exact_candidates.unwrap_or_else(|| {
                        if message_cache_can_supply_the_base {
                            Vec::new()
                        } else {
                            indexed_frame_candidates_for_append(
                                query,
                                &cold_frame.index,
                                cold_frame.record_count,
                                bounds.tenant.as_ref(),
                            )
                        }
                    });
                    if (!message_cache_can_supply_the_base && candidates.is_empty())
                        || !timestamp_bounds_overlap(
                            query,
                            cold_frame.min_timestamp_unix_nanos,
                            cold_frame.max_timestamp_unix_nanos,
                        )
                    {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame,
                        candidates,
                        range_index,
                        exact_candidates_cached,
                        message_cache_can_supply_the_base,
                    ));
                }
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                candidates,
                range_index,
                exact_candidates_cached,
                message_cache_can_supply_the_base,
            ) in selected
            {
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index.clone(),
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                let candidates = if message_cache_can_supply_the_base || exact_candidates_cached {
                    candidates
                } else if let Some(tokens) = exact_tokens
                    .as_deref()
                    .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                {
                    self.exact_indexed_frame_candidates(
                        query,
                        &bounds,
                        &frame,
                        tokens,
                        &exact_fields,
                    )?
                    .unwrap_or(candidates)
                } else {
                    candidates
                };
                let frame_matches = self.decode_indexed_frame_messages(
                    query,
                    &bounds,
                    &frame,
                    &candidates,
                    message_predicate_key,
                )?;
                matches.reserve(frame_matches.len());
                matches.extend(frame_matches);
            }
        }
        Ok(matches)
    }

    fn query_tiered_groups_trace_ids(
        &self,
        query: &LogQuery,
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<Vec<TraceId>> {
        let Some(state) = &self.tier else {
            return Ok(Vec::new());
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(Vec::new());
        };
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        let mut trace_ids = Vec::new();
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges: Vec<std::ops::Range<u64>> = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds)
                    || query.exact_fields.iter().any(|field| {
                        field.key.as_ref() == "resource.loki.tenant"
                            && field.value.as_ref() != bounds.tenant.as_ref()
                    })
                {
                    continue;
                }
                for cold_frame in &append.frames {
                    let cached_exact_candidates = exact_tokens
                        .as_deref()
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                &exact_fields,
                            )
                        });
                    let exact_candidates_cached = cached_exact_candidates.is_some();
                    let candidates = cached_exact_candidates.unwrap_or_else(|| {
                        indexed_frame_candidates_for_append(
                            query,
                            &cold_frame.index,
                            cold_frame.record_count,
                            bounds.tenant.as_ref(),
                        )
                    });
                    if candidates.is_empty()
                        || !timestamp_bounds_overlap(
                            query,
                            cold_frame.min_timestamp_unix_nanos,
                            cold_frame.max_timestamp_unix_nanos,
                        )
                    {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame,
                        candidates,
                        range_index,
                        exact_candidates_cached,
                    ));
                }
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                candidates,
                range_index,
                exact_candidates_cached,
            ) in selected
            {
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index.clone(),
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                let candidates = if exact_candidates_cached {
                    candidates
                } else if let Some(tokens) = exact_tokens
                    .as_deref()
                    .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                {
                    self.exact_indexed_frame_candidates(
                        query,
                        &bounds,
                        &frame,
                        tokens,
                        &exact_fields,
                    )?
                    .unwrap_or(candidates)
                } else {
                    candidates
                };
                trace_ids.extend(self.decode_indexed_frame_trace_ids(
                    query,
                    &bounds,
                    &frame,
                    &candidates,
                    message_predicate_key,
                )?);
            }
        }
        Ok(trace_ids)
    }

    fn count_tiered_groups(
        &self,
        query: &LogQuery,
        exact_tokens: Option<&[(&str, CaseSensitivity)]>,
        exact_fields: &[(Arc<str>, Arc<str>)],
        message_predicate_key: Option<&Arc<str>>,
    ) -> TelemetryResult<u64> {
        let Some(state) = &self.tier else {
            return Ok(0);
        };
        let Some(tier) = state.tiers.get(&query.topic_partition) else {
            return Ok(0);
        };
        let groups = tier.candidate_groups_cached(
            TierQueryRange {
                first_offset: query.start_offset.map(LogicalOffset::get),
                last_offset: query.end_offset.map(LogicalOffset::get),
                min_timestamp_unix_nanos: query.start_timestamp_unix_nanos,
                max_timestamp_unix_nanos: query.end_timestamp_unix_nanos,
                signal_identity: None,
            },
            &state.control_cache,
        )?;
        let mut total = 0_u64;
        for group in groups {
            let manifest = tier.load_group_cached(&group, &state.control_cache)?;
            let query_artifact = manifest
                .artifact(TierArtifactKind::QueryIndex)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no query index".into()))?;
            let appends = self.read_tier_ingest_group_cached(
                tier,
                query_artifact,
                &manifest.blocks,
                &state.control_cache,
            )?;
            let payload_artifact = manifest
                .artifact(TierArtifactKind::PayloadPack)
                .ok_or_else(|| TelemetryError::CorruptTier("group has no payload pack".into()))?;
            let payload_metadata = ObjectMetadata {
                bytes: payload_artifact.bytes,
                version_token: payload_artifact.checksum.clone(),
                content_digest: payload_artifact.checksum.clone(),
            };
            let mut selected = Vec::new();
            let mut ranges = Vec::new();
            for append in appends.iter() {
                let bounds = IndexedFrameAppend {
                    tenant: Arc::from(append.tenant.as_str()),
                    first_offset: append.first_offset,
                    last_offset: append.last_offset,
                    record_count: append.record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                if !append_matches_query_bounds(query, &bounds) {
                    continue;
                }
                if query.exact_fields.iter().any(|field| {
                    field.key.as_ref() == "resource.loki.tenant"
                        && field.value.as_ref() != bounds.tenant.as_ref()
                }) {
                    continue;
                }
                for cold_frame in &append.frames {
                    if exact_tokens.is_none()
                        && exact_fields.is_empty()
                        && query.terms.is_empty()
                        && cached_message_predicate_is_exact(&query.predicate)
                        && let Some(candidates) = self
                            .cached_message_predicate_candidates_arc_if_present(
                                cold_frame.frame_id,
                                message_predicate_key,
                            )
                        && let Some(count) = self.count_cached_message_predicate_candidates(
                            query,
                            cold_frame.frame_id,
                            &candidates,
                        )
                    {
                        total = total
                            .checked_add(count)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        continue;
                    }
                    let cached_exact_candidates = exact_tokens
                        .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
                        .and_then(|tokens| {
                            self.cached_exact_frame_candidates(
                                cold_frame.frame_id,
                                tokens,
                                exact_fields,
                            )
                        });
                    if let Some(candidates) = cached_exact_candidates.as_deref()
                        && let Some(count) = self.count_cached_exact_candidates(
                            query,
                            cold_frame.frame_id,
                            candidates,
                        )
                    {
                        total = total
                            .checked_add(count)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        continue;
                    }
                    let candidates = cached_exact_candidates.unwrap_or_else(|| {
                        indexed_frame_candidates_for_append(
                            query,
                            &cold_frame.index,
                            cold_frame.record_count,
                            bounds.tenant.as_ref(),
                        )
                    });
                    if candidates.is_empty()
                        || !timestamp_bounds_overlap(
                            query,
                            cold_frame.min_timestamp_unix_nanos,
                            cold_frame.max_timestamp_unix_nanos,
                        )
                    {
                        continue;
                    }
                    let range_index = if self
                        .cached_indexed_frame_if_present(cold_frame.frame_id)
                        .is_some()
                    {
                        None
                    } else {
                        let range_end = cold_frame
                            .payload_offset
                            .checked_add(cold_frame.payload_bytes)
                            .ok_or(TelemetryError::RecordTooLarge)?;
                        let range_index = ranges.len();
                        ranges.push(cold_frame.payload_offset..range_end);
                        Some(range_index)
                    };
                    selected.push((
                        Arc::clone(&bounds.tenant),
                        bounds.first_offset,
                        bounds.last_offset,
                        bounds.record_count,
                        cold_frame.clone(),
                        candidates,
                        range_index,
                    ));
                }
            }
            let mut payloads = if ranges.is_empty() {
                Vec::new()
            } else {
                state.payload_cache.read_ranges_with_metadata(
                    tier.object_store(),
                    &payload_artifact.object_key,
                    &payload_metadata,
                    &ranges,
                )?
            };
            for (
                tenant,
                first_offset,
                last_offset,
                record_count,
                cold_frame,
                _candidates,
                range_index,
            ) in selected
            {
                let compressed = range_index
                    .map(|index| Bytes::from(std::mem::take(&mut payloads[index])))
                    .unwrap_or_default();
                if range_index.is_some()
                    && blake3::hash(&compressed).to_hex().as_str() != cold_frame.payload_checksum
                {
                    return Err(TelemetryError::CorruptTier(format!(
                        "tiered frame {} payload checksum failed",
                        cold_frame.frame_id
                    )));
                }
                let frame = IndexedIngestFrame {
                    frame_id: cold_frame.frame_id,
                    cohort: cold_frame.cohort,
                    record_count: cold_frame.record_count,
                    structural_bytes: cold_frame.structural_bytes,
                    min_timestamp_unix_nanos: cold_frame.min_timestamp_unix_nanos,
                    max_timestamp_unix_nanos: cold_frame.max_timestamp_unix_nanos,
                    compressed,
                    index: cold_frame.index,
                };
                let bounds = IndexedFrameAppend {
                    tenant,
                    first_offset,
                    last_offset,
                    record_count,
                    frames: Vec::new(),
                    next_checkpoint: None,
                };
                total = total
                    .checked_add(self.count_indexed_frame_matches(
                        query,
                        &bounds,
                        &frame,
                        exact_tokens,
                        exact_fields,
                        message_predicate_key,
                    )?)
                    .ok_or(TelemetryError::RecordTooLarge)?;
            }
        }
        Ok(total)
    }

    fn query_indexed_frame(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        if query.sort == crate::QuerySort::Timestamp
            && query.limit.is_some_and(|limit| limit > 0)
            && let Some(candidates) =
                self.embedded_indexed_frame_candidates(query, append, frame)?
        {
            return self.decode_embedded_indexed_frame_candidates(
                query,
                append,
                frame,
                candidates,
                include_typed_metadata,
                include_fields,
            );
        }
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        let exact_candidates_are_exact = exact_tokens
            .as_deref()
            .is_some_and(|tokens| !tokens.is_empty() || !exact_fields.is_empty());
        let candidates = if let Some(tokens) = exact_tokens
            .as_deref()
            .filter(|tokens| !tokens.is_empty() || !exact_fields.is_empty())
        {
            self.exact_indexed_frame_candidates(query, append, frame, tokens, &exact_fields)?
                .unwrap_or_else(|| {
                    indexed_frame_candidates_for_append(
                        query,
                        &frame.index,
                        frame.record_count,
                        append.tenant.as_ref(),
                    )
                })
        } else {
            indexed_frame_candidates_for_append(
                query,
                &frame.index,
                frame.record_count,
                append.tenant.as_ref(),
            )
        };
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        self.decode_indexed_frame_candidates(
            query,
            append,
            frame,
            candidates,
            include_typed_metadata,
            include_fields,
            exact_candidates_are_exact,
        )
    }

    fn embedded_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
    ) -> TelemetryResult<Option<Vec<u32>>> {
        let exact_tokens = query.exact_message_token_conjunction();
        let exact_fields = query
            .exact_fields
            .iter()
            .filter(|field| field.key.as_ref() != "resource.loki.tenant")
            .map(|field| (field.key.clone(), field.value.clone()))
            .collect::<Vec<_>>();
        if exact_tokens
            .as_deref()
            .is_none_or(|tokens| tokens.is_empty())
            && exact_fields.is_empty()
        {
            return Ok(None);
        }
        // Keep token-only timestamp queries on the exact posting path. The
        // embedded field index is the bounded candidate driver here; without
        // a field constraint it would add no useful narrowing.
        if exact_fields.is_empty() {
            return Ok(None);
        }
        if query.exact_fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() != append.tenant.as_ref()
        }) {
            return Ok(Some(Vec::new()));
        }
        let mut candidates = None;
        let cached = self.cached_indexed_frame(frame)?;
        if let Some(tokens) = exact_tokens.filter(|tokens| !tokens.is_empty()) {
            for (token, _) in tokens {
                intersect_frame_candidate_slice(
                    &mut candidates,
                    &frame.index.term_candidate_ordinals(token),
                );
            }
        }
        for (key, value) in exact_fields {
            intersect_frame_candidate_slice(
                &mut candidates,
                &frame.index.field_candidate_ordinals(&key, &value),
            );
        }
        let mut candidates = candidates.unwrap_or_default();
        if candidates.is_empty() {
            return Ok(Some(candidates));
        }
        // The live/recovered frame already retains the embedded index. Avoid
        // decompressing and decoding structural state for frames that the
        // index proves cannot contain this exact token/field conjunction.
        retain_cached_timestamp_candidates(query, &cached, &mut candidates);
        Ok(Some(candidates))
    }

    fn decode_embedded_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        mut candidates: Vec<u32>,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let limit = query.limit.expect("bounded timestamp query has a limit");
        let cached = self.cached_indexed_frame(frame)?;
        let decode_fields = include_fields || !query.exact_fields.is_empty();
        let batch_len = limit.saturating_mul(2).max(256);
        let mut matches = Vec::with_capacity(limit.min(candidates.len()));
        while !candidates.is_empty() && matches.len() < limit {
            let take = batch_len.min(candidates.len());
            if take < candidates.len() {
                candidates.select_nth_unstable_by(take - 1, |left, right| {
                    let left = usize::try_from(*left).expect("embedded ordinal fits usize");
                    let right = usize::try_from(*right).expect("embedded ordinal fits usize");
                    let ordering = cached.timestamps[left]
                        .cmp(&cached.timestamps[right])
                        .then_with(|| cached.offsets[left].cmp(&cached.offsets[right]));
                    match query.order {
                        QueryOrder::OldestFirst => ordering,
                        QueryOrder::NewestFirst => ordering.reverse(),
                    }
                });
            }
            let mut batch: Vec<u32> = candidates.drain(..take).collect();
            batch.sort_unstable();
            matches.extend(self.decode_decompressed_frame_candidates(
                query,
                append,
                frame,
                &cached.structural,
                &cached.embedded_index,
                &cached.templates,
                &cached.offsets,
                &cached.timestamps,
                &cached.attribute_tables,
                &cached,
                &batch,
                include_typed_metadata,
                decode_fields,
                None,
                None,
                false,
                false,
            )?);
        }
        sort_and_limit_matches(&mut matches, query, limit);
        Ok(matches)
    }

    fn exact_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        exact_tokens: &[(&str, CaseSensitivity)],
        exact_fields: &[(Arc<str>, Arc<str>)],
    ) -> TelemetryResult<Option<Vec<u32>>> {
        if exact_tokens.is_empty() && exact_fields.is_empty() {
            return Ok(None);
        }
        if query.exact_fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.tenant"
                && field.value.as_ref() != append.tenant.as_ref()
        }) {
            return Ok(Some(Vec::new()));
        }
        let cached = self.cached_indexed_frame(frame)?;
        let message_postings = exact_tokens
            .iter()
            .map(|(token, case_sensitivity)| {
                self.cached_exact_posting(&exact_message_posting_key(
                    frame.frame_id,
                    token,
                    *case_sensitivity,
                ))
            })
            .collect::<Vec<_>>();
        let message_postings = if message_postings.iter().all(Option::is_some) {
            message_postings
        } else if exact_tokens.is_empty() {
            Vec::new()
        } else {
            let computed = self.cached_exact_message_terms(&cached, exact_tokens)?;
            for ((token, case_sensitivity), posting) in exact_tokens.iter().zip(&computed) {
                if let Some(posting) = posting {
                    self.cache_exact_posting(
                        exact_message_posting_key(frame.frame_id, token, *case_sensitivity),
                        Arc::clone(posting),
                    );
                }
            }
            computed
        };
        let field_postings = exact_fields
            .iter()
            .map(|(key, value)| {
                self.cached_exact_posting(&ExactPostingKey::Field(
                    frame.frame_id,
                    Arc::clone(key),
                    Arc::clone(value),
                ))
            })
            .collect::<Vec<_>>();
        let field_postings = if field_postings.iter().all(Option::is_some) {
            field_postings
        } else if exact_fields.is_empty() {
            Vec::new()
        } else {
            let computed = self.cached_exact_fields(&cached, exact_fields)?;
            for ((key, value), posting) in exact_fields.iter().zip(&computed) {
                if let Some(posting) = posting {
                    self.cache_exact_posting(
                        ExactPostingKey::Field(frame.frame_id, Arc::clone(key), Arc::clone(value)),
                        Arc::clone(posting),
                    );
                }
            }
            computed
        };
        let mut candidates = None;
        for posting in message_postings.into_iter().chain(field_postings) {
            let Some(posting) = posting else {
                return Ok(Some(Vec::new()));
            };
            intersect_frame_candidate_slice(&mut candidates, &posting);
            if candidates.as_ref().is_some_and(Vec::is_empty) {
                return Ok(Some(Vec::new()));
            }
        }
        let mut candidates = candidates.expect("an exact frame constraint has a posting");
        retain_cached_timestamp_candidates(query, &cached, &mut candidates);
        Ok(Some(candidates))
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_indexed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        candidates: Vec<u32>,
        include_typed_metadata: bool,
        include_fields: bool,
        candidates_are_exact: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let mut candidates =
            self.indexed_frame_field_predicate_candidates_owned(query, frame, candidates)?;
        if !matches!(query.predicate, LogPredicate::MatchAll)
            && let Some(message_candidates) =
                self.cached_message_predicate_candidates(query, frame)?
        {
            let mut current = Some(candidates);
            intersect_frame_candidate_slice(&mut current, &message_candidates);
            candidates = current.unwrap_or_default();
        }
        // Structural projection decoders consume record ordinals in ascending
        // order. Some bounded timestamp paths select candidates in query order
        // (newest first), so restore the decoder invariant before any cached
        // message or field projection is attempted.
        normalize_structural_candidate_ordinals(&mut candidates);
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let cached = self.cached_indexed_frame(frame)?;
        let typed_metadata = include_typed_metadata
            .then(|| self.cached_typed_metadata(&cached, frame.record_count))
            .transpose()?;
        let message_filterable = query.message_candidate_matches("").is_some();
        let message_only_query = message_filterable
            && query
                .exact_fields
                .iter()
                .all(|field| field.key.as_ref() == "resource.loki.tenant");
        let tenant_only_without_residual = query
            .exact_fields
            .iter()
            .all(|field| field.key.as_ref() == "resource.loki.tenant")
            && !query.has_residual_predicate();
        let decode_fields = include_typed_metadata
            || include_fields
            || !(query.has_residual_predicate() && message_filterable && message_only_query
                || tenant_only_without_residual
                || candidates_are_exact);
        if query.sort == crate::QuerySort::Timestamp
            && (!query.has_residual_predicate() || message_filterable)
            && let Some(limit) = query.limit
            && candidates.len() > limit.saturating_mul(2).max(256)
        {
            let structural = cached.structural.as_ref();
            if frame.index.timestamp_offset_ordinal_ordered() {
                let filter_messages_first = query.has_residual_predicate() && message_filterable;
                let batch_len = if filter_messages_first {
                    limit.saturating_mul(4).max(1_024)
                } else {
                    limit.saturating_mul(2).max(256)
                };
                let mut matches = Vec::with_capacity(limit);
                let mut consumed = 0usize;
                while matches.len() < limit && consumed < candidates.len() {
                    let mut selected_messages = None;
                    let mut batch = match query.order {
                        QueryOrder::OldestFirst => {
                            let start = consumed;
                            let end = candidates.len().min(start.saturating_add(batch_len));
                            consumed = end;
                            candidates[start..end].to_vec()
                        }
                        QueryOrder::NewestFirst => {
                            let end = candidates.len().saturating_sub(consumed);
                            let start = end.saturating_sub(batch_len);
                            consumed = consumed.saturating_add(end - start);
                            candidates[start..end].to_vec()
                        }
                    };
                    if filter_messages_first {
                        let messages =
                            decode_structural_messages_with_embedded_index_and_templates(
                                structural,
                                &batch,
                                &cached.embedded_index,
                                &cached.templates,
                            )?;
                        let mut filtered_batch = Vec::with_capacity(messages.len());
                        let mut filtered_messages = Vec::with_capacity(messages.len());
                        for (ordinal, message) in batch.into_iter().zip(messages) {
                            if query.message_candidate_matches(&message).unwrap_or(false) {
                                filtered_batch.push(ordinal);
                                filtered_messages.push(message);
                            }
                        }
                        batch = filtered_batch;
                        selected_messages = Some(filtered_messages);
                    }
                    let message_predicate_checked = filter_messages_first && message_only_query;
                    if !batch.is_empty() {
                        matches.extend(
                            self.decode_decompressed_frame_candidates(
                                query,
                                append,
                                frame,
                                structural,
                                &cached.embedded_index,
                                &cached.templates,
                                &cached.offsets,
                                &cached.timestamps,
                                &cached.attribute_tables,
                                &cached,
                                &batch,
                                include_typed_metadata,
                                decode_fields,
                                selected_messages.as_deref(),
                                typed_metadata
                                    .as_ref()
                                    .map(|metadata| metadata.packed.as_ref()),
                                candidates_are_exact,
                                message_predicate_checked,
                            )?,
                        );
                    }
                }
                sort_and_limit_matches(&mut matches, query, limit);
                return Ok(matches);
            }
            let offsets = cached.offsets.as_ref();
            let timestamps = cached.timestamps.as_ref();
            let mut ranked = candidates.to_vec();
            for ordinal in &ranked {
                let index = usize::try_from(*ordinal).map_err(|_| {
                    TelemetryError::InvalidBlockEncoding("record ordinal does not fit usize")
                })?;
                if index >= offsets.len() || index >= timestamps.len() {
                    return Err(TelemetryError::InvalidBlockEncoding(
                        "compressed ingest candidate ordinal is out of range",
                    ));
                }
            }
            let compare_positions = |left: &u32, right: &u32| {
                let left = usize::try_from(*left).expect("candidate ordinal was validated");
                let right = usize::try_from(*right).expect("candidate ordinal was validated");
                (timestamps[left], offsets[left]).cmp(&(timestamps[right], offsets[right]))
            };
            if !query.has_residual_predicate() {
                // Candidate membership is exact here, so select the requested page before
                // decoding structural records instead of sorting the whole frame.
                let keep = ranked.len().min(limit);
                ranked.select_nth_unstable_by(keep - 1, |left, right| {
                    let ordering = compare_positions(left, right);
                    match query.order {
                        QueryOrder::OldestFirst => ordering,
                        QueryOrder::NewestFirst => ordering.reverse(),
                    }
                });
                ranked.truncate(keep);
                ranked.sort_unstable();
                let mut matches = self.decode_decompressed_frame_candidates(
                    query,
                    append,
                    frame,
                    structural,
                    &cached.embedded_index,
                    &cached.templates,
                    &cached.offsets,
                    &cached.timestamps,
                    &cached.attribute_tables,
                    &cached,
                    &ranked,
                    include_typed_metadata,
                    decode_fields,
                    None,
                    typed_metadata
                        .as_ref()
                        .map(|metadata| metadata.packed.as_ref()),
                    candidates_are_exact,
                    false,
                )?;
                sort_and_limit_matches(&mut matches, query, limit);
                return Ok(matches);
            }
            let filter_messages_first = query.has_residual_predicate() && message_filterable;
            let batch_len = if filter_messages_first {
                limit.saturating_mul(4).max(1_024)
            } else {
                limit.saturating_mul(2).max(256)
            };
            let mut matches = Vec::with_capacity(limit);
            let already_ascending = ranked
                .windows(2)
                .all(|pair| compare_positions(&pair[0], &pair[1]).is_le());
            if already_ascending && query.order == QueryOrder::NewestFirst {
                ranked.reverse();
            }
            let mut consumed = 0usize;
            while matches.len() < limit && consumed < ranked.len() {
                let remaining = ranked.len().saturating_sub(consumed);
                let take = remaining.min(batch_len);
                if !already_ascending && remaining > take {
                    // Residual predicates may reject this batch, so select the next
                    // timestamp page repeatedly without sorting the whole frame.
                    ranked[consumed..].select_nth_unstable_by(take - 1, |left, right| {
                        let ordering = compare_positions(left, right);
                        match query.order {
                            QueryOrder::OldestFirst => ordering,
                            QueryOrder::NewestFirst => ordering.reverse(),
                        }
                    });
                }
                let end = consumed + take;
                let mut selected_messages = None;
                let mut batch = ranked[consumed..end].to_vec();
                batch.sort_unstable();
                if filter_messages_first {
                    let messages = decode_structural_messages_with_embedded_index_and_templates(
                        structural,
                        &batch,
                        &cached.embedded_index,
                        &cached.templates,
                    )?;
                    let mut filtered_batch = Vec::with_capacity(messages.len());
                    let mut filtered_messages = Vec::with_capacity(messages.len());
                    for (ordinal, message) in batch.into_iter().zip(messages) {
                        if query.message_candidate_matches(&message).unwrap_or(false) {
                            filtered_batch.push(ordinal);
                            filtered_messages.push(message);
                        }
                    }
                    batch = filtered_batch;
                    selected_messages = Some(filtered_messages);
                }
                let message_predicate_checked = filter_messages_first && message_only_query;
                if !batch.is_empty() {
                    matches.extend(
                        self.decode_decompressed_frame_candidates(
                            query,
                            append,
                            frame,
                            structural,
                            &cached.embedded_index,
                            &cached.templates,
                            &cached.offsets,
                            &cached.timestamps,
                            &cached.attribute_tables,
                            &cached,
                            &batch,
                            include_typed_metadata,
                            decode_fields,
                            selected_messages.as_deref(),
                            typed_metadata
                                .as_ref()
                                .map(|metadata| metadata.packed.as_ref()),
                            candidates_are_exact,
                            message_predicate_checked,
                        )?,
                    );
                }
                consumed = end;
            }
            sort_and_limit_matches(&mut matches, query, limit);
            return Ok(matches);
        }
        let mut matches = Vec::with_capacity(
            candidates
                .len()
                .min(query.limit.unwrap_or(candidates.len())),
        );
        let cached_messages = cached.cached_messages(&candidates);
        let cache_miss = cached_messages.is_none();
        let decode_all_fields = include_typed_metadata || decode_fields;
        let cached_fields = decode_all_fields
            .then(|| cached.cached_fields(&candidates))
            .flatten();
        let field_cache_miss = decode_all_fields && cached_fields.is_none();
        let decoded = decode_structural_records_with_cached_frame_data_and_fields(
            &cached.structural,
            &candidates,
            &cached.embedded_index,
            &cached.templates,
            &cached.offsets,
            &cached.timestamps,
            include_typed_metadata,
            decode_all_fields,
            cached_messages.as_deref(),
            typed_metadata
                .as_ref()
                .map(|metadata| metadata.packed.as_ref()),
            Some(&cached.attribute_tables),
            cached_fields.as_deref(),
        )?;
        cached.cache_messages(&candidates, &decoded);
        if decode_all_fields {
            cached.cache_fields(&candidates, &decoded);
        }
        if cache_miss || field_cache_miss {
            self.indexed_frame_query_cache
                .lock()
                .expect("indexed frame query cache lock is not poisoned")
                .enforce_budget();
        }
        for decoded in decoded {
            self.push_decoded_frame_match(
                query,
                append,
                frame,
                decoded,
                &mut matches,
                candidates_are_exact,
                false,
            )?;
        }
        Ok(matches)
    }

    #[allow(clippy::too_many_arguments)]
    fn decode_decompressed_frame_candidates(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        structural: &[u8],
        embedded_index: &EmbeddedFrameIndex,
        templates: &[Vec<Vec<u8>>],
        offsets: &[LogicalOffset],
        timestamps: &[u64],
        attribute_tables: &DecodedAttributeTables,
        cached_frame: &CachedIndexedFrame,
        candidates: &[u32],
        include_typed_metadata: bool,
        include_fields: bool,
        cached_messages: Option<&[Arc<str>]>,
        typed_metadata: Option<&PackedLogMetadata>,
        candidates_are_exact: bool,
        message_predicate_checked: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let mut matches = Vec::with_capacity(
            candidates
                .len()
                .min(query.limit.unwrap_or(candidates.len())),
        );
        let supplied_messages = cached_messages.is_some();
        let owned_cached_messages = if supplied_messages {
            None
        } else {
            cached_frame.cached_messages(candidates)
        };
        let cached_messages = cached_messages.or(owned_cached_messages.as_deref());
        let cache_miss = !supplied_messages && cached_messages.is_none();
        let decode_all_fields =
            include_typed_metadata || include_fields || !message_predicate_checked;
        let cached_fields = decode_all_fields
            .then(|| cached_frame.cached_fields(candidates))
            .flatten();
        let field_cache_miss = decode_all_fields && cached_fields.is_none();
        let decoded = decode_structural_records_with_cached_frame_data_and_fields(
            structural,
            candidates,
            embedded_index,
            templates,
            offsets,
            timestamps,
            include_typed_metadata,
            decode_all_fields,
            cached_messages,
            typed_metadata,
            Some(attribute_tables),
            cached_fields.as_deref(),
        )?;
        cached_frame.cache_messages(candidates, &decoded);
        if decode_all_fields {
            cached_frame.cache_fields(candidates, &decoded);
        }
        if cache_miss || field_cache_miss {
            self.indexed_frame_query_cache
                .lock()
                .expect("indexed frame query cache lock is not poisoned")
                .enforce_budget();
        }
        for decoded in decoded {
            self.push_decoded_frame_match(
                query,
                append,
                frame,
                decoded,
                &mut matches,
                candidates_are_exact,
                message_predicate_checked,
            )?;
        }
        Ok(matches)
    }

    #[allow(clippy::too_many_arguments)]
    fn push_decoded_frame_match(
        &self,
        query: &LogQuery,
        append: &IndexedFrameAppend,
        frame: &IndexedIngestFrame,
        decoded: crate::DecodedStructuralRecord,
        matches: &mut Vec<LogMatch>,
        candidates_are_exact: bool,
        message_predicate_checked: bool,
    ) -> TelemetryResult<()> {
        let relative_offset = decoded.offset.get();
        if relative_offset >= u64::from(append.record_count) {
            return Err(TelemetryError::InvalidBlockEncoding(
                "compressed ingest record ordinal is out of range",
            ));
        }
        let absolute_offset = append
            .first_offset
            .get()
            .checked_add(relative_offset)
            .map(LogicalOffset::new)
            .ok_or(TelemetryError::OffsetExhausted(query.topic_partition))?;
        let severity_text = if decoded.severity_text.is_empty() {
            decoded
                .fields
                .iter()
                .find(|field| {
                    matches!(
                        field.key.as_ref(),
                        "otel.severity_text" | "attr.loki.metadata.severity_text"
                    )
                })
                .map(|field| Arc::clone(&field.value))
                .unwrap_or(decoded.severity_text)
        } else {
            decoded.severity_text
        };
        let record = DurableLog {
            stream_shard_id: self.stream_shard_id,
            record_ref: TelemetryRecordRef::new(query.topic_partition, absolute_offset),
            timestamp_unix_nanos: decoded.timestamp_unix_nanos,
            observed_timestamp_unix_nanos: decoded.observed_timestamp_unix_nanos,
            body: decoded.body,
            message: decoded.message,
            fields: decoded.fields,
            attributes: decoded.attributes,
            resource: decoded.resource,
            scope: decoded.scope,
            severity_number: decoded.severity_number,
            severity_text,
            dropped_attributes_count: decoded.dropped_attributes_count,
            flags: decoded.flags,
            trace_id: decoded.trace_id,
            span_id: decoded.span_id,
            event_name: decoded.event_name,
            compression_cohort: frame.cohort,
        };
        let tenant_exact_fields_only = query
            .exact_fields
            .iter()
            .all(|field| field.key.as_ref() == "resource.loki.tenant");
        if candidates_are_exact
            || message_predicate_checked
            || (tenant_exact_fields_only && !query.has_residual_predicate())
        {
            if query.matches_index_bounds(&record) {
                matches.push(LogMatch { record });
            }
        } else if query.matches(&record) {
            matches.push(LogMatch { record });
        }
        Ok(())
    }

    fn query_ordinals(&self, query: &LogQuery, partition: &PartitionIndex) -> Vec<u32> {
        if query.limit == Some(0) || query.has_invalid_range() {
            return Vec::new();
        }
        let constraints = query.required_index_constraints();
        if constraints.impossible {
            return Vec::new();
        }
        let mut record_range =
            ordinal_record_window(&partition.records, query.start_offset, query.end_offset);
        let cursor_offset_applied = if query.sort == crate::QuerySort::Offset {
            if let Some(cursor) = query.after {
                match query.order {
                    QueryOrder::OldestFirst => {
                        let first_after = partition.records.partition_point(|record| {
                            record.record.record_ref.offset <= cursor.offset
                        });
                        record_range.start = record_range.start.max(first_after);
                    }
                    QueryOrder::NewestFirst => {
                        let first_at_or_after = partition.records.partition_point(|record| {
                            record.record.record_ref.offset < cursor.offset
                        });
                        record_range.end = record_range.end.min(first_at_or_after);
                    }
                }
                true
            } else {
                false
            }
        } else {
            false
        };
        if record_range.start > record_range.end {
            record_range.start = record_range.end;
        }
        let posting_start =
            u32::try_from(record_range.start).expect("record ordinal was bounded by ingest");
        let posting_end =
            u32::try_from(record_range.end).expect("record ordinal was bounded by ingest");
        // A fully indexable predicate already represents every leaf below
        // `query.predicate`. Keeping those leaves in `posting_lists` would
        // collect and intersect them once here and then repeat the same work
        // while building `predicate_candidates`. The legacy query builders
        // (`with_term`/`with_field`) remain separate constraints and still
        // need to be combined with the predicate result.
        let predicate_shape_is_index_exact = !matches!(query.predicate, LogPredicate::MatchAll)
            && hot_predicate_candidates_are_exact(&query.predicate);
        let predicate_limit = (predicate_shape_is_index_exact
            && query.terms.is_empty()
            && query.exact_fields.is_empty()
            && query.start_offset.is_none()
            && query.end_offset.is_none()
            && query.after.is_none()
            && query.sort == crate::QuerySort::Offset
            && query.order == QueryOrder::OldestFirst)
            .then_some(query.limit)
            .flatten();
        let mut predicate_candidates = if matches!(query.predicate, LogPredicate::MatchAll) {
            None
        } else {
            optimized_hot_predicate_candidates(
                &query.predicate,
                partition,
                posting_start,
                posting_end,
                predicate_limit,
            )
        };
        let predicate_is_index_exact = matches!(&query.predicate, LogPredicate::MatchAll)
            || (predicate_candidates.is_some() && predicate_shape_is_index_exact);
        let direct_terms = if predicate_is_index_exact {
            query.terms.iter().map(AsRef::as_ref).collect::<Vec<_>>()
        } else {
            constraints.terms.clone()
        };
        let direct_fields = if predicate_is_index_exact {
            query
                .exact_fields
                .iter()
                .map(|field| (field.key.as_ref(), field.value.as_ref()))
                .collect::<Vec<_>>()
        } else {
            constraints.fields.clone()
        };
        let mut posting_lists = Vec::<&HotPostingList>::with_capacity(
            direct_terms.len().saturating_add(direct_fields.len()),
        );
        for term in direct_terms {
            let normalized = normalize_term(term);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Vec::new();
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Vec::new();
            };
            if postings.is_empty_in(posting_start, posting_end) {
                return Vec::new();
            }
            posting_lists.push(postings);
        }
        for (key, value) in direct_fields {
            let Some(field_id) = partition
                .field_ids
                .get(key)
                .and_then(|values| values.get(value))
            else {
                return Vec::new();
            };
            let Some(postings) = partition.field_postings.get(*field_id) else {
                return Vec::new();
            };
            if postings.is_empty_in(posting_start, posting_end) {
                return Vec::new();
            }
            posting_lists.push(postings);
        }

        let mut predicate_postings = Vec::<Vec<u32>>::new();
        if predicate_candidates.is_none() {
            for key in constraints.field_exists {
                let Some(postings) = partition.field_presence_postings.get(key) else {
                    return Vec::new();
                };
                predicate_postings.push(postings.collect_in(
                    posting_start,
                    posting_end,
                    QueryOrder::OldestFirst,
                    None,
                ));
            }
            for (key, values) in constraints.field_in {
                let Some(value_ids) = partition.field_ids.get(key) else {
                    return Vec::new();
                };
                let mut field_postings = Vec::with_capacity(values.len());
                for value in values {
                    let Some(field_id) = value_ids.get(value) else {
                        continue;
                    };
                    let Some(posting) = partition.field_postings.get(*field_id) else {
                        continue;
                    };
                    field_postings.push(posting);
                }
                let ordinals =
                    collect_hot_posting_union(&field_postings, posting_start, posting_end, None);
                if ordinals.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(ordinals);
            }
            for (key, matcher) in constraints.field_text {
                let Some(candidates) =
                    hot_field_text_candidates(partition, key, matcher, posting_start, posting_end)
                else {
                    return Vec::new();
                };
                if candidates.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(candidates);
            }
            for (key, regex) in constraints.field_regex {
                let Some(candidates) = hot_field_predicate_candidates(
                    partition,
                    key,
                    |value| regex.is_match(value),
                    posting_start,
                    posting_end,
                ) else {
                    return Vec::new();
                };
                if candidates.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(candidates);
            }
            for (key, comparison, target) in constraints.field_numeric {
                let Some(candidates) = hot_numeric_field_candidates(
                    partition,
                    key,
                    comparison,
                    target,
                    posting_start,
                    posting_end,
                ) else {
                    return Vec::new();
                };
                if candidates.is_empty() {
                    return Vec::new();
                }
                predicate_postings.push(candidates);
            }
        }
        let needs_record_filter = if predicate_is_index_exact {
            query.start_timestamp_unix_nanos.is_some()
                || query.end_timestamp_unix_nanos.is_some()
                || (query.after.is_some() && !cursor_offset_applied)
        } else {
            query.needs_record_filter()
        };
        let mut ordinals_in_query_order = false;
        let mut ordinals = if posting_lists.is_empty() {
            if let Some(candidates) = predicate_candidates.take() {
                candidates
            } else if !needs_record_filter && predicate_postings.is_empty() {
                if query.sort == crate::QuerySort::Offset {
                    return collect_ordered_range(record_range, query.order, query.limit);
                }
                if partition.timestamp_order == TimestampOrder::NonDecreasing
                    && query.start_timestamp_unix_nanos.is_none()
                    && query.end_timestamp_unix_nanos.is_none()
                    && query.after.is_none()
                    && let Some(limit) = query.limit
                {
                    let limit = limit.min(record_range.len());
                    return match query.order {
                        QueryOrder::OldestFirst => (record_range.start
                            ..record_range.start.saturating_add(limit))
                            .map(|ordinal| {
                                u32::try_from(ordinal).expect("record ordinal was bounded")
                            })
                            .collect(),
                        QueryOrder::NewestFirst => (record_range.end.saturating_sub(limit)
                            ..record_range.end)
                            .rev()
                            .map(|ordinal| {
                                u32::try_from(ordinal).expect("record ordinal was bounded")
                            })
                            .collect(),
                    };
                }
                record_range
                    .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
                    .collect::<Vec<_>>()
            } else {
                record_range
                    .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
                    .collect::<Vec<_>>()
            }
        } else {
            posting_lists.sort_unstable_by_key(|postings| postings.cardinality);
            if posting_lists.len() == 1
                && !needs_record_filter
                && predicate_candidates.is_none()
                && predicate_postings.is_empty()
            {
                return posting_lists[0].collect_in(
                    posting_start,
                    posting_end,
                    query.order,
                    query.limit,
                );
            }
            let can_limit_intersection = predicate_postings.is_empty()
                && predicate_candidates.is_none()
                && !needs_record_filter
                && query.sort == crate::QuerySort::Offset;
            let ordinals = collect_hot_posting_intersection(
                &posting_lists,
                posting_start,
                posting_end,
                if can_limit_intersection {
                    query.order
                } else {
                    QueryOrder::OldestFirst
                },
                can_limit_intersection.then_some(query.limit).flatten(),
            );
            ordinals_in_query_order = can_limit_intersection;
            ordinals
        };
        if let Some(candidates) = predicate_candidates {
            let mut current = Some(ordinals);
            intersect_frame_candidate_slice(&mut current, &candidates);
            if current.as_ref().is_some_and(Vec::is_empty) {
                return Vec::new();
            }
            ordinals = current.unwrap_or_default();
        }
        if !predicate_postings.is_empty() {
            let mut current = Some(ordinals);
            for candidates in &predicate_postings {
                intersect_frame_candidate_slice(&mut current, candidates);
                if current.as_ref().is_some_and(Vec::is_empty) {
                    return Vec::new();
                }
            }
            ordinals = current.unwrap_or_default();
        }
        if needs_record_filter {
            ordinals.retain(|ordinal| {
                partition
                    .records
                    .get(*ordinal as usize)
                    .is_some_and(|record| {
                        if predicate_is_index_exact {
                            query.matches_index_bounds(&record.record)
                        } else {
                            query.matches_index_candidate(&record.record)
                        }
                    })
            });
        }
        if query.sort == crate::QuerySort::Timestamp {
            if let Some(limit) = query.limit
                && ordinals.len() > limit.saturating_mul(2).max(256)
            {
                retain_top_timestamp_ordinals(&mut ordinals, partition, query, limit);
            } else {
                ordinals.sort_unstable_by(|left, right| {
                    compare_timestamp_ordinals(partition, query.order, *left, *right)
                });
            }
        } else if query.order == QueryOrder::NewestFirst && !ordinals_in_query_order {
            ordinals.reverse();
        }
        if let Some(limit) = query.limit {
            ordinals.truncate(limit);
        }
        ordinals
    }

    fn validate_offset(&self, record: &DurableLog) -> TelemetryResult<()> {
        let Some(previous) = self
            .partitions
            .get(&record.record_ref.topic_partition)
            .and_then(PartitionIndex::last_offset)
        else {
            return Ok(());
        };
        let expected = previous
            .get()
            .checked_add(1)
            .map(LogicalOffset::new)
            .ok_or(TelemetryError::OffsetExhausted(
                record.record_ref.topic_partition,
            ))?;
        if record.record_ref.offset <= previous {
            return Err(TelemetryError::OffsetOutOfOrder {
                partition: record.record_ref.topic_partition,
                expected,
                observed: record.record_ref.offset,
            });
        }
        Ok(())
    }

    fn apply_durable_idempotent(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        let topic_partition = record.record_ref.topic_partition;
        let offset = record.record_ref.offset;
        let existing = self.partitions.get(&topic_partition).and_then(|partition| {
            let last = partition.records.last()?;
            match last.record.record_ref.offset.cmp(&offset) {
                std::cmp::Ordering::Less => None,
                std::cmp::Ordering::Equal => Some(last),
                std::cmp::Ordering::Greater => partition.record(offset),
            }
        });
        if let Some(existing) = existing {
            if existing.record != record {
                return Err(TelemetryError::ConflictingRecord {
                    partition: topic_partition,
                    offset,
                });
            }
            return Ok(IndexReceipt {
                record_ref: record.record_ref,
                indexed_through: self.indexed_through(topic_partition).unwrap_or(offset),
                compression_temperature: existing.temperature,
                tentative_compression_placement: existing.tentative_placement,
                sealed_blocks: Vec::new(),
            });
        }
        self.apply_durable_new(record)
    }

    fn can_index_as_homogeneous_range(
        &self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: &[OtlpLogEvent],
    ) -> bool {
        if events.len() < 2 {
            return false;
        }
        let first = events
            .first()
            .expect("the homogeneous range minimum length was checked");
        if self
            .partitions
            .get(&topic_partition)
            .and_then(PartitionIndex::last_offset)
            .is_some_and(|last_offset| first_offset <= last_offset)
        {
            return false;
        }
        events[1..].iter().all(|event| {
            same_message(&first.message, &event.message)
                && same_fields(&first.fields, &event.fields)
        })
    }

    fn apply_homogeneous_events(
        &mut self,
        topic_partition: TopicPartition,
        first_offset: LogicalOffset,
        events: Vec<OtlpLogEvent>,
    ) -> TelemetryResult<Vec<IndexReceipt>> {
        let mut events = events.into_iter().enumerate();
        let (first_index, first_event) = events
            .next()
            .expect("homogeneous event ranges contain at least two records");
        debug_assert_eq!(first_index, 0);
        let field_keys = first_event
            .fields
            .iter()
            .map(|field| Arc::clone(&field.key))
            .collect::<Vec<_>>();
        let first_applied = self.apply_durable_new_inner(
            first_event.into_durable(self.stream_shard_id, topic_partition, first_offset),
            true,
        )?;
        let first_ordinal = first_applied.ordinal;
        let term_ids = first_applied
            .term_ids
            .expect("the first homogeneous record was indexed");
        let message_trigram_keys = first_applied.message_trigram_keys;
        let field_ids = first_applied
            .field_ids
            .expect("the first homogeneous record was indexed");
        let mut receipts = Vec::with_capacity(events.size_hint().0.saturating_add(1));
        receipts.push(first_applied.receipt);
        let mut last_deferred = None;

        for (index, event) in events {
            let offset = batch_offset(topic_partition, first_offset, index)?;
            match self.apply_durable_new_inner(
                event.into_durable(self.stream_shard_id, topic_partition, offset),
                false,
            ) {
                Ok(applied) => {
                    debug_assert_eq!(
                        applied.ordinal,
                        first_ordinal
                            .checked_add(u32::try_from(index).expect("batch offset was bounded"))
                            .expect("record ordinal was bounded")
                    );
                    last_deferred = Some((applied.ordinal, offset));
                    receipts.push(applied.receipt);
                }
                Err(error) => {
                    if let Some((last_ordinal, last_offset)) = last_deferred {
                        self.publish_homogeneous_posting_range(
                            topic_partition,
                            first_ordinal + 1,
                            last_ordinal,
                            last_offset,
                            &term_ids,
                            message_trigram_keys.as_deref(),
                            &field_ids,
                            &field_keys,
                        );
                    }
                    return Err(error);
                }
            }
        }

        let (last_ordinal, last_offset) =
            last_deferred.expect("homogeneous event ranges contain a deferred record");
        self.publish_homogeneous_posting_range(
            topic_partition,
            first_ordinal + 1,
            last_ordinal,
            last_offset,
            &term_ids,
            message_trigram_keys.as_deref(),
            &field_ids,
            &field_keys,
        );
        Ok(receipts)
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_homogeneous_posting_range(
        &mut self,
        topic_partition: TopicPartition,
        first_ordinal: u32,
        last_ordinal: u32,
        last_offset: LogicalOffset,
        term_ids: &[usize],
        message_trigram_keys: Option<&[u32]>,
        field_ids: &[usize],
        field_keys: &[Arc<str>],
    ) {
        debug_assert!(first_ordinal <= last_ordinal);
        let partition = self
            .partitions
            .get_mut(&topic_partition)
            .expect("homogeneous records were inserted");
        for term_id in term_ids {
            partition
                .term_postings
                .get_mut(*term_id)
                .expect("interned term has a posting slot")
                .push_range(first_ordinal, last_ordinal);
        }
        if let Some(message_trigram_keys) = message_trigram_keys {
            for key in message_trigram_keys {
                if let Some(posting) = partition.message_trigram_postings.get_mut(key) {
                    posting.push_range(first_ordinal, last_ordinal);
                }
            }
        }
        for field_id in field_ids {
            partition
                .field_postings
                .get_mut(*field_id)
                .expect("interned field has a posting slot")
                .push_range(first_ordinal, last_ordinal);
        }
        for field_key in field_keys {
            partition
                .field_presence_postings
                .entry(Arc::clone(field_key))
                .or_default()
                .push_range(first_ordinal, last_ordinal);
        }
        // This assignment is the publication barrier for the deferred range.
        partition.indexed_through = Some(last_offset);
    }

    fn index_message_trigrams(
        &mut self,
        record: &DurableLog,
        record_ordinal: u32,
    ) -> Option<Arc<[u32]>> {
        let keys = collect_message_trigram_keys(&record.message);
        let partition = self
            .partitions
            .get_mut(&record.record_ref.topic_partition)
            .expect("record partition was inserted");
        if !record.message.is_ascii() {
            partition.message_trigram_ascii_only = false;
        }
        if !partition.message_trigram_index_complete {
            return None;
        }
        for key in &keys {
            if !partition.message_trigram_postings.contains_key(key)
                && partition.message_trigram_postings.len() >= MAX_HOT_MESSAGE_TRIGRAM_KEYS
            {
                partition.message_trigram_index_complete = false;
                partition.message_trigram_postings.clear();
                return None;
            }
            partition
                .message_trigram_postings
                .entry(*key)
                .or_default()
                .push(record_ordinal);
        }
        Some(Arc::from(keys))
    }

    fn index_terms(&mut self, record: &DurableLog, record_ordinal: u32) -> Arc<[usize]> {
        let topic_partition = record.record_ref.topic_partition;
        let cache_slot = message_term_cache_slot(topic_partition, record.message.as_bytes());
        let term_ids = if let Some(cached) = &self.message_term_cache[cache_slot]
            && cached.topic_partition == topic_partition
            && same_message(&cached.message, &record.message)
        {
            Arc::clone(&cached.term_ids)
        } else {
            let mut message_term_ids = Vec::new();
            {
                let partition = self
                    .partitions
                    .get_mut(&topic_partition)
                    .expect("record partition was inserted");
                scan_message_terms(&record.message, |term| {
                    let normalized = normalize_term(term);
                    let term_id = match partition.term_ids.get(normalized.as_ref()).copied() {
                        Some(term_id) => term_id,
                        None => {
                            let term_id = partition.term_postings.len();
                            partition
                                .term_ids
                                .insert(Arc::from(normalized.as_ref()), term_id);
                            partition.term_postings.push(HotPostingList::default());
                            term_id
                        }
                    };
                    if !message_term_ids.contains(&term_id) {
                        message_term_ids.push(term_id);
                    }
                });
            }
            let term_ids = Arc::<[usize]>::from(message_term_ids);
            self.message_term_cache[cache_slot] = Some(CachedMessageTerms {
                topic_partition,
                message: Arc::clone(&record.message),
                term_ids: Arc::clone(&term_ids),
            });
            term_ids
        };
        let term_postings = &mut self
            .partitions
            .get_mut(&topic_partition)
            .expect("record partition was inserted")
            .term_postings;
        for term_id in term_ids.iter().copied() {
            term_postings
                .get_mut(term_id)
                .expect("interned term has a posting slot")
                .push(record_ordinal);
        }
        term_ids
    }

    fn index_fields(&mut self, record: &DurableLog, record_ordinal: u32) -> Arc<[usize]> {
        let topic_partition = record.record_ref.topic_partition;
        let cache_slot = field_cache_slot(topic_partition, &record.fields);
        let field_ids = if let Some(cached) = &self.field_cache[cache_slot]
            && cached.topic_partition == topic_partition
            && same_fields(&cached.fields, &record.fields)
        {
            Arc::clone(&cached.field_ids)
        } else {
            let mut record_field_ids = Vec::with_capacity(record.fields.len());
            let partition = self
                .partitions
                .get_mut(&topic_partition)
                .expect("record partition was inserted");
            for (index, field) in record.fields.iter().enumerate() {
                if record.fields[..index]
                    .iter()
                    .any(|existing| existing.key == field.key && existing.value == field.value)
                {
                    continue;
                }
                let field_id = match partition
                    .field_ids
                    .get(field.key.as_ref())
                    .and_then(|values| values.get(field.value.as_ref()))
                    .copied()
                {
                    Some(field_id) => field_id,
                    None => {
                        let field_id = partition.field_postings.len();
                        partition
                            .field_ids
                            .entry(Arc::clone(&field.key))
                            .or_default()
                            .insert(Arc::clone(&field.value), field_id);
                        if let Ok(observed) = field.value.parse::<i128>() {
                            partition
                                .numeric_field_values
                                .entry(Arc::clone(&field.key))
                                .or_default()
                                .push((observed, field_id));
                        }
                        partition.field_postings.push(HotPostingList::default());
                        field_id
                    }
                };
                record_field_ids.push(field_id);
            }
            let field_ids = Arc::<[usize]>::from(record_field_ids);
            self.field_cache[cache_slot] = Some(CachedFields {
                topic_partition,
                fields: Arc::clone(&record.fields),
                field_ids: Arc::clone(&field_ids),
            });
            field_ids
        };
        let partition = self
            .partitions
            .get_mut(&topic_partition)
            .expect("record partition was inserted");
        let field_postings = &mut partition.field_postings;
        for field_id in field_ids.iter().copied() {
            field_postings
                .get_mut(field_id)
                .expect("interned field has a posting slot")
                .push(record_ordinal);
        }
        let mut seen_keys = Vec::<&str>::new();
        for field in record.fields.iter() {
            if seen_keys.contains(&field.key.as_ref()) {
                continue;
            }
            seen_keys.push(field.key.as_ref());
            partition
                .field_presence_postings
                .entry(Arc::clone(&field.key))
                .or_default()
                .push(record_ordinal);
        }
        field_ids
    }

    fn resolve_dictionary(
        &mut self,
        placement_id: CompressionPlacementId,
    ) -> TelemetryResult<DictionarySelection> {
        let Some(dictionary_id) = self.placement_dictionaries.get(&placement_id).copied() else {
            return Ok(DictionarySelection {
                dictionary_id: None,
                payload: None,
            });
        };
        if let Some(payload) = self.dictionary_cache.get(dictionary_id) {
            return Ok(DictionarySelection {
                dictionary_id: Some(dictionary_id),
                payload: Some(payload),
            });
        }

        let payload = self
            .dictionary_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.dictionary(dictionary_id))
            .ok_or(TelemetryError::MissingDictionary(dictionary_id))?;
        self.dictionary_cache
            .insert(dictionary_id, Arc::clone(&payload))?;
        Ok(DictionarySelection {
            dictionary_id: Some(dictionary_id),
            payload: Some(payload),
        })
    }

    fn rebalance_block(
        &mut self,
        initial_key: ActiveBlockKey,
        initial_block: ActiveBlock,
        force_seal: bool,
    ) -> TelemetryResult<Vec<BlockDescriptor>> {
        if !self.block_collator.is_enabled() {
            let temperature = CompressionTemperature::new(0);
            let placement =
                CompressionPlacement::base(initial_key.source_compression_cohort, temperature);
            let score = CompressionBlockScore {
                temperature,
                shape_hash: 0,
                internal_variance_q8: 0,
                max_deviation: 0,
                source_bytes: initial_block.source_bytes,
                record_count: initial_block.records.len(),
            };
            return self
                .stage_active_block(initial_key, &initial_block, placement, score)
                .map(|descriptor| vec![descriptor]);
        }

        let mut work = vec![(initial_key, initial_block)];
        let mut sealed = Vec::new();
        while let Some((home_key, active)) = work.pop() {
            let home_dictionary_payload = active.dictionary_payload.clone();
            let next_pass = active.rebalance_passes.saturating_add(1);
            let locality_records = active
                .records
                .iter()
                .map(PendingRecord::locality)
                .collect::<Vec<_>>();
            let assignments = self.block_collator.collate(
                home_key.source_compression_cohort,
                home_key.placement_id,
                &locality_records,
            );
            let assignment_count = assignments.len();
            let mut records = active.records.into_iter().map(Some).collect::<Vec<_>>();

            for assignment in assignments {
                let placement = assignment.placement;
                let score = assignment.score;
                let group_records = assignment
                    .record_indices()
                    .map(|index| {
                        records[index]
                            .take()
                            .expect("collation membership contains each record once")
                    })
                    .collect::<Vec<_>>();
                let (target_key, dictionary_payload) =
                    if placement.placement_id == home_key.placement_id {
                        (home_key, home_dictionary_payload.clone())
                    } else {
                        let dictionary = self.resolve_dictionary(placement.placement_id)?;
                        (
                            ActiveBlockKey {
                                topic_partition: home_key.topic_partition,
                                source_compression_cohort: home_key.source_compression_cohort,
                                placement_id: placement.placement_id,
                                dictionary_id: dictionary.dictionary_id,
                            },
                            dictionary.payload,
                        )
                    };
                let mut group =
                    ActiveBlock::from_records(group_records, dictionary_payload, next_pass);

                if force_seal {
                    sealed.push(self.stage_active_block(target_key, &group, placement, score)?);
                    continue;
                }

                let merged_existing = if let Some(existing) = self.active_blocks.remove(&target_key)
                {
                    let mut existing = existing;
                    existing.append_block(group);
                    group = existing;
                    true
                } else {
                    false
                };
                if group.source_bytes < self.config.target_block_bytes {
                    self.active_blocks.insert(target_key, group);
                    continue;
                }

                let stable_home_block = !merged_existing
                    && assignment_count == 1
                    && target_key.placement_id == home_key.placement_id
                    && score.internal_variance_q8
                        <= self.config.compression_locality.split_variance_q8;
                if stable_home_block || group.rebalance_passes >= MAX_REBALANCE_PASSES {
                    sealed.push(self.stage_active_block(target_key, &group, placement, score)?);
                } else {
                    work.push((target_key, group));
                }
            }
            debug_assert!(records.iter().all(Option::is_none));
        }
        Ok(sealed)
    }

    fn stage_active_block(
        &mut self,
        key: ActiveBlockKey,
        active: &ActiveBlock,
        placement: CompressionPlacement,
        score: CompressionBlockScore,
    ) -> TelemetryResult<BlockDescriptor> {
        let structural = encode_structural_records(&active.records)?;
        let structural_bytes = u64::try_from(structural.len()).unwrap_or(u64::MAX);
        let compressed = self.compressor.compress(
            &structural,
            key.dictionary_id,
            active.dictionary_payload.as_deref(),
        )?;
        let stored_bytes = u64::try_from(compressed.len()).unwrap_or(u64::MAX);
        if let Some(observer) = &self.realtime_dictionary {
            let _ = observer.observe_structural_block(key.placement_id, structural);
        }
        for pending in &active.records {
            if let Some(record) = self
                .partitions
                .get_mut(&pending.record.record_ref.topic_partition)
                .and_then(|partition| partition.record_mut(pending.record.record_ref.offset))
            {
                record.final_placement = Some(placement);
            }
        }
        Ok(self.catalog.seal(
            BlockDescriptor {
                block_id: BlockId::new(0),
                stream_shard_id: self.stream_shard_id,
                topic_partition: key.topic_partition,
                source_compression_cohort: key.source_compression_cohort,
                placement_id: key.placement_id,
                dictionary_id: key.dictionary_id,
                compression_codec: CompressionCodec::Zstd,
                compression_level: self.config.compression_level,
                first_offset: active.first_offset,
                last_offset: active.last_offset,
                record_count: active.record_count,
                source_bytes: active.source_bytes,
                structural_bytes,
                stored_bytes,
                min_timestamp_unix_nanos: active.min_timestamp_unix_nanos,
                max_timestamp_unix_nanos: active.max_timestamp_unix_nanos,
                compression_temperature: score.temperature.get(),
                compression_shape_hash: score.shape_hash,
                compression_temperature_variance_q8: score.internal_variance_q8,
                max_compression_temperature_deviation: score.max_deviation,
                object_key: None,
                object_offset: None,
            },
            Arc::from(compressed),
        ))
    }
}

impl ShardStreamDurableSink for LogStripe {
    fn on_durable_append(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        self.apply_durable(record)
    }
}

/// Container for independently owned ShardTelemetry log stripes.
///
/// A deployment should hand each [`LogStripe`] to the matching shard-stream
/// worker and invoke [`Self::apply_durable`] in that worker. This container is
/// useful for single-process tests and embedded deployments; it never creates a
/// shared global hot index.
#[derive(Debug)]
pub struct ShardTelemetry {
    stripes: HashMap<ShardId, LogStripe>,
}

impl ShardTelemetry {
    /// Creates one log stripe for each supplied shard-stream shard.
    pub fn new(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(shard_ids, config, None)
    }

    /// Creates one stripe per physical shard, all observing one immutable
    /// dictionary publication catalog at explicit batch boundaries.
    pub fn with_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
        dictionary_catalog: Arc<DictionaryCatalog>,
    ) -> TelemetryResult<Self> {
        Self::new_with_optional_dictionary_catalog(shard_ids, config, Some(dictionary_catalog))
    }

    /// Creates one stripe per shard and attaches all of them to one bounded
    /// real-time dictionary trainer.
    pub fn with_realtime_dictionary(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
        trainer: &RealtimeDictionaryTrainer,
    ) -> TelemetryResult<Self> {
        let mut database = Self::with_dictionary_catalog(shard_ids, config, trainer.catalog())?;
        for stripe in database.stripes.values_mut() {
            stripe.attach_realtime_dictionary(trainer.observer());
        }
        Ok(database)
    }

    fn new_with_optional_dictionary_catalog(
        shard_ids: impl IntoIterator<Item = ShardId>,
        config: StripeConfig,
        dictionary_catalog: Option<Arc<DictionaryCatalog>>,
    ) -> TelemetryResult<Self> {
        let mut stripes = HashMap::new();
        for shard_id in shard_ids {
            if stripes.contains_key(&shard_id) {
                return Err(TelemetryError::DuplicateStripe(shard_id));
            }
            let stripe = match &dictionary_catalog {
                Some(dictionary_catalog) => LogStripe::with_dictionary_catalog(
                    shard_id,
                    config.clone(),
                    Arc::clone(dictionary_catalog),
                )?,
                None => LogStripe::new(shard_id, config.clone())?,
            };
            stripes.insert(shard_id, stripe);
        }
        if stripes.is_empty() {
            return Err(TelemetryError::InvalidConfig(
                "at least one stripe is required",
            ));
        }
        Ok(Self { stripes })
    }

    /// Returns the stripe owned by a shard-stream worker.
    #[must_use]
    pub fn stripe(&self, stream_shard_id: ShardId) -> Option<&LogStripe> {
        self.stripes.get(&stream_shard_id)
    }

    /// Returns the stripe owned by a shard-stream worker.
    pub fn stripe_mut(&mut self, stream_shard_id: ShardId) -> Option<&mut LogStripe> {
        self.stripes.get_mut(&stream_shard_id)
    }

    /// Routes an already durable shard-stream record to its matching log stripe.
    pub fn apply_durable(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        self.stripes
            .get_mut(&record.stream_shard_id)
            .ok_or(TelemetryError::UnknownStripe(record.stream_shard_id))?
            .apply_durable(record)
    }

    /// Runs a partition-local query through one stripe.
    pub fn query(
        &self,
        stream_shard_id: ShardId,
        query: &LogQuery,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let stripe = self
            .stripes
            .get(&stream_shard_id)
            .ok_or(TelemetryError::UnknownStripe(stream_shard_id))?;
        Ok(stripe.query(query))
    }

    /// Fans a partition-local query across every physical stripe and merges
    /// the bounded results in the query's deterministic order.
    #[must_use]
    pub fn query_all(&self, query: &LogQuery) -> Vec<LogMatch> {
        self.merge_queries(self.stripes.values(), query)
    }

    /// Fans a partition-local query across selected physical stripes and
    /// returns an error if any requested stripe is unknown.
    pub fn query_stripes(
        &self,
        stream_shard_ids: impl IntoIterator<Item = ShardId>,
        query: &LogQuery,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let mut seen = HashSet::new();
        let mut stripes = Vec::new();
        for stream_shard_id in stream_shard_ids {
            if seen.insert(stream_shard_id) {
                stripes.push(
                    self.stripes
                        .get(&stream_shard_id)
                        .ok_or(TelemetryError::UnknownStripe(stream_shard_id))?,
                );
            }
        }
        Ok(self.merge_queries(stripes, query))
    }

    fn merge_queries<'a>(
        &self,
        stripes: impl IntoIterator<Item = &'a LogStripe>,
        query: &LogQuery,
    ) -> Vec<LogMatch> {
        let mut matches = stripes
            .into_iter()
            .flat_map(|stripe| stripe.query(query))
            .collect::<Vec<_>>();
        matches.sort_unstable_by(|left, right| {
            query.compare(&left.record, &right.record).then_with(|| {
                left.record
                    .stream_shard_id
                    .cmp(&right.record.stream_shard_id)
            })
        });
        if let Some(limit) = query.limit {
            matches.truncate(limit);
        }
        matches
    }
}

impl ShardStreamDurableSink for ShardTelemetry {
    fn on_durable_append(&mut self, record: DurableLog) -> TelemetryResult<IndexReceipt> {
        self.apply_durable(record)
    }
}

fn normalize_term(term: &str) -> Cow<'_, str> {
    if term.chars().any(char::is_uppercase) {
        Cow::Owned(term.to_lowercase())
    } else {
        Cow::Borrowed(term)
    }
}

fn exact_message_posting_key(
    frame_id: u64,
    token: &str,
    case_sensitivity: CaseSensitivity,
) -> ExactPostingKey {
    ExactPostingKey::Message(
        frame_id,
        match case_sensitivity {
            CaseSensitivity::Sensitive => Arc::from(token),
            CaseSensitivity::Insensitive => Arc::from(token.to_ascii_lowercase()),
        },
        case_sensitivity,
    )
}

fn validate_batch_offset_range(
    topic_partition: TopicPartition,
    first_offset: LogicalOffset,
    record_count: usize,
) -> TelemetryResult<()> {
    let Some(last_index) = record_count.checked_sub(1) else {
        return Ok(());
    };
    batch_offset(topic_partition, first_offset, last_index).map(|_| ())
}

fn batch_offset(
    topic_partition: TopicPartition,
    first_offset: LogicalOffset,
    index: usize,
) -> TelemetryResult<LogicalOffset> {
    let relative_offset =
        u64::try_from(index).map_err(|_| TelemetryError::OffsetExhausted(topic_partition))?;
    first_offset
        .get()
        .checked_add(relative_offset)
        .map(LogicalOffset::new)
        .ok_or(TelemetryError::OffsetExhausted(topic_partition))
}

#[inline]
fn same_message(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && (std::ptr::eq(left.as_ptr(), right.as_ptr()) || left.as_bytes() == right.as_bytes())
}

#[inline]
fn message_term_cache_slot(topic_partition: TopicPartition, message: &[u8]) -> usize {
    let mut hash = message.len() as u64
        ^ u64::from(topic_partition.partition_id.get()).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    if message.len() >= 16 {
        let first = u64::from_le_bytes(
            message[..8]
                .try_into()
                .expect("eight-byte prefix is present"),
        );
        let last = u64::from_le_bytes(
            message[message.len() - 8..]
                .try_into()
                .expect("eight-byte suffix is present"),
        );
        hash ^= first.rotate_left(17) ^ last.rotate_left(41);
    } else {
        for byte in message {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash as usize & (MESSAGE_TERM_CACHE_ENTRIES - 1)
}

fn collect_message_trigram_keys(message: &str) -> Vec<u32> {
    let bytes = message.as_bytes();
    if bytes.len() < 3 {
        return Vec::new();
    }
    let mut keys = bytes
        .windows(3)
        .map(|window| {
            u32::from(window[0].to_ascii_lowercase())
                | (u32::from(window[1].to_ascii_lowercase()) << 8)
                | (u32::from(window[2].to_ascii_lowercase()) << 16)
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

#[inline]
fn field_cache_slot(
    topic_partition: TopicPartition,
    fields: &Arc<Vec<crate::MetadataField>>,
) -> usize {
    let pointer = Arc::as_ptr(fields) as usize;
    let partition = topic_partition.partition_id.get() as usize;
    (pointer.rotate_left(17) ^ partition.wrapping_mul(0x9e37_79b9)) & (FIELD_CACHE_ENTRIES - 1)
}

#[inline]
fn same_fields(
    left: &Arc<Vec<crate::MetadataField>>,
    right: &Arc<Vec<crate::MetadataField>>,
) -> bool {
    Arc::ptr_eq(left, right) || left.as_slice() == right.as_slice()
}

fn ordinal_record_window(
    records: &[IndexedRecord],
    start: Option<LogicalOffset>,
    end: Option<LogicalOffset>,
) -> std::ops::Range<usize> {
    let start_index = start.map_or(0, |start| {
        records.partition_point(|record| record.record.record_ref.offset < start)
    });
    let end_index = end.map_or(records.len(), |end| {
        records.partition_point(|record| record.record.record_ref.offset < end)
    });
    start_index.min(end_index)..end_index
}

fn collect_ordered_range(
    ordinals: std::ops::Range<usize>,
    order: QueryOrder,
    limit: Option<usize>,
) -> Vec<u32> {
    let take = limit.unwrap_or(ordinals.len()).min(ordinals.len());
    match order {
        QueryOrder::OldestFirst => ordinals
            .take(take)
            .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
            .collect(),
        QueryOrder::NewestFirst => ordinals
            .rev()
            .take(take)
            .map(|ordinal| u32::try_from(ordinal).expect("record ordinal was bounded"))
            .collect(),
    }
}

fn same_query_across_partition(left: &LogQuery, right: &LogQuery) -> bool {
    let mut normalized = left.clone();
    normalized.topic_partition = right.topic_partition;
    normalized == *right
}

fn append_matches_query_bounds(query: &LogQuery, append: &IndexedFrameAppend) -> bool {
    query.end_offset.is_none_or(|end| end > append.first_offset)
        && query
            .start_offset
            .is_none_or(|start| start <= append.last_offset)
}

fn frame_matches_query_bounds(query: &LogQuery, frame: &IndexedIngestFrame) -> bool {
    timestamp_bounds_overlap(
        query,
        frame.min_timestamp_unix_nanos,
        frame.max_timestamp_unix_nanos,
    )
}

fn timestamp_bounds_overlap(query: &LogQuery, minimum: u64, maximum: u64) -> bool {
    query
        .end_timestamp_unix_nanos
        .is_none_or(|end| end > minimum)
        && query
            .start_timestamp_unix_nanos
            .is_none_or(|start| start <= maximum)
}

fn indexed_frame_candidates_for_append(
    query: &LogQuery,
    index: &EmbeddedFrameIndex,
    record_count: u32,
    tenant: &str,
) -> Vec<u32> {
    indexed_frame_candidates_for_append_with_phrase_mode(query, index, record_count, tenant, false)
}

fn indexed_frame_candidates_for_append_with_phrase_mode(
    query: &LogQuery,
    index: &EmbeddedFrameIndex,
    record_count: u32,
    tenant: &str,
    include_message_phrases: bool,
) -> Vec<u32> {
    let mut constraints = query.required_index_constraints();
    if include_message_phrases {
        constraints = query.required_index_constraints_with_message_phrases(true);
    }
    if query
        .exact_fields
        .iter()
        .any(|field| field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() != tenant)
    {
        return Vec::new();
    }
    constraints
        .fields
        .retain(|(key, _)| *key != "resource.loki.tenant");
    indexed_frame_candidates_from_constraints(query, index, record_count, constraints)
}

fn indexed_frame_candidates_from_constraints(
    query: &LogQuery,
    index: &EmbeddedFrameIndex,
    record_count: u32,
    constraints: crate::query::RequiredIndexConstraints<'_>,
) -> Vec<u32> {
    if constraints.impossible {
        return Vec::new();
    }
    let mut candidates = None::<Vec<u32>>;
    for term in &constraints.terms {
        intersect_frame_candidates(&mut candidates, index.term_candidate_ordinals(term));
        if candidates.as_ref().is_some_and(Vec::is_empty) {
            return Vec::new();
        }
    }
    for (key, value) in &constraints.fields {
        intersect_frame_candidates(&mut candidates, index.field_candidate_ordinals(key, value));
        if candidates.as_ref().is_some_and(Vec::is_empty) {
            return Vec::new();
        }
    }
    if let Some(predicate_candidates) =
        embedded_message_predicate_candidates(&query.predicate, index)
    {
        intersect_frame_candidate_slice(&mut candidates, &predicate_candidates);
        if candidates.as_ref().is_some_and(Vec::is_empty) {
            return Vec::new();
        }
    }
    candidates.unwrap_or_else(|| (0..record_count).collect())
}

fn normalize_structural_candidate_ordinals(candidates: &mut Vec<u32>) {
    if candidates.windows(2).all(|pair| pair[0] < pair[1]) {
        return;
    }
    candidates.sort_unstable();
    candidates.dedup();
}

fn trace_predicate_candidates_are_exact(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll | LogPredicate::MatchNone => true,
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessageTokenPrefix {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessagePhrase { .. } => true,
        LogPredicate::MessageTokenRegex(regex)
            if regex.case_sensitivity() == CaseSensitivity::Insensitive =>
        {
            true
        }
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(trace_predicate_candidates_are_exact)
        }
        LogPredicate::Term(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::Not(_) => false,
    }
}

fn embedded_message_predicate_candidates(
    predicate: &LogPredicate,
    index: &EmbeddedFrameIndex,
) -> Option<Vec<u32>> {
    if let Some((tokens, minimum)) = message_token_min_match_shape(predicate) {
        let mut counts = vec![0_u8; index.record_count() as usize];
        for (token, _) in tokens {
            for ordinal in index.term_candidate_ordinals(token.as_ref()) {
                if let Some(count) = counts.get_mut(ordinal as usize) {
                    *count = count.saturating_add(1);
                }
            }
        }
        return Some(
            counts
                .into_iter()
                .enumerate()
                .filter_map(|(ordinal, count)| {
                    (usize::from(count) >= minimum).then_some(ordinal as u32)
                })
                .collect(),
        );
    }
    if let Some(terms) = simple_message_token_or_terms(predicate) {
        return Some(index.term_candidate_ordinals_union(&terms));
    }
    match predicate {
        LogPredicate::MatchAll => Some((0..index.record_count()).collect()),
        LogPredicate::MatchNone => Some(Vec::new()),
        LogPredicate::Term(term) | LogPredicate::MessageToken { value: term, .. } => {
            if term.is_empty() || term.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                Some(Vec::new())
            } else {
                Some(index.term_candidate_ordinals(term))
            }
        }
        LogPredicate::And(predicates) => {
            let mut candidates = None;
            for predicate in predicates {
                if let Some(child) = embedded_message_predicate_candidates(predicate, index) {
                    intersect_frame_candidate_slice(&mut candidates, &child);
                    if candidates.as_ref().is_some_and(Vec::is_empty) {
                        return Some(Vec::new());
                    }
                }
            }
            candidates
        }
        LogPredicate::Or(predicates) => {
            let mut candidates = Vec::new();
            for predicate in predicates {
                union_sorted_ordinals(
                    &mut candidates,
                    embedded_message_predicate_candidates(predicate, index)?,
                );
            }
            Some(candidates)
        }
        LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::Not(_) => None,
    }
}

fn simple_message_token_or_terms(predicate: &LogPredicate) -> Option<Vec<&str>> {
    let LogPredicate::Or(predicates) = predicate else {
        return None;
    };
    if predicates.is_empty() {
        return None;
    }
    predicates
        .iter()
        .map(|predicate| match predicate {
            LogPredicate::Term(term) | LogPredicate::MessageToken { value: term, .. }
                if !term.is_empty() && term.bytes().all(|byte| byte.is_ascii_alphanumeric()) =>
            {
                Some(term.as_ref())
            }
            _ => None,
        })
        .collect()
}

fn remove_published_spool_file(path: &std::path::Path) {
    if let Err(error) = fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!(
            "shard-telemetry retained published tier spool {} after cleanup failed: {error}",
            path.display()
        );
    }
}

#[allow(clippy::type_complexity)]
fn message_token_min_match_shape(
    predicate: &LogPredicate,
) -> Option<(Vec<(Arc<str>, CaseSensitivity)>, usize)> {
    let LogPredicate::Or(predicates) = predicate else {
        return None;
    };
    if predicates.len() < 2 {
        return None;
    }
    let mut tokens = Vec::<(Arc<str>, CaseSensitivity)>::new();
    let mut subsets = Vec::<Vec<usize>>::with_capacity(predicates.len());
    let mut minimum = None;
    for predicate in predicates {
        let LogPredicate::And(children) = predicate else {
            return None;
        };
        if children.is_empty() {
            return None;
        }
        let mut subset = Vec::with_capacity(children.len());
        for child in children {
            let LogPredicate::MessageToken {
                value,
                case_sensitivity,
            } = child
            else {
                return None;
            };
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return None;
            }
            let index = tokens
                .iter()
                .position(|(known, known_case)| {
                    known.as_ref() == value.as_ref() && *known_case == *case_sensitivity
                })
                .unwrap_or_else(|| {
                    tokens.push((Arc::clone(value), *case_sensitivity));
                    tokens.len() - 1
                });
            if subset.contains(&index) {
                return None;
            }
            subset.push(index);
        }
        subset.sort_unstable();
        if minimum.is_some_and(|known| known != subset.len()) {
            return None;
        }
        minimum = Some(subset.len());
        subsets.push(subset);
    }
    let minimum = minimum?;
    if minimum == 0 || minimum > tokens.len() {
        return None;
    }
    subsets.sort_unstable();
    subsets.dedup();
    let expected = bounded_combination_count(tokens.len(), minimum)?;
    (expected == subsets.len()).then_some((tokens, minimum))
}

fn bounded_combination_count(n: usize, k: usize) -> Option<usize> {
    let k = k.min(n.saturating_sub(k));
    let mut count = 1usize;
    for index in 1..=k {
        count = count.checked_mul(n - k + index)?.checked_div(index)?;
    }
    Some(count)
}

fn cached_message_token_min_match_candidates(
    postings: &MessageTokenPostings,
    record_count: u32,
    tokens: &[(Arc<str>, CaseSensitivity)],
    minimum: usize,
) -> Vec<u32> {
    let mut counts = vec![0u16; record_count as usize];
    for (token, _) in tokens {
        let Some(posting) = postings.get(normalize_term(token).as_ref()) else {
            continue;
        };
        for ordinal in posting.ordinals.iter().copied() {
            if let Some(count) = counts.get_mut(ordinal as usize) {
                *count = count.saturating_add(1);
            }
        }
    }
    counts
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, count)| {
            (usize::from(count) >= minimum).then_some(
                u32::try_from(ordinal)
                    .expect("indexed frame record count is bounded by a u32 ordinal"),
            )
        })
        .collect()
}

fn exact_message_token_min_match_candidates(
    postings: &[Option<Arc<[u32]>>],
    record_count: u32,
    minimum: usize,
) -> Vec<u32> {
    let mut counts = vec![0u16; record_count as usize];
    for posting in postings.iter().flatten() {
        for ordinal in posting.iter().copied() {
            if let Some(count) = counts.get_mut(ordinal as usize) {
                *count = count.saturating_add(1);
            }
        }
    }
    counts
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, count)| {
            (usize::from(count) >= minimum).then_some(
                u32::try_from(ordinal)
                    .expect("indexed frame record count is bounded by a u32 ordinal"),
            )
        })
        .collect()
}

fn hot_message_token_min_match_candidates(
    partition: &PartitionIndex,
    tokens: &[(Arc<str>, CaseSensitivity)],
    minimum: usize,
    start: u32,
    end: u32,
) -> Vec<u32> {
    if start >= end {
        return Vec::new();
    }
    let mut counts = vec![0u16; (end - start) as usize];
    for (token, _) in tokens {
        let Some(term_id) = partition.term_ids.get(normalize_term(token).as_ref()) else {
            continue;
        };
        let Some(postings) = partition.term_postings.get(*term_id) else {
            continue;
        };
        for run in &postings.runs {
            if run.last < start {
                continue;
            }
            if run.first >= end {
                break;
            }
            let first = run.first.max(start);
            let last = run.last.min(end - 1);
            for ordinal in first..=last {
                let index = (ordinal - start) as usize;
                counts[index] = counts[index].saturating_add(1);
            }
        }
    }
    counts
        .into_iter()
        .enumerate()
        .filter_map(|(index, count)| {
            (usize::from(count) >= minimum).then_some(start + index as u32)
        })
        .collect()
}

fn hot_predicate_candidates(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if let Some((tokens, minimum)) = message_token_min_match_shape(predicate) {
        return Some(hot_message_token_min_match_candidates(
            partition, &tokens, minimum, start, end,
        ));
    }
    match predicate {
        LogPredicate::MatchAll => Some((start..end).collect()),
        LogPredicate::MatchNone => Some(Vec::new()),
        LogPredicate::Term(term) => {
            let normalized = normalize_term(term);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Some(Vec::new());
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Some(Vec::new());
            };
            Some(postings.collect_in(start, end, QueryOrder::OldestFirst, None))
        }
        LogPredicate::MessageToken { value, .. } => {
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return Some(Vec::new());
            }
            let normalized = normalize_term(value);
            let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
                return Some(Vec::new());
            };
            let Some(postings) = partition.term_postings.get(*term_id) else {
                return Some(Vec::new());
            };
            Some(postings.collect_in(start, end, QueryOrder::OldestFirst, None))
        }
        LogPredicate::MessageTokenPrefix { value, .. } => {
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return Some(Vec::new());
            }
            let prefix = normalize_term(value);
            hot_message_token_candidates(partition, start, end, |token| {
                token.starts_with(prefix.as_ref())
            })
        }
        LogPredicate::MessageTokenRegex(regex) => {
            if regex.case_sensitivity() == CaseSensitivity::Sensitive
                && regex
                    .pattern()
                    .bytes()
                    .any(|byte| byte.is_ascii_uppercase())
            {
                return Some((start..end).collect());
            }
            hot_message_token_candidates(partition, start, end, |token| regex.is_match(token))
        }
        LogPredicate::MessagePhrase { terms, .. } => {
            hot_message_phrase_candidates(partition, terms, start, end)
        }
        LogPredicate::MessageFuzzy {
            value,
            max_distance,
        } => {
            if value.is_empty() || value.bytes().any(|byte| !byte.is_ascii_alphanumeric()) {
                return Some(Vec::new());
            }
            let value = normalize_term(value);
            hot_message_token_candidates(partition, start, end, |token| {
                bounded_levenshtein(token, value.as_ref(), usize::from(*max_distance))
            })
        }
        LogPredicate::FieldExists(key) => Some(
            partition
                .field_presence_postings
                .get(key)
                .map(|postings| postings.collect_in(start, end, QueryOrder::OldestFirst, None))
                .unwrap_or_default(),
        ),
        LogPredicate::Field { key, matcher } => {
            hot_field_text_candidates(partition, key, matcher, start, end)
        }
        LogPredicate::FieldIn { key, values } => {
            // Merge the value postings directly. Materializing one ordinal
            // vector per requested value only to union them doubles the
            // allocation and copy work for common two-value filters.
            let Some(value_ids) = partition.field_ids.get(key) else {
                return Some(Vec::new());
            };
            let mut field_postings = Vec::with_capacity(values.len());
            for value in values {
                if let Some(field_id) = value_ids.get(value.as_ref())
                    && let Some(posting) = partition.field_postings.get(*field_id)
                {
                    field_postings.push(posting);
                }
            }
            Some(collect_hot_posting_union(&field_postings, start, end, None))
        }
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => hot_numeric_field_candidates(partition, key, *comparison, *value, start, end),
        LogPredicate::And(predicates) => {
            let mut current = None;
            for predicate in predicates {
                if matches!(predicate, LogPredicate::MatchAll) {
                    continue;
                }
                let candidates = hot_predicate_candidates(predicate, partition, start, end)?;
                intersect_frame_candidate_slice(&mut current, &candidates);
                if current.as_ref().is_some_and(Vec::is_empty) {
                    return Some(Vec::new());
                }
            }
            Some(current.unwrap_or_else(|| (start..end).collect()))
        }
        LogPredicate::Or(predicates) => {
            let mut candidates = Vec::new();
            for predicate in predicates {
                if matches!(predicate, LogPredicate::MatchNone) {
                    continue;
                }
                if matches!(predicate, LogPredicate::MatchAll) {
                    return Some((start..end).collect());
                }
                union_sorted_ordinals(
                    &mut candidates,
                    hot_predicate_candidates(predicate, partition, start, end)?,
                );
            }
            Some(candidates)
        }
        LogPredicate::Not(predicate) if hot_predicate_candidates_are_exact(predicate) => {
            let excluded = hot_predicate_candidates(predicate, partition, start, end)?;
            let mut candidates = Vec::with_capacity(
                (end.saturating_sub(start) as usize).saturating_sub(excluded.len()),
            );
            let mut next = start;
            for ordinal in excluded {
                if ordinal < next || ordinal >= end {
                    continue;
                }
                candidates.extend(next..ordinal);
                next = ordinal.saturating_add(1);
            }
            if next < end {
                candidates.extend(next..end);
            }
            Some(candidates)
        }
        LogPredicate::Message(matcher) => {
            hot_message_literal_candidates(partition, matcher, start, end)
        }
        LogPredicate::MessageRegex(regex) => {
            hot_message_regex_token_candidates(partition, regex, start, end)
        }
        LogPredicate::Not(_) => None,
        LogPredicate::FieldRegex { key, regex } => hot_field_predicate_candidates(
            partition,
            key,
            |value| regex.is_match(value),
            start,
            end,
        ),
    }
}

fn hot_message_literal_candidates(
    partition: &PartitionIndex,
    matcher: &crate::TextMatcher,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if matcher.value.is_empty() {
        return None;
    }
    if matcher.case_sensitivity == CaseSensitivity::Insensitive
        && !partition.message_trigram_ascii_only
    {
        // The resident token/trigram indexes only implement ASCII folding.
        // Once a partition contains Unicode, retain the exact Unicode scan
        // for every insensitive literal rather than risking a false negative.
        return None;
    }
    if !matcher.value.is_ascii() {
        // Token and resident-trigram indexes use ASCII boundary/folding
        // rules. Unicode literals stay on the exact residual matcher.
        return None;
    }
    if matcher.kind == crate::TextMatchKind::Contains
        && matcher.value.len() >= 3
        && (matcher.case_sensitivity == CaseSensitivity::Sensitive
            || partition.message_trigram_ascii_only)
        && let Some(candidates) =
            hot_message_trigram_candidates(partition, &matcher.value, start, end)
    {
        return Some(candidates);
    }
    let bytes = matcher.value.as_bytes();
    let mut runs = Vec::new();
    let mut run_start = None;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte.is_ascii_alphanumeric() {
            run_start.get_or_insert(index);
        } else if let Some(begin) = run_start.take() {
            runs.push((begin, index));
        }
    }
    if let Some(begin) = run_start {
        runs.push((begin, bytes.len()));
    }
    if runs.len() != 1 {
        return None;
    }
    let (begin, finish) = runs[0];
    let leading_boundary = begin > 0
        && bytes[..begin]
            .iter()
            .all(|byte| !byte.is_ascii_alphanumeric());
    let trailing_boundary = finish < bytes.len()
        && bytes[finish..]
            .iter()
            .all(|byte| !byte.is_ascii_alphanumeric());
    let term = normalize_term(&matcher.value[begin..finish]);
    match matcher.kind {
        crate::TextMatchKind::Contains if leading_boundary && trailing_boundary => {
            hot_predicate_candidates(
                &LogPredicate::Term(Arc::from(term.as_ref())),
                partition,
                start,
                end,
            )
        }
        crate::TextMatchKind::Contains if leading_boundary => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.starts_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Contains if trailing_boundary => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.ends_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Contains => {
            // The resident directory folds ASCII bytes only. Unicode
            // case-insensitive matching uses full lowercase expansion, so it
            // must retain the verified scan unless the caller is sensitive.
            (matcher.case_sensitivity == CaseSensitivity::Sensitive
                || partition.message_trigram_ascii_only)
                .then(|| hot_message_trigram_candidates(partition, &matcher.value, start, end))
                .flatten()
        }
        crate::TextMatchKind::Prefix => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.starts_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Suffix => {
            hot_message_token_candidates(partition, start, end, |token| {
                token.ends_with(term.as_ref())
            })
        }
        crate::TextMatchKind::Exact => None,
    }
}

fn regex_boundary_safe_literal(pattern: &str) -> Option<&str> {
    let bytes = pattern.as_bytes();
    let mut run_start = None;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if byte == b'b' && index > 0 && bytes[index - 1] == b'\\' {
            continue;
        }
        if byte.is_ascii_alphanumeric() {
            run_start.get_or_insert(index);
            continue;
        }
        let Some(begin) = run_start.take() else {
            continue;
        };
        let left_boundary = begin == 0
            || bytes[begin - 1] == b'^'
            || (begin >= 2 && bytes[begin - 2..begin] == *b"\\b");
        let right_boundary = index == bytes.len()
            || bytes[index] == b'$'
            || (index + 2 <= bytes.len() && bytes[index..index + 2] == *b"\\b");
        if left_boundary && right_boundary {
            return Some(&pattern[begin..index]);
        }
    }
    let begin = run_start?;
    let left_boundary = begin == 0
        || bytes[begin - 1] == b'^'
        || (begin >= 2 && bytes[begin - 2..begin] == *b"\\b");
    if left_boundary {
        return Some(&pattern[begin..]);
    }
    None
}

fn hot_message_regex_token_candidates(
    partition: &PartitionIndex,
    regex: &crate::LogRegex,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    let literal = regex_boundary_safe_literal(regex.pattern())?;
    let term_id = partition.term_ids.get(normalize_term(literal).as_ref())?;
    let postings = partition.term_postings.get(*term_id)?;
    Some(postings.collect_in(start, end, QueryOrder::OldestFirst, None))
}

fn cached_message_predicate_is_exact(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll | LogPredicate::MatchNone => true,
        LogPredicate::MessageToken {
            case_sensitivity, ..
        } => *case_sensitivity == CaseSensitivity::Insensitive,
        LogPredicate::MessageTokenRegex(regex) => {
            regex.case_sensitivity() == CaseSensitivity::Insensitive
        }
        LogPredicate::MessageTokenPrefix {
            case_sensitivity, ..
        } => *case_sensitivity == CaseSensitivity::Insensitive,
        LogPredicate::MessagePhrase { .. } => true,
        LogPredicate::MessageFuzzy { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            !predicates.is_empty() && predicates.iter().all(cached_message_predicate_is_exact)
        }
        LogPredicate::Not(predicate) => cached_message_predicate_is_exact(predicate),
        _ => false,
    }
}

fn message_predicate_is_message_only(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Term(_)
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(message_predicate_is_message_only)
        }
        LogPredicate::Not(predicate) => message_predicate_is_message_only(predicate),
        LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => false,
    }
}

fn cached_message_predicate_candidates_are_cheap(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            !predicates.is_empty()
                && predicates
                    .iter()
                    .all(cached_message_predicate_candidates_are_cheap)
        }
        LogPredicate::Not(predicate) => cached_message_predicate_candidates_are_cheap(predicate),
        _ => false,
    }
}

fn message_cache_can_supply_frame_base(query: &LogQuery, tenant: &str) -> bool {
    query.terms.is_empty()
        && query.exact_fields.iter().all(|field| {
            field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() == tenant
        })
        && cached_message_predicate_candidates_are_cheap(&query.predicate)
}

fn predicate_is_indexed_conjunction(predicate: &LogPredicate) -> bool {
    fn indexed_message_atom(predicate: &LogPredicate) -> bool {
        match predicate {
            LogPredicate::Term(_) => true,
            LogPredicate::MessageToken {
                case_sensitivity: CaseSensitivity::Insensitive,
                ..
            }
            | LogPredicate::MessageTokenPrefix {
                case_sensitivity: CaseSensitivity::Insensitive,
                ..
            } => true,
            LogPredicate::MessageTokenRegex(regex)
                if regex.case_sensitivity() == CaseSensitivity::Insensitive =>
            {
                true
            }
            _ => false,
        }
    }

    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::Term(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessageTokenPrefix {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::MessageTokenRegex(regex)
            if regex.case_sensitivity() == CaseSensitivity::Insensitive =>
        {
            true
        }
        LogPredicate::Or(predicates) => {
            !predicates.is_empty() && predicates.iter().all(indexed_message_atom)
        }
        LogPredicate::And(predicates) => predicates.iter().all(predicate_is_indexed_conjunction),
        _ => false,
    }
}

fn hot_message_token_candidates(
    partition: &PartitionIndex,
    start: u32,
    end: u32,
    mut matches_token: impl FnMut(&str) -> bool,
) -> Option<Vec<u32>> {
    let mut postings = Vec::new();
    for (token, term_id) in &partition.term_ids {
        if matches_token(token)
            && let Some(posting) = partition.term_postings.get(*term_id)
        {
            postings.push(posting);
        }
    }
    Some(collect_hot_posting_union(&postings, start, end, None))
}

fn hot_message_trigram_candidates(
    partition: &PartitionIndex,
    literal: &str,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if !partition.message_trigram_index_complete {
        return None;
    }
    let keys = collect_message_trigram_keys(literal);
    if keys.is_empty() {
        return None;
    }
    let mut postings = Vec::with_capacity(keys.len());
    for key in keys {
        let Some(posting) = partition.message_trigram_postings.get(&key) else {
            return Some(Vec::new());
        };
        postings.push(posting);
    }
    Some(collect_hot_posting_intersection(
        &postings,
        start,
        end,
        QueryOrder::OldestFirst,
        None,
    ))
}

fn hot_message_phrase_candidates(
    partition: &PartitionIndex,
    terms: &[Arc<str>],
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    if terms.is_empty() {
        return Some((start..end).collect());
    }
    let mut current = None;
    for term in terms {
        let normalized = normalize_term(term);
        let Some(term_id) = partition.term_ids.get(normalized.as_ref()) else {
            return Some(Vec::new());
        };
        let Some(postings) = partition.term_postings.get(*term_id) else {
            return Some(Vec::new());
        };
        let candidates = postings.collect_in(start, end, QueryOrder::OldestFirst, None);
        intersect_frame_candidate_slice(&mut current, &candidates);
        if current.as_ref().is_some_and(Vec::is_empty) {
            return Some(Vec::new());
        }
    }
    Some(current.unwrap_or_default())
}

fn hot_predicate_postings<'a>(
    predicate: &LogPredicate,
    partition: &'a PartitionIndex,
) -> Option<Vec<&'a HotPostingList>> {
    match predicate {
        LogPredicate::MatchNone => Some(Vec::new()),
        LogPredicate::Term(term) => Some(
            partition
                .term_ids
                .get(normalize_term(term).as_ref())
                .and_then(|term_id| partition.term_postings.get(*term_id))
                .into_iter()
                .collect(),
        ),
        LogPredicate::MessageToken {
            value,
            case_sensitivity: CaseSensitivity::Insensitive,
        } => Some(
            partition
                .term_ids
                .get(normalize_term(value).as_ref())
                .and_then(|term_id| partition.term_postings.get(*term_id))
                .into_iter()
                .collect(),
        ),
        LogPredicate::FieldExists(key) => Some(
            partition
                .field_presence_postings
                .get(key)
                .into_iter()
                .collect(),
        ),
        LogPredicate::Field { key, matcher } => Some(
            partition
                .field_ids
                .get(key)
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(value, _)| text_matches(value, matcher))
                .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::FieldIn { key, values } => Some(
            partition
                .field_ids
                .get(key)
                .into_iter()
                .flat_map(|field_ids| {
                    values
                        .iter()
                        .filter_map(|value| field_ids.get(value.as_ref()))
                })
                .filter_map(|field_id| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::FieldRegex { key, regex } => Some(
            partition
                .field_ids
                .get(key)
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(value, _)| regex.is_match(value))
                .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => Some(
            partition
                .numeric_field_values
                .get(key)
                .into_iter()
                .flat_map(|values| values.iter())
                .filter(|(observed, _)| numeric_comparison_matches(*comparison, *observed, *value))
                .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
                .collect(),
        ),
        LogPredicate::Or(predicates) => {
            let mut postings = Vec::new();
            for predicate in predicates {
                postings.extend(hot_predicate_postings(predicate, partition)?);
            }
            Some(postings)
        }
        _ => None,
    }
}

fn visit_hot_predicate_candidates(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
    visit: impl FnMut(u32) -> bool,
) -> Option<bool> {
    let postings = hot_predicate_postings(predicate, partition)?;
    Some(visit_hot_posting_union(&postings, start, end, visit))
}

fn optimized_hot_predicate_candidates(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
    limit: Option<usize>,
) -> Option<Vec<u32>> {
    if limit.is_none() {
        return hot_predicate_candidates(predicate, partition, start, end);
    }
    if !hot_predicate_candidates_are_exact(predicate) {
        return hot_predicate_candidates(predicate, partition, start, end);
    }
    if let LogPredicate::And(predicates) = predicate
        && predicates.len() >= 2
    {
        let driver_index = predicates
            .iter()
            .enumerate()
            .filter(|(_, child)| hot_predicate_postings(child, partition).is_some())
            .min_by_key(|(_, child)| hot_predicate_cardinality(child, partition, start, end))
            .map(|(index, _)| index);
        if let Some(driver_index) = driver_index {
            let take = limit.unwrap_or(usize::MAX);
            let mut selected = Vec::with_capacity(take.min(hot_predicate_cardinality(
                &predicates[driver_index],
                partition,
                start,
                end,
            )));
            if visit_hot_predicate_candidates(
                &predicates[driver_index],
                partition,
                start,
                end,
                |ordinal| {
                    if predicates.iter().enumerate().all(|(index, child)| {
                        index == driver_index
                            || hot_predicate_matches_ordinal(child, partition, ordinal)
                    }) {
                        selected.push(ordinal);
                    }
                    selected.len() < take
                },
            )
            .is_some()
            {
                return Some(selected);
            }
        }
    }
    if let Some(driver) = hot_predicate_driver_posting(predicate, partition, start, end) {
        let take = limit.unwrap_or(usize::MAX);
        let mut selected = Vec::with_capacity(take.min(driver.cardinality_in(start, end)));
        driver.visit_in(start, end, QueryOrder::OldestFirst, |ordinal| {
            if hot_predicate_matches_ordinal(predicate, partition, ordinal) {
                selected.push(ordinal);
            }
            selected.len() < take
        });
        return Some(selected);
    }
    let LogPredicate::And(predicates) = predicate else {
        let mut candidates = hot_predicate_candidates(predicate, partition, start, end)?;
        if let Some(limit) = limit {
            candidates.truncate(limit);
        }
        return Some(candidates);
    };
    if predicates.len() < 2 {
        let mut candidates = hot_predicate_candidates(predicate, partition, start, end)?;
        if let Some(limit) = limit {
            candidates.truncate(limit);
        }
        return Some(candidates);
    }

    let driver_index = predicates
        .iter()
        .enumerate()
        .min_by_key(|(_, child)| hot_predicate_cardinality(child, partition, start, end))
        .map(|(index, _)| index)?;
    let take = limit.unwrap_or(usize::MAX);
    let mut selected = Vec::new();
    if visit_hot_predicate_candidates(
        &predicates[driver_index],
        partition,
        start,
        end,
        |ordinal| {
            if predicates.iter().enumerate().all(|(index, child)| {
                index == driver_index || hot_predicate_matches_ordinal(child, partition, ordinal)
            }) {
                selected.push(ordinal);
            }
            selected.len() < take
        },
    )
    .is_some()
    {
        return Some(selected);
    }

    let mut driver = hot_predicate_candidates(&predicates[driver_index], partition, start, end)?;
    selected.reserve(take.min(driver.len()));
    for ordinal in driver.drain(..) {
        if predicates.iter().enumerate().all(|(index, child)| {
            index == driver_index || hot_predicate_matches_ordinal(child, partition, ordinal)
        }) {
            selected.push(ordinal);
            if selected.len() == take {
                break;
            }
        }
    }
    Some(selected)
}

fn hot_predicate_driver_posting<'a>(
    predicate: &LogPredicate,
    partition: &'a PartitionIndex,
    start: u32,
    end: u32,
) -> Option<&'a HotPostingList> {
    match predicate {
        LogPredicate::Term(term) => partition
            .term_ids
            .get(normalize_term(term).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id)),
        LogPredicate::FieldExists(key) => partition.field_presence_postings.get(key),
        LogPredicate::Field { key, matcher } => {
            let mut driver = None;
            for (value, field_id) in partition.field_ids.get(key)? {
                if text_matches(value, matcher) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::FieldIn { key, values } => {
            let mut driver = None;
            let field_ids = partition.field_ids.get(key)?;
            for value in values {
                if let Some(field_id) = field_ids.get(value.as_ref()) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => {
            let mut driver = None;
            for (observed, field_id) in partition.numeric_field_values.get(key)? {
                if numeric_comparison_matches(*comparison, *observed, *value) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::FieldRegex { key, regex } => {
            let mut driver = None;
            for (value, field_id) in partition.field_ids.get(key)? {
                if regex.is_match(value) {
                    let postings = partition.field_postings.get(*field_id)?;
                    if driver.is_some() {
                        return None;
                    }
                    driver = Some(postings);
                }
            }
            driver
        }
        LogPredicate::And(predicates) => predicates
            .iter()
            .filter_map(|predicate| hot_predicate_driver_posting(predicate, partition, start, end))
            .min_by_key(|postings| postings.cardinality_in(start, end)),
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Or(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::Not(_) => None,
    }
}

fn hot_single_posting_predicate(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::Term(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::FieldIn { values, .. } => values.len() == 1,
        _ => false,
    }
}

fn hot_predicate_cardinality(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    start: u32,
    end: u32,
) -> usize {
    match predicate {
        LogPredicate::MatchAll => end.saturating_sub(start) as usize,
        LogPredicate::MatchNone => 0,
        LogPredicate::Term(term) => partition
            .term_ids
            .get(normalize_term(term).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id))
            .map_or(0, |postings| postings.cardinality_in(start, end)),
        LogPredicate::FieldExists(key) => partition
            .field_presence_postings
            .get(key)
            .map_or(0, |postings| postings.cardinality_in(start, end)),
        LogPredicate::Field { key, matcher } => partition
            .field_ids
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .filter(|(value, _)| text_matches(value, matcher))
            .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
            .map(|postings| postings.cardinality_in(start, end))
            .sum(),
        LogPredicate::FieldIn { key, values } => values
            .iter()
            .filter_map(|value| {
                partition
                    .field_ids
                    .get(key)
                    .and_then(|ids| ids.get(value.as_ref()))
                    .and_then(|field_id| partition.field_postings.get(*field_id))
            })
            .map(|postings| postings.cardinality_in(start, end))
            .sum(),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => partition
            .numeric_field_values
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .filter(|(observed, _)| numeric_comparison_matches(*comparison, *observed, *value))
            .filter_map(|(_, field_id)| partition.field_postings.get(*field_id))
            .map(|postings| postings.cardinality_in(start, end))
            .sum(),
        LogPredicate::And(predicates) => predicates
            .iter()
            .map(|predicate| hot_predicate_cardinality(predicate, partition, start, end))
            .min()
            .unwrap_or_else(|| end.saturating_sub(start) as usize),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| hot_predicate_cardinality(predicate, partition, start, end))
            .fold(0usize, usize::saturating_add)
            .min(end.saturating_sub(start) as usize),
        LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::Not(_)
        | LogPredicate::FieldRegex { .. } => end.saturating_sub(start) as usize,
    }
}

fn hot_predicate_matches_ordinal(
    predicate: &LogPredicate,
    partition: &PartitionIndex,
    ordinal: u32,
) -> bool {
    match predicate {
        LogPredicate::MatchAll => true,
        LogPredicate::MatchNone => false,
        LogPredicate::Term(term) => partition
            .term_ids
            .get(normalize_term(term).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id))
            .is_some_and(|postings| postings.contains(ordinal)),
        LogPredicate::FieldExists(key) => partition
            .field_presence_postings
            .get(key)
            .is_some_and(|postings| postings.contains(ordinal)),
        LogPredicate::Field { key, matcher } => partition
            .field_ids
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .any(|(value, field_id)| {
                text_matches(value, matcher)
                    && partition
                        .field_postings
                        .get(*field_id)
                        .is_some_and(|postings| postings.contains(ordinal))
            }),
        LogPredicate::FieldIn { key, values } => values.iter().any(|value| {
            partition
                .field_ids
                .get(key)
                .and_then(|ids| ids.get(value.as_ref()))
                .and_then(|field_id| partition.field_postings.get(*field_id))
                .is_some_and(|postings| postings.contains(ordinal))
        }),
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } => partition
            .numeric_field_values
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .any(|(observed, field_id)| {
                numeric_comparison_matches(*comparison, *observed, *value)
                    && partition
                        .field_postings
                        .get(*field_id)
                        .is_some_and(|postings| postings.contains(ordinal))
            }),
        LogPredicate::And(predicates) => predicates
            .iter()
            .all(|predicate| hot_predicate_matches_ordinal(predicate, partition, ordinal)),
        LogPredicate::Or(predicates) => predicates
            .iter()
            .any(|predicate| hot_predicate_matches_ordinal(predicate, partition, ordinal)),
        LogPredicate::FieldRegex { key, regex } => partition
            .field_ids
            .get(key)
            .into_iter()
            .flat_map(|values| values.iter())
            .any(|(value, field_id)| {
                regex.is_match(value)
                    && partition
                        .field_postings
                        .get(*field_id)
                        .is_some_and(|postings| postings.contains(ordinal))
            }),
        LogPredicate::MessageToken {
            value,
            case_sensitivity: CaseSensitivity::Insensitive,
        } => partition
            .term_ids
            .get(normalize_term(value).as_ref())
            .and_then(|term_id| partition.term_postings.get(*term_id))
            .is_some_and(|postings| postings.contains(ordinal)),
        LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_) => false,
        LogPredicate::Not(predicate) => {
            hot_predicate_candidates_are_exact(predicate)
                && !hot_predicate_matches_ordinal(predicate, partition, ordinal)
        }
    }
}

fn hot_predicate_candidates_are_exact(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::Term(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(hot_predicate_candidates_are_exact)
        }
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageToken { .. }
        | LogPredicate::MessageTokenPrefix { .. }
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. }
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_) => false,
        LogPredicate::Not(predicate) => hot_predicate_candidates_are_exact(predicate),
    }
}

fn hot_field_text_candidates(
    partition: &PartitionIndex,
    key: &str,
    matcher: &crate::TextMatcher,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    hot_field_predicate_candidates(
        partition,
        key,
        |value| text_matches(value, matcher),
        start,
        end,
    )
}

fn hot_field_predicate_candidates(
    partition: &PartitionIndex,
    key: &str,
    mut matches_value: impl FnMut(&str) -> bool,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    let Some(value_ids) = partition.field_ids.get(key) else {
        return Some(Vec::new());
    };
    let mut field_postings = Vec::new();
    for (value, field_id) in value_ids {
        if matches_value(value)
            && let Some(posting) = partition.field_postings.get(*field_id)
        {
            field_postings.push(posting);
        }
    }
    Some(collect_hot_posting_union(&field_postings, start, end, None))
}

fn numeric_comparison_matches(comparison: NumericComparison, observed: i128, target: i128) -> bool {
    match comparison {
        NumericComparison::Equal => observed == target,
        NumericComparison::NotEqual => observed != target,
        NumericComparison::LessThan => observed < target,
        NumericComparison::LessThanOrEqual => observed <= target,
        NumericComparison::GreaterThan => observed > target,
        NumericComparison::GreaterThanOrEqual => observed >= target,
    }
}

fn hot_numeric_field_candidates(
    partition: &PartitionIndex,
    key: &str,
    comparison: NumericComparison,
    target: i128,
    start: u32,
    end: u32,
) -> Option<Vec<u32>> {
    let Some(value_ids) = partition.numeric_field_values.get(key) else {
        return Some(Vec::new());
    };
    let mut field_postings = Vec::new();
    for (observed, field_id) in value_ids {
        let matches = match comparison {
            NumericComparison::Equal => *observed == target,
            NumericComparison::NotEqual => *observed != target,
            NumericComparison::LessThan => *observed < target,
            NumericComparison::LessThanOrEqual => *observed <= target,
            NumericComparison::GreaterThan => *observed > target,
            NumericComparison::GreaterThanOrEqual => *observed >= target,
        };
        if matches && let Some(posting) = partition.field_postings.get(*field_id) {
            field_postings.push(posting);
        }
    }
    Some(collect_hot_posting_union(&field_postings, start, end, None))
}

fn retain_top_timestamp_ordinals(
    ordinals: &mut Vec<u32>,
    partition: &PartitionIndex,
    query: &LogQuery,
    limit: usize,
) {
    if limit == 0 {
        ordinals.clear();
        return;
    }
    let keep = limit.min(ordinals.len());
    if keep < ordinals.len() {
        if partition.timestamp_order == TimestampOrder::NonDecreasing {
            match query.order {
                QueryOrder::OldestFirst => ordinals.truncate(keep),
                QueryOrder::NewestFirst => {
                    let mut selected = ordinals.split_off(ordinals.len() - keep);
                    selected.reverse();
                    *ordinals = selected;
                }
            }
            return;
        }
        let mut ascending = true;
        let mut descending = true;
        for pair in ordinals.windows(2) {
            match query.compare(
                &partition.records[pair[0] as usize].record,
                &partition.records[pair[1] as usize].record,
            ) {
                std::cmp::Ordering::Less => descending = false,
                std::cmp::Ordering::Greater => ascending = false,
                std::cmp::Ordering::Equal => {}
            }
            if !ascending && !descending {
                break;
            }
        }
        if ascending {
            ordinals.truncate(keep);
            return;
        }
        if descending {
            let mut selected = ordinals.split_off(ordinals.len() - keep);
            selected.reverse();
            *ordinals = selected;
            return;
        }
        ordinals.select_nth_unstable_by(keep - 1, |left, right| {
            compare_timestamp_ordinals(partition, query.order, *left, *right)
        });
        ordinals.truncate(keep);
    }
    ordinals.sort_unstable_by(|left, right| {
        compare_timestamp_ordinals(partition, query.order, *left, *right)
    });
}

fn compare_timestamp_ordinals(
    partition: &PartitionIndex,
    order: QueryOrder,
    left: u32,
    right: u32,
) -> std::cmp::Ordering {
    let left = &partition
        .records
        .get(left as usize)
        .expect("indexed reference has a visible record")
        .record;
    let right = &partition
        .records
        .get(right as usize)
        .expect("indexed reference has a visible record")
        .record;
    let ordering = left
        .timestamp_unix_nanos
        .cmp(&right.timestamp_unix_nanos)
        .then_with(|| left.record_ref.offset.cmp(&right.record_ref.offset));
    match order {
        QueryOrder::OldestFirst => ordering,
        QueryOrder::NewestFirst => ordering.reverse(),
    }
}

fn sort_and_limit_matches(matches: &mut Vec<LogMatch>, query: &LogQuery, limit: usize) {
    let already_sorted = matches
        .windows(2)
        .all(|pair| query.compare(&pair[0].record, &pair[1].record) != std::cmp::Ordering::Greater);
    if !already_sorted {
        matches.sort_unstable_by(|left, right| query.compare(&left.record, &right.record));
    }
    matches.truncate(limit);
}

fn intersect_frame_candidates(current: &mut Option<Vec<u32>>, mut incoming: Vec<u32>) {
    let Some(existing) = current.as_mut() else {
        *current = Some(incoming);
        return;
    };
    if existing.len() > incoming.len() {
        std::mem::swap(existing, &mut incoming);
    }
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    let mut write_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => existing_index += 1,
            std::cmp::Ordering::Greater => incoming_index += 1,
            std::cmp::Ordering::Equal => {
                existing[write_index] = existing[existing_index];
                write_index += 1;
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    existing.truncate(write_index);
}

fn intersect_frame_candidate_slice(current: &mut Option<Vec<u32>>, incoming: &[u32]) {
    let Some(existing) = current.as_mut() else {
        *current = Some(incoming.to_vec());
        return;
    };
    if existing.len() <= incoming.len() {
        if existing.len().saturating_mul(4) < incoming.len() {
            existing.retain(|ordinal| incoming.binary_search(ordinal).is_ok());
            return;
        }
        let mut existing_index = 0usize;
        let mut incoming_index = 0usize;
        let mut write_index = 0usize;
        while existing_index < existing.len() && incoming_index < incoming.len() {
            match existing[existing_index].cmp(&incoming[incoming_index]) {
                std::cmp::Ordering::Less => existing_index += 1,
                std::cmp::Ordering::Greater => incoming_index += 1,
                std::cmp::Ordering::Equal => {
                    existing[write_index] = existing[existing_index];
                    write_index += 1;
                    existing_index += 1;
                    incoming_index += 1;
                }
            }
        }
        existing.truncate(write_index);
        return;
    }
    if incoming.len().saturating_mul(4) < existing.len() {
        let mut result = Vec::with_capacity(incoming.len());
        for ordinal in incoming {
            if existing.binary_search(ordinal).is_ok() {
                result.push(*ordinal);
            }
        }
        *existing = result;
        return;
    }
    let mut result = Vec::with_capacity(incoming.len());
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => existing_index += 1,
            std::cmp::Ordering::Greater => incoming_index += 1,
            std::cmp::Ordering::Equal => {
                result.push(existing[existing_index]);
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    *existing = result;
}

#[cfg(test)]
fn intersect_ordinal_runs(candidates: &mut Vec<u32>, runs: &[OrdinalRun], start: u32, end: u32) {
    let mut candidate_index = 0usize;
    let mut run_index = runs.partition_point(|run| run.last < start);
    let mut write_index = 0usize;
    while candidate_index < candidates.len() && run_index < runs.len() {
        let candidate = candidates[candidate_index];
        let run = runs[run_index];
        if candidate >= end || run.first >= end {
            break;
        }
        if candidate < run.first {
            candidate_index += 1;
        } else if candidate > run.last {
            run_index += 1;
        } else {
            candidates[write_index] = candidate;
            write_index += 1;
            candidate_index += 1;
        }
    }
    candidates.truncate(write_index);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message;
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

    use super::*;
    use crate::{
        CaseSensitivity, LocalityGranularity, LogPredicate, MetadataField, TextMatchKind,
        TextMatcher, ingest_pack::prepare_ingest_pack,
    };

    fn partition() -> TopicPartition {
        TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3))
    }

    #[test]
    fn structural_candidate_ordinals_are_sorted_and_deduplicated() {
        let mut candidates = vec![210, 42, 210, 7, 42];

        normalize_structural_candidate_ordinals(&mut candidates);

        assert_eq!(candidates, vec![7, 42, 210]);

        let mut sorted = vec![7, 42, 210];
        normalize_structural_candidate_ordinals(&mut sorted);
        assert_eq!(sorted, vec![7, 42, 210]);
    }

    fn record(offset: u64, message: &str) -> DurableLog {
        record_on(ShardId::new(7), offset, message)
    }

    fn record_on(stream_shard_id: ShardId, offset: u64, message: &str) -> DurableLog {
        DurableLog::new(
            stream_shard_id,
            partition(),
            LogicalOffset::new(offset),
            offset * 10,
            message,
            CompressionCohortId::new(4),
        )
    }

    #[test]
    fn batched_message_scores_match_indexed_scores_for_sorted_candidates() {
        let mut request = crate::AnalyticsScanRequest::for_relation(
            Arc::from("tenant"),
            crate::AnalyticsRelation::Logs,
        );
        request.case_insensitive_message_tokens = vec![Arc::from("error"), Arc::from("failed")];
        let scorer = crate::analytics::RelevanceScorer::from_request(&request);
        let posting = |ordinals: &[u32], frequencies: &[u32]| {
            Arc::new(MessageTokenPosting {
                ordinals: Arc::from(ordinals.to_vec()),
                frequencies: Arc::from(frequencies.to_vec()),
            })
        };
        let mut postings = HashMap::new();
        postings.insert(Arc::from("error"), posting(&[0, 2], &[1, 2]));
        postings.insert(Arc::from("failed"), posting(&[1, 2], &[3, 1]));
        let stats = CachedMessageTokenStats {
            postings,
            document_lengths: Arc::from(vec![4, 8, 6]),
            messages: Arc::from(vec![Arc::from(""), Arc::from(""), Arc::from("")]),
            token_ids_by_term: HashMap::new(),
            token_sequence: Arc::from(Vec::<u32>::new()),
            token_offsets: Arc::from(vec![0, 0, 0, 0]),
        };
        let ordinals = [0, 1, 2];
        let mut batched = Vec::new();
        stats
            .score_batch(&scorer, ordinals.into_iter(), |score| {
                batched.push(score);
                Ok::<_, ()>(())
            })
            .expect("batched score emission succeeds");
        let expected = ordinals
            .iter()
            .map(|ordinal| {
                scorer.score_indexed_by_index(stats.document_lengths[*ordinal as usize], |index| {
                    let term = scorer.terms()[index].as_ref();
                    let Some(posting) = stats.postings.get(term) else {
                        return 0;
                    };
                    posting
                        .ordinals
                        .binary_search(ordinal)
                        .ok()
                        .and_then(|position| posting.frequencies.get(position).copied())
                        .unwrap_or_default()
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(batched, expected);
    }

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(value.into())),
            }),
            key_strindex: 0,
        }
    }

    #[test]
    fn hot_predicate_indexes_preserve_text_numeric_regex_and_cursor_results() {
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                ..StripeConfig::default()
            },
        )
        .expect("stripe opens");
        for (offset, message, service, status) in [
            (0, "request rare", "api", "503"),
            (1, "request common", "api", "200"),
            (2, "worker common", "worker", "404"),
            (3, "request rare", "worker", "503"),
        ] {
            stripe
                .apply_durable(
                    record(offset, message)
                        .with_field("service", service)
                        .with_field("status", status),
                )
                .expect("record indexes");
        }
        let offsets = |query: LogQuery| {
            stripe
                .query_checked(&query)
                .expect("hot query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::field(
                    "service",
                    TextMatcher::new("PI", TextMatchKind::Contains, CaseSensitivity::Insensitive),
                ))
            ),
            vec![0, 1]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::field_regex("status", r"5\d+", CaseSensitivity::Sensitive)
                        .expect("regex compiles"),
                )
            ),
            vec![0, 3]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
                    "status",
                    NumericComparison::GreaterThanOrEqual,
                    500,
                ))
            ),
            vec![0, 3]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(" rare"))
            ),
            vec![0, 3]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::message_regex(r"request.*rare$", CaseSensitivity::Sensitive)
                        .expect("regex compiles"),
                )
            ),
            vec![0, 3]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                    LogPredicate::message_contains(" rare"),
                    LogPredicate::field(
                        "service",
                        TextMatcher::new("api", TextMatchKind::Exact, CaseSensitivity::Sensitive,),
                    ),
                ])),
            ),
            vec![0]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition())
                    .newest_first()
                    .after(crate::QueryCursor::new(20, LogicalOffset::new(2)))
                    .with_limit(2),
            ),
            vec![1, 0]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition())
                    .sort_by_timestamp()
                    .newest_first()
                    .with_limit(2),
            ),
            vec![3, 2]
        );
    }

    #[test]
    fn bounded_boolean_queries_stream_union_candidates_until_residual_matches() {
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                ..StripeConfig::default()
            },
        )
        .expect("stripe opens");
        for offset in 0..256 {
            stripe
                .apply_durable(
                    record(
                        offset,
                        if offset % 2 == 0 {
                            "common event"
                        } else {
                            "ordinary event"
                        },
                    )
                    .with_field("service", if offset >= 56 { "late" } else { "early" }),
                )
                .expect("record indexes");
        }

        let query = LogQuery::new(partition())
            .where_predicate(LogPredicate::and(vec![
                LogPredicate::or(vec![
                    LogPredicate::term("common"),
                    LogPredicate::term("rare"),
                ]),
                LogPredicate::field_equals("service", "late"),
            ]))
            .with_limit(100);
        let offsets = stripe
            .query_checked(&query)
            .expect("bounded boolean query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>();

        assert_eq!(offsets.len(), 100);
        assert_eq!(offsets.first(), Some(&56));
        assert_eq!(offsets.last(), Some(&254));
    }

    #[test]
    fn message_posting_candidates_keep_contains_and_regex_exact() {
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                ..StripeConfig::default()
            },
        )
        .expect("stripe opens");
        for (offset, message) in [
            (0, "rare"),
            (1, "request rare"),
            (2, "rarely"),
            (3, "connection refused"),
            (4, "request_id=123 slow rare"),
            (5, "ÄBC"),
        ] {
            stripe
                .apply_durable(record(offset, message))
                .expect("record indexes");
        }

        let offsets = |query: LogQuery| {
            stripe
                .query_checked(&query)
                .expect("hot query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>()
        };
        assert!(crate::query::text_matches(
            "ÄBC",
            &TextMatcher::new("äbc", TextMatchKind::Contains, CaseSensitivity::Insensitive)
        ));
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(" rare")),
            ),
            vec![1, 4]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::message_contains("rare")),
            ),
            vec![0, 1, 2, 4]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::message_contains("äbc")),
            ),
            vec![5]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::message_regex(r"request.*rare$", CaseSensitivity::Sensitive)
                        .expect("regex compiles"),
                ),
            ),
            vec![1, 4]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(LogPredicate::message(
                    TextMatcher::new("conn", TextMatchKind::Prefix, CaseSensitivity::Sensitive),
                ))
            ),
            vec![3]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::message_regex(r"conn.*", CaseSensitivity::Sensitive)
                        .expect("regex compiles"),
                ),
            ),
            vec![3]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::message_regex(r"\brare$", CaseSensitivity::Sensitive)
                        .expect("regex compiles"),
                ),
            ),
            vec![0, 1, 4]
        );
        assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::message_regex(
                        r"request_id=\d+.*\brare$",
                        CaseSensitivity::Sensitive,
                    )
                    .expect("regex compiles"),
                ),
            ),
            vec![4]
        );
    }

    #[test]
    fn min_match_posting_candidates_preserve_all_matches() {
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                ..StripeConfig::default()
            },
        )
        .expect("stripe opens");
        let records = [
            (0, "error failed"),
            (1, "error"),
            (2, "failed charge"),
            (3, "charge cache"),
            (4, "error failed charge cache"),
            (5, "error cache"),
            (6, "healthy"),
        ];
        for (offset, message) in records {
            stripe
                .apply_durable(record(offset, message))
                .expect("record indexes");
        }
        let tokens = ["error", "failed", "charge", "cache"];
        let mut combinations = Vec::new();
        for left in 0..tokens.len() {
            for right in (left + 1)..tokens.len() {
                combinations.push(LogPredicate::and(vec![
                    LogPredicate::message_token(tokens[left], CaseSensitivity::Insensitive),
                    LogPredicate::message_token(tokens[right], CaseSensitivity::Insensitive),
                ]));
            }
        }
        let query = LogQuery::new(partition()).where_predicate(LogPredicate::or(combinations));
        let offsets = stripe
            .query_checked(&query)
            .expect("min-match query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>();
        assert_eq!(offsets, vec![0, 2, 3, 4, 5]);

        let structural_records = records
            .into_iter()
            .map(|(offset, message)| record(offset, message))
            .collect::<Vec<_>>();
        let indexed = crate::encode_indexed_structural_records(&structural_records)
            .expect("indexed structural block encodes");
        let candidates = embedded_message_predicate_candidates(&query.predicate, &indexed.index)
            .expect("embedded min-match candidates are indexable");
        assert!(
            [0, 2, 3, 4, 5]
                .into_iter()
                .all(|ordinal| candidates.binary_search(&ordinal).is_ok())
        );
    }

    #[test]
    fn timestamp_top_k_falls_back_for_out_of_order_ingest() {
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                ..StripeConfig::default()
            },
        )
        .expect("stripe opens");
        for offset in 0..1_024 {
            let mut record = record(offset, &format!("request {offset}"));
            record.timestamp_unix_nanos = 1_024 - offset;
            stripe.apply_durable(record).expect("record indexes");
        }

        let matches = stripe.query(
            &LogQuery::new(partition())
                .sort_by_timestamp()
                .newest_first()
                .with_limit(2),
        );
        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn rebalanced_sub_blocks_merge_in_logical_offset_order() {
        let pending = |offset| {
            let record = record(offset, &format!("request id={offset} completed"));
            PendingRecord {
                source_bytes: row_source_bytes(&record).expect("record size fits"),
                fingerprint: fingerprint_message(&record.message, &record.fields),
                record,
            }
        };
        let mut even = ActiveBlock::from_records(vec![pending(4), pending(0), pending(2)], None, 1);
        let odd = ActiveBlock::from_records(vec![pending(5), pending(1), pending(3)], None, 1);
        even.append_block(odd);

        assert_eq!(
            even.records
                .iter()
                .map(|pending| pending.record.record_ref.offset)
                .collect::<Vec<_>>(),
            (0..6).map(LogicalOffset::new).collect::<Vec<_>>()
        );
        let borrowed = encode_structural_records(&even.records).expect("borrowed encoding");
        let owned = crate::encode_structural_block(
            &even
                .records
                .iter()
                .map(|pending| pending.record.clone())
                .collect::<Vec<_>>(),
        )
        .expect("owned encoding");
        assert_eq!(borrowed, owned);
    }

    #[test]
    fn durable_records_become_visible_to_term_and_metadata_queries() {
        let mut database = ShardTelemetry::new([ShardId::new(7)], StripeConfig::default())
            .expect("database opens");
        database
            .apply_durable(record(0, "ERROR cannot connect").with_field("service", "api"))
            .expect("first append indexes");
        database
            .apply_durable(record(1, "error timeout").with_field("service", "worker"))
            .expect("second append indexes");

        let matches = database
            .query(
                ShardId::new(7),
                &LogQuery::new(partition())
                    .with_term("error")
                    .with_field("service", "api"),
            )
            .expect("query succeeds");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].record.record_ref.offset, LogicalOffset::new(0));
        assert_eq!(
            database
                .stripe(ShardId::new(7))
                .expect("stripe exists")
                .indexed_through(partition()),
            Some(LogicalOffset::new(1))
        );
    }

    #[test]
    fn hot_count_queries_match_materialized_results_without_index_vector_storage() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for offset in 0..1_024 {
            stripe
                .apply_durable(
                    record(
                        offset,
                        if offset % 2 == 0 {
                            "error api"
                        } else {
                            "info worker"
                        },
                    )
                    .with_field("service", if offset % 2 == 0 { "api" } else { "worker" })
                    .with_field("status", if offset % 10 == 0 { "500" } else { "200" }),
                )
                .expect("record indexes");
        }
        let queries = [
            LogQuery::new(partition()).where_predicate(LogPredicate::field_exists("service")),
            LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                LogPredicate::term("error"),
                LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
            ])),
            LogQuery::new(partition())
                .where_predicate(LogPredicate::field_in("service", ["api", "worker"])),
            LogQuery::new(partition())
                .with_field("service", "api")
                .where_predicate(LogPredicate::message_contains("error"))
                .with_timestamp_range(2_000, 8_000),
            LogQuery::new(partition()).newest_first().with_limit(17),
        ];
        for query in queries {
            let expected = stripe.query_checked(&query).expect("query succeeds").len() as u64;
            assert_eq!(
                stripe.count_query_checked(&query).expect("count succeeds"),
                expected,
                "count and materialized query diverged for {query:?}"
            );
        }
    }

    #[test]
    fn active_tenant_partition_cache_invalidates_after_indexed_append() {
        let events = vec![OtlpLogEvent {
            timestamp_unix_nanos: 1_000,
            message: Arc::from("cache invalidation"),
            ..OtlpLogEvent::default()
        }];
        let payload = Bytes::from(
            prepare_ingest_pack(&events)
                .expect("indexed ingest pack prepares")
                .payload,
        );
        let second_partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(4));
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");

        stripe
            .apply_indexed_ingest_pack(partition(), LogicalOffset::new(0), 1, payload.clone())
            .expect("first indexed append installs");
        assert_eq!(
            stripe
                .tenant_partitions("test-tenant")
                .expect("first tenant partition lookup"),
            vec![partition()]
        );

        stripe
            .apply_indexed_ingest_pack(second_partition, LogicalOffset::new(0), 1, payload)
            .expect("second indexed append installs");
        assert_eq!(
            stripe
                .tenant_partitions("test-tenant")
                .expect("cached tenant partition lookup refreshes"),
            vec![partition(), second_partition]
        );
    }

    #[test]
    fn compressed_frame_queries_preserve_interleaved_offsets_and_full_exactness() {
        let events = (0..12)
            .map(|ordinal| OtlpLogEvent {
                timestamp_unix_nanos: 1_000 + ordinal,
                message: Arc::from(if ordinal % 2 == 0 {
                    format!("ERROR request id={ordinal} failed")
                } else {
                    format!("INFO request id={ordinal} completed")
                }),
                fields: Arc::new(vec![
                    MetadataField::new("service", if ordinal % 2 == 0 { "api" } else { "worker" }),
                    MetadataField::new("trace", format!("trace-{ordinal}")),
                    MetadataField::new("status", if ordinal % 2 == 0 { "503" } else { "200" }),
                ]),
                compression_cohort: CompressionCohortId::new(ordinal % 3),
                ..OtlpLogEvent::default()
            })
            .collect::<Vec<_>>();
        let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
        let payload = Bytes::from(prepared.payload);
        let first_offset = LogicalOffset::new(50);
        let mut live =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("live stripe opens");
        live.apply_indexed_ingest_pack(
            partition(),
            first_offset,
            events.len() as u32,
            payload.clone(),
        )
        .expect("live frame indexes install");
        assert_eq!(
            live.count_tenant_records("test-tenant", &[partition()])
                .expect("resident count"),
            events.len() as u64
        );
        assert_eq!(
            live.count_tenant_records("another-tenant", &[partition()])
                .expect("other tenant count"),
            0
        );
        let mut recovered = LogStripe::new(ShardId::new(7), StripeConfig::default())
            .expect("recovered stripe opens");
        recovered
            .apply_indexed_ingest_pack(partition(), first_offset, events.len() as u32, payload)
            .expect("durable frame indexes recover");

        let queries = [
            LogQuery::new(partition())
                .with_term("error")
                .with_field("service", "api"),
            LogQuery::new(partition())
                .with_term("7")
                .with_field("trace", "trace-7"),
            LogQuery::new(partition())
                .with_offset_range(LogicalOffset::new(53), LogicalOffset::new(58))
                .sort_by_timestamp()
                .newest_first()
                .with_limit(3),
            LogQuery::new(partition()).with_field("service", "missing"),
            LogQuery::new(partition()).where_predicate(LogPredicate::field_exists("service")),
            LogQuery::new(partition()).where_predicate(LogPredicate::field_in("service", ["api"])),
            LogQuery::new(partition()).where_predicate(LogPredicate::field(
                "service",
                TextMatcher::new("ork", TextMatchKind::Contains, CaseSensitivity::Sensitive),
            )),
            LogQuery::new(partition()).where_predicate(
                LogPredicate::field_regex("service", "^a", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            ),
            LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
                "status",
                NumericComparison::GreaterThanOrEqual,
                500,
            )),
        ];
        let expected = [
            vec![50, 52, 54, 56, 58, 60],
            vec![57],
            vec![57, 56, 55],
            vec![],
            (50..62).collect::<Vec<_>>(),
            vec![50, 52, 54, 56, 58, 60],
            vec![51, 53, 55, 57, 59, 61],
            vec![50, 52, 54, 56, 58, 60],
            vec![50, 52, 54, 56, 58, 60],
        ];
        for (query, expected) in queries.iter().zip(expected) {
            let live_offsets = live
                .query_checked(query)
                .expect("live query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>();
            let recovered_offsets = recovered
                .query_checked(query)
                .expect("recovered query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>();
            assert_eq!(live_offsets, expected);
            assert_eq!(recovered_offsets, expected);
        }
        assert_eq!(
            live.query_refs(
                &LogQuery::new(partition())
                    .with_term("error")
                    .with_field("service", "api"),
            )
            .into_iter()
            .map(|record_ref| record_ref.offset.get())
            .collect::<Vec<_>>(),
            vec![50, 52, 54, 56, 58, 60]
        );
        assert_eq!(
            live.query_refs(
                &LogQuery::new(partition())
                    .where_predicate(LogPredicate::field_exists("service"))
                    .newest_first()
                    .with_limit(3),
            )
            .into_iter()
            .map(|record_ref| record_ref.offset.get())
            .collect::<Vec<_>>(),
            vec![61, 60, 59]
        );
        let case_insensitive_count = LogQuery::new(partition()).where_predicate(
            LogPredicate::message_token("error", CaseSensitivity::Insensitive),
        );
        assert_eq!(
            live.count_query_checked(&case_insensitive_count)
                .expect("indexed exact-token count"),
            6
        );
        let parity_queries = [
            LogQuery::new(partition()).where_predicate(LogPredicate::message_token(
                "ERROR",
                CaseSensitivity::Sensitive,
            )),
            LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                LogPredicate::message_token("error", CaseSensitivity::Insensitive),
                LogPredicate::message_token("failed", CaseSensitivity::Sensitive),
            ])),
            LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                LogPredicate::message_token("error", CaseSensitivity::Insensitive),
                LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
            ])),
            LogQuery::new(partition())
                .with_field("service", "api")
                .where_predicate(LogPredicate::message_token(
                    "error",
                    CaseSensitivity::Insensitive,
                ))
                .with_timestamp_range(1_004, 1_010)
                .sort_by_timestamp()
                .newest_first(),
            LogQuery::new(partition())
                .with_field("service", "api")
                .with_timestamp_range(1_004, 1_010)
                .with_limit(2),
            LogQuery::new(partition()).where_predicate(
                LogPredicate::field_regex("service", "^a", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            ),
            LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
                "status",
                NumericComparison::GreaterThanOrEqual,
                500,
            )),
        ];
        for query in parity_queries {
            let expected = live
                .query_checked(&query)
                .expect("materialized exact query succeeds");
            assert_eq!(
                live.count_query_checked(&query)
                    .expect("cardinality exact query succeeds"),
                expected.len() as u64,
                "count and materialized query diverged for {query:?}"
            );
            assert_eq!(
                recovered
                    .query_checked(&query)
                    .expect("recovered exact query succeeds")
                    .len(),
                expected.len(),
                "recovered and live query diverged for {query:?}"
            );
        }
        assert_eq!(
            live.indexed_through(partition()),
            Some(LogicalOffset::new(61))
        );
        assert!(!live.partitions.contains_key(&partition()));
    }

    #[test]
    fn indexed_group_queries_preserve_single_and_two_key_counts() {
        let events = [
            ("ERROR", "frontend", "error request"),
            ("ERROR", "frontend", "error retry"),
            ("INFO", "frontend", "error completed"),
            ("ERROR", "payments", "error declined"),
            ("INFO", "payments", "healthy"),
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, (severity, scope, message))| OtlpLogEvent {
            timestamp_unix_nanos: ordinal as u64,
            message: Arc::from(message),
            fields: Arc::new(vec![
                MetadataField::new("attr.loki.metadata.severity_text", severity),
                MetadataField::new("attr.loki.metadata.scope_name", scope),
            ]),
            compression_cohort: CompressionCohortId::new(1),
            ..OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
        let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_indexed_ingest_pack(
                partition(),
                LogicalOffset::new(0),
                events.len() as u32,
                Bytes::from(prepared.payload),
            )
            .expect("frame append indexes");
        let query = LogQuery::new(partition()).where_predicate(LogPredicate::message_token(
            "error",
            CaseSensitivity::Insensitive,
        ));

        let by_severity = stripe
            .group_query_partitions_checked(
                std::slice::from_ref(&query),
                &[AnalyticsGroupKey::SeverityText],
            )
            .expect("single-key grouping succeeds");
        assert_eq!(by_severity.get(&vec![Some(Arc::from("ERROR"))]), Some(&3));
        assert_eq!(by_severity.get(&vec![Some(Arc::from("INFO"))]), Some(&1));

        let by_pair = stripe
            .group_query_partitions_checked(
                &[query],
                &[
                    AnalyticsGroupKey::SeverityText,
                    AnalyticsGroupKey::ScopeName,
                ],
            )
            .expect("two-key grouping succeeds");
        assert_eq!(
            by_pair.get(&vec![Some(Arc::from("ERROR")), Some(Arc::from("frontend"))]),
            Some(&2)
        );
        assert_eq!(
            by_pair.get(&vec![Some(Arc::from("INFO")), Some(Arc::from("frontend"))]),
            Some(&1)
        );
        assert_eq!(
            by_pair.get(&vec![Some(Arc::from("ERROR")), Some(Arc::from("payments"))]),
            Some(&1)
        );
    }

    #[test]
    fn projected_severity_text_matches_typed_metadata() {
        let events = vec![OtlpLogEvent {
            timestamp_unix_nanos: 1,
            message: Arc::from("projected severity"),
            fields: Arc::new(vec![MetadataField::new("otel.severity_text", "WARN")]),
            severity_text: Arc::from("WARN"),
            compression_cohort: CompressionCohortId::new(1),
            ..OtlpLogEvent::default()
        }];
        let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_indexed_ingest_pack(
                partition(),
                LogicalOffset::new(0),
                1,
                Bytes::from(prepared.payload),
            )
            .expect("frame append indexes");
        let query = LogQuery::new(partition()).with_term("projected");
        let typed = stripe
            .query_partitions_checked_projected(std::slice::from_ref(&query), true)
            .expect("typed query succeeds");
        let projected = stripe
            .query_partitions_checked_projected(std::slice::from_ref(&query), false)
            .expect("projected query succeeds");
        assert_eq!(typed.len(), 1);
        assert_eq!(projected.len(), 1);
        assert_eq!(
            typed[0].record.severity_text,
            projected[0].record.severity_text
        );
    }

    #[test]
    fn compressed_frame_partition_fanout_applies_one_global_timestamp_limit() {
        let other_partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(4));
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for (topic_partition, timestamp_groups) in [
            (partition(), [[100, 101], [300, 301]]),
            (other_partition, [[200, 201], [400, 401]]),
        ] {
            for (batch, timestamps) in timestamp_groups.into_iter().enumerate() {
                let events = timestamps.map(|timestamp| OtlpLogEvent {
                    timestamp_unix_nanos: timestamp,
                    message: Arc::from(format!("ERROR request {timestamp} failed")),
                    fields: Arc::new(vec![MetadataField::new("service", "api")]),
                    compression_cohort: CompressionCohortId::new(1),
                    ..OtlpLogEvent::default()
                });
                let prepared = prepare_ingest_pack(&events).expect("pack prepares");
                stripe
                    .apply_indexed_ingest_pack(
                        topic_partition,
                        LogicalOffset::new((batch * 2) as u64),
                        events.len() as u32,
                        Bytes::from(prepared.payload),
                    )
                    .expect("frame append indexes");
            }
        }
        let queries = [partition(), other_partition].map(|topic_partition| {
            LogQuery::new(topic_partition)
                .with_term("error")
                .sort_by_timestamp()
                .newest_first()
                .with_limit(3)
        });
        assert_eq!(
            stripe
                .query_partitions_checked(&queries)
                .expect("newest fanout query succeeds")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![401, 400, 301]
        );
        let queries = [partition(), other_partition].map(|topic_partition| {
            LogQuery::new(topic_partition)
                .with_term("error")
                .sort_by_timestamp()
                .with_limit(3)
        });
        assert_eq!(
            stripe
                .query_partitions_checked(&queries)
                .expect("oldest fanout query succeeds")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![100, 101, 200]
        );
    }

    #[test]
    fn high_cardinality_indexed_fields_use_selective_fallback() {
        let events = (0..4_100u64)
            .map(|ordinal| OtlpLogEvent {
                timestamp_unix_nanos: ordinal,
                message: Arc::from("field fallback"),
                fields: Arc::new(vec![MetadataField::new(
                    "trace",
                    format!("trace-{ordinal}"),
                )]),
                compression_cohort: CompressionCohortId::new(1),
                ..OtlpLogEvent::default()
            })
            .collect::<Vec<_>>();
        let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_indexed_ingest_pack(
                partition(),
                LogicalOffset::new(0),
                events.len() as u32,
                Bytes::from(prepared.payload),
            )
            .expect("frame append indexes");

        let query = LogQuery::new(partition()).where_predicate(
            LogPredicate::field_regex("trace", "^trace-4096$", CaseSensitivity::Sensitive)
                .expect("regex compiles"),
        );
        let matches = stripe.query_checked(&query).expect("query succeeds");
        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![4096]
        );
        assert_eq!(
            stripe.count_query_checked(&query).expect("count succeeds"),
            1
        );
    }

    #[test]
    fn compressed_frame_timestamp_top_k_selects_before_full_record_decode() {
        let fields = Arc::new(vec![crate::MetadataField::new("docker_stream", "stderr")]);
        let events = (0..1_024u64)
            .map(|ordinal| OtlpLogEvent {
                timestamp_unix_nanos: ordinal,
                message: Arc::from(if matches!(ordinal, 10 | 20 | 30) {
                    "prefix target suffix".to_owned()
                } else {
                    "prefix_target suffix".to_owned()
                }),
                fields: Arc::clone(&fields),
                compression_cohort: CompressionCohortId::new(1),
                ..OtlpLogEvent::default()
            })
            .collect::<Vec<_>>();
        let prepared = prepare_ingest_pack(&events).expect("pack prepares");
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_indexed_ingest_pack(
                partition(),
                LogicalOffset::new(0),
                events.len() as u32,
                Bytes::from(prepared.payload),
            )
            .expect("frame append indexes");

        let latest = LogQuery::new(partition())
            .sort_by_timestamp()
            .newest_first()
            .with_limit(3);
        assert_eq!(
            stripe
                .query_checked(&latest)
                .expect("latest query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![1_023, 1_022, 1_021]
        );

        let latest_stream = LogQuery::new(partition())
            .with_field("docker_stream", "stderr")
            .sort_by_timestamp()
            .newest_first()
            .with_limit(3);
        assert_eq!(
            stripe
                .query_checked(&latest_stream)
                .expect("latest exact-stream query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![1_023, 1_022, 1_021]
        );
        let latest_stream_window = latest_stream.clone().with_timestamp_range(1_000, 1_024);
        assert_eq!(
            stripe
                .query_checked(&latest_stream_window)
                .expect("latest exact-stream timestamp window query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![1_023, 1_022, 1_021]
        );

        let sparse_residual = LogQuery::new(partition())
            .where_predicate(LogPredicate::message_token(
                "target",
                CaseSensitivity::Sensitive,
            ))
            .sort_by_timestamp()
            .newest_first()
            .with_limit(2);
        assert_eq!(
            stripe
                .query_checked(&sparse_residual)
                .expect("sparse residual query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![30, 20]
        );

        let phrase = LogQuery::new(partition()).where_predicate(LogPredicate::message_phrase(
            ["prefix", "target"],
            0,
            CaseSensitivity::Sensitive,
        ));
        assert_eq!(
            stripe
                .query_checked(&phrase)
                .expect("phrase query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(
            stripe.count_query_checked(&phrase).expect("phrase count"),
            3
        );
        let proximity = LogQuery::new(partition()).where_predicate(LogPredicate::message_phrase(
            ["prefix", "suffix"],
            1,
            CaseSensitivity::Insensitive,
        ));
        assert_eq!(
            stripe
                .count_query_checked(&proximity)
                .expect("indexed proximity count"),
            3
        );
        let regex_prefix = LogQuery::new(partition()).where_predicate(
            LogPredicate::message_token_regex("^prefix.*$", CaseSensitivity::Insensitive)
                .expect("token regex compiles"),
        );
        assert_eq!(
            stripe
                .count_query_checked(&regex_prefix)
                .expect("indexed token regex count"),
            1_024
        );
        let token_prefix = LogQuery::new(partition()).where_predicate(
            LogPredicate::message_token_prefix("prefix", CaseSensitivity::Insensitive),
        );
        assert_eq!(
            stripe
                .count_query_checked(&token_prefix)
                .expect("indexed token prefix count"),
            1_024
        );
        let fuzzy =
            LogQuery::new(partition()).where_predicate(LogPredicate::message_fuzzy("targit", 1));
        assert_eq!(
            stripe
                .count_query_checked(&fuzzy)
                .expect("indexed fuzzy count"),
            3
        );
        let fuzzy_and_prefix = LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::message_fuzzy("targit", 1),
            LogPredicate::message_token_prefix("prefix", CaseSensitivity::Insensitive),
        ]));
        assert_eq!(
            stripe
                .count_query_checked(&fuzzy_and_prefix)
                .expect("indexed fuzzy and prefix count"),
            3
        );

        let static_phrase = LogQuery::new(partition()).where_predicate(
            LogPredicate::message_phrase(["prefix", "target"], 0, CaseSensitivity::Insensitive),
        );
        assert_eq!(
            stripe
                .query_checked(&static_phrase)
                .expect("static phrase query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(
            stripe
                .count_query_checked(&static_phrase)
                .expect("static phrase count"),
            3
        );

        let literal =
            LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(" target "));
        assert_eq!(
            stripe
                .query_checked(&literal)
                .expect("message literal query")
                .into_iter()
                .map(|matched| matched.record.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![10, 20, 30]
        );
        assert_eq!(
            stripe
                .count_query_checked(&literal)
                .expect("message literal count"),
            3
        );
    }

    #[test]
    fn homogeneous_event_batches_publish_one_posting_range_per_value() {
        let message: Arc<str> = Arc::from("repeated request completed");
        let fields = Arc::new(vec![crate::MetadataField::new("service", "api")]);
        let event = OtlpLogEvent {
            timestamp_unix_nanos: 42,
            message,
            fields,
            compression_cohort: CompressionCohortId::new(4),
            ..OtlpLogEvent::default()
        };
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        let receipts = stripe
            .apply_otlp_events(partition(), LogicalOffset::new(0), vec![event; 1_024])
            .expect("homogeneous event range indexes");

        assert_eq!(receipts.len(), 1_024);
        assert_eq!(
            stripe.indexed_through(partition()),
            Some(LogicalOffset::new(1_023))
        );
        let indexed = stripe
            .partitions
            .get(&partition())
            .expect("partition was indexed");
        let repeated_id = indexed.term_ids["repeated"];
        assert_eq!(
            indexed.term_postings[repeated_id].runs,
            vec![OrdinalRun {
                first: 0,
                last: 1_023
            }]
        );
        let service_id = indexed.field_ids["service"]["api"];
        assert_eq!(
            indexed.field_postings[service_id].runs,
            vec![OrdinalRun {
                first: 0,
                last: 1_023
            }]
        );
        assert_eq!(
            stripe
                .query_refs(
                    &LogQuery::new(partition())
                        .with_term("repeated")
                        .with_field("service", "api")
                )
                .len(),
            1_024
        );
    }

    #[test]
    fn heterogeneous_event_batches_retain_exact_sparse_postings() {
        let fields = Arc::new(vec![crate::MetadataField::new("service", "api")]);
        let event = |message: &'static str| OtlpLogEvent {
            timestamp_unix_nanos: 42,
            message: Arc::from(message),
            fields: Arc::clone(&fields),
            compression_cohort: CompressionCohortId::new(4),
            ..OtlpLogEvent::default()
        };
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_otlp_events(
                partition(),
                LogicalOffset::new(0),
                [
                    event("repeated request completed"),
                    event("different request failed"),
                    event("repeated request completed"),
                ],
            )
            .expect("heterogeneous events index");

        let indexed = stripe
            .partitions
            .get(&partition())
            .expect("partition was indexed");
        let repeated_id = indexed.term_ids["repeated"];
        assert_eq!(
            indexed.term_postings[repeated_id].runs,
            vec![
                OrdinalRun { first: 0, last: 0 },
                OrdinalRun { first: 2, last: 2 }
            ]
        );
        assert_eq!(
            stripe
                .query_refs(&LogQuery::new(partition()).with_term("repeated"))
                .into_iter()
                .map(|record_ref| record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn database_fanout_merges_selected_stripes_without_duplicate_results() {
        let mut database =
            ShardTelemetry::new([ShardId::new(7), ShardId::new(8)], StripeConfig::default())
                .expect("database opens");
        for offset in 0..10 {
            let shard = if offset < 5 {
                ShardId::new(7)
            } else {
                ShardId::new(8)
            };
            database
                .apply_durable(
                    record_on(shard, offset, &format!("request {offset} completed"))
                        .with_field("service", "api"),
                )
                .expect("record indexes");
        }
        let query = LogQuery::new(partition())
            .with_predicate(crate::LogPredicate::field_exists("service"))
            .newest_first()
            .with_limit(3);
        assert_eq!(
            database
                .query_all(&query)
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![9, 8, 7]
        );
        assert_eq!(
            database
                .query_stripes([ShardId::new(8), ShardId::new(8), ShardId::new(7)], &query,)
                .expect("selected query succeeds")
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![9, 8, 7]
        );
        assert!(matches!(
            database.query_stripes([ShardId::new(99)], &query),
            Err(TelemetryError::UnknownStripe(shard)) if shard == ShardId::new(99)
        ));
    }

    #[test]
    fn query_intersection_is_order_independent_and_offset_sorted() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for offset in 0..1_000u64 {
            let mut message = format!("common request_id={offset}");
            if offset % 10 == 0 {
                message.push_str(" medium");
            }
            if offset % 100 == 0 {
                message.push_str(" rare");
            }
            stripe
                .apply_durable(
                    record(offset, &message)
                        .with_field("service", if offset % 20 == 0 { "api" } else { "worker" }),
                )
                .expect("record indexes");
        }

        let common_first = stripe.query_refs(
            &LogQuery::new(partition())
                .with_term("common")
                .with_term("medium")
                .with_term("rare")
                .with_field("service", "api"),
        );
        let rare_first = stripe.query_refs(
            &LogQuery::new(partition())
                .with_field("service", "api")
                .with_term("rare")
                .with_term("medium")
                .with_term("common"),
        );

        assert_eq!(common_first, rare_first);
        assert_eq!(
            common_first
                .iter()
                .map(|reference| reference.offset.get())
                .collect::<Vec<_>>(),
            (0..1_000).step_by(100).collect::<Vec<_>>()
        );
    }

    #[test]
    fn query_ranges_order_and_limit_bound_materialized_results() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for offset in 0..100u64 {
            stripe
                .apply_durable(record(offset, "common event"))
                .expect("record indexes");
        }

        let matches = stripe.query(
            &LogQuery::new(partition())
                .with_term("common")
                .with_offset_range(LogicalOffset::new(20), LogicalOffset::new(80))
                .with_timestamp_range(300, 700)
                .newest_first()
                .with_limit(3),
        );
        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![69, 68, 67]
        );

        assert!(
            stripe
                .query(
                    &LogQuery::new(partition())
                        .with_offset_range(LogicalOffset::new(5), LogicalOffset::new(5))
                )
                .is_empty()
        );
        assert!(
            stripe
                .query(&LogQuery::new(partition()).with_limit(0))
                .is_empty()
        );
    }

    #[test]
    fn bounded_exact_boolean_queries_preserve_oldest_order() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        for offset in 0..100_u64 {
            stripe
                .apply_durable(
                    record(offset, "common event")
                        .with_field("severity", if offset % 4 == 0 { "ERROR" } else { "INFO" }),
                )
                .expect("record indexes");
        }

        let query = LogQuery::new(partition())
            .where_predicate(LogPredicate::and(vec![
                LogPredicate::term("common"),
                LogPredicate::or(vec![
                    LogPredicate::field_equals("severity", "ERROR"),
                    LogPredicate::term("rare"),
                ]),
            ]))
            .with_limit(3);
        assert_eq!(
            stripe
                .query(&query)
                .into_iter()
                .map(|matched| matched.record.record_ref.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 4, 8]
        );
    }

    #[test]
    fn sorted_posting_intersection_handles_disjoint_and_overlapping_ranges() {
        let mut candidates = vec![1, 3, 4, 8, 10];
        let runs = [
            OrdinalRun { first: 0, last: 0 },
            OrdinalRun { first: 3, last: 5 },
            OrdinalRun {
                first: 10,
                last: 10,
            },
            OrdinalRun {
                first: 12,
                last: 12,
            },
        ];
        intersect_ordinal_runs(&mut candidates, &runs, 0, u32::MAX);
        assert_eq!(candidates, [3, 4, 10]);

        intersect_ordinal_runs(
            &mut candidates,
            &[OrdinalRun {
                first: 20,
                last: 20,
            }],
            0,
            u32::MAX,
        );
        assert!(candidates.is_empty());
    }

    #[test]
    fn skewed_frame_candidate_intersection_uses_the_sparse_side() {
        let sparse = vec![0, 1_000, 50_000, 99_999];
        let dense = (0..100_000).collect::<Vec<_>>();
        let mut current = Some(sparse.clone());
        intersect_frame_candidate_slice(&mut current, &dense);
        assert_eq!(current, Some(sparse.clone()));

        let mut current = Some(dense);
        intersect_frame_candidate_slice(&mut current, &sparse);
        assert_eq!(current, Some(sparse));
    }

    #[test]
    fn skewed_posting_intersection_preserves_sparse_matches() {
        let mut candidates = vec![0, 1_000, 50_000, 99_999];
        let runs = [OrdinalRun {
            first: 0,
            last: 99_999,
        }];
        intersect_ordinal_runs(&mut candidates, &runs, 0, 100_000);
        assert_eq!(candidates, [0, 1_000, 50_000, 99_999]);

        let mut candidates = vec![0, 1_001, 50_000, 99_998];
        let runs = (0..100_000)
            .step_by(1_000)
            .map(|ordinal| OrdinalRun {
                first: ordinal,
                last: ordinal,
            })
            .collect::<Vec<_>>();
        intersect_ordinal_runs(&mut candidates, &runs, 0, 100_000);
        assert_eq!(candidates, [0, 50_000]);
    }

    #[test]
    fn bounded_hot_posting_intersection_stops_in_requested_order() {
        let mut dense = HotPostingList::default();
        dense.push_range(0, 999_999);
        let mut every_thousand = HotPostingList::default();
        for ordinal in (0..1_000_000).step_by(1_000) {
            every_thousand.push(ordinal);
        }
        let postings = [&dense, &every_thousand];
        assert_eq!(
            collect_hot_posting_intersection(
                &postings,
                0,
                1_000_000,
                QueryOrder::OldestFirst,
                Some(3),
            ),
            [0, 1_000, 2_000]
        );
        assert_eq!(
            collect_hot_posting_intersection(
                &postings,
                0,
                1_000_000,
                QueryOrder::NewestFirst,
                Some(3),
            ),
            [999_000, 998_000, 997_000]
        );
    }

    #[test]
    fn hot_posting_union_merges_runs_without_duplicates() {
        let mut left = HotPostingList::default();
        left.push_range(0, 2);
        left.push_range(8, 9);
        let mut right = HotPostingList::default();
        right.push(2);
        right.push_range(4, 8);
        let postings = [&left, &right];
        assert_eq!(
            collect_hot_posting_union(&postings, 1, 9, None),
            [1, 2, 4, 5, 6, 7, 8]
        );
        assert_eq!(
            collect_hot_posting_union(&postings, 1, 9, Some(3)),
            [1, 2, 4]
        );

        let mut third = HotPostingList::default();
        third.push_range(1, 7);
        let postings = [&left, &right, &third];
        assert_eq!(
            collect_hot_posting_union(&postings, 0, 10, None),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
    }

    #[test]
    fn lane_global_offset_gaps_are_accepted_but_regressions_are_rejected() {
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        stripe
            .apply_durable(record(5, "first"))
            .expect("first append");
        stripe
            .apply_durable(record(7, "lane gap"))
            .expect("offsets occupied by sibling lane partitions may be skipped");
        let error = stripe
            .apply_durable(record(6, "regressed"))
            .expect_err("offset regression is rejected");
        assert_eq!(
            error,
            TelemetryError::OffsetOutOfOrder {
                partition: partition(),
                expected: LogicalOffset::new(8),
                observed: LogicalOffset::new(6),
            }
        );
        assert_eq!(
            stripe.indexed_through(partition()),
            Some(LogicalOffset::new(7))
        );
    }

    #[test]
    fn disabled_locality_seals_without_collator_work() {
        let config = StripeConfig {
            target_block_bytes: 1,
            ..StripeConfig::default()
        };
        let mut stripe = LogStripe::new(ShardId::new(7), config).expect("stripe opens");
        let receipt = stripe
            .apply_durable(record(0, "repeated message"))
            .expect("record indexes");
        assert_eq!(receipt.sealed_blocks.len(), 1);
        assert_eq!(
            receipt.sealed_blocks[0].placement_id,
            CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4))
        );
        assert_eq!(receipt.sealed_blocks[0].compression_temperature, 0);
        assert_eq!(
            receipt.sealed_blocks[0].compression_temperature_variance_q8,
            0
        );
        let stats = stripe.compression_collation_stats();
        assert_eq!(stats.observations, 0);
        assert_eq!(stats.blocks_scored, 0);
    }

    #[test]
    fn sealing_records_dictionary_identity_and_object_location() {
        let config = StripeConfig {
            target_block_bytes: 1,
            dictionary_cache_bytes: 8,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig {
                enabled: false,
                ..CompressionLocalityConfig::default()
            },
        };
        let mut stripe = LogStripe::new(ShardId::new(7), config).expect("stripe opens");
        stripe
            .install_dictionary(
                CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4)),
                DictionaryId::new(11),
                Arc::from(&b"dict"[..]),
            )
            .expect("dictionary installs");
        let receipt = stripe
            .apply_durable(record(0, "message"))
            .expect("record indexes");
        let block = receipt
            .sealed_blocks
            .into_iter()
            .next()
            .expect("small target seals block");
        assert_eq!(block.dictionary_id, Some(DictionaryId::new(11)));
        assert_eq!(block.compression_codec, CompressionCodec::Zstd);
        let compressed = stripe
            .catalog()
            .staged_payload(block.block_id)
            .expect("sealed payload remains staged until offload");
        assert_eq!(
            u64::try_from(compressed.len()).expect("payload length fits"),
            block.stored_bytes
        );
        let decoded = zstd::bulk::Decompressor::with_dictionary(&b"dict"[..])
            .expect("decoder opens")
            .decompress(
                &compressed,
                usize::try_from(block.structural_bytes).expect("structural size fits"),
            )
            .expect("payload decompresses");
        assert_eq!(
            u64::try_from(decoded.len()).expect("structural size fits"),
            block.structural_bytes
        );
        assert_eq!(
            block.source_bytes,
            row_source_bytes(&record(0, "message")).expect("source accounting succeeds")
        );
        let records = crate::structural::decode_structural_block(&decoded)
            .expect("structural payload decodes");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, LogicalOffset::new(0));
        assert_eq!(records[0].timestamp_unix_nanos, 0);
        assert_eq!(records[0].message.as_ref(), "message");
        assert!(records[0].fields.is_empty());
        stripe
            .mark_block_offloaded(block.block_id, "objects/7/00000000.log")
            .expect("block is known");
        assert!(stripe.catalog().staged_payload(block.block_id).is_none());
        assert_eq!(
            stripe
                .catalog()
                .get(block.block_id)
                .expect("block exists")
                .object_key
                .as_deref(),
            Some("objects/7/00000000.log")
        );
    }

    #[test]
    fn locality_placement_preserves_utf8_records_queries_and_block_diagnostics() {
        let locality = CompressionLocalityConfig {
            enabled: true,
            min_split_records: 2,
            min_split_bytes: 1,
            min_admission_bytes: 1,
            ..CompressionLocalityConfig::default()
        };
        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: 150,
                dictionary_cache_bytes: 128,
                compression_level: 1,
                compression_locality: locality,
            },
        )
        .expect("stripe opens");
        let source = CompressionCohortId::new(4);
        let records = [
            DurableLog::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(0),
                10,
                "Échec request 123 at 東京",
                source,
            )
            .with_field("service", "paiements"),
            DurableLog::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(1),
                20,
                "Échec request 456 at 東京",
                source,
            )
            .with_field("service", "paiements"),
            DurableLog::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(2),
                30,
                "Échec request 890 at 東京",
                source,
            )
            .with_field("service", "paiements"),
            DurableLog::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(3),
                40,
                "Échec request 042 at 東京",
                source,
            )
            .with_field("service", "paiements"),
        ];

        let first = stripe
            .apply_durable(records[0].clone())
            .expect("first record indexes");
        let second = stripe
            .apply_durable(records[1].clone())
            .expect("second record indexes");
        assert_eq!(
            first.tentative_compression_placement.granularity,
            LocalityGranularity::Base
        );
        assert_eq!(
            second.tentative_compression_placement.granularity,
            LocalityGranularity::Base
        );
        let third = stripe
            .apply_durable(records[2].clone())
            .expect("third record indexes");
        let fourth = stripe
            .apply_durable(records[3].clone())
            .expect("fourth record indexes");
        assert_eq!(
            third.tentative_compression_placement.granularity,
            LocalityGranularity::Collated
        );
        assert_eq!(
            fourth.tentative_compression_placement.granularity,
            LocalityGranularity::Collated
        );

        let matches = stripe.query(
            &LogQuery::new(partition())
                .with_term("東京")
                .with_term("échec")
                .with_field("service", "paiements"),
        );
        assert_eq!(
            matches
                .iter()
                .map(|matched| matched.record.record_ref.offset)
                .collect::<Vec<_>>(),
            (0..4).map(LogicalOffset::new).collect::<Vec<_>>()
        );

        stripe
            .seal_active_blocks()
            .expect("remaining active blocks seal");
        let mut reconstructed = Vec::new();
        for block in stripe.catalog().iter() {
            assert_eq!(block.source_compression_cohort, source);
            assert!(block.record_count > 0);
            assert!(block.max_compression_temperature_deviation <= 20);
            let compressed = stripe
                .catalog()
                .staged_payload(block.block_id)
                .expect("payload is staged");
            let structural = zstd::bulk::decompress(
                &compressed,
                usize::try_from(block.structural_bytes).expect("structural bytes fit"),
            )
            .expect("payload decompresses");
            reconstructed.extend(
                crate::structural::decode_structural_block(&structural)
                    .expect("structural records decode"),
            );
        }
        reconstructed.sort_unstable_by_key(|record| record.offset);
        assert_eq!(reconstructed.len(), records.len());
        for (decoded, original) in reconstructed.iter().zip(records) {
            assert!(
                stripe
                    .final_compression_placement(original.record_ref)
                    .is_some()
            );
            assert_eq!(decoded.offset, original.record_ref.offset);
            assert_eq!(decoded.timestamp_unix_nanos, original.timestamp_unix_nanos);
            assert_eq!(decoded.message, original.message);
            assert_eq!(decoded.fields.as_ref(), original.fields.as_ref());
        }
    }

    #[test]
    fn mixed_blocks_filter_deviations_and_refill_compression_shards() {
        let candidates = [
            "alpha scheduler accepted static work",
            "database replica checkpoint completed",
            "network listener rejected malformed frame",
            "payment gateway authorized transaction",
            "kernel allocator reclaimed cold pages",
            "telemetry exporter flushed pending spans",
        ];
        let mut selected = (candidates[0], candidates[1], 0u8);
        for left in candidates {
            for right in candidates {
                let distance =
                    CompressionTemperature::new(fingerprint_message(left, &[]).locality_signature)
                        .distance(CompressionTemperature::new(
                            fingerprint_message(right, &[]).locality_signature,
                        ));
                if distance > selected.2 {
                    selected = (left, right, distance);
                }
            }
        }
        assert!(selected.2 >= 2, "test messages need separated temperatures");

        let mut stripe = LogStripe::new(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: 400,
                dictionary_cache_bytes: 1024,
                compression_level: 1,
                compression_locality: CompressionLocalityConfig {
                    enabled: true,
                    min_split_records: 2,
                    min_split_bytes: 1,
                    split_variance_q8: 1,
                    max_shard_variance_q8: u16::MAX,
                    max_assignment_distance: selected.2.saturating_sub(1),
                    min_admission_bytes: 1,
                    ..CompressionLocalityConfig::default()
                },
            },
        )
        .expect("stripe opens");
        let source = CompressionCohortId::new(44);
        let messages = (0..8)
            .map(|index| {
                if index % 2 == 0 {
                    selected.0
                } else {
                    selected.1
                }
            })
            .chain((0..8).map(|_| selected.0))
            .chain((0..8).map(|_| selected.1))
            .collect::<Vec<_>>();
        for (index, message) in messages.iter().enumerate() {
            stripe
                .apply_durable(DurableLog::new(
                    ShardId::new(7),
                    partition(),
                    LogicalOffset::new(u64::try_from(index).expect("offset fits")),
                    u64::try_from(index).expect("timestamp fits"),
                    *message,
                    source,
                ))
                .expect("record indexes");
        }
        stripe
            .seal_active_blocks()
            .expect("remaining compression shards seal");

        let placements = stripe
            .catalog()
            .iter()
            .map(|block| block.placement_id)
            .collect::<HashSet<_>>();
        assert!(placements.len() >= 2);
        assert!(stripe.compression_collation_stats().blocks_split > 0);
        assert!(stripe.compression_collation_stats().records_reassigned > 0);

        let mut reconstructed = Vec::new();
        for block in stripe.catalog().iter() {
            let compressed = stripe
                .catalog()
                .staged_payload(block.block_id)
                .expect("payload staged");
            let structural = zstd::bulk::decompress(
                &compressed,
                usize::try_from(block.structural_bytes).expect("size fits"),
            )
            .expect("block decompresses");
            reconstructed.extend(
                crate::structural::decode_structural_block(&structural)
                    .expect("block reconstructs"),
            );
        }
        reconstructed.sort_unstable_by_key(|record| record.offset);
        assert_eq!(reconstructed.len(), messages.len());
        assert_eq!(
            reconstructed
                .iter()
                .map(|record| record.message.as_ref())
                .collect::<Vec<_>>(),
            messages
        );
    }

    #[test]
    fn dictionary_cache_refreshes_lru_before_eviction() {
        let mut cache = DictionaryCache::new(4).expect("cache opens");
        cache
            .insert(DictionaryId::new(1), Arc::from(&b"aa"[..]))
            .expect("first dictionary");
        cache
            .insert(DictionaryId::new(2), Arc::from(&b"bb"[..]))
            .expect("second dictionary");
        let _ = cache
            .get(DictionaryId::new(1))
            .expect("first dictionary cached");
        let insert = cache
            .insert(DictionaryId::new(3), Arc::from(&b"cc"[..]))
            .expect("third dictionary");
        assert_eq!(insert.evicted, vec![DictionaryId::new(2)]);
        assert!(cache.contains(DictionaryId::new(1)));
        assert!(cache.contains(DictionaryId::new(3)));
    }

    #[test]
    fn catalog_shares_immutable_bytes_but_each_stripe_owns_its_lru_and_compressor() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let dictionary_id = DictionaryId::new(42);
        catalog
            .publish(
                CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4)),
                dictionary_id,
                Arc::from(&b"repeated clickhouse exception service context"[..]),
            )
            .expect("dictionary publishes");
        let config = StripeConfig {
            target_block_bytes: 1,
            dictionary_cache_bytes: 128,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig::default(),
        };
        let mut first = LogStripe::with_dictionary_catalog(
            ShardId::new(7),
            config.clone(),
            Arc::clone(&catalog),
        )
        .expect("first stripe opens");
        let mut second =
            LogStripe::with_dictionary_catalog(ShardId::new(8), config, Arc::clone(&catalog))
                .expect("second stripe opens");

        first
            .apply_durable(record_on(ShardId::new(7), 0, "repeated exception"))
            .expect("first stripe indexes");
        second
            .apply_durable(record_on(ShardId::new(8), 0, "repeated exception"))
            .expect("second stripe indexes");

        let first_payload = first
            .dictionary_cache_mut()
            .get(dictionary_id)
            .expect("first stripe caches dictionary");
        let second_payload = second
            .dictionary_cache_mut()
            .get(dictionary_id)
            .expect("second stripe caches dictionary");
        assert!(Arc::ptr_eq(&first_payload, &second_payload));
        assert_eq!(first.dictionary_generation(), 1);
        assert_eq!(second.dictionary_generation(), 1);
        assert_eq!(first.catalog().len(), 1);
        assert_eq!(second.catalog().len(), 1);
    }

    #[test]
    fn dictionary_rotation_only_affects_new_active_blocks_after_refresh() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let cohort = CompressionCohortId::new(4);
        let placement_id = CompressionPlacementId::from_source_cohort(cohort);
        let first_dictionary = DictionaryId::new(101);
        let second_dictionary = DictionaryId::new(102);
        catalog
            .publish(
                placement_id,
                first_dictionary,
                Arc::from(&b"first dictionary"[..]),
            )
            .expect("first dictionary publishes");
        let mut stripe = LogStripe::with_dictionary_catalog(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: u64::MAX,
                dictionary_cache_bytes: 128,
                compression_level: 1,
                compression_locality: CompressionLocalityConfig::default(),
            },
            Arc::clone(&catalog),
        )
        .expect("stripe opens");
        stripe
            .apply_durable(record(0, "first message"))
            .expect("first record indexes");

        catalog
            .publish(
                placement_id,
                second_dictionary,
                Arc::from(&b"second dictionary"[..]),
            )
            .expect("second dictionary publishes");
        assert!(
            stripe
                .refresh_dictionary_catalog()
                .expect("stripe refreshes catalog")
        );
        stripe
            .apply_durable(record(1, "second message"))
            .expect("second record indexes");

        let mut dictionary_ids = stripe
            .seal_active_blocks()
            .expect("active blocks seal")
            .into_iter()
            .map(|block| block.dictionary_id.expect("dictionary selected"))
            .collect::<Vec<_>>();
        dictionary_ids.sort_unstable();
        assert_eq!(dictionary_ids, vec![first_dictionary, second_dictionary]);
        assert_eq!(stripe.dictionary_generation(), 2);
    }

    #[test]
    fn realtime_dictionary_publications_are_adopted_by_future_blocks() {
        let catalog = Arc::new(DictionaryCatalog::new());
        let trainer = RealtimeDictionaryTrainer::start(
            crate::RealtimeDictionaryConfig {
                max_block_sample_bytes: 1024,
                training_sample_bytes: 8 * 1024,
                dictionary_bytes: 1024,
                holdout_blocks: 8,
                queue_blocks: 64,
                max_placements: 4,
                min_net_savings_bytes: 1,
                min_net_savings_bps: 1,
                retrain_after_bytes: u64::MAX,
            },
            1,
            Arc::clone(&catalog),
        )
        .expect("trainer starts");
        let placement_id = CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4));
        let observer = trainer.observer();
        for index in 0..16u64 {
            let mut state = 0x4d59_5df4_d0f3_3173u64;
            let mut sample = Vec::with_capacity(1024);
            for _ in 0..512 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                sample.push(state as u8);
            }
            sample.extend_from_slice(format!(" unique suffix {index:020}").as_bytes());
            while sample.len() < 1024 {
                sample.push(index.wrapping_mul(31).wrapping_add(sample.len() as u64) as u8);
            }
            assert!(observer.observe_structural_block(placement_id, sample));
        }
        trainer.flush().expect("trainer flushes");
        assert_eq!(trainer.stats().dictionaries_published, 1);

        let mut stripe = LogStripe::with_realtime_dictionary(
            ShardId::new(7),
            StripeConfig {
                target_block_bytes: 1,
                dictionary_cache_bytes: 4096,
                compression_level: 1,
                compression_locality: CompressionLocalityConfig::default(),
            },
            &trainer,
        )
        .expect("stripe opens");
        let block = stripe
            .apply_durable(record(0, "future block uses the learned dictionary"))
            .expect("record indexes")
            .sealed_blocks
            .into_iter()
            .next()
            .expect("block seals");
        assert!(block.dictionary_id.is_some());
        let payload = stripe
            .catalog()
            .staged_payload(block.block_id)
            .expect("payload staged");
        let dictionary = catalog
            .snapshot()
            .expect("catalog snapshot")
            .dictionary(block.dictionary_id.expect("dictionary id"))
            .expect("dictionary payload");
        let structural = zstd::bulk::Decompressor::with_dictionary(&dictionary)
            .expect("decompressor opens")
            .decompress(
                &payload,
                usize::try_from(block.structural_bytes).expect("size fits"),
            )
            .expect("block decompresses");
        let decoded =
            crate::structural::decode_structural_block(&structural).expect("block reconstructs");
        assert_eq!(
            decoded[0].message.as_ref(),
            "future block uses the learned dictionary"
        );
    }

    #[test]
    fn otlp_export_is_decoded_and_published_on_the_owning_stripe() {
        let export = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_attribute("service.name", "billing")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 9,
                        observed_time_unix_nano: 0,
                        severity_number: 17,
                        severity_text: "ERROR".into(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("card declined".into())),
                        }),
                        attributes: Vec::new(),
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: Vec::new(),
                        span_id: Vec::new(),
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let mut stripe =
            LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
        let events = OtlpLogDecoder
            .decode(&export.encode_to_vec())
            .expect("OTLP export decodes before append");
        let receipts = stripe
            .apply_otlp_events(partition(), LogicalOffset::new(0), events)
            .expect("OTLP events index after append");
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            stripe.query(
                &LogQuery::new(partition())
                    .with_term("declined")
                    .with_field("service.name", "billing")
            ),
            vec![LogMatch {
                record: stripe
                    .partitions
                    .get(&partition())
                    .and_then(|partition| partition.record(LogicalOffset::new(0)))
                    .expect("record retained")
                    .record
                    .clone(),
            }]
        );
    }
}
