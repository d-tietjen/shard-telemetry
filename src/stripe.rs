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

mod hot_postings;
use hot_postings::*;

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

#[derive(Debug, Default)]
struct IndexedFrameQueryCache {
    entries: HashMap<u64, Arc<CachedIndexedFrame>>,
    eviction_order: VecDeque<u64>,
    bytes: usize,
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

mod append;
mod cache;
pub(crate) use cache::*;
mod lifecycle;
mod object_tier;
mod query_aggregate;
mod query_cache;
mod query_core;
mod query_frame;
mod query_hot;
mod query_tier;

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

mod query_helpers;
use query_helpers::*;

mod predicates;
use predicates::*;

mod ordering;
use ordering::*;

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
mod tests;
