use std::borrow::Cow;
use std::cell::RefCell;
use std::hash::{BuildHasher, Hash};
use std::mem::size_of;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use foldhash::{HashMap, HashMapExt};
use pco::standalone::{simple_compress, simple_decompress_into};
use pco::{ChunkConfig, DeltaSpec, ModeSpec};
use serde::{Deserialize, Serialize};
use shard_stream_core::LogicalOffset;

mod decoder;
pub(crate) use decoder::*;
mod encoder;
use encoder::*;
mod index;
use index::*;
mod lanes;
use lanes::*;
mod wire;
use wire::*;

use crate::{
    DurableLog, MetadataField, ResourceContext, ScopeContext, SpanId, TelemetryAttribute,
    TelemetryError, TelemetryResult, TelemetryValue, TraceId,
};

const STRUCTURAL_BLOCK_MAGIC: &[u8; 4] = b"STLG";
const DIRECT_ATTRIBUTE_VALUE: u8 = 0;
const DICTIONARY_ATTRIBUTE_VALUE: u8 = 1;
const TIMESTAMP_PCO_LEVEL: usize = 8;
const TYPED_METADATA_ZSTD_LEVEL: i32 = 1;
const MESSAGE_LAYOUT_CACHE_ENTRIES: usize = 1_024;
const FIELD_SET_CACHE_ENTRIES: usize = 1_024;
const EMPTY_MESSAGE_LAYOUT: u32 = u32::MAX;
const EMPTY_FIELD_SET: u32 = u32::MAX;
const EMPTY_ATTRIBUTE_KEY: u32 = u32::MAX;
const LINEAR_ATTRIBUTE_DICTIONARY_LIMIT: usize = 16;
const ATTRIBUTE_KEY_CACHE_ENTRIES: usize = 32;
const SEEK_CHECKPOINT_INTERVAL: usize = 256;
// Candidate filters are fail-open and only avoid obviously absent block reads.
// A compact filter is deliberately preferred here: false positives cost one
// selective verification, while every filter byte is retained in every frame.
const EMBEDDED_MEMBERSHIP_FILTER_WORDS: usize = 1;
const INDEX_FINGERPRINT_MASK: u32 = 0x00ff_ffff;
const PACKED_IDS_BITPACKED: u8 = 0;
const PACKED_IDS_RUN_LENGTH: u8 = 1;
const PACKED_IDS_POSITION_ORDERED: u8 = 1 << 7;

thread_local! {
    static TYPED_METADATA_COMPRESSOR: RefCell<zstd::bulk::Compressor<'static>> =
        RefCell::new(zstd::bulk::Compressor::new(TYPED_METADATA_ZSTD_LEVEL)
            .expect("typed metadata zstd level is valid"));
    static TYPED_METADATA_DECOMPRESSOR: RefCell<zstd::bulk::Decompressor<'static>> =
        RefCell::new(zstd::bulk::Decompressor::new()
            .expect("typed metadata zstd decompressor initializes"));
}

pub(crate) type DecodedAttributeTables = (Vec<Arc<str>>, Vec<Vec<Arc<str>>>);

struct SeekableRecordLane<'a> {
    interval: usize,
    checkpoints: Vec<usize>,
    payload: &'a [u8],
}

impl SeekableRecordLane<'_> {
    fn checkpoint_payload(&self, checkpoint: usize) -> TelemetryResult<&[u8]> {
        let start =
            *self
                .checkpoints
                .get(checkpoint)
                .ok_or(TelemetryError::InvalidBlockEncoding(
                    "record lane checkpoint is missing",
                ))?;
        let end = self
            .checkpoints
            .get(checkpoint + 1)
            .copied()
            .unwrap_or(self.payload.len());
        self.payload
            .get(start..end)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "record lane checkpoint is invalid",
            ))
    }
}

/// A record reconstructed from the single current structural block layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedStructuralRecord {
    /// Durable logical offset inside the descriptor's topic partition.
    pub offset: LogicalOffset,
    /// Original event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Exact UTF-8 body reconstructed from the body lane.
    pub message: Arc<str>,
    /// Exact metadata fields reconstructed from the attribute lanes.
    pub fields: Arc<Vec<MetadataField>>,
    /// Original observed timestamp.
    pub observed_timestamp_unix_nanos: u64,
    /// Exact typed OTLP body.
    pub body: Option<TelemetryValue>,
    /// Exact typed record attributes.
    pub attributes: Arc<Vec<TelemetryAttribute>>,
    /// Exact resource context.
    pub resource: Arc<ResourceContext>,
    /// Exact instrumentation scope context.
    pub scope: Arc<ScopeContext>,
    /// Raw OTLP severity enum value.
    pub severity_number: i32,
    /// Exact severity text.
    pub severity_text: Arc<str>,
    /// Dropped record-attribute count.
    pub dropped_attributes_count: u32,
    /// Raw OTLP flags.
    pub flags: u32,
    /// Optional binary trace ID.
    pub trace_id: Option<TraceId>,
    /// Optional binary span ID.
    pub span_id: Option<SpanId>,
    /// Exact event name.
    pub event_name: Arc<str>,
}

/// Borrowed exact OTLP metadata exposed to the structural encoder.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct StructuralLogMetadataRef<'a> {
    /// Original observed timestamp.
    pub observed_timestamp_unix_nanos: u64,
    /// Exact body, including an explicitly empty `AnyValue`.
    pub body: Option<&'a TelemetryValue>,
    /// Record attributes.
    pub attributes: &'a [TelemetryAttribute],
    /// Resource context.
    pub resource: &'a ResourceContext,
    /// Scope context.
    pub scope: &'a ScopeContext,
    /// Raw severity enum value.
    pub severity_number: i32,
    /// Severity text.
    pub severity_text: &'a str,
    /// Dropped record-attribute count.
    pub dropped_attributes_count: u32,
    /// Raw log flags.
    pub flags: u32,
    /// Optional binary trace ID.
    pub trace_id: Option<TraceId>,
    /// Optional binary span ID.
    pub span_id: Option<SpanId>,
    /// Event name.
    pub event_name: &'a str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StructuralLogMetadata {
    observed_timestamp_unix_nanos: u64,
    body: Option<TelemetryValue>,
    attributes: Arc<Vec<TelemetryAttribute>>,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    severity_number: i32,
    severity_text: Arc<str>,
    dropped_attributes_count: u32,
    flags: u32,
    trace_id: Option<TraceId>,
    span_id: Option<SpanId>,
    event_name: Arc<str>,
}

const ABSENT_LOG_BODY_ID: u32 = 0;
const MESSAGE_LOG_BODY_ID: u32 = 1;
const LOG_BODY_DICTIONARY_ID_BASE: u32 = 2;
const EMPTY_STRING_ID: u32 = 0;
const STRING_DICTIONARY_ID_BASE: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PackedLogMetadataRow {
    observed_timestamp_delta: i64,
    body_id: u32,
    attributes_id: u32,
    resource_id: u32,
    scope_id: u32,
    severity_number: i32,
    severity_text_id: u32,
    dropped_attributes_count: u32,
    flags: u32,
    trace_id: Option<TraceId>,
    trace_id_from_fields: bool,
    span_id: Option<SpanId>,
    span_id_from_fields: bool,
    event_name_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PackedLogMetadata {
    bodies: Vec<TelemetryValue>,
    attribute_sets: Vec<Arc<Vec<TelemetryAttribute>>>,
    resources: Vec<Arc<ResourceContext>>,
    scopes: Vec<Arc<ScopeContext>>,
    strings: Vec<Arc<str>>,
    rows: Vec<Option<PackedLogMetadataRow>>,
}

struct MetadataInterner<T> {
    values: Vec<T>,
    candidates: HashMap<u64, Vec<u32>>,
}

impl<T> MetadataInterner<T> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::new(),
            candidates: HashMap::with_capacity(capacity.min(4_096)),
        }
    }

    fn intern<Q: Hash + ?Sized>(
        &mut self,
        value: &Q,
        equals: impl Fn(&T, &Q) -> bool,
        own: impl FnOnce(&Q) -> T,
    ) -> TelemetryResult<u32> {
        let hash = foldhash::fast::FixedState::with_seed(0x5354_5255_4354_5552).hash_one(value);
        if let Some(candidates) = self.candidates.get(&hash) {
            for candidate in candidates {
                let index =
                    usize::try_from(*candidate).map_err(|_| TelemetryError::RecordTooLarge)?;
                if equals(&self.values[index], value) {
                    return Ok(*candidate);
                }
            }
        }
        let id = u32::try_from(self.values.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        self.values.push(own(value));
        self.candidates.entry(hash).or_default().push(id);
        Ok(id)
    }

    fn into_values(self) -> Vec<T> {
        self.values
    }
}

#[derive(Debug)]
struct ParsedMessage<'a> {
    message: &'a [u8],
    signature_hash: u64,
    literals: Vec<Range<usize>>,
    values: Vec<Range<usize>>,
    terms: Vec<Range<usize>>,
}

#[derive(Debug)]
struct ParsedMessages<'a> {
    layouts: Vec<ParsedMessage<'a>>,
    layout_ids: Vec<u32>,
    layout_counts: Vec<usize>,
}

#[derive(Debug)]
struct TemplateEntry {
    literals: Vec<Vec<u8>>,
}

#[derive(Debug)]
struct TemplateGroup {
    representative: usize,
    count: usize,
    template_id: Option<usize>,
}

#[derive(Debug)]
struct AttributeTables {
    keys: Vec<Vec<u8>>,
    values: Vec<AttributeValueTable>,
}

#[derive(Debug)]
struct AttributeValueTable {
    entries: Vec<Arc<[u8]>>,
    resolved_entry_ids: Vec<u32>,
    dictionary_len: usize,
}

impl AttributeValueTable {
    fn dictionary(&self) -> &[Arc<[u8]>] {
        &self.entries[..self.dictionary_len]
    }

    fn resolve(&self, unresolved_id: u32) -> TelemetryResult<(usize, &[u8])> {
        let entry_id = *self.resolved_entry_ids.get(unresolved_id as usize).ok_or(
            TelemetryError::InvalidBlockEncoding("attribute value ID is out of range"),
        )? as usize;
        let value = self
            .entries
            .get(entry_id)
            .ok_or(TelemetryError::InvalidBlockEncoding(
                "resolved attribute value ID is out of range",
            ))?;
        Ok((entry_id, value))
    }
}

#[derive(Debug)]
struct ResolvedField {
    key_id: u32,
    value_id: u32,
}

#[derive(Debug)]
struct ResolvedFields {
    entries: Vec<ResolvedField>,
    record_ends: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
struct CachedAttributeKey {
    address: usize,
    length: usize,
    key_id: u32,
}

const EMPTY_CACHED_ATTRIBUTE_KEY: CachedAttributeKey = CachedAttributeKey {
    address: 0,
    length: 0,
    key_id: EMPTY_ATTRIBUTE_KEY,
};

#[derive(Debug)]
struct ParsedFieldSets {
    sets: Vec<Vec<(u32, u32)>>,
    set_ids: Vec<u32>,
    membership_filter: MembershipFilter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PackedIdColumn {
    bits_per_id: u8,
    values: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddedTermLocator {
    fingerprint: u32,
    layout_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EmbeddedFieldLocator {
    fingerprint: u32,
    field_set_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MembershipFilter {
    words: Box<[u64]>,
}

impl MembershipFilter {
    fn new() -> Self {
        Self {
            words: vec![0; EMBEDDED_MEMBERSHIP_FILTER_WORDS].into_boxed_slice(),
        }
    }

    fn insert(&mut self, value: &[u8]) {
        self.insert_hash(membership_hash(value));
    }

    fn insert_pair(&mut self, key: &[u8], value: &[u8]) {
        self.insert_hash(membership_pair_hash(key, value));
    }

    fn insert_hash(&mut self, hash: u64) {
        let second = hash.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15;
        for candidate in [hash, second] {
            let bit =
                candidate as usize & (EMBEDDED_MEMBERSHIP_FILTER_WORDS * u64::BITS as usize - 1);
            self.words[bit / u64::BITS as usize] |= 1u64 << (bit % u64::BITS as usize);
        }
    }

    fn might_contain(&self, value: &[u8]) -> bool {
        self.might_contain_hash(membership_hash(value))
    }

    fn might_contain_pair(&self, key: &[u8], value: &[u8]) -> bool {
        self.might_contain_hash(membership_pair_hash(key, value))
    }

    fn might_contain_hash(&self, hash: u64) -> bool {
        let second = hash.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15;
        [hash, second].into_iter().all(|candidate| {
            let bit =
                candidate as usize & (EMBEDDED_MEMBERSHIP_FILTER_WORDS * u64::BITS as usize - 1);
            self.words[bit / u64::BITS as usize] & (1u64 << (bit % u64::BITS as usize)) != 0
        })
    }

    fn merge(&mut self, other: &Self) {
        for (word, other) in self.words.iter_mut().zip(other.words.iter()) {
            *word |= *other;
        }
    }
}

/// Lossless compressed-domain candidate index embedded in one structural frame.
///
/// Deterministic term and metadata fingerprints reference compressor template
/// and field-set IDs. High-cardinality values fail open through bounded
/// membership filters and are checked after selective decode. Fingerprint
/// collisions can produce extra candidates but cannot suppress an exact match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedFrameIndex {
    record_count: u32,
    timestamp_offset_ordinal_ordered: bool,
    layout_count: u32,
    layout_ids: PackedIdColumn,
    residual_layout_ids: Vec<u32>,
    term_membership: MembershipFilter,
    terms: Vec<EmbeddedTermLocator>,
    field_set_count: u32,
    field_set_ids: PackedIdColumn,
    field_membership: MembershipFilter,
    fields: Vec<EmbeddedFieldLocator>,
}

/// Structural bytes and their already-built compressed-domain index.
///
/// Live ingestion can retain `index` for immediate query visibility while
/// persisting `structural` as the authoritative compressed frame. Recovery
/// reconstructs the same index from the embedded structural section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedStructuralBlock {
    /// Exact structural bytes, including the embedded index section.
    pub structural: Vec<u8>,
    /// In-memory view produced by the same dictionary-building pass.
    pub index: EmbeddedFrameIndex,
    /// Exact encoded index bytes produced by that same pass.
    ///
    /// Live ingest can forward this sidecar without serializing the index a
    /// second time. Recovery still reads the copy embedded in `structural`.
    pub embedded_index: Vec<u8>,
    /// Structural bytes occupied by the embedded index section before outer compression.
    pub embedded_index_bytes: usize,
}

#[derive(Debug)]
struct AttributeValueCounts {
    entries: Vec<(Arc<[u8]>, usize)>,
    ids: Option<HashMap<Arc<[u8]>, usize>>,
    last_address: usize,
    last_length: usize,
    last_entry_id: u32,
}

impl Default for AttributeValueCounts {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            ids: None,
            last_address: 0,
            last_length: 0,
            last_entry_id: u32::MAX,
        }
    }
}

impl AttributeValueCounts {
    #[inline(always)]
    fn increment(&mut self, value: &[u8]) -> TelemetryResult<(u32, bool)> {
        let address = value.as_ptr() as usize;
        if self.last_entry_id != u32::MAX
            && self.last_address == address
            && self.last_length == value.len()
        {
            let entry_id = self.last_entry_id as usize;
            self.entries[entry_id].1 = self.entries[entry_id]
                .1
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?;
            return Ok((self.last_entry_id, false));
        }
        self.increment_slow(value, address)
    }

    #[inline(never)]
    fn increment_slow(&mut self, value: &[u8], address: usize) -> TelemetryResult<(u32, bool)> {
        let entry_id = if let Some(ids) = &self.ids {
            ids.get(value).copied()
        } else {
            self.entries
                .iter()
                .position(|(candidate, _)| candidate.as_ref() == value)
        };
        if let Some(entry_id) = entry_id {
            self.entries[entry_id].1 = self.entries[entry_id]
                .1
                .checked_add(1)
                .ok_or(TelemetryError::RecordTooLarge)?;
            let entry_id = u32::try_from(entry_id).map_err(|_| TelemetryError::RecordTooLarge)?;
            self.last_address = address;
            self.last_length = value.len();
            self.last_entry_id = entry_id;
            return Ok((entry_id, false));
        }

        if self.entries.len() == LINEAR_ATTRIBUTE_DICTIONARY_LIMIT {
            self.ids = Some(
                self.entries
                    .iter()
                    .enumerate()
                    .map(|(id, (entry, _))| (entry.clone(), id))
                    .collect(),
            );
        }
        let entry_id = self.entries.len();
        let value = Arc::<[u8]>::from(value);
        self.entries.push((Arc::clone(&value), 1));
        if let Some(ids) = &mut self.ids {
            ids.insert(value, entry_id);
        }
        let entry_id = u32::try_from(entry_id).map_err(|_| TelemetryError::RecordTooLarge)?;
        self.last_address = address;
        self.last_length = self.entries[entry_id as usize].0.len();
        self.last_entry_id = entry_id;
        Ok((entry_id, true))
    }

    fn into_table(self) -> TelemetryResult<AttributeValueTable> {
        let entry_count = self.entries.len();
        let mut dictionary = Vec::new();
        let mut direct = Vec::new();
        for (entry_id, (value, count)) in self.entries.into_iter().enumerate() {
            if count >= 2 {
                dictionary.push((entry_id, value));
            } else {
                direct.push((entry_id, value));
            }
        }
        dictionary.sort_unstable_by(|left, right| left.1.cmp(&right.1));
        let dictionary_len = dictionary.len();
        let mut entries = Vec::with_capacity(entry_count);
        let mut resolved_entry_ids = vec![0_u32; entry_count];
        for (unresolved_id, value) in dictionary.into_iter().chain(direct) {
            let entry_id =
                u32::try_from(entries.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
            resolved_entry_ids[unresolved_id] = entry_id;
            entries.push(value);
        }
        Ok(AttributeValueTable {
            entries,
            resolved_entry_ids,
            dictionary_len,
        })
    }
}

/// Read-only normalized record fields consumed by the structural encoder.
///
/// Implementations can expose thread-local parser output directly, avoiding
/// transient [`DurableLog`] and [`Arc`] allocation before a block seals.
pub trait StructuralRecordView {
    /// Durable logical offset inside the record's topic partition.
    fn structural_offset(&self) -> LogicalOffset;

    /// Event timestamp in Unix nanoseconds.
    fn structural_timestamp_unix_nanos(&self) -> u64;

    /// Exact UTF-8 log body.
    fn structural_message(&self) -> &str;

    /// Number of normalized metadata fields.
    fn structural_field_count(&self) -> usize;

    /// Metadata field at `index`, if present.
    fn structural_field(&self, index: usize) -> Option<(&str, &str)>;

    /// Returns exact typed OTLP metadata when this record originated as OTLP.
    fn structural_log_metadata(&self) -> Option<StructuralLogMetadataRef<'_>> {
        None
    }

    /// Visits normalized metadata fields in their durable order.
    ///
    /// Implementations with segmented storage can override this method to
    /// avoid repeatedly resolving an indexed field accessor.
    #[inline]
    fn try_for_each_structural_field<F>(&self, mut visitor: F) -> TelemetryResult<()>
    where
        F: FnMut(&str, &str) -> TelemetryResult<()>,
    {
        for field_index in 0..self.structural_field_count() {
            let (key, value) =
                self.structural_field(field_index)
                    .ok_or(TelemetryError::InvalidBlockEncoding(
                        "record field count changed while encoding",
                    ))?;
            visitor(key, value)?;
        }
        Ok(())
    }
}

impl StructuralRecordView for DurableLog {
    fn structural_offset(&self) -> LogicalOffset {
        self.record_ref.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        self.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }

    fn structural_log_metadata(&self) -> Option<StructuralLogMetadataRef<'_>> {
        typed_metadata_ref(
            self.observed_timestamp_unix_nanos,
            self.body.as_ref(),
            &self.attributes,
            &self.resource,
            &self.scope,
            self.severity_number,
            &self.severity_text,
            self.dropped_attributes_count,
            self.flags,
            self.trace_id,
            self.span_id,
            &self.event_name,
        )
    }
}

impl StructuralRecordView for DecodedStructuralRecord {
    fn structural_offset(&self) -> LogicalOffset {
        self.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        self.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }

    fn structural_log_metadata(&self) -> Option<StructuralLogMetadataRef<'_>> {
        typed_metadata_ref(
            self.observed_timestamp_unix_nanos,
            self.body.as_ref(),
            &self.attributes,
            &self.resource,
            &self.scope,
            self.severity_number,
            &self.severity_text,
            self.dropped_attributes_count,
            self.flags,
            self.trace_id,
            self.span_id,
            &self.event_name,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn typed_metadata_ref<'a>(
    observed_timestamp_unix_nanos: u64,
    body: Option<&'a TelemetryValue>,
    attributes: &'a [TelemetryAttribute],
    resource: &'a ResourceContext,
    scope: &'a ScopeContext,
    severity_number: i32,
    severity_text: &'a str,
    dropped_attributes_count: u32,
    flags: u32,
    trace_id: Option<TraceId>,
    span_id: Option<SpanId>,
    event_name: &'a str,
) -> Option<StructuralLogMetadataRef<'a>> {
    let present = observed_timestamp_unix_nanos != 0
        || body.is_some()
        || !attributes.is_empty()
        || resource != &ResourceContext::default()
        || scope != &ScopeContext::default()
        || severity_number != 0
        || !severity_text.is_empty()
        || dropped_attributes_count != 0
        || flags != 0
        || trace_id.is_some()
        || span_id.is_some()
        || !event_name.is_empty();
    present.then_some(StructuralLogMetadataRef {
        observed_timestamp_unix_nanos,
        body,
        attributes,
        resource,
        scope,
        severity_number,
        severity_text,
        dropped_attributes_count,
        flags,
        trace_id,
        span_id,
        event_name,
    })
}

/// Returns the legacy row-byte accounting used for block sealing and storage
/// ratio reporting. The structural wire layout may be smaller or larger before
/// compression, but this byte count continues to represent the logical record
/// payload that the block stores.
pub(crate) fn row_source_bytes(record: &DurableLog) -> TelemetryResult<u64> {
    validate_u32_length(record.message.len())?;
    validate_u32_length(record.fields.len())?;
    let mut total = 24u64
        .checked_add(
            u64::try_from(record.message.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        )
        .ok_or(TelemetryError::RecordTooLarge)?;
    for field in record.fields.iter() {
        validate_u32_length(field.key.len())?;
        validate_u32_length(field.value.len())?;
        total = total
            .checked_add(8)
            .and_then(|value| value.checked_add(u64::try_from(field.key.len()).ok()?))
            .and_then(|value| value.checked_add(u64::try_from(field.value.len()).ok()?))
            .ok_or(TelemetryError::RecordTooLarge)?;
    }
    Ok(total)
}

/// Encodes records into the one pre-release structural block layout.
///
/// The resulting bytes must be compressed with the descriptor's codec before
/// storage and can be reconstructed with [`decode_structural_block`].
pub fn encode_structural_block(records: &[DurableLog]) -> TelemetryResult<Vec<u8>> {
    encode_structural_records(records)
}

/// Encodes any zero-copy normalized record view into the current structural
/// block layout.
pub fn encode_structural_records<R: StructuralRecordView>(
    records: &[R],
) -> TelemetryResult<Vec<u8>> {
    Ok(encode_indexed_structural_records(records)?.structural)
}

/// Encodes structural data and builds its compressed-domain index in the same
/// template and metadata dictionary pass.
pub fn encode_indexed_structural_records<R: StructuralRecordView>(
    records: &[R],
) -> TelemetryResult<IndexedStructuralBlock> {
    let parsed_messages = parse_messages(records)?;
    let (templates, template_ids) = select_templates(&parsed_messages)?;
    let (attributes, resolved_fields, field_membership) = build_attribute_tables(records)?;

    let offsets = encode_offsets(records)?;
    let timestamps = encode_timestamps(records)?;
    let template_bytes = encode_templates(&templates)?;
    let bodies = encode_bodies(&parsed_messages, &template_ids)?;
    let attribute_tables = encode_attribute_tables(&attributes)?;
    let (fields, parsed_fields) = encode_fields(&resolved_fields, &attributes, field_membership)?;
    let typed_metadata = encode_typed_metadata(records)?;
    let timestamp_offset_ordinal_ordered = records.windows(2).all(|pair| {
        (
            pair[0].structural_timestamp_unix_nanos(),
            pair[0].structural_offset(),
        ) <= (
            pair[1].structural_timestamp_unix_nanos(),
            pair[1].structural_offset(),
        )
    });
    let index = EmbeddedFrameIndex::build(
        &parsed_messages,
        &template_ids,
        &attributes,
        &parsed_fields,
        timestamp_offset_ordinal_ordered,
    )?;
    let embedded_index = index.encode()?;
    let embedded_index_bytes = embedded_index.len();

    let mut encoded = Vec::new();
    encoded.extend_from_slice(STRUCTURAL_BLOCK_MAGIC);
    write_varint(
        u64::try_from(records.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        &mut encoded,
    );
    for section in [
        offsets,
        timestamps,
        template_bytes,
        bodies,
        attribute_tables,
        fields,
        typed_metadata,
    ] {
        append_bytes(&mut encoded, &section)?;
    }
    append_bytes(&mut encoded, &embedded_index)?;
    Ok(IndexedStructuralBlock {
        structural: encoded,
        index,
        embedded_index,
        embedded_index_bytes,
    })
}

/// Opens the exact embedded index without reconstructing record bodies or
/// metadata values.
pub fn decode_embedded_frame_index(encoded: &[u8]) -> TelemetryResult<EmbeddedFrameIndex> {
    let (record_count, embedded_index) = structural_sections(encoded)?;
    decode_embedded_frame_index_section(embedded_index, record_count)
}

pub(crate) fn decode_embedded_frame_index_section(
    encoded: &[u8],
    expected_record_count: usize,
) -> TelemetryResult<EmbeddedFrameIndex> {
    let expected_record_count =
        u32::try_from(expected_record_count).map_err(|_| TelemetryError::RecordTooLarge)?;
    let index = EmbeddedFrameIndex::decode(encoded, expected_record_count)?;
    if index.record_count != expected_record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    Ok(index)
}

/// Reconstructs exact record data from one decompressed structural block.
///
/// The caller supplies the descriptor's partition, shard, and compression
/// cohort when rebuilding a complete [`DurableLog`].
pub fn decode_structural_block(encoded: &[u8]) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    if encoded.get(..STRUCTURAL_BLOCK_MAGIC.len()) != Some(STRUCTURAL_BLOCK_MAGIC) {
        return Err(TelemetryError::InvalidBlockEncoding(
            "missing structural block magic",
        ));
    }
    let mut cursor = STRUCTURAL_BLOCK_MAGIC.len();
    let record_count = read_usize(encoded, &mut cursor)?;
    ensure_count_within(
        record_count,
        encoded.len().saturating_sub(cursor),
        "record count",
    )?;
    let offsets_section = read_section(encoded, &mut cursor)?;
    let timestamps_section = read_section(encoded, &mut cursor)?;
    let templates_section = read_section(encoded, &mut cursor)?;
    let bodies_section = read_section(encoded, &mut cursor)?;
    let attributes_section = read_section(encoded, &mut cursor)?;
    let fields_section = read_section(encoded, &mut cursor)?;
    let typed_metadata_section = read_section(encoded, &mut cursor)?;
    let embedded_index_section = read_section(encoded, &mut cursor)?;
    if cursor != encoded.len() {
        return Err(TelemetryError::InvalidBlockEncoding("trailing bytes"));
    }
    let embedded_index = EmbeddedFrameIndex::decode(
        embedded_index_section,
        u32::try_from(record_count).map_err(|_| TelemetryError::RecordTooLarge)?,
    )?;
    if embedded_index.record_count as usize != record_count {
        return Err(TelemetryError::InvalidBlockEncoding(
            "embedded index record count mismatch",
        ));
    }
    let offsets = decode_offsets(offsets_section, record_count)?;
    let timestamps = decode_timestamps(timestamps_section, record_count)?;
    let templates = decode_templates(templates_section)?;
    let messages = decode_bodies(bodies_section, &templates, &embedded_index, record_count)?;
    let attributes = decode_attribute_tables(attributes_section)?;
    let fields = decode_fields(fields_section, &attributes, record_count)?;
    let typed_metadata = decode_typed_metadata(
        typed_metadata_section,
        record_count,
        &timestamps,
        &messages,
        &fields,
    )?;
    Ok(offsets
        .into_iter()
        .zip(timestamps)
        .zip(messages)
        .zip(fields)
        .zip(typed_metadata)
        .map(
            |((((offset, timestamp_unix_nanos), message), fields), metadata)| {
                DecodedStructuralRecord {
                    offset,
                    timestamp_unix_nanos,
                    message,
                    fields,
                    observed_timestamp_unix_nanos: metadata.observed_timestamp_unix_nanos,
                    body: metadata.body,
                    attributes: metadata.attributes,
                    resource: metadata.resource,
                    scope: metadata.scope,
                    severity_number: metadata.severity_number,
                    severity_text: metadata.severity_text,
                    dropped_attributes_count: metadata.dropped_attributes_count,
                    flags: metadata.flags,
                    trace_id: metadata.trace_id,
                    span_id: metadata.span_id,
                    event_name: metadata.event_name,
                }
            },
        )
        .collect())
}

/// Reconstructs only selected record ordinals from one structural block.
///
/// `record_ordinals` must be strictly increasing. The enclosing zstd frame and
/// Pco timestamp page are still decoded as a unit. Body and field lanes use
/// record checkpoints, so only the checkpoint neighborhoods containing selected
/// records are scanned.
pub fn decode_structural_records(
    encoded: &[u8],
    record_ordinals: &[u32],
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal(encoded, record_ordinals, true, None, None)
}

#[cfg(test)]
pub(crate) fn decode_structural_records_without_typed_metadata_with_embedded_index_and_templates(
    encoded: &[u8],
    record_ordinals: &[u32],
    embedded_index: &EmbeddedFrameIndex,
    templates: &[Vec<Vec<u8>>],
    include_fields: bool,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal_with_projection_and_messages(
        encoded,
        record_ordinals,
        false,
        None,
        Some(embedded_index),
        if include_fields {
            FieldProjection::All
        } else {
            FieldProjection::SeverityText
        },
        None,
        Some(templates),
        None,
        None,
        None,
    )
}

#[cfg(test)]
pub(crate) fn decode_structural_records_without_typed_metadata_with_embedded_index_and_severity_text(
    encoded: &[u8],
    record_ordinals: &[u32],
    embedded_index: &EmbeddedFrameIndex,
) -> TelemetryResult<Vec<DecodedStructuralRecord>> {
    decode_structural_records_internal_with_projection(
        encoded,
        record_ordinals,
        false,
        None,
        Some(embedded_index),
        FieldProjection::SeverityText,
    )
}

#[derive(Clone, Copy)]
#[allow(dead_code)]
enum FieldProjection {
    All,
    SeverityText,
    None,
}

/// Decodes only selected message bodies, leaving typed metadata and field
/// values compressed until a message predicate has been verified.
#[cfg(test)]
pub(crate) fn decode_structural_messages(
    encoded: &[u8],
    record_ordinals: &[u32],
) -> TelemetryResult<Vec<Arc<str>>> {
    decode_structural_messages_internal(encoded, record_ordinals, None, None)
}

pub(crate) fn decode_structural_messages_with_embedded_index_and_templates(
    encoded: &[u8],
    record_ordinals: &[u32],
    embedded_index: &EmbeddedFrameIndex,
    templates: &[Vec<Vec<u8>>],
) -> TelemetryResult<Vec<Arc<str>>> {
    decode_structural_messages_internal(
        encoded,
        record_ordinals,
        Some(embedded_index),
        Some(templates),
    )
}

/// Reuses the structural encoder's lossless token classifier to render a
/// Loki-compatible pattern with dynamic values replaced by `<_>`.
#[must_use]
pub fn message_pattern(message: &str) -> String {
    let parsed = parse_message(message.as_bytes());
    if parsed.values.is_empty() {
        return message.to_owned();
    }
    let mut pattern = String::with_capacity(message.len());
    for (index, literal) in parsed.literals.iter().enumerate() {
        pattern.push_str(&message[literal.clone()]);
        if index < parsed.values.len() {
            pattern.push_str("<_>");
        }
    }
    pattern
}

#[cfg(test)]
mod tests;
