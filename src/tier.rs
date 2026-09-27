//! Object-tier ownership: object stores, catalog transactions and queries, SSD cache, and storage I/O.
//! The root keeps catalog models and stable exports; child modules own their behavior.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use shard_stream_core::{ShardId, TopicPartition};

use crate::tier_ingest::DecodedTierIngestAppend;
use crate::{
    BlockCatalog, BlockDescriptor, BlockId, CompressionCodec, CorrelationBlockFilter,
    CorrelationQuery, TelemetryError, TelemetryResult, TelemetrySignal,
};

mod cache;
mod catalog_open;
mod catalog_publish;
mod catalog_query;
mod catalog_retention;
mod io;
mod object_store;
use io::*;
pub(crate) use io::{hash_file, validate_object_key};
pub use io::{mark_group_offloaded, write_staged_payload_pack};
#[cfg(test)]
mod tests;
pub use object_store::{
    LocalObjectStore, ObjectMetadata, ObjectStoreStats, SharedTelemetryObjectStore,
    TelemetryObjectStore,
};

const TIER_FORMAT_VERSION: u8 = 1;
const CHECKSUM_ALGORITHM: &str = "blake3";
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const POINTER_READ_LIMIT: u64 = 64 * 1024;
const CACHE_HEADER_MAGIC: &[u8; 8] = b"SLCACHE1";
const SIGNAL_INDEX_MAGIC: &[u8; 4] = b"STSI";
const SIGNAL_INDEX_HEADER_BYTES: usize = 16;
const CACHE_HEADER_BYTES: usize = 8 + 8 + 32;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Kind of immutable artifact attached to one block group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierArtifactKind {
    /// Concatenated compressed block payloads.
    PayloadPack,
    /// Independent persistent query-index segment for the group.
    QueryIndex,
    /// Immutable compression dictionary payload.
    Dictionary,
    /// Immutable placement-to-dictionary assignment catalog.
    DictionaryCatalog,
}

impl TierArtifactKind {
    fn key_name(self) -> &'static str {
        match self {
            Self::PayloadPack => "payload",
            Self::QueryIndex => "query-index",
            Self::Dictionary => "dictionary",
            Self::DictionaryCatalog => "dictionary-catalog",
        }
    }
}

/// Local source file to publish as one immutable group artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierArtifactSource {
    /// Artifact role.
    pub kind: TierArtifactKind,
    /// Stable, path-free artifact name.
    pub name: String,
    /// Local sealed file to upload.
    pub path: PathBuf,
}

/// Immutable object metadata for one published group artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierArtifact {
    /// Artifact role.
    pub kind: TierArtifactKind,
    /// Stable, path-free artifact name.
    pub name: String,
    /// Immutable object-store key.
    pub object_key: String,
    /// Exact object length.
    pub bytes: u64,
    /// Checksum algorithm, currently BLAKE3.
    pub checksum_algorithm: String,
    /// Lowercase BLAKE3 checksum.
    pub checksum: String,
}

/// Durable block metadata and payload extent inside a block-group pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierBlockEntry {
    /// Stripe-local block identifier.
    pub block_id: u64,
    /// Source compression cohort.
    pub source_compression_cohort: u64,
    /// Final compression placement.
    pub placement_id: u64,
    /// Optional immutable dictionary identifier as a decimal `u128`.
    pub dictionary_id: Option<String>,
    /// Compression codec name.
    pub compression_codec: String,
    /// Compression level.
    pub compression_level: i32,
    /// Lowest durable logical offset in the block.
    pub first_offset: u64,
    /// Highest durable logical offset in the block.
    pub last_offset: u64,
    /// Number of records in the block.
    pub record_count: u32,
    /// Raw source bytes represented by the block.
    pub source_bytes: u64,
    /// Structural bytes before byte compression.
    pub structural_bytes: u64,
    /// Stored compressed bytes.
    pub stored_bytes: u64,
    /// Lowest event timestamp in the block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp in the block.
    pub max_timestamp_unix_nanos: u64,
    /// Block compression temperature.
    pub compression_temperature: u16,
    /// Representative template shape.
    pub compression_shape_hash: u64,
    /// Internal temperature variance in Q8.
    pub compression_temperature_variance_q8: u16,
    /// Maximum record-to-block temperature deviation.
    pub max_compression_temperature_deviation: u8,
    /// Byte offset in the payload-pack artifact.
    pub payload_offset: u64,
    /// Byte length in the payload-pack artifact.
    pub payload_bytes: u64,
    /// Lowercase BLAKE3 checksum of this block's compressed bytes.
    pub payload_checksum: String,
    /// Lowest trace ID or metric-series fingerprint represented by this block.
    pub min_signal_identity: Option<u128>,
    /// Highest trace ID or metric-series fingerprint represented by this block.
    pub max_signal_identity: Option<u128>,
    /// Compact shared-identity filter for cold cross-signal lookup.
    pub correlation_filter: Option<CorrelationBlockFilter>,
}

/// Durable sink watermark covered by an immutable object-tier group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierCheckpoint {
    /// First placement sequence not represented by this or an older group.
    pub next_placement_sequence: u64,
    /// First logical offset not represented by this or an older group.
    pub next_offset: u64,
}

impl TierCheckpoint {
    fn covers(self, other: Self) -> bool {
        self.next_placement_sequence >= other.next_placement_sequence
            && self.next_offset >= other.next_offset
    }
}

impl TierBlockEntry {
    fn from_descriptor(
        descriptor: &BlockDescriptor,
        payload_offset: u64,
        payload_checksum: String,
    ) -> Self {
        Self {
            block_id: descriptor.block_id.get(),
            source_compression_cohort: descriptor.source_compression_cohort.get(),
            placement_id: descriptor.placement_id.get(),
            dictionary_id: descriptor
                .dictionary_id
                .map(|dictionary_id| dictionary_id.get().to_string()),
            compression_codec: match descriptor.compression_codec {
                CompressionCodec::Zstd => "zstd".into(),
            },
            compression_level: descriptor.compression_level,
            first_offset: descriptor.first_offset.get(),
            last_offset: descriptor.last_offset.get(),
            record_count: descriptor.record_count,
            source_bytes: descriptor.source_bytes,
            structural_bytes: descriptor.structural_bytes,
            stored_bytes: descriptor.stored_bytes,
            min_timestamp_unix_nanos: descriptor.min_timestamp_unix_nanos,
            max_timestamp_unix_nanos: descriptor.max_timestamp_unix_nanos,
            compression_temperature: descriptor.compression_temperature,
            compression_shape_hash: descriptor.compression_shape_hash,
            compression_temperature_variance_q8: descriptor.compression_temperature_variance_q8,
            max_compression_temperature_deviation: descriptor.max_compression_temperature_deviation,
            payload_offset,
            payload_bytes: descriptor.stored_bytes,
            payload_checksum,
            min_signal_identity: None,
            max_signal_identity: None,
            correlation_filter: None,
        }
    }

    /// Creates catalog metadata for one signal-native trace block or metric chunk.
    #[allow(clippy::too_many_arguments)]
    pub fn for_signal_payload(
        signal: TelemetrySignal,
        block_id: u64,
        min_signal_identity: u128,
        max_signal_identity: u128,
        first_offset: u64,
        last_offset: u64,
        record_count: u32,
        min_timestamp_unix_nanos: u64,
        max_timestamp_unix_nanos: u64,
        payload_offset: u64,
        payload_bytes: u64,
        payload_checksum: String,
        correlation_filter: CorrelationBlockFilter,
    ) -> TelemetryResult<Self> {
        if !matches!(signal, TelemetrySignal::Traces | TelemetrySignal::Metrics) {
            return Err(TelemetryError::InvalidConfig(
                "signal-native tier payload must be a trace block or metric chunk",
            ));
        }
        Ok(Self {
            block_id,
            source_compression_cohort: 0,
            placement_id: 0,
            dictionary_id: None,
            compression_codec: match signal {
                TelemetrySignal::Traces => "trace-native".into(),
                TelemetrySignal::Metrics => "metric-native".into(),
                TelemetrySignal::Logs => unreachable!("validated above"),
            },
            compression_level: 0,
            first_offset,
            last_offset,
            record_count,
            source_bytes: payload_bytes,
            structural_bytes: payload_bytes,
            stored_bytes: payload_bytes,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            compression_temperature: 0,
            compression_shape_hash: 0,
            compression_temperature_variance_q8: 0,
            max_compression_temperature_deviation: 0,
            payload_offset,
            payload_bytes,
            payload_checksum,
            min_signal_identity: Some(min_signal_identity),
            max_signal_identity: Some(max_signal_identity),
            correlation_filter: Some(correlation_filter),
        })
    }

    fn validate(&self, payload_bytes: u64) -> TelemetryResult<()> {
        if self.first_offset > self.last_offset
            || self.min_timestamp_unix_nanos > self.max_timestamp_unix_nanos
            || self.record_count == 0
            || self.stored_bytes == 0
            || self.payload_bytes != self.stored_bytes
            || !matches!(
                self.compression_codec.as_str(),
                "zstd" | "trace-native" | "metric-native"
            )
            || self.min_signal_identity.is_some() != self.max_signal_identity.is_some()
            || self
                .min_signal_identity
                .zip(self.max_signal_identity)
                .is_some_and(|(minimum, maximum)| minimum > maximum)
            || (self.compression_codec == "zstd") != self.min_signal_identity.is_none()
            || (self.compression_codec == "zstd") != self.correlation_filter.is_none()
            || !valid_checksum(&self.payload_checksum)
            || self
                .payload_offset
                .checked_add(self.payload_bytes)
                .is_none_or(|end| end > payload_bytes)
            || self
                .dictionary_id
                .as_ref()
                .is_some_and(|value| value.parse::<u128>().is_err())
        {
            return Err(TelemetryError::CorruptTier(
                "group contains invalid block metadata".into(),
            ));
        }
        Ok(())
    }
}

/// Complete local input needed to publish one block group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierGroupSource {
    /// Monotonic sequence within one physical-shard partition namespace.
    pub group_sequence: u64,
    /// Durable sink watermark covered after this complete group is published.
    pub checkpoint: TierCheckpoint,
    /// Block payload extents in pack order.
    pub blocks: Vec<TierBlockEntry>,
    /// Sealed local artifacts, including exactly one payload and query index.
    pub artifacts: Vec<TierArtifactSource>,
}

/// One already encoded trace block or metric chunk awaiting immutable publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalTierPayload {
    /// Stripe-local resident identifier used only to retire memory after publication.
    pub resident_id: u64,
    /// Signal partition whose catalog owns this payload.
    pub topic_partition: TopicPartition,
    /// Lowest trace ID or canonical metric-series fingerprint in the payload.
    pub min_signal_identity: u128,
    /// Highest trace ID or canonical metric-series fingerprint in the payload.
    pub max_signal_identity: u128,
    /// First durable offset represented by the payload.
    pub first_offset: u64,
    /// Last durable offset represented by the payload.
    pub last_offset: u64,
    /// Number of spans or points represented by the payload.
    pub record_count: u32,
    /// Lowest signal timestamp represented by the payload.
    pub min_timestamp_unix_nanos: u64,
    /// Highest signal timestamp represented by the payload.
    pub max_timestamp_unix_nanos: u64,
    /// Complete self-verifying signal-native bytes.
    pub payload: Arc<[u8]>,
    /// Compact shared-identity filter computed before the mutable head is released.
    pub correlation_filter: CorrelationBlockFilter,
}

/// Stages one complete trace or metric object-tier group.
#[allow(clippy::too_many_arguments)]
pub fn stage_signal_group(
    spool_directory: impl AsRef<Path>,
    file_stem: &str,
    signal: TelemetrySignal,
    group_sequence: u64,
    first_block_id: u64,
    checkpoint: TierCheckpoint,
    payloads: &[SignalTierPayload],
    recovery_state: &[u8],
) -> TelemetryResult<TierGroupSource> {
    if payloads.is_empty() || !matches!(signal, TelemetrySignal::Traces | TelemetrySignal::Metrics)
    {
        return Err(TelemetryError::ObjectStore(
            "signal group requires trace or metric payloads".into(),
        ));
    }
    validate_artifact_name(file_stem)?;
    let directory = spool_directory.as_ref();
    fs::create_dir_all(directory).map_err(|error| storage_io("create signal tier spool", error))?;
    let payload_path = directory.join(format!("{file_stem}.payload"));
    let index_path = directory.join(format!("{file_stem}.index"));
    let mut payload_pack = Vec::new();
    let mut blocks = Vec::with_capacity(payloads.len());
    let topic_partition = payloads[0].topic_partition;
    for (ordinal, payload) in payloads.iter().enumerate() {
        if payload.resident_id == 0
            || payload.topic_partition != topic_partition
            || payload.topic_partition.topic_id != signal.topic_id()
            || payload.payload.is_empty()
            || payload.record_count == 0
            || payload.first_offset > payload.last_offset
            || payload.max_timestamp_unix_nanos < payload.min_timestamp_unix_nanos
            || payload.last_offset >= checkpoint.next_offset
        {
            return Err(TelemetryError::ObjectStore(
                "signal tier payload has invalid bounds".into(),
            ));
        }
        let payload_offset =
            u64::try_from(payload_pack.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        let payload_bytes =
            u64::try_from(payload.payload.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
        payload_pack.extend_from_slice(&payload.payload);
        blocks.push(TierBlockEntry::for_signal_payload(
            signal,
            first_block_id
                .checked_add(u64::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?)
                .ok_or(TelemetryError::RecordTooLarge)?,
            payload.min_signal_identity,
            payload.max_signal_identity,
            payload.first_offset,
            payload.last_offset,
            payload.record_count,
            payload.min_timestamp_unix_nanos,
            payload.max_timestamp_unix_nanos,
            payload_offset,
            payload_bytes,
            checksum_bytes(&payload.payload),
            payload.correlation_filter.clone(),
        )?);
    }
    let index = encode_signal_index(signal, recovery_state)?;
    write_bytes_atomically(&payload_path, &payload_pack)?;
    write_bytes_atomically(&index_path, &index)?;
    Ok(TierGroupSource {
        group_sequence,
        checkpoint,
        blocks,
        artifacts: vec![
            TierArtifactSource {
                kind: TierArtifactKind::PayloadPack,
                name: "signal.payload".into(),
                path: payload_path,
            },
            TierArtifactSource {
                kind: TierArtifactKind::QueryIndex,
                name: "signal.index".into(),
                path: index_path,
            },
        ],
    })
}

fn encode_signal_index(signal: TelemetrySignal, recovery_state: &[u8]) -> TelemetryResult<Vec<u8>> {
    if !matches!(signal, TelemetrySignal::Traces | TelemetrySignal::Metrics) {
        return Err(TelemetryError::ObjectStore(
            "signal index requires traces or metrics".into(),
        ));
    }
    let recovery_bytes =
        u64::try_from(recovery_state.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    let mut encoded = Vec::with_capacity(
        SIGNAL_INDEX_HEADER_BYTES
            .saturating_add(recovery_state.len())
            .saturating_add(32),
    );
    encoded.extend_from_slice(SIGNAL_INDEX_MAGIC);
    encoded.push(signal as u8);
    encoded.extend_from_slice(&[0; 3]);
    encoded.extend_from_slice(&recovery_bytes.to_le_bytes());
    encoded.extend_from_slice(recovery_state);
    encoded.extend_from_slice(blake3::hash(&encoded).as_bytes());
    Ok(encoded)
}

/// Decodes the signal-local recovery snapshot from the current pre-release index format.
pub fn decode_signal_recovery_state(
    encoded: &[u8],
    expected_signal: TelemetrySignal,
) -> TelemetryResult<Vec<u8>> {
    if encoded.len() < SIGNAL_INDEX_HEADER_BYTES + 32
        || &encoded[..4] != SIGNAL_INDEX_MAGIC
        || encoded[4] != expected_signal as u8
        || encoded[5..8] != [0; 3]
    {
        return Err(TelemetryError::CorruptTier(
            "invalid signal recovery index header".into(),
        ));
    }
    let payload_end = encoded.len() - 32;
    if blake3::hash(&encoded[..payload_end]).as_bytes() != &encoded[payload_end..] {
        return Err(TelemetryError::CorruptTier(
            "signal recovery index checksum failed".into(),
        ));
    }
    let recovery_bytes = usize::try_from(u64::from_le_bytes(
        encoded[8..16]
            .try_into()
            .expect("fixed signal index length"),
    ))
    .map_err(|_| TelemetryError::CorruptTier("signal recovery index is too large".into()))?;
    if SIGNAL_INDEX_HEADER_BYTES
        .checked_add(recovery_bytes)
        .is_none_or(|end| end != payload_end)
    {
        return Err(TelemetryError::CorruptTier(
            "signal recovery index length mismatch".into(),
        ));
    }
    Ok(encoded[SIGNAL_INDEX_HEADER_BYTES..payload_end].to_vec())
}

/// Immutable manifest for one independently queryable block group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierGroupManifest {
    /// Storage-format version.
    pub format_version: u8,
    /// Monotonic group sequence.
    pub group_sequence: u64,
    /// Durable sink watermark covered by this complete group.
    pub checkpoint: TierCheckpoint,
    /// Owning physical shard.
    pub shard_id: u32,
    /// Logical topic as a decimal `u128`.
    pub topic_id: String,
    /// Logical partition.
    pub partition_id: u32,
    /// Ordered compressed blocks.
    pub blocks: Vec<TierBlockEntry>,
    /// Immutable payload, query-index, and dictionary artifacts.
    pub artifacts: Vec<TierArtifact>,
}

impl TierGroupManifest {
    /// Returns the artifact with the requested role.
    #[must_use]
    pub fn artifact(&self, kind: TierArtifactKind) -> Option<&TierArtifact> {
        self.artifacts.iter().find(|artifact| artifact.kind == kind)
    }

    fn validate(
        &self,
        shard_id: ShardId,
        partition: TopicPartition,
        max_blocks_per_group: usize,
        max_group_payload_bytes: u64,
    ) -> TelemetryResult<()> {
        if self.format_version != TIER_FORMAT_VERSION
            || self.shard_id != shard_id.get()
            || self.topic_id != partition.topic_id.get().to_string()
            || self.partition_id != partition.partition_id.get()
            || self.blocks.is_empty()
            || self.blocks.len() > max_blocks_per_group
            || self.artifacts.is_empty()
            || self
                .blocks
                .iter()
                .any(|block| block.last_offset >= self.checkpoint.next_offset)
        {
            return Err(TelemetryError::CorruptTier(
                "group manifest identity or cardinality is invalid".into(),
            ));
        }
        let payloads = self
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == TierArtifactKind::PayloadPack)
            .count();
        let indexes = self
            .artifacts
            .iter()
            .filter(|artifact| artifact.kind == TierArtifactKind::QueryIndex)
            .count();
        if payloads != 1 || indexes != 1 {
            return Err(TelemetryError::CorruptTier(
                "group requires exactly one payload and one query-index artifact".into(),
            ));
        }
        for artifact in &self.artifacts {
            validate_artifact(artifact)?;
        }
        if self.artifacts.iter().enumerate().any(|(index, artifact)| {
            self.artifacts[index + 1..]
                .iter()
                .any(|other| artifact.kind == other.kind && artifact.name == other.name)
        }) {
            return Err(TelemetryError::CorruptTier(
                "group contains duplicate artifact names".into(),
            ));
        }
        let payload_bytes = self
            .artifact(TierArtifactKind::PayloadPack)
            .expect("payload cardinality was checked")
            .bytes;
        if payload_bytes > max_group_payload_bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "group payload is {payload_bytes} bytes, exceeding limit {max_group_payload_bytes}"
            )));
        }
        let mut previous_block = None;
        let mut previous_payload_end = 0;
        for block in &self.blocks {
            block.validate(payload_bytes)?;
            if previous_block.is_some_and(|previous| previous >= block.block_id)
                || block.payload_offset < previous_payload_end
            {
                return Err(TelemetryError::CorruptTier(
                    "group blocks are not strictly ordered".into(),
                ));
            }
            previous_block = Some(block.block_id);
            previous_payload_end = block.payload_offset + block.payload_bytes;
        }
        Ok(())
    }
}

/// Bounded catalog entry pointing to one immutable group manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogGroupEntry {
    /// Group sequence.
    pub group_sequence: u64,
    /// Durable sink watermark covered by this group.
    pub checkpoint: TierCheckpoint,
    /// Immutable group-manifest object key.
    pub manifest_key: String,
    /// Group-manifest length.
    pub manifest_bytes: u64,
    /// Group-manifest BLAKE3 checksum.
    pub manifest_checksum: String,
    /// First logical offset covered by any block.
    pub first_offset: u64,
    /// Last logical offset covered by any block.
    pub last_offset: u64,
    /// Lowest event timestamp in any block.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp in any block.
    pub max_timestamp_unix_nanos: u64,
    /// Number of blocks in the group.
    pub block_count: u32,
    /// Total compressed payload bytes represented by the group.
    pub payload_bytes: u64,
    /// Lowest optional trace ID or series fingerprint represented by the group.
    pub min_signal_identity: Option<u128>,
    /// Highest optional trace ID or series fingerprint represented by the group.
    pub max_signal_identity: Option<u128>,
    /// Union filter for shared trace/resource/scope/attribute identities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_filter: Option<CorrelationBlockFilter>,
}

impl CatalogGroupEntry {
    fn from_manifest(
        manifest: &TierGroupManifest,
        manifest_key: String,
        metadata: &ObjectMetadata,
    ) -> TelemetryResult<Self> {
        let first_offset = manifest
            .blocks
            .iter()
            .map(|block| block.first_offset)
            .min()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no block bounds".into()))?;
        let last_offset = manifest
            .blocks
            .iter()
            .map(|block| block.last_offset)
            .max()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no block bounds".into()))?;
        let min_timestamp_unix_nanos = manifest
            .blocks
            .iter()
            .map(|block| block.min_timestamp_unix_nanos)
            .min()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no time bounds".into()))?;
        let max_timestamp_unix_nanos = manifest
            .blocks
            .iter()
            .map(|block| block.max_timestamp_unix_nanos)
            .max()
            .ok_or_else(|| TelemetryError::CorruptTier("group has no time bounds".into()))?;
        let block_count = u32::try_from(manifest.blocks.len())
            .map_err(|_| TelemetryError::CorruptTier("group has too many blocks".into()))?;
        let payload_bytes = manifest
            .blocks
            .iter()
            .try_fold(0u64, |total, block| total.checked_add(block.payload_bytes))
            .ok_or_else(|| TelemetryError::CorruptTier("group payload bytes overflow".into()))?;
        let min_signal_identity = manifest
            .blocks
            .iter()
            .filter_map(|block| block.min_signal_identity)
            .min();
        let max_signal_identity = manifest
            .blocks
            .iter()
            .filter_map(|block| block.max_signal_identity)
            .max();
        let correlation_filter = manifest
            .blocks
            .iter()
            .filter_map(|block| block.correlation_filter.as_ref())
            .fold(None::<CorrelationBlockFilter>, |filter, block| {
                let mut filter = filter.unwrap_or_default();
                filter.union_assign(block);
                Some(filter)
            });
        Ok(Self {
            group_sequence: manifest.group_sequence,
            checkpoint: manifest.checkpoint,
            manifest_key,
            manifest_bytes: metadata.bytes,
            manifest_checksum: metadata.content_digest.clone(),
            first_offset,
            last_offset,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            block_count,
            payload_bytes,
            min_signal_identity,
            max_signal_identity,
            correlation_filter,
        })
    }

    fn validate(&self) -> TelemetryResult<()> {
        validate_object_key(&self.manifest_key)?;
        if self.manifest_bytes == 0
            || !valid_checksum(&self.manifest_checksum)
            || self.first_offset > self.last_offset
            || self.min_timestamp_unix_nanos > self.max_timestamp_unix_nanos
            || self.block_count == 0
            || self.payload_bytes == 0
            || self.last_offset >= self.checkpoint.next_offset
            || self.min_signal_identity.is_some() != self.max_signal_identity.is_some()
            || self.min_signal_identity.is_some() != self.correlation_filter.is_some()
            || self
                .min_signal_identity
                .zip(self.max_signal_identity)
                .is_some_and(|(min, max)| min > max)
        {
            return Err(TelemetryError::CorruptTier(
                "catalog contains an invalid group entry".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable bounded page of group catalog entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPage {
    /// Storage-format version.
    pub format_version: u8,
    /// Page sequence in this partition namespace.
    pub page_sequence: u64,
    /// Owning physical shard.
    pub shard_id: u32,
    /// Logical topic as a decimal `u128`.
    pub topic_id: String,
    /// Logical partition.
    pub partition_id: u32,
    /// Ordered group entries.
    pub groups: Vec<CatalogGroupEntry>,
}

impl CatalogPage {
    fn validate(
        &self,
        shard_id: ShardId,
        partition: TopicPartition,
        groups_per_page: usize,
    ) -> TelemetryResult<()> {
        if self.format_version != TIER_FORMAT_VERSION
            || self.shard_id != shard_id.get()
            || self.topic_id != partition.topic_id.get().to_string()
            || self.partition_id != partition.partition_id.get()
            || self.groups.is_empty()
            || self.groups.len() > groups_per_page
        {
            return Err(TelemetryError::CorruptTier(
                "catalog page identity or cardinality is invalid".into(),
            ));
        }
        let mut previous = None;
        let mut previous_checkpoint = None;
        for group in &self.groups {
            group.validate()?;
            if previous.is_some_and(|sequence| sequence >= group.group_sequence)
                || previous_checkpoint
                    .is_some_and(|checkpoint: TierCheckpoint| !group.checkpoint.covers(checkpoint))
            {
                return Err(TelemetryError::CorruptTier(
                    "catalog group sequences or checkpoints are not increasing".into(),
                ));
            }
            previous = Some(group.group_sequence);
            previous_checkpoint = Some(group.checkpoint);
        }
        Ok(())
    }
}

/// Root-level coarse bounds and immutable pointer for one catalog page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPageRef {
    /// Page sequence.
    pub page_sequence: u64,
    /// Immutable page object key.
    pub page_key: String,
    /// Page object length.
    pub page_bytes: u64,
    /// Page object BLAKE3 checksum.
    pub page_checksum: String,
    /// First group sequence in the page.
    pub first_group_sequence: u64,
    /// Last group sequence in the page.
    pub last_group_sequence: u64,
    /// Latest durable sink watermark covered by the page.
    pub last_checkpoint: TierCheckpoint,
    /// Number of group entries.
    pub group_count: u32,
    /// Lowest logical offset covered by the page.
    pub first_offset: u64,
    /// Highest logical offset covered by the page.
    pub last_offset: u64,
    /// Lowest event timestamp covered by the page.
    pub min_timestamp_unix_nanos: u64,
    /// Highest event timestamp covered by the page.
    pub max_timestamp_unix_nanos: u64,
    /// Lowest optional trace ID or series fingerprint covered by the page.
    pub min_signal_identity: Option<u128>,
    /// Highest optional trace ID or series fingerprint covered by the page.
    pub max_signal_identity: Option<u128>,
    /// Union filter for shared trace/resource/scope/attribute identities.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_filter: Option<CorrelationBlockFilter>,
}

impl CatalogPageRef {
    fn from_page(
        page: &CatalogPage,
        page_key: String,
        metadata: &ObjectMetadata,
    ) -> TelemetryResult<Self> {
        let first = page
            .groups
            .first()
            .ok_or_else(|| TelemetryError::CorruptTier("catalog page is empty".into()))?;
        let last = page
            .groups
            .last()
            .ok_or_else(|| TelemetryError::CorruptTier("catalog page is empty".into()))?;
        Ok(Self {
            page_sequence: page.page_sequence,
            page_key,
            page_bytes: metadata.bytes,
            page_checksum: metadata.content_digest.clone(),
            first_group_sequence: first.group_sequence,
            last_group_sequence: last.group_sequence,
            last_checkpoint: last.checkpoint,
            group_count: u32::try_from(page.groups.len())
                .map_err(|_| TelemetryError::CorruptTier("catalog page is too large".into()))?,
            first_offset: page
                .groups
                .iter()
                .map(|group| group.first_offset)
                .min()
                .expect("page is nonempty"),
            last_offset: page
                .groups
                .iter()
                .map(|group| group.last_offset)
                .max()
                .expect("page is nonempty"),
            min_timestamp_unix_nanos: page
                .groups
                .iter()
                .map(|group| group.min_timestamp_unix_nanos)
                .min()
                .expect("page is nonempty"),
            max_timestamp_unix_nanos: page
                .groups
                .iter()
                .map(|group| group.max_timestamp_unix_nanos)
                .max()
                .expect("page is nonempty"),
            min_signal_identity: page
                .groups
                .iter()
                .filter_map(|group| group.min_signal_identity)
                .min(),
            max_signal_identity: page
                .groups
                .iter()
                .filter_map(|group| group.max_signal_identity)
                .max(),
            correlation_filter: page
                .groups
                .iter()
                .filter_map(|group| group.correlation_filter.as_ref())
                .fold(None::<CorrelationBlockFilter>, |filter, group| {
                    let mut filter = filter.unwrap_or_default();
                    filter.union_assign(group);
                    Some(filter)
                }),
        })
    }

    fn validate(&self) -> TelemetryResult<()> {
        validate_object_key(&self.page_key)?;
        if self.page_bytes == 0
            || !valid_checksum(&self.page_checksum)
            || self.first_group_sequence > self.last_group_sequence
            || self.group_count == 0
            || self.first_offset > self.last_offset
            || self.min_timestamp_unix_nanos > self.max_timestamp_unix_nanos
            || self.last_offset >= self.last_checkpoint.next_offset
            || self.min_signal_identity.is_some() != self.max_signal_identity.is_some()
            || self.min_signal_identity.is_some() != self.correlation_filter.is_some()
            || self
                .min_signal_identity
                .zip(self.max_signal_identity)
                .is_some_and(|(min, max)| min > max)
        {
            return Err(TelemetryError::CorruptTier(
                "catalog root contains an invalid page reference".into(),
            ));
        }
        Ok(())
    }
}

/// Immutable catalog root for one physical-shard logical-partition pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogRoot {
    /// Storage-format version.
    pub format_version: u8,
    /// Monotonically increasing publication generation.
    pub generation: u64,
    /// Owning physical shard.
    pub shard_id: u32,
    /// Logical topic as a decimal `u128`.
    pub topic_id: String,
    /// Logical partition.
    pub partition_id: u32,
    /// Latest durable sink watermark selected by this root.
    pub latest_checkpoint: Option<TierCheckpoint>,
    /// First block identifier not used by any published group.
    pub next_block_id: u64,
    /// Ordered immutable catalog pages.
    pub pages: Vec<CatalogPageRef>,
    /// Exact keys retired by this generation and awaiting their final reader.
    ///
    /// This is a bounded, crash-replayable ownership handoff. It is not a
    /// tracing garbage-collection root and is never populated by object listing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired_objects: Vec<RetiredObject>,
}

/// One exact object whose previous catalog generation relinquished ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredObject {
    /// Exact key formerly owned by the retired generation.
    pub object_key: String,
    /// Earliest wall-clock millisecond when cross-process readers cannot remain.
    pub delete_after_unix_millis: u64,
}

/// Result of one bounded exact-key object-tier retention transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TierRetentionReport {
    /// Complete immutable groups removed from the selected catalog.
    pub retired_groups: u64,
    /// Compressed payload bytes removed from the selected catalog.
    pub retired_payload_bytes: u64,
    /// Exact object keys transferred to deferred reclamation ownership.
    pub retired_objects: u64,
}

impl CatalogRoot {
    fn empty(shard_id: ShardId, partition: TopicPartition) -> Self {
        Self {
            format_version: TIER_FORMAT_VERSION,
            generation: 0,
            shard_id: shard_id.get(),
            topic_id: partition.topic_id.get().to_string(),
            partition_id: partition.partition_id.get(),
            latest_checkpoint: None,
            next_block_id: 0,
            pages: Vec::new(),
            retired_objects: Vec::new(),
        }
    }

    fn validate(&self, shard_id: ShardId, partition: TopicPartition) -> TelemetryResult<()> {
        if self.format_version != TIER_FORMAT_VERSION
            || self.shard_id != shard_id.get()
            || self.topic_id != partition.topic_id.get().to_string()
            || self.partition_id != partition.partition_id.get()
        {
            return Err(TelemetryError::CorruptTier(
                "catalog root belongs to a different namespace".into(),
            ));
        }
        let mut previous_page = None;
        let mut previous_group = None;
        let mut previous_checkpoint = None;
        for page in &self.pages {
            page.validate()?;
            if previous_page.is_some_and(|sequence| sequence >= page.page_sequence)
                || previous_group.is_some_and(|sequence| sequence >= page.first_group_sequence)
                || previous_checkpoint.is_some_and(|checkpoint: TierCheckpoint| {
                    !page.last_checkpoint.covers(checkpoint)
                })
            {
                return Err(TelemetryError::CorruptTier(
                    "catalog root pages or checkpoints are not strictly increasing".into(),
                ));
            }
            previous_page = Some(page.page_sequence);
            previous_group = Some(page.last_group_sequence);
            previous_checkpoint = Some(page.last_checkpoint);
        }
        if self.latest_checkpoint != previous_checkpoint {
            return Err(TelemetryError::CorruptTier(
                "catalog root latest checkpoint disagrees with its final page".into(),
            ));
        }
        let namespace_prefix = format!("{}/", catalog_namespace(shard_id, partition));
        for (index, retired) in self.retired_objects.iter().enumerate() {
            validate_object_key(&retired.object_key)?;
            if !retired.object_key.starts_with(&namespace_prefix)
                || retired.object_key.ends_with("/CURRENT")
                || retired.object_key.ends_with("/PENDING")
                || self.retired_objects[index + 1..]
                    .iter()
                    .any(|other| other.object_key == retired.object_key)
                || self
                    .pages
                    .iter()
                    .any(|page| page.page_key == retired.object_key)
            {
                return Err(TelemetryError::CorruptTier(
                    "catalog root contains an invalid retired-object ownership set".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Read lease for one immutable catalog generation.
///
/// Cloning a lease is equivalent to cloning an `Arc`: a retired root and any
/// page it replaced cannot be reclaimed until the final lease is dropped.
#[derive(Debug, Clone)]
pub struct CatalogLease {
    root: Arc<CatalogRoot>,
}

impl CatalogLease {
    /// Returns the immutable catalog root owned by this lease.
    #[must_use]
    pub fn root(&self) -> &CatalogRoot {
        &self.root
    }
}

/// Small mutable pointer atomically selecting the authoritative catalog root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogPointer {
    /// Storage-format version.
    pub format_version: u8,
    /// Selected root generation.
    pub generation: u64,
    /// Immutable root object key.
    pub root_key: String,
    /// Root object length.
    pub root_bytes: u64,
    /// Root object BLAKE3 checksum.
    pub root_checksum: String,
}

/// Crash-replayable ownership of every object prepared before `CURRENT` moves.
///
/// Each catalog namespace has exactly one fixed `PENDING` key. A publisher
/// conditionally acquires it before creating any transaction object. Recovery
/// can therefore delete the recorded exact keys when the target root was not
/// selected, without listing storage or tracing reachability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CatalogTransaction {
    format_version: u8,
    transaction_id: String,
    target_generation: u64,
    target_root_key: String,
    reclaim_after_unix_millis: u64,
    owned_objects: Vec<String>,
}

impl CatalogTransaction {
    fn validate(&self, namespace: &str, max_objects: usize) -> TelemetryResult<()> {
        let owned_prefix = format!("{namespace}/transactions/{}/", self.transaction_id);
        if self.format_version != TIER_FORMAT_VERSION
            || self.transaction_id.len() != 32
            || !self
                .transaction_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.target_generation == 0
            || self.reclaim_after_unix_millis == 0
            || self.owned_objects.is_empty()
            || self.owned_objects.len() > max_objects
            || !self
                .owned_objects
                .iter()
                .any(|key| key == &self.target_root_key)
            || !self.target_root_key.starts_with(&owned_prefix)
            || self
                .owned_objects
                .iter()
                .any(|key| !key.starts_with(&owned_prefix))
        {
            return Err(TelemetryError::CorruptTier(
                "catalog PENDING transaction is invalid".into(),
            ));
        }
        validate_object_key(&self.target_root_key)?;
        for (index, key) in self.owned_objects.iter().enumerate() {
            validate_object_key(key)?;
            if self.owned_objects[index + 1..].contains(key) {
                return Err(TelemetryError::CorruptTier(
                    "catalog PENDING transaction repeats an owned key".into(),
                ));
            }
        }
        Ok(())
    }
}

impl CatalogPointer {
    fn validate(&self) -> TelemetryResult<()> {
        validate_object_key(&self.root_key)?;
        if self.format_version != TIER_FORMAT_VERSION
            || self.root_bytes == 0
            || !valid_checksum(&self.root_checksum)
        {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Coarse inclusive bounds used to prune catalog pages and groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TierQueryRange {
    /// Optional first logical offset.
    pub first_offset: Option<u64>,
    /// Optional last logical offset.
    pub last_offset: Option<u64>,
    /// Optional lowest event timestamp.
    pub min_timestamp_unix_nanos: Option<u64>,
    /// Optional highest event timestamp.
    pub max_timestamp_unix_nanos: Option<u64>,
    /// Optional exact trace ID or metric-series fingerprint.
    pub signal_identity: Option<u128>,
}

impl TierQueryRange {
    fn validate(self) -> TelemetryResult<()> {
        if self
            .first_offset
            .zip(self.last_offset)
            .is_some_and(|(first, last)| first > last)
            || self
                .min_timestamp_unix_nanos
                .zip(self.max_timestamp_unix_nanos)
                .is_some_and(|(first, last)| first > last)
        {
            return Err(TelemetryError::InvalidQuery(
                "tier query range starts after its end".into(),
            ));
        }
        Ok(())
    }

    fn overlaps(
        self,
        first_offset: u64,
        last_offset: u64,
        min_timestamp: u64,
        max_timestamp: u64,
        min_signal_identity: Option<u128>,
        max_signal_identity: Option<u128>,
    ) -> bool {
        self.first_offset
            .is_none_or(|query_first| last_offset >= query_first)
            && self
                .last_offset
                .is_none_or(|query_last| first_offset <= query_last)
            && self
                .min_timestamp_unix_nanos
                .is_none_or(|query_min| max_timestamp >= query_min)
            && self
                .max_timestamp_unix_nanos
                .is_none_or(|query_max| min_timestamp <= query_max)
            && self.signal_identity.is_none_or(|identity| {
                min_signal_identity
                    .zip(max_signal_identity)
                    .is_some_and(|(min, max)| identity >= min && identity <= max)
            })
    }
}

fn catalog_correlation_may_match(
    filter: Option<&CorrelationBlockFilter>,
    min_signal_identity: Option<u128>,
    max_signal_identity: Option<u128>,
    query: &CorrelationQuery,
    signal: TelemetrySignal,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    if signal == TelemetrySignal::Traces {
        min_signal_identity
            .zip(max_signal_identity)
            .is_none_or(|(minimum, maximum)| filter.may_match_trace_block(query, minimum, maximum))
    } else {
        filter.may_match(query)
    }
}

/// Bounded object-tier catalog configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectTierConfig {
    /// Preferred compressed payload bytes per block group.
    pub target_group_payload_bytes: u64,
    /// Hard limit for one block-group payload object.
    pub max_group_payload_bytes: u64,
    /// Hard limit for independently compressed blocks in one group.
    pub max_blocks_per_group: usize,
    /// Maximum group entries per immutable catalog page.
    pub groups_per_page: usize,
    /// Maximum bytes read for any root, page, or group manifest.
    pub max_control_object_bytes: u64,
    /// Maximum exact retired keys carried by one catalog generation.
    pub max_retired_objects: usize,
    /// Safety window for readers in another process that leased the old root.
    pub retirement_grace: std::time::Duration,
    /// Maximum expected duration of one publication before failover may reclaim it.
    pub transaction_lease: std::time::Duration,
}

impl Default for ObjectTierConfig {
    fn default() -> Self {
        Self {
            target_group_payload_bytes: 1024 * 1024 * 1024,
            max_group_payload_bytes: 2 * 1024 * 1024 * 1024,
            max_blocks_per_group: 4_096,
            groups_per_page: 1_024,
            max_control_object_bytes: 64 * 1024 * 1024,
            max_retired_objects: 4_096,
            retirement_grace: std::time::Duration::from_secs(10 * 60),
            transaction_lease: std::time::Duration::from_secs(30 * 60),
        }
    }
}

impl ObjectTierConfig {
    pub(crate) fn validate(self) -> TelemetryResult<()> {
        if self.target_group_payload_bytes == 0
            || self.max_group_payload_bytes < self.target_group_payload_bytes
        {
            return Err(TelemetryError::InvalidConfig(
                "object tier group payload limits are invalid",
            ));
        }
        if self.max_blocks_per_group == 0 {
            return Err(TelemetryError::InvalidConfig(
                "object tier max_blocks_per_group must be nonzero",
            ));
        }
        if self.groups_per_page == 0 {
            return Err(TelemetryError::InvalidConfig(
                "object tier groups_per_page must be nonzero",
            ));
        }
        if self.max_control_object_bytes < POINTER_READ_LIMIT {
            return Err(TelemetryError::InvalidConfig(
                "object tier control-object limit must be at least 64 KiB",
            ));
        }
        if self.max_retired_objects < 2 {
            return Err(TelemetryError::InvalidConfig(
                "object tier must allow at least two retired ownership keys",
            ));
        }
        if self.retirement_grace > std::time::Duration::from_secs(24 * 60 * 60) {
            return Err(TelemetryError::InvalidConfig(
                "object tier retirement grace cannot exceed 24 hours",
            ));
        }
        if self.transaction_lease.is_zero()
            || self.transaction_lease > std::time::Duration::from_secs(24 * 60 * 60)
        {
            return Err(TelemetryError::InvalidConfig(
                "object tier transaction lease must be between one nanosecond and 24 hours",
            ));
        }
        Ok(())
    }
}

/// Partition-scoped immutable object tier with a conditionally published root.
#[derive(Debug)]
pub struct TelemetryObjectTier<S> {
    store: S,
    shard_id: ShardId,
    partition: TopicPartition,
    namespace: String,
    config: ObjectTierConfig,
    root: Arc<CatalogRoot>,
    current_root_key: Option<String>,
    current_version: Option<String>,
    retired_leases: HashMap<String, Weak<CatalogRoot>>,
}

/// Configuration for one byte-bounded SSD object-range cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SsdCacheConfig {
    /// Maximum cache bytes, including per-chunk integrity headers.
    pub max_bytes: u64,
    /// Object range chunk size.
    pub chunk_bytes: u64,
    /// Maximum bytes returned by one cache read.
    pub max_read_bytes: u64,
    /// Maximum verified immutable chunk bytes retained in RAM.
    pub memory_bytes: u64,
    /// Maximum decoded catalog-page and group-manifest bytes retained in RAM.
    pub parsed_memory_bytes: u64,
}

impl Default for SsdCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024 * 1024,
            chunk_bytes: 4 * 1024 * 1024,
            max_read_bytes: 64 * 1024 * 1024,
            memory_bytes: 256 * 1024 * 1024,
            parsed_memory_bytes: 64 * 1024 * 1024,
        }
    }
}

impl SsdCacheConfig {
    fn validate(self) -> TelemetryResult<()> {
        if self.max_bytes == 0 || self.chunk_bytes == 0 || self.max_read_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "SSD cache byte limits must be nonzero",
            ));
        }
        if self.chunk_bytes > self.max_read_bytes {
            return Err(TelemetryError::InvalidConfig(
                "SSD cache chunks cannot exceed the read limit",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CacheEntry {
    path: PathBuf,
    bytes: u64,
    stamp: u64,
}

#[derive(Debug)]
struct MemoryCacheEntry {
    bytes: Arc<[u8]>,
    stamp: u64,
}

#[derive(Debug, Clone)]
enum ParsedControlObject {
    CatalogPage(Arc<CatalogPage>),
    GroupManifest(Arc<TierGroupManifest>),
    TierIngestGroup(Arc<[DecodedTierIngestAppend]>),
}

#[derive(Debug)]
struct ParsedControlEntry {
    object: ParsedControlObject,
    accounted_bytes: u64,
    stamp: u64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<String, CacheEntry>,
    used_bytes: u64,
    memory_entries: HashMap<String, MemoryCacheEntry>,
    memory_used_bytes: u64,
    parsed_entries: HashMap<String, ParsedControlEntry>,
    parsed_used_bytes: u64,
    clock: u64,
    hits: u64,
    memory_hits: u64,
    parsed_hits: u64,
    misses: u64,
    source_bytes: u64,
}

/// Runtime diagnostics for one recoverable SSD object cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SsdCacheStats {
    /// Immutable chunks currently retained on SSD.
    pub entries: usize,
    /// Framed bytes currently retained on SSD.
    pub used_bytes: u64,
    /// Successful SSD or RAM cache hits since open.
    pub hits: u64,
    /// Hits served from already verified immutable RAM chunks.
    pub memory_hits: u64,
    /// Hits served from decoded immutable catalog pages or group manifests.
    pub parsed_hits: u64,
    /// Immutable chunks and decoded control objects currently retained in RAM.
    pub memory_entries: usize,
    /// Verified raw and conservatively accounted decoded bytes retained in RAM.
    pub memory_used_bytes: u64,
    /// Decoded immutable control objects currently retained in RAM.
    pub parsed_entries: usize,
    /// Conservative decoded-control memory accounted against the RAM budget.
    pub parsed_used_bytes: u64,
    /// Chunks fetched from object storage since open.
    pub misses: u64,
    /// Object-store payload bytes fetched by cache misses since open.
    pub source_bytes: u64,
}

/// One immutable object range backed by a shared verified cache chunk.
#[derive(Debug, Clone)]
pub struct CachedObjectRange {
    bytes: Arc<[u8]>,
    start: usize,
    end: usize,
}

impl CachedObjectRange {
    fn empty() -> Self {
        Self::from_owned(Vec::new())
    }

    fn from_owned(bytes: Vec<u8>) -> Self {
        let end = bytes.len();
        Self {
            bytes: Arc::from(bytes),
            start: 0,
            end,
        }
    }
}

impl AsRef<[u8]> for CachedObjectRange {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[self.start..self.end]
    }
}

/// Recoverable, integrity-checked SSD cache for immutable object ranges.
///
/// Deployments normally use separate instances and budgets for control/index
/// objects and payload data so a scan cannot evict all query metadata.
#[derive(Debug)]
pub struct SsdObjectCache {
    root: PathBuf,
    config: SsdCacheConfig,
    state: Mutex<CacheState>,
}
