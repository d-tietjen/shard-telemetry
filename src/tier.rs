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

use crate::{
    BlockCatalog, BlockDescriptor, BlockId, CompressionCodec, CorrelationBlockFilter,
    CorrelationQuery, TelemetryError, TelemetryResult, TelemetrySignal,
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

/// Metadata returned for one object-store object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// Exact object length in bytes.
    pub bytes: u64,
    /// Opaque object-store version used for conditional publication.
    ///
    /// This is deliberately not treated as a content checksum. For example,
    /// an S3 multipart ETag is a version token but is not a BLAKE3 digest.
    pub version_token: String,
    /// Lowercase BLAKE3 digest calculated over the complete object contents.
    pub content_digest: String,
}

/// Minimal object-store contract needed by the ShardTelemetry tier.
///
/// Immutable data uses put-if-absent operations. Only the small `CURRENT`
/// pointer is mutable, and it is replaced with an object-version
/// compare-and-swap.
pub trait TelemetryObjectStore: Send + Sync {
    /// Creates an immutable object from bytes, or verifies an identical retry.
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata>;

    /// Creates an immutable object from a local file without buffering it all.
    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata>;

    /// Reads an entire object subject to a caller-provided allocation limit.
    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>>;

    /// Reads exactly one byte range from an object.
    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>>;

    /// Returns object metadata, or `None` when the key is absent.
    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>>;

    /// Deletes one exact object key.
    ///
    /// Deletion is idempotent: an already absent key is a successful outcome.
    /// Catalog ownership code never calls this with a discovered or listed key.
    fn delete(&self, key: &str) -> TelemetryResult<()>;

    /// Conditionally replaces a small mutable object.
    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata>;
}

#[derive(Debug, Default)]
struct ObjectStoreCounters {
    put_requests: AtomicU64,
    put_bytes: AtomicU64,
    get_requests: AtomicU64,
    get_bytes: AtomicU64,
    range_requests: AtomicU64,
    range_bytes: AtomicU64,
    head_requests: AtomicU64,
    compare_and_swaps: AtomicU64,
    delete_requests: AtomicU64,
    failures: AtomicU64,
}

/// Process-local object-tier operation counters shared by all stripe owners.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectStoreStats {
    /// Immutable byte and file put attempts.
    pub put_requests: u64,
    /// Source bytes accepted by successful put operations.
    pub put_bytes: u64,
    /// Complete-object read attempts.
    pub get_requests: u64,
    /// Bytes returned by complete-object reads.
    pub get_bytes: u64,
    /// Byte-range read attempts.
    pub range_requests: u64,
    /// Bytes returned by byte-range reads.
    pub range_bytes: u64,
    /// Metadata lookup attempts.
    pub head_requests: u64,
    /// Conditional `CURRENT` publication attempts.
    pub compare_and_swaps: u64,
    /// Exact-key idempotent deletion attempts.
    pub delete_requests: u64,
    /// Failed object-store operations of any kind.
    pub failures: u64,
}

/// Cloneable type-erased object-store handle used by production stripe owners.
#[derive(Clone)]
pub struct SharedTelemetryObjectStore {
    inner: Arc<dyn TelemetryObjectStore>,
    counters: Arc<ObjectStoreCounters>,
}

impl SharedTelemetryObjectStore {
    /// Wraps an object-store adapter for use by independently owned stripes.
    #[must_use]
    pub fn new(store: Arc<dyn TelemetryObjectStore>) -> Self {
        Self {
            inner: store,
            counters: Arc::new(ObjectStoreCounters::default()),
        }
    }

    /// Returns operation and transfer counters shared by every clone.
    #[must_use]
    pub fn stats(&self) -> ObjectStoreStats {
        ObjectStoreStats {
            put_requests: self.counters.put_requests.load(Ordering::Relaxed),
            put_bytes: self.counters.put_bytes.load(Ordering::Relaxed),
            get_requests: self.counters.get_requests.load(Ordering::Relaxed),
            get_bytes: self.counters.get_bytes.load(Ordering::Relaxed),
            range_requests: self.counters.range_requests.load(Ordering::Relaxed),
            range_bytes: self.counters.range_bytes.load(Ordering::Relaxed),
            head_requests: self.counters.head_requests.load(Ordering::Relaxed),
            compare_and_swaps: self.counters.compare_and_swaps.load(Ordering::Relaxed),
            delete_requests: self.counters.delete_requests.load(Ordering::Relaxed),
            failures: self.counters.failures.load(Ordering::Relaxed),
        }
    }

    fn record_failure<T>(&self, result: &TelemetryResult<T>) {
        if result.is_err() {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl std::fmt::Debug for SharedTelemetryObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SharedTelemetryObjectStore(..)")
    }
}

impl From<LocalObjectStore> for SharedTelemetryObjectStore {
    fn from(store: LocalObjectStore) -> Self {
        Self::new(Arc::new(store))
    }
}

impl TelemetryObjectStore for SharedTelemetryObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put_bytes_if_absent(key, bytes);
        self.record_failure(&result);
        if result.is_ok() {
            self.counters.put_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        result
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        self.counters.put_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.put_file_if_absent(key, source);
        self.record_failure(&result);
        if let Ok(metadata) = &result {
            self.counters
                .put_bytes
                .fetch_add(metadata.bytes, Ordering::Relaxed);
        }
        result
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        self.counters.get_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get(key, max_bytes);
        self.record_failure(&result);
        if let Ok(bytes) = &result {
            self.counters.get_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        result
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        self.counters.range_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_range(key, range);
        self.record_failure(&result);
        if let Ok(bytes) = &result {
            self.counters.range_bytes.fetch_add(
                u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        result
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        self.counters.head_requests.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.head(key);
        self.record_failure(&result);
        result
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        self.counters
            .delete_requests
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.delete(key);
        self.record_failure(&result);
        result
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        self.counters
            .compare_and_swaps
            .fetch_add(1, Ordering::Relaxed);
        let result = self.inner.compare_and_swap(key, expected_version, bytes);
        self.record_failure(&result);
        result
    }
}

/// Filesystem implementation of [`TelemetryObjectStore`] used for local operation
/// and deterministic testing of S3-style immutable publication.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// Opens or creates a local object-store root.
    pub fn open(root: impl AsRef<Path>) -> TelemetryResult<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)
            .map_err(|error| storage_io("create local object-store root", error))?;
        Ok(Self { root })
    }

    /// Returns the backing filesystem root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn object_path(&self, key: &str) -> TelemetryResult<PathBuf> {
        validate_object_key(key)?;
        Ok(self.root.join(key))
    }

    fn update_lock(&self) -> TelemetryResult<File> {
        let path = self.root.join(".shard-telemetry-object-store.lock");
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|error| storage_io("open object-store update lock", error))?;
        FileExt::lock_exclusive(&lock)
            .map_err(|error| storage_io("lock object-store update lock", error))?;
        Ok(lock)
    }
}

impl TelemetryObjectStore for LocalObjectStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let expected = metadata_for_bytes(bytes);
        if let Some(observed) = metadata_for_path_if_present(&path)? {
            unlock_file(&lock)?;
            if observed == expected {
                return Ok(observed);
            }
            return Err(TelemetryError::ObjectStore(format!(
                "immutable object key {key} already contains different bytes"
            )));
        }
        write_bytes_atomically(&path, bytes)?;
        unlock_file(&lock)?;
        Ok(expected)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let source_metadata = source
            .metadata()
            .map_err(|error| storage_io("inspect immutable object source", error))?;
        if !source_metadata.is_file() || source_metadata.len() == 0 {
            return Err(TelemetryError::ObjectStore(
                "immutable object source must be a nonempty regular file".into(),
            ));
        }
        let lock = self.update_lock()?;
        let expected = hash_file(source)?;
        if let Some(observed) = metadata_for_path_if_present(&path)? {
            unlock_file(&lock)?;
            if observed == expected {
                return Ok(observed);
            }
            return Err(TelemetryError::ObjectStore(format!(
                "immutable object key {key} already contains different bytes"
            )));
        }
        let copied = copy_file_atomically(source, &path)?;
        unlock_file(&lock)?;
        if copied != expected {
            return Err(TelemetryError::CorruptTier(
                "object source changed while it was copied".into(),
            ));
        }
        Ok(copied)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        let path = self.object_path(key)?;
        let metadata = path
            .metadata()
            .map_err(|error| object_io(key, "inspect", error))?;
        if metadata.len() > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "object {key} is {} bytes, exceeding read limit {max_bytes}",
                metadata.len()
            )));
        }
        fs::read(path).map_err(|error| object_io(key, "read", error))
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end {
            return Err(TelemetryError::ObjectStore(
                "object byte range starts after its end".into(),
            ));
        }
        let path = self.object_path(key)?;
        let mut file = File::open(path).map_err(|error| object_io(key, "open", error))?;
        let object_bytes = file
            .metadata()
            .map_err(|error| object_io(key, "inspect", error))?
            .len();
        if range.end > object_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "object range {}..{} exceeds {key} length {object_bytes}",
                range.start, range.end
            )));
        }
        let bytes = usize::try_from(range.end - range.start).map_err(|_| {
            TelemetryError::ObjectStore("object byte range cannot fit in memory".into())
        })?;
        file.seek(SeekFrom::Start(range.start))
            .map_err(|error| object_io(key, "seek", error))?;
        let mut output = vec![0; bytes];
        file.read_exact(&mut output)
            .map_err(|error| object_io(key, "read range", error))?;
        Ok(output)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        let path = self.object_path(key)?;
        metadata_for_path_if_present(&path)
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let result = match fs::remove_file(&path) {
            Ok(()) => sync_parent(&path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(object_io(key, "delete", error)),
        };
        let unlock = unlock_file(&lock);
        result.and(unlock)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        let path = self.object_path(key)?;
        let lock = self.update_lock()?;
        let observed = metadata_for_path_if_present(&path)?;
        if observed
            .as_ref()
            .map(|metadata| metadata.version_token.as_str())
            != expected_version
        {
            unlock_file(&lock)?;
            return Err(TelemetryError::StaleCatalog {
                expected: expected_version.map(str::to_owned),
                observed: observed.map(|metadata| metadata.version_token),
            });
        }
        write_bytes_atomically(&path, bytes)?;
        unlock_file(&lock)?;
        Ok(metadata_for_bytes(bytes))
    }
}

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

impl<S: TelemetryObjectStore> TelemetryObjectTier<S> {
    /// Opens the current partition catalog without listing object storage.
    ///
    /// Startup validates only `CURRENT` and its root. Catalog pages, group
    /// manifests, and payloads are checked lazily as queries touch them.
    pub fn open(
        store: S,
        shard_id: ShardId,
        partition: TopicPartition,
        config: ObjectTierConfig,
    ) -> TelemetryResult<Self> {
        config.validate()?;
        let namespace = catalog_namespace(shard_id, partition);
        let current_key = format!("{namespace}/CURRENT");
        let Some(current_metadata) = store.head(&current_key)? else {
            let mut tier = Self {
                store,
                shard_id,
                partition,
                namespace,
                config,
                root: Arc::new(CatalogRoot::empty(shard_id, partition)),
                current_root_key: None,
                current_version: None,
                retired_leases: HashMap::new(),
            };
            tier.recover_pending_transaction()?;
            return Ok(tier);
        };
        if current_metadata.bytes > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let pointer_bytes = store.get(&current_key, POINTER_READ_LIMIT)?;
        verify_bytes_metadata(&pointer_bytes, &current_metadata, "catalog CURRENT")?;
        let pointer: CatalogPointer = decode_json(&pointer_bytes, "catalog CURRENT")?;
        pointer.validate()?;
        let root_bytes = store.get(&pointer.root_key, config.max_control_object_bytes)?;
        verify_expected_object(
            &root_bytes,
            pointer.root_bytes,
            &pointer.root_checksum,
            "catalog root",
        )?;
        let root: CatalogRoot = decode_json(&root_bytes, "catalog root")?;
        root.validate(shard_id, partition)?;
        if root.generation != pointer.generation {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT and root generations disagree".into(),
            ));
        }
        if root.retired_objects.len() > config.max_retired_objects {
            return Err(TelemetryError::CorruptTier(
                "catalog root exceeds the retired-object ownership bound".into(),
            ));
        }
        let current_root_key = pointer.root_key;
        let mut tier = Self {
            store,
            shard_id,
            partition,
            namespace,
            config,
            root: Arc::new(root),
            current_root_key: Some(current_root_key),
            current_version: Some(current_metadata.version_token),
            retired_leases: HashMap::new(),
        };
        tier.recover_pending_transaction()?;
        tier.reclaim_retired_objects()?;
        Ok(tier)
    }

    /// Returns the currently selected immutable root.
    #[must_use]
    pub fn root(&self) -> &CatalogRoot {
        &self.root
    }

    /// Acquires an immutable generation lease for a query or background read.
    #[must_use]
    pub fn catalog_lease(&self) -> CatalogLease {
        CatalogLease {
            root: Arc::clone(&self.root),
        }
    }

    /// Returns the object-store adapter.
    #[must_use]
    pub fn object_store(&self) -> &S {
        &self.store
    }

    /// Returns exact retired keys still waiting for an in-process reader lease.
    #[must_use]
    pub fn pending_retired_objects(&self) -> usize {
        self.root.retired_objects.len()
    }

    pub(crate) fn reclaim_retired_objects(&mut self) -> TelemetryResult<()> {
        if self.root.retired_objects.is_empty() {
            return Ok(());
        }
        let retired = std::mem::take(&mut Arc::make_mut(&mut self.root).retired_objects);
        let mut pending = Vec::new();
        let mut first_error = None;
        let now = unix_time_millis();
        for retired_object in retired {
            let key = &retired_object.object_key;
            let leased = self
                .retired_leases
                .get(key)
                .and_then(Weak::upgrade)
                .is_some();
            if leased || now < retired_object.delete_after_unix_millis {
                pending.push(retired_object);
                continue;
            }
            match self.store.delete(key) {
                Ok(()) => {
                    self.retired_leases.remove(key);
                }
                Err(error) => {
                    pending.push(retired_object);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        Arc::make_mut(&mut self.root).retired_objects = pending;
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn verify_current_pointer(&self) -> TelemetryResult<()> {
        let current_key = format!("{}/CURRENT", self.namespace);
        let observed = self.store.head(&current_key)?;
        let observed_version = observed
            .as_ref()
            .map(|metadata| metadata.version_token.as_str());
        if observed_version != self.current_version.as_deref() {
            return Err(TelemetryError::StaleCatalog {
                expected: self.current_version.clone(),
                observed: observed.map(|metadata| metadata.version_token),
            });
        }
        Ok(())
    }

    fn pending_key(&self) -> String {
        format!("{}/PENDING", self.namespace)
    }

    fn max_transaction_objects(&self) -> usize {
        self.config.max_retired_objects.saturating_add(4)
    }

    fn transaction_id(&self, generation: u64) -> String {
        let sequence = TRANSACTION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let identity = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            std::process::id(),
            timestamp,
            sequence,
            self.shard_id.get(),
            self.partition.topic_id.get(),
            self.partition.partition_id.get(),
            generation
        );
        checksum_bytes(identity.as_bytes())[..32].to_owned()
    }

    fn recover_pending_transaction(&mut self) -> TelemetryResult<()> {
        let pending_key = self.pending_key();
        let Some(metadata) = self.store.head(&pending_key)? else {
            return Ok(());
        };
        if metadata.bytes > self.config.max_control_object_bytes {
            return Err(TelemetryError::CorruptTier(
                "catalog PENDING transaction exceeds its read limit".into(),
            ));
        }
        let bytes = self
            .store
            .get(&pending_key, self.config.max_control_object_bytes)?;
        verify_bytes_metadata(&bytes, &metadata, "catalog PENDING transaction")?;
        let transaction: CatalogTransaction = decode_json(&bytes, "catalog PENDING transaction")?;
        transaction.validate(&self.namespace, self.max_transaction_objects())?;

        let committed = self.current_root_key.as_deref()
            == Some(transaction.target_root_key.as_str())
            && self.root.generation == transaction.target_generation;
        if !committed {
            if unix_time_millis() < transaction.reclaim_after_unix_millis {
                return Err(TelemetryError::ObjectStore(format!(
                    "catalog transaction {} is still owned by an active writer lease",
                    transaction.transaction_id
                )));
            }
            for key in &transaction.owned_objects {
                self.store.delete(key)?;
            }
        }
        self.store.delete(&pending_key)
    }

    fn begin_transaction(&mut self, transaction: &CatalogTransaction) -> TelemetryResult<()> {
        self.recover_pending_transaction()?;
        self.verify_current_pointer()?;
        transaction.validate(&self.namespace, self.max_transaction_objects())?;
        let bytes = encode_json(transaction, "catalog PENDING transaction")?;
        ensure_control_size(
            bytes.len(),
            self.config.max_control_object_bytes,
            "catalog PENDING transaction",
        )?;
        self.store
            .compare_and_swap(&self.pending_key(), None, &bytes)?;
        Ok(())
    }

    fn abort_transaction(
        &self,
        transaction: &CatalogTransaction,
        primary: TelemetryError,
    ) -> TelemetryError {
        let mut cleanup_error = None;
        for key in &transaction.owned_objects {
            if let Err(error) = self.store.delete(key)
                && cleanup_error.is_none()
            {
                cleanup_error = Some(error);
            }
        }
        if cleanup_error.is_none()
            && let Err(error) = self.store.delete(&self.pending_key())
        {
            cleanup_error = Some(error);
        }
        if let Some(cleanup) = cleanup_error {
            TelemetryError::ObjectStore(format!(
                "{primary}; exact-key transaction cleanup will retry from PENDING: {cleanup}"
            ))
        } else {
            primary
        }
    }

    fn complete_transaction(&self) {
        // `CURRENT` already makes the target root authoritative. If this
        // idempotent cleanup fails, startup observes that target and removes
        // only PENDING, preserving every selected object.
        let _ = self.store.delete(&self.pending_key());
    }

    /// Publishes a complete immutable group and conditionally advances `CURRENT`.
    ///
    /// A fixed `PENDING` ownership record is selected before any immutable
    /// object is created. A crash or stale writer can therefore relinquish
    /// every exact transaction key without object listing or tracing GC.
    pub fn publish_group(&mut self, source: TierGroupSource) -> TelemetryResult<TierGroupManifest> {
        self.reclaim_retired_objects()?;
        self.recover_pending_transaction()?;
        self.verify_current_pointer()?;
        validate_source(&source)?;
        let next_generation =
            self.root.generation.checked_add(1).ok_or_else(|| {
                TelemetryError::ObjectStore("catalog generation exhausted".into())
            })?;
        let transaction_id = self.transaction_id(next_generation);
        let mut artifacts = Vec::with_capacity(source.artifacts.len());
        let mut artifact_objects = Vec::with_capacity(source.artifacts.len());
        for artifact_source in &source.artifacts {
            let source_metadata = hash_file(&artifact_source.path)?;
            let object_key = format!(
                "{}/transactions/{}/groups/{:020}/{}-{}-{}",
                self.namespace,
                transaction_id,
                source.group_sequence,
                artifact_source.kind.key_name(),
                artifact_source.name,
                source_metadata.content_digest
            );
            artifacts.push(TierArtifact {
                kind: artifact_source.kind,
                name: artifact_source.name.clone(),
                object_key: object_key.clone(),
                bytes: source_metadata.bytes,
                checksum_algorithm: CHECKSUM_ALGORITHM.into(),
                checksum: source_metadata.content_digest.clone(),
            });
            artifact_objects.push((object_key, artifact_source.path.clone(), source_metadata));
        }
        let manifest = TierGroupManifest {
            format_version: TIER_FORMAT_VERSION,
            group_sequence: source.group_sequence,
            checkpoint: source.checkpoint,
            shard_id: self.shard_id.get(),
            topic_id: self.partition.topic_id.get().to_string(),
            partition_id: self.partition.partition_id.get(),
            blocks: source.blocks.clone(),
            artifacts,
        };
        manifest.validate(
            self.shard_id,
            self.partition,
            self.config.max_blocks_per_group,
            self.config.max_group_payload_bytes,
        )?;
        let manifest_bytes = encode_json(&manifest, "group manifest")?;
        ensure_control_size(
            manifest_bytes.len(),
            self.config.max_control_object_bytes,
            "group manifest",
        )?;
        let manifest_checksum = checksum_bytes(&manifest_bytes);
        let manifest_key = format!(
            "{}/transactions/{}/groups/{:020}/manifest-{}.json",
            self.namespace, transaction_id, manifest.group_sequence, manifest_checksum
        );
        let manifest_metadata = ObjectMetadata {
            bytes: u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: manifest_checksum,
        };
        let entry = CatalogGroupEntry::from_manifest(&manifest, manifest_key, &manifest_metadata)?;

        if let Some(last_page_ref) = self.root.pages.last() {
            let last_page = self.load_page(last_page_ref)?;
            let last_group = last_page
                .groups
                .last()
                .expect("validated catalog pages are nonempty");
            if source.group_sequence == last_group.group_sequence {
                let existing = self.load_group(last_group)?;
                if !same_group_contents(&existing, &manifest) {
                    return Err(TelemetryError::CorruptTier(
                        "group sequence was retried with different contents".into(),
                    ));
                }
                return Ok(existing);
            }
            if source.group_sequence < last_group.group_sequence {
                return Err(TelemetryError::ObjectStore(
                    "group sequences must be published in increasing order".into(),
                ));
            }
            if !source.checkpoint.covers(last_group.checkpoint) {
                return Err(TelemetryError::ObjectStore(
                    "group checkpoints must advance monotonically".into(),
                ));
            }
        }

        let (page, replace_last) = match self.root.pages.last() {
            Some(last_ref) => {
                let mut last = self.load_page(last_ref)?;
                if last.groups.len() < self.config.groups_per_page {
                    last.groups.push(entry.clone());
                    (last, true)
                } else {
                    (
                        CatalogPage {
                            format_version: TIER_FORMAT_VERSION,
                            page_sequence: last.page_sequence.checked_add(1).ok_or_else(|| {
                                TelemetryError::ObjectStore(
                                    "catalog page sequence exhausted".into(),
                                )
                            })?,
                            shard_id: self.shard_id.get(),
                            topic_id: self.partition.topic_id.get().to_string(),
                            partition_id: self.partition.partition_id.get(),
                            groups: vec![entry.clone()],
                        },
                        false,
                    )
                }
            }
            None => (
                CatalogPage {
                    format_version: TIER_FORMAT_VERSION,
                    page_sequence: 0,
                    shard_id: self.shard_id.get(),
                    topic_id: self.partition.topic_id.get().to_string(),
                    partition_id: self.partition.partition_id.get(),
                    groups: vec![entry.clone()],
                },
                false,
            ),
        };
        page.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
        let page_bytes = encode_json(&page, "catalog page")?;
        ensure_control_size(
            page_bytes.len(),
            self.config.max_control_object_bytes,
            "catalog page",
        )?;
        let page_checksum = checksum_bytes(&page_bytes);
        let page_key = format!(
            "{}/transactions/{}/pages/page-{:020}-{}.json",
            self.namespace, transaction_id, page.page_sequence, page_checksum
        );
        let page_metadata = ObjectMetadata {
            bytes: u64::try_from(page_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: page_checksum,
        };
        let page_ref = CatalogPageRef::from_page(&page, page_key, &page_metadata)?;

        let mut next_root = (*self.root).clone();
        next_root.generation = next_generation;
        next_root.latest_checkpoint = Some(source.checkpoint);
        let first_block_id = manifest
            .blocks
            .first()
            .expect("validated group has blocks")
            .block_id;
        if first_block_id < self.root.next_block_id {
            return Err(TelemetryError::ObjectStore(
                "group block identifiers overlap an older group".into(),
            ));
        }
        next_root.next_block_id = manifest
            .blocks
            .last()
            .expect("validated group has blocks")
            .block_id
            .checked_add(1)
            .ok_or_else(|| TelemetryError::ObjectStore("block identifier exhausted".into()))?;
        if replace_last {
            *next_root
                .pages
                .last_mut()
                .expect("a replaced page has an existing reference") = page_ref.clone();
        } else {
            next_root.pages.push(page_ref.clone());
        }
        let mut newly_retired = Vec::with_capacity(2);
        let delete_after_unix_millis = unix_time_millis().saturating_add(
            u64::try_from(self.config.retirement_grace.as_millis()).unwrap_or(u64::MAX),
        );
        if let Some(root_key) = &self.current_root_key {
            newly_retired.push(RetiredObject {
                object_key: root_key.clone(),
                delete_after_unix_millis,
            });
        }
        if replace_last {
            newly_retired.push(RetiredObject {
                object_key: self
                    .root
                    .pages
                    .last()
                    .expect("a replaced page has an existing reference")
                    .page_key
                    .clone(),
                delete_after_unix_millis,
            });
        }
        next_root
            .retired_objects
            .extend(newly_retired.iter().cloned());
        next_root
            .retired_objects
            .sort_unstable_by(|left, right| left.object_key.cmp(&right.object_key));
        next_root
            .retired_objects
            .dedup_by(|left, right| left.object_key == right.object_key);
        if next_root.retired_objects.len() > self.config.max_retired_objects {
            return Err(TelemetryError::ObjectStore(
                "catalog generation exhausted its exact retired-object ownership bound".into(),
            ));
        }
        next_root.validate(self.shard_id, self.partition)?;
        let root_bytes = encode_json(&next_root, "catalog root")?;
        ensure_control_size(
            root_bytes.len(),
            self.config.max_control_object_bytes,
            "catalog root",
        )?;
        let root_checksum = checksum_bytes(&root_bytes);
        let root_key = format!(
            "{}/transactions/{}/roots/root-{:020}-{}.json",
            self.namespace, transaction_id, next_root.generation, root_checksum
        );
        let root_metadata = ObjectMetadata {
            bytes: u64::try_from(root_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: root_checksum,
        };
        let pointer = CatalogPointer {
            format_version: TIER_FORMAT_VERSION,
            generation: next_root.generation,
            root_key: root_key.clone(),
            root_bytes: root_metadata.bytes,
            root_checksum: root_metadata.content_digest.clone(),
        };
        let pointer_bytes = encode_json(&pointer, "catalog CURRENT")?;
        if u64::try_from(pointer_bytes.len()).unwrap_or(u64::MAX) > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let mut owned_objects = artifact_objects
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<Vec<_>>();
        owned_objects.extend([
            entry.manifest_key.clone(),
            page_ref.page_key.clone(),
            root_key,
        ]);
        let transaction = CatalogTransaction {
            format_version: TIER_FORMAT_VERSION,
            transaction_id,
            target_generation: next_generation,
            target_root_key: pointer.root_key.clone(),
            reclaim_after_unix_millis: unix_time_millis().saturating_add(
                u64::try_from(self.config.transaction_lease.as_millis()).unwrap_or(u64::MAX),
            ),
            owned_objects,
        };
        self.begin_transaction(&transaction)?;

        let publication = (|| -> TelemetryResult<ObjectMetadata> {
            for (key, path, expected) in &artifact_objects {
                let stored = self.store.put_file_if_absent(key, path)?;
                verify_object_metadata(&stored, expected, "group artifact")?;
            }
            let stored_manifest = self
                .store
                .put_bytes_if_absent(&entry.manifest_key, &manifest_bytes)?;
            verify_object_metadata(&stored_manifest, &manifest_metadata, "group manifest")?;
            let stored_page = self
                .store
                .put_bytes_if_absent(&page_ref.page_key, &page_bytes)?;
            verify_object_metadata(&stored_page, &page_metadata, "catalog page")?;
            let stored_root = self
                .store
                .put_bytes_if_absent(&pointer.root_key, &root_bytes)?;
            verify_object_metadata(&stored_root, &root_metadata, "catalog root")?;

            self.store.compare_and_swap(
                &format!("{}/CURRENT", self.namespace),
                self.current_version.as_deref(),
                &pointer_bytes,
            )
        })();
        let current_metadata = match publication {
            Ok(metadata) => metadata,
            Err(error) => return Err(self.abort_transaction(&transaction, error)),
        };
        let retired_root = Arc::clone(&self.root);
        self.root = Arc::new(next_root);
        self.current_root_key = Some(pointer.root_key);
        self.current_version = Some(current_metadata.version_token);
        let retired_lease = Arc::downgrade(&retired_root);
        for retired in newly_retired {
            self.retired_leases
                .insert(retired.object_key, Weak::clone(&retired_lease));
        }
        drop(retired_root);
        self.complete_transaction();
        self.reclaim_retired_objects()?;
        Ok(manifest)
    }

    /// Removes complete groups older than `cutoff_timestamp_unix_nanos`.
    ///
    /// The transaction is bounded by `max_retired_objects`, rewrites only pages
    /// that actually lose groups, and records every relinquished key in the new
    /// root before `CURRENT` advances. The newest group remains as the durable
    /// recovery checkpoint anchor. No object listing or reachability scan occurs.
    pub fn retain_since_timestamp(
        &mut self,
        cutoff_timestamp_unix_nanos: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        self.reclaim_retired_objects()?;
        self.recover_pending_transaction()?;
        self.verify_current_pointer()?;
        let Some(final_group_sequence) =
            self.root.pages.last().map(|page| page.last_group_sequence)
        else {
            return Ok(TierRetentionReport::default());
        };
        let available_retirements = self
            .config
            .max_retired_objects
            .saturating_sub(self.root.retired_objects.len())
            .saturating_sub(1);
        if available_retirements < 3 {
            return Ok(TierRetentionReport::default());
        }
        let next_generation =
            self.root.generation.checked_add(1).ok_or_else(|| {
                TelemetryError::ObjectStore("catalog generation exhausted".into())
            })?;
        let transaction_id = self.transaction_id(next_generation);

        let mut next_pages = Vec::with_capacity(self.root.pages.len());
        let mut replacement_objects = Vec::new();
        let mut retired_keys = Vec::new();
        let mut report = TierRetentionReport::default();
        for page_ref in &self.root.pages {
            let page = self.load_page(page_ref)?;
            let mut retained_groups = Vec::with_capacity(page.groups.len());
            let mut page_changed = false;
            for group in page.groups {
                if group.group_sequence == final_group_sequence
                    || group.max_timestamp_unix_nanos >= cutoff_timestamp_unix_nanos
                {
                    retained_groups.push(group);
                    continue;
                }
                let manifest = self.load_group(&group)?;
                let required = 1usize.saturating_add(manifest.artifacts.len());
                let page_key_cost = usize::from(!page_changed);
                if retired_keys
                    .len()
                    .saturating_add(required)
                    .saturating_add(page_key_cost)
                    > available_retirements
                {
                    retained_groups.push(group);
                    continue;
                }
                if !page_changed {
                    retired_keys.push(page_ref.page_key.clone());
                    page_changed = true;
                }
                retired_keys.push(group.manifest_key.clone());
                retired_keys.extend(
                    manifest
                        .artifacts
                        .iter()
                        .map(|artifact| artifact.object_key.clone()),
                );
                report.retired_groups = report.retired_groups.saturating_add(1);
                report.retired_payload_bytes = report
                    .retired_payload_bytes
                    .saturating_add(group.payload_bytes);
            }
            if !page_changed {
                next_pages.push(page_ref.clone());
                continue;
            }
            if retained_groups.is_empty() {
                continue;
            }
            let replacement = CatalogPage {
                format_version: TIER_FORMAT_VERSION,
                page_sequence: page.page_sequence,
                shard_id: page.shard_id,
                topic_id: page.topic_id,
                partition_id: page.partition_id,
                groups: retained_groups,
            };
            replacement.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
            let bytes = encode_json(&replacement, "retained catalog page")?;
            ensure_control_size(
                bytes.len(),
                self.config.max_control_object_bytes,
                "retained catalog page",
            )?;
            let checksum = checksum_bytes(&bytes);
            let key = format!(
                "{}/transactions/{}/pages/page-{:020}-{}.json",
                self.namespace, transaction_id, replacement.page_sequence, checksum
            );
            let metadata = ObjectMetadata {
                bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                version_token: String::new(),
                content_digest: checksum,
            };
            next_pages.push(CatalogPageRef::from_page(
                &replacement,
                key.clone(),
                &metadata,
            )?);
            replacement_objects.push((key, bytes, metadata));
        }
        if report.retired_groups == 0 {
            return Ok(report);
        }

        let current_root_key = self.current_root_key.clone().ok_or_else(|| {
            TelemetryError::CorruptTier("nonempty catalog has no selected root key".into())
        })?;
        retired_keys.push(current_root_key);
        retired_keys.sort_unstable();
        retired_keys.dedup();
        let delete_after_unix_millis = unix_time_millis().saturating_add(
            u64::try_from(self.config.retirement_grace.as_millis()).unwrap_or(u64::MAX),
        );
        let newly_retired = retired_keys
            .into_iter()
            .map(|object_key| RetiredObject {
                object_key,
                delete_after_unix_millis,
            })
            .collect::<Vec<_>>();
        report.retired_objects = u64::try_from(newly_retired.len()).unwrap_or(u64::MAX);

        let mut next_root = (*self.root).clone();
        next_root.generation = next_generation;
        next_root.pages = next_pages;
        next_root
            .retired_objects
            .extend(newly_retired.iter().cloned());
        next_root
            .retired_objects
            .sort_unstable_by(|left, right| left.object_key.cmp(&right.object_key));
        next_root
            .retired_objects
            .dedup_by(|left, right| left.object_key == right.object_key);
        if next_root.retired_objects.len() > self.config.max_retired_objects {
            return Err(TelemetryError::ObjectStore(
                "retention exhausted the exact retired-object ownership bound".into(),
            ));
        }
        next_root.validate(self.shard_id, self.partition)?;
        let root_bytes = encode_json(&next_root, "retained catalog root")?;
        ensure_control_size(
            root_bytes.len(),
            self.config.max_control_object_bytes,
            "retained catalog root",
        )?;
        let root_checksum = checksum_bytes(&root_bytes);
        let root_key = format!(
            "{}/transactions/{}/roots/root-{:020}-{}.json",
            self.namespace, transaction_id, next_root.generation, root_checksum
        );
        let root_metadata = ObjectMetadata {
            bytes: u64::try_from(root_bytes.len()).unwrap_or(u64::MAX),
            version_token: String::new(),
            content_digest: root_checksum,
        };
        let pointer = CatalogPointer {
            format_version: TIER_FORMAT_VERSION,
            generation: next_root.generation,
            root_key: root_key.clone(),
            root_bytes: root_metadata.bytes,
            root_checksum: root_metadata.content_digest.clone(),
        };
        let pointer_bytes = encode_json(&pointer, "catalog CURRENT")?;
        if u64::try_from(pointer_bytes.len()).unwrap_or(u64::MAX) > POINTER_READ_LIMIT {
            return Err(TelemetryError::CorruptTier(
                "catalog CURRENT pointer exceeds its read limit".into(),
            ));
        }
        let mut owned_objects = replacement_objects
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<Vec<_>>();
        owned_objects.push(root_key);
        let transaction = CatalogTransaction {
            format_version: TIER_FORMAT_VERSION,
            transaction_id,
            target_generation: next_generation,
            target_root_key: pointer.root_key.clone(),
            reclaim_after_unix_millis: unix_time_millis().saturating_add(
                u64::try_from(self.config.transaction_lease.as_millis()).unwrap_or(u64::MAX),
            ),
            owned_objects,
        };
        self.begin_transaction(&transaction)?;
        let publication = (|| -> TelemetryResult<ObjectMetadata> {
            for (key, bytes, expected) in &replacement_objects {
                let stored = self.store.put_bytes_if_absent(key, bytes)?;
                verify_object_metadata(&stored, expected, "retained catalog page")?;
            }
            let stored_root = self
                .store
                .put_bytes_if_absent(&pointer.root_key, &root_bytes)?;
            verify_object_metadata(&stored_root, &root_metadata, "retained catalog root")?;
            self.store.compare_and_swap(
                &format!("{}/CURRENT", self.namespace),
                self.current_version.as_deref(),
                &pointer_bytes,
            )
        })();
        let current_metadata = match publication {
            Ok(metadata) => metadata,
            Err(error) => return Err(self.abort_transaction(&transaction, error)),
        };
        let retired_root = Arc::clone(&self.root);
        self.root = Arc::new(next_root);
        self.current_root_key = Some(pointer.root_key);
        self.current_version = Some(current_metadata.version_token);
        let retired_lease = Arc::downgrade(&retired_root);
        for retired in newly_retired {
            self.retired_leases
                .insert(retired.object_key, Weak::clone(&retired_lease));
        }
        drop(retired_root);
        self.complete_transaction();
        self.reclaim_retired_objects()?;
        Ok(report)
    }

    /// Removes the oldest complete groups until selected compressed payloads
    /// fit `max_payload_bytes`.
    ///
    /// The newest group remains as the recovery anchor even when it alone is
    /// larger than the configured budget. Callers should therefore configure
    /// a budget at least as large as one maximum group payload.
    pub fn retain_to_payload_bytes(
        &mut self,
        max_payload_bytes: u64,
    ) -> TelemetryResult<TierRetentionReport> {
        if max_payload_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "object-tier payload budget must be nonzero",
            ));
        }
        let Some(final_group_sequence) =
            self.root.pages.last().map(|page| page.last_group_sequence)
        else {
            return Ok(TierRetentionReport::default());
        };
        let mut total = 0_u64;
        let mut candidates = Vec::new();
        for page_ref in &self.root.pages {
            let page = self.load_page(page_ref)?;
            for group in &page.groups {
                total = total.saturating_add(group.payload_bytes);
                if group.group_sequence != final_group_sequence {
                    candidates.push((
                        group.max_timestamp_unix_nanos,
                        group.group_sequence,
                        group.payload_bytes,
                    ));
                }
            }
        }
        if total <= max_payload_bytes {
            return Ok(TierRetentionReport::default());
        }
        candidates.sort_unstable();
        let mut cutoff = None;
        for (timestamp, _, bytes) in candidates {
            if total <= max_payload_bytes {
                break;
            }
            total = total.saturating_sub(bytes);
            cutoff = Some(timestamp.saturating_add(1));
        }
        cutoff.map_or(Ok(TierRetentionReport::default()), |cutoff| {
            self.retain_since_timestamp(cutoff)
        })
    }

    /// Returns group entries whose coarse bounds overlap the query.
    ///
    /// Only overlapping catalog pages are loaded. This never lists objects.
    pub fn candidate_groups(
        &self,
        range: TierQueryRange,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        let mut groups = Vec::new();
        self.for_each_candidate_group(range, |group| {
            groups.push(group.clone());
            Ok(true)
        })?;
        Ok(groups)
    }

    /// Returns overlapping groups while serving immutable catalog pages from
    /// the integrity-checked SSD cache when they fit its read bound.
    pub fn candidate_groups_cached(
        &self,
        range: TierQueryRange,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        let mut groups = Vec::new();
        self.for_each_candidate_group_with(
            range,
            |reference| self.load_page_cached(reference, cache),
            |group| {
                groups.push(group.clone());
                Ok(true)
            },
        )?;
        Ok(groups)
    }

    /// Returns correlation candidates after pruning immutable catalog pages
    /// and groups, before any group manifest or payload is loaded.
    pub fn candidate_groups_cached_for_correlation(
        &self,
        range: TierQueryRange,
        cache: &SsdObjectCache,
        query: &CorrelationQuery,
        signal: TelemetrySignal,
    ) -> TelemetryResult<Vec<CatalogGroupEntry>> {
        range.validate()?;
        let mut groups = Vec::new();
        for page_ref in &self.root.pages {
            if !range.overlaps(
                page_ref.first_offset,
                page_ref.last_offset,
                page_ref.min_timestamp_unix_nanos,
                page_ref.max_timestamp_unix_nanos,
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
            ) || !catalog_correlation_may_match(
                page_ref.correlation_filter.as_ref(),
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
                query,
                signal,
            ) {
                continue;
            }
            let page = self.load_page_cached(page_ref, cache)?;
            for group in &page.groups {
                if range.overlaps(
                    group.first_offset,
                    group.last_offset,
                    group.min_timestamp_unix_nanos,
                    group.max_timestamp_unix_nanos,
                    group.min_signal_identity,
                    group.max_signal_identity,
                ) && catalog_correlation_may_match(
                    group.correlation_filter.as_ref(),
                    group.min_signal_identity,
                    group.max_signal_identity,
                    query,
                    signal,
                ) {
                    groups.push(group.clone());
                }
            }
        }
        Ok(groups)
    }

    /// Loads only the final catalog page and returns its newest group entry.
    pub fn latest_group(&self) -> TelemetryResult<Option<CatalogGroupEntry>> {
        let Some(reference) = self.root.pages.last() else {
            return Ok(None);
        };
        Ok(self.load_page(reference)?.groups.last().cloned())
    }

    /// Loads the newest group through the bounded catalog-page cache.
    pub fn latest_group_cached(
        &self,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Option<CatalogGroupEntry>> {
        let Some(reference) = self.root.pages.last() else {
            return Ok(None);
        };
        Ok(self
            .load_page_cached(reference, cache)?
            .groups
            .last()
            .cloned())
    }

    /// Visits overlapping groups one at a time without materializing a
    /// corpus-sized candidate vector.
    ///
    /// Returning `false` from `visit` stops traversal successfully. Catalog
    /// pages are loaded and verified lazily, and object storage is never
    /// listed.
    pub fn for_each_candidate_group(
        &self,
        range: TierQueryRange,
        visit: impl FnMut(&CatalogGroupEntry) -> TelemetryResult<bool>,
    ) -> TelemetryResult<()> {
        self.for_each_candidate_group_with(
            range,
            |reference| self.load_page(reference).map(Arc::new),
            visit,
        )
    }

    fn for_each_candidate_group_with(
        &self,
        range: TierQueryRange,
        mut load_page: impl FnMut(&CatalogPageRef) -> TelemetryResult<Arc<CatalogPage>>,
        mut visit: impl FnMut(&CatalogGroupEntry) -> TelemetryResult<bool>,
    ) -> TelemetryResult<()> {
        range.validate()?;
        for page_ref in &self.root.pages {
            if !range.overlaps(
                page_ref.first_offset,
                page_ref.last_offset,
                page_ref.min_timestamp_unix_nanos,
                page_ref.max_timestamp_unix_nanos,
                page_ref.min_signal_identity,
                page_ref.max_signal_identity,
            ) {
                continue;
            }
            let page = load_page(page_ref)?;
            for group in &page.groups {
                if range.overlaps(
                    group.first_offset,
                    group.last_offset,
                    group.min_timestamp_unix_nanos,
                    group.max_timestamp_unix_nanos,
                    group.min_signal_identity,
                    group.max_signal_identity,
                ) && !visit(group)?
                {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Loads and validates one group manifest selected from a catalog page.
    pub fn load_group(&self, entry: &CatalogGroupEntry) -> TelemetryResult<TierGroupManifest> {
        entry.validate()?;
        let bytes = self
            .store
            .get(&entry.manifest_key, self.config.max_control_object_bytes)?;
        self.decode_group(entry, &bytes)
    }

    /// Loads and validates a group manifest through the immutable SSD cache.
    pub fn load_group_cached(
        &self,
        entry: &CatalogGroupEntry,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Arc<TierGroupManifest>> {
        entry.validate()?;
        if entry.manifest_bytes > cache.max_read_bytes() {
            return self.load_group(entry).map(Arc::new);
        }
        if let Some(manifest) = cache.parsed_manifest_hit(&entry.manifest_key)? {
            return Ok(manifest);
        }
        let metadata = ObjectMetadata {
            bytes: entry.manifest_bytes,
            version_token: entry.manifest_checksum.clone(),
            content_digest: entry.manifest_checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &entry.manifest_key,
            &metadata,
            0..entry.manifest_bytes,
        )?;
        let manifest = Arc::new(self.decode_group(entry, &bytes)?);
        cache.admit_parsed_control(
            entry.manifest_key.clone(),
            ParsedControlObject::GroupManifest(Arc::clone(&manifest)),
            entry.manifest_bytes,
        )?;
        Ok(manifest)
    }

    /// Reads and verifies a complete immutable artifact on demand.
    pub fn read_artifact(
        &self,
        artifact: &TierArtifact,
        max_bytes: u64,
    ) -> TelemetryResult<Vec<u8>> {
        validate_artifact(artifact)?;
        if artifact.bytes > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "artifact {} exceeds read limit {max_bytes}",
                artifact.name
            )));
        }
        let bytes = self.store.get(&artifact.object_key, max_bytes)?;
        verify_expected_object(&bytes, artifact.bytes, &artifact.checksum, "group artifact")?;
        Ok(bytes)
    }

    /// Reads a complete immutable artifact through the bounded SSD cache.
    pub fn read_artifact_cached(
        &self,
        artifact: &TierArtifact,
        max_bytes: u64,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Vec<u8>> {
        validate_artifact(artifact)?;
        if artifact.bytes > max_bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "artifact {} exceeds read limit {max_bytes}",
                artifact.name
            )));
        }
        if artifact.bytes > cache.max_read_bytes() {
            return self.read_artifact(artifact, max_bytes);
        }
        let metadata = ObjectMetadata {
            bytes: artifact.bytes,
            version_token: artifact.checksum.clone(),
            content_digest: artifact.checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &artifact.object_key,
            &metadata,
            0..artifact.bytes,
        )?;
        verify_expected_object(&bytes, artifact.bytes, &artifact.checksum, "group artifact")?;
        Ok(bytes)
    }

    fn load_page(&self, reference: &CatalogPageRef) -> TelemetryResult<CatalogPage> {
        reference.validate()?;
        let bytes = self
            .store
            .get(&reference.page_key, self.config.max_control_object_bytes)?;
        self.decode_page(reference, &bytes)
    }

    fn load_page_cached(
        &self,
        reference: &CatalogPageRef,
        cache: &SsdObjectCache,
    ) -> TelemetryResult<Arc<CatalogPage>> {
        reference.validate()?;
        if reference.page_bytes > cache.max_read_bytes() {
            return self.load_page(reference).map(Arc::new);
        }
        if let Some(page) = cache.parsed_page_hit(&reference.page_key)? {
            return Ok(page);
        }
        let metadata = ObjectMetadata {
            bytes: reference.page_bytes,
            version_token: reference.page_checksum.clone(),
            content_digest: reference.page_checksum.clone(),
        };
        let bytes = cache.read_range_with_metadata(
            &self.store,
            &reference.page_key,
            &metadata,
            0..reference.page_bytes,
        )?;
        let page = Arc::new(self.decode_page(reference, &bytes)?);
        cache.admit_parsed_control(
            reference.page_key.clone(),
            ParsedControlObject::CatalogPage(Arc::clone(&page)),
            reference.page_bytes,
        )?;
        Ok(page)
    }

    fn decode_page(
        &self,
        reference: &CatalogPageRef,
        bytes: &[u8],
    ) -> TelemetryResult<CatalogPage> {
        verify_expected_object(
            bytes,
            reference.page_bytes,
            &reference.page_checksum,
            "catalog page",
        )?;
        let page: CatalogPage = decode_json(bytes, "catalog page")?;
        page.validate(self.shard_id, self.partition, self.config.groups_per_page)?;
        if page.page_sequence != reference.page_sequence {
            return Err(TelemetryError::CorruptTier(
                "catalog root and page sequences disagree".into(),
            ));
        }
        Ok(page)
    }

    fn decode_group(
        &self,
        entry: &CatalogGroupEntry,
        bytes: &[u8],
    ) -> TelemetryResult<TierGroupManifest> {
        verify_expected_object(
            bytes,
            entry.manifest_bytes,
            &entry.manifest_checksum,
            "group manifest",
        )?;
        let manifest: TierGroupManifest = decode_json(bytes, "group manifest")?;
        manifest.validate(
            self.shard_id,
            self.partition,
            self.config.max_blocks_per_group,
            self.config.max_group_payload_bytes,
        )?;
        if manifest.group_sequence != entry.group_sequence {
            return Err(TelemetryError::CorruptTier(
                "catalog group and manifest sequences disagree".into(),
            ));
        }
        Ok(manifest)
    }
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

impl SsdObjectCache {
    /// Opens an SSD cache and reconstructs its bounded local directory.
    pub fn open(root: impl AsRef<Path>, config: SsdCacheConfig) -> TelemetryResult<Self> {
        config.validate()?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(|error| storage_io("create SSD cache", error))?;
        let mut state = CacheState::default();
        let entries = fs::read_dir(&root).map_err(|error| storage_io("scan SSD cache", error))?;
        for entry in entries {
            let entry = entry.map_err(|error| storage_io("read SSD cache entry", error))?;
            let path = entry.path();
            let Some(name) = path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.ends_with(".chunk") && name.len() == 70)
            else {
                continue;
            };
            let metadata = entry
                .metadata()
                .map_err(|error| storage_io("inspect SSD cache entry", error))?;
            if !metadata.is_file() {
                continue;
            }
            let bytes = metadata.len();
            state.used_bytes = state.used_bytes.saturating_add(bytes);
            state.entries.insert(
                name[..64].to_owned(),
                CacheEntry {
                    path,
                    bytes,
                    stamp: 0,
                },
            );
        }
        let cache = Self {
            root,
            config,
            state: Mutex::new(state),
        };
        cache.evict_to_budget()?;
        Ok(cache)
    }

    /// Returns currently occupied cache bytes.
    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.state.lock().map(|state| state.used_bytes).unwrap_or(0)
    }

    /// Returns bounded cache occupancy and read-amplification counters.
    #[must_use]
    pub fn stats(&self) -> SsdCacheStats {
        self.state.lock().map_or_else(
            |_| SsdCacheStats::default(),
            |state| SsdCacheStats {
                entries: state.entries.len(),
                used_bytes: state.used_bytes,
                hits: state.hits,
                memory_hits: state.memory_hits,
                parsed_hits: state.parsed_hits,
                memory_entries: state
                    .memory_entries
                    .len()
                    .saturating_add(state.parsed_entries.len()),
                memory_used_bytes: state
                    .memory_used_bytes
                    .saturating_add(state.parsed_used_bytes),
                parsed_entries: state.parsed_entries.len(),
                parsed_used_bytes: state.parsed_used_bytes,
                misses: state.misses,
                source_bytes: state.source_bytes,
            },
        )
    }

    /// Returns the largest object extent accepted by one cache operation.
    #[must_use]
    pub const fn max_read_bytes(&self) -> u64 {
        self.config.max_read_bytes
    }

    /// Admits a newly published immutable artifact directly from its local
    /// staging file.
    ///
    /// S3-backed embedded deployments use this write-through path so the most
    /// recently published payloads and indexes remain locally queryable without
    /// first downloading them. The ordinary LRU budget expels older chunks.
    pub fn admit_file(&self, artifact: &TierArtifact, source: &Path) -> TelemetryResult<()> {
        let source_metadata = hash_file(source)?;
        if source_metadata.bytes != artifact.bytes
            || source_metadata.content_digest != artifact.checksum
        {
            return Err(TelemetryError::CorruptTier(
                "published artifact source changed before SSD-cache admission".into(),
            ));
        }
        let mut file = File::open(source)
            .map_err(|error| storage_io("open SSD-cache admission source", error))?;
        let mut chunk_index = 0_u64;
        let mut remaining = artifact.bytes;
        while remaining > 0 {
            let chunk_bytes = remaining.min(self.config.chunk_bytes);
            let chunk_len = usize::try_from(chunk_bytes).map_err(|_| {
                TelemetryError::StorageIo("SSD-cache admission chunk cannot fit in memory".into())
            })?;
            let mut bytes = vec![0; chunk_len];
            file.read_exact(&mut bytes)
                .map_err(|error| storage_io("read SSD-cache admission source", error))?;
            let cache_key = checksum_bytes(
                format!(
                    "{}\0{}\0{chunk_index}",
                    artifact.object_key, artifact.checksum
                )
                .as_bytes(),
            );
            self.install_chunk(&cache_key, &bytes)?;
            remaining -= chunk_bytes;
            chunk_index = chunk_index.saturating_add(1);
        }
        Ok(())
    }

    /// Reads an object range, filling and reusing fixed immutable SSD chunks.
    pub fn read_range<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end || range.end - range.start > self.config.max_read_bytes {
            return Err(TelemetryError::ObjectStore(
                "SSD cache read range is invalid or exceeds its limit".into(),
            ));
        }
        let metadata = store.head(object_key)?.ok_or_else(|| {
            TelemetryError::ObjectStore(format!("object {object_key} does not exist"))
        })?;
        self.read_range_with_metadata(store, object_key, &metadata, range)
    }

    /// Reads an object range using immutable metadata already held in a
    /// manifest, avoiding a remote HEAD request on the query path.
    pub fn read_range_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        if range.start > range.end || range.end - range.start > self.config.max_read_bytes {
            return Err(TelemetryError::ObjectStore(
                "SSD cache read range is invalid or exceeds its limit".into(),
            ));
        }
        if range.end > metadata.bytes {
            return Err(TelemetryError::ObjectStore(format!(
                "SSD cache range exceeds object {object_key}"
            )));
        }
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
            TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
        })?;
        let mut output = Vec::with_capacity(output_bytes);
        let first_chunk = range.start / self.config.chunk_bytes;
        let last_chunk = (range.end - 1) / self.config.chunk_bytes;
        for chunk_index in first_chunk..=last_chunk {
            let chunk_start = chunk_index
                .checked_mul(self.config.chunk_bytes)
                .ok_or_else(|| TelemetryError::ObjectStore("cache chunk offset overflow".into()))?;
            let chunk_end = chunk_start
                .saturating_add(self.config.chunk_bytes)
                .min(metadata.bytes);
            let chunk = self.load_or_fetch_chunk(
                store,
                object_key,
                metadata,
                chunk_index,
                chunk_start..chunk_end,
            )?;
            let copy_start = range.start.max(chunk_start) - chunk_start;
            let copy_end = range.end.min(chunk_end) - chunk_start;
            let copy_start = usize::try_from(copy_start)
                .map_err(|_| TelemetryError::ObjectStore("cache slice offset overflow".into()))?;
            let copy_end = usize::try_from(copy_end)
                .map_err(|_| TelemetryError::ObjectStore("cache slice offset overflow".into()))?;
            output.extend_from_slice(&chunk[copy_start..copy_end]);
        }
        Ok(output)
    }

    /// Reads sorted, non-overlapping immutable ranges while retaining the
    /// most recently loaded cache chunk for the complete batch.
    ///
    /// Payload packs place block and frame extents in ascending order. A
    /// batched query therefore reads or fetches each shared SSD chunk once
    /// instead of reopening that chunk for every selected extent.
    pub fn read_ranges_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        ranges: &[Range<u64>],
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        if ranges.windows(2).any(|pair| pair[0].end > pair[1].start) {
            return Err(TelemetryError::ObjectStore(
                "batched SSD cache ranges must be sorted and non-overlapping".into(),
            ));
        }
        let mut outputs = Vec::with_capacity(ranges.len());
        let mut loaded = None::<(u64, u64, Vec<u8>)>;
        for range in ranges {
            if range.start > range.end
                || range.end > metadata.bytes
                || range.end - range.start > self.config.max_read_bytes
            {
                return Err(TelemetryError::ObjectStore(
                    "batched SSD cache range is invalid or exceeds its limit".into(),
                ));
            }
            let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
                TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
            })?;
            let mut output = Vec::with_capacity(output_bytes);
            if !range.is_empty() {
                let first_chunk = range.start / self.config.chunk_bytes;
                let last_chunk = (range.end - 1) / self.config.chunk_bytes;
                for chunk_index in first_chunk..=last_chunk {
                    let chunk_start = chunk_index
                        .checked_mul(self.config.chunk_bytes)
                        .ok_or_else(|| {
                            TelemetryError::ObjectStore("cache chunk offset overflow".into())
                        })?;
                    let chunk_end = chunk_start
                        .saturating_add(self.config.chunk_bytes)
                        .min(metadata.bytes);
                    if loaded
                        .as_ref()
                        .is_none_or(|(index, _, _)| *index != chunk_index)
                    {
                        loaded = Some((
                            chunk_index,
                            chunk_start,
                            self.load_or_fetch_chunk(
                                store,
                                object_key,
                                metadata,
                                chunk_index,
                                chunk_start..chunk_end,
                            )?,
                        ));
                    }
                    let (_, loaded_start, chunk) =
                        loaded.as_ref().expect("requested cache chunk was loaded");
                    let copy_start = usize::try_from(range.start.max(chunk_start) - *loaded_start)
                        .map_err(|_| {
                            TelemetryError::ObjectStore("cache slice offset overflow".into())
                        })?;
                    let copy_end = usize::try_from(range.end.min(chunk_end) - *loaded_start)
                        .map_err(|_| {
                            TelemetryError::ObjectStore("cache slice offset overflow".into())
                        })?;
                    output.extend_from_slice(&chunk[copy_start..copy_end]);
                }
            }
            outputs.push(output);
        }
        Ok(outputs)
    }

    /// Reads sorted immutable ranges as shared verified chunk slices.
    ///
    /// Ranges contained in one cache chunk allocate no payload copy after the
    /// chunk has been admitted to RAM. A range spanning chunks is assembled
    /// into one owned shared buffer.
    pub fn read_shared_ranges_with_metadata<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        ranges: &[Range<u64>],
    ) -> TelemetryResult<Vec<CachedObjectRange>> {
        if ranges.windows(2).any(|pair| pair[0].end > pair[1].start) {
            return Err(TelemetryError::ObjectStore(
                "shared SSD cache ranges must be sorted and non-overlapping".into(),
            ));
        }
        let mut outputs = Vec::with_capacity(ranges.len());
        let mut loaded = None::<(u64, u64, Arc<[u8]>)>;
        for range in ranges {
            if range.start > range.end
                || range.end > metadata.bytes
                || range.end - range.start > self.config.max_read_bytes
            {
                return Err(TelemetryError::ObjectStore(
                    "shared SSD cache range is invalid or exceeds its limit".into(),
                ));
            }
            if range.is_empty() {
                outputs.push(CachedObjectRange::empty());
                continue;
            }
            let first_chunk = range.start / self.config.chunk_bytes;
            let last_chunk = (range.end - 1) / self.config.chunk_bytes;
            if first_chunk == last_chunk {
                let chunk_start = first_chunk
                    .checked_mul(self.config.chunk_bytes)
                    .ok_or_else(|| {
                        TelemetryError::ObjectStore("cache chunk offset overflow".into())
                    })?;
                let chunk_end = chunk_start
                    .saturating_add(self.config.chunk_bytes)
                    .min(metadata.bytes);
                if loaded
                    .as_ref()
                    .is_none_or(|(index, _, _)| *index != first_chunk)
                {
                    loaded = Some((
                        first_chunk,
                        chunk_start,
                        self.load_or_fetch_shared_chunk(
                            store,
                            object_key,
                            metadata,
                            first_chunk,
                            chunk_start..chunk_end,
                        )?,
                    ));
                }
                let (_, loaded_start, chunk) =
                    loaded.as_ref().expect("requested cache chunk was loaded");
                let start = usize::try_from(range.start - *loaded_start).map_err(|_| {
                    TelemetryError::ObjectStore("cache slice offset overflow".into())
                })?;
                let end = usize::try_from(range.end - *loaded_start).map_err(|_| {
                    TelemetryError::ObjectStore("cache slice offset overflow".into())
                })?;
                outputs.push(CachedObjectRange {
                    bytes: Arc::clone(chunk),
                    start,
                    end,
                });
                continue;
            }

            let output_bytes = usize::try_from(range.end - range.start).map_err(|_| {
                TelemetryError::ObjectStore("SSD cache read cannot fit in memory".into())
            })?;
            let mut output = Vec::with_capacity(output_bytes);
            for chunk_index in first_chunk..=last_chunk {
                let chunk_start = chunk_index
                    .checked_mul(self.config.chunk_bytes)
                    .ok_or_else(|| {
                        TelemetryError::ObjectStore("cache chunk offset overflow".into())
                    })?;
                let chunk_end = chunk_start
                    .saturating_add(self.config.chunk_bytes)
                    .min(metadata.bytes);
                let chunk = self.load_or_fetch_shared_chunk(
                    store,
                    object_key,
                    metadata,
                    chunk_index,
                    chunk_start..chunk_end,
                )?;
                let copy_start = usize::try_from(range.start.max(chunk_start) - chunk_start)
                    .map_err(|_| {
                        TelemetryError::ObjectStore("cache slice offset overflow".into())
                    })?;
                let copy_end =
                    usize::try_from(range.end.min(chunk_end) - chunk_start).map_err(|_| {
                        TelemetryError::ObjectStore("cache slice offset overflow".into())
                    })?;
                output.extend_from_slice(&chunk[copy_start..copy_end]);
            }
            outputs.push(CachedObjectRange::from_owned(output));
        }
        Ok(outputs)
    }

    fn load_or_fetch_chunk<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        chunk_index: u64,
        range: Range<u64>,
    ) -> TelemetryResult<Vec<u8>> {
        Ok(self
            .load_or_fetch_shared_chunk(store, object_key, metadata, chunk_index, range)?
            .as_ref()
            .to_vec())
    }

    fn load_or_fetch_shared_chunk<S: TelemetryObjectStore>(
        &self,
        store: &S,
        object_key: &str,
        metadata: &ObjectMetadata,
        chunk_index: u64,
        range: Range<u64>,
    ) -> TelemetryResult<Arc<[u8]>> {
        let cache_key = checksum_bytes(
            format!("{object_key}\0{}\0{chunk_index}", metadata.version_token).as_bytes(),
        );
        if let Some(bytes) = self.memory_cache_hit(&cache_key)? {
            return Ok(bytes);
        }
        if let Some(path) = self.cache_hit(&cache_key)? {
            match read_cache_chunk(&path) {
                Ok(bytes)
                    if u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                        == range.end - range.start =>
                {
                    self.record_cache_hit()?;
                    let bytes = Arc::<[u8]>::from(bytes);
                    self.admit_memory_chunk(cache_key, Arc::clone(&bytes))?;
                    return Ok(bytes);
                }
                Ok(_) | Err(_) => self.remove_entry(&cache_key)?,
            }
        }
        let bytes = store.get_range(object_key, range)?;
        self.record_cache_miss(u64::try_from(bytes.len()).unwrap_or(u64::MAX))?;
        self.install_chunk(&cache_key, &bytes)?;
        let bytes = Arc::<[u8]>::from(bytes);
        self.admit_memory_chunk(cache_key, Arc::clone(&bytes))?;
        Ok(bytes)
    }

    fn memory_cache_hit(&self, cache_key: &str) -> TelemetryResult<Option<Arc<[u8]>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let bytes = state.memory_entries.get_mut(cache_key).map(|entry| {
            entry.stamp = stamp;
            Arc::clone(&entry.bytes)
        });
        if bytes.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.memory_hits = state.memory_hits.saturating_add(1);
        }
        Ok(bytes)
    }

    fn parsed_page_hit(&self, cache_key: &str) -> TelemetryResult<Option<Arc<CatalogPage>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let page = state
            .parsed_entries
            .get_mut(cache_key)
            .and_then(|entry| match &entry.object {
                ParsedControlObject::CatalogPage(page) => {
                    entry.stamp = stamp;
                    Some(Arc::clone(page))
                }
                ParsedControlObject::GroupManifest(_) => None,
            });
        if page.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.parsed_hits = state.parsed_hits.saturating_add(1);
        }
        Ok(page)
    }

    fn parsed_manifest_hit(
        &self,
        cache_key: &str,
    ) -> TelemetryResult<Option<Arc<TierGroupManifest>>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        let manifest =
            state
                .parsed_entries
                .get_mut(cache_key)
                .and_then(|entry| match &entry.object {
                    ParsedControlObject::GroupManifest(manifest) => {
                        entry.stamp = stamp;
                        Some(Arc::clone(manifest))
                    }
                    ParsedControlObject::CatalogPage(_) => None,
                });
        if manifest.is_some() {
            state.hits = state.hits.saturating_add(1);
            state.parsed_hits = state.parsed_hits.saturating_add(1);
        }
        Ok(manifest)
    }

    fn admit_parsed_control(
        &self,
        cache_key: String,
        object: ParsedControlObject,
        source_bytes: u64,
    ) -> TelemetryResult<()> {
        // JSON control objects expand into strings and vectors. Four times the
        // immutable source length is a conservative charge that keeps parsed
        // state under the same hard RAM budget as verified raw chunks.
        let accounted_bytes = source_bytes.saturating_mul(4);
        if self.config.parsed_memory_bytes == 0 || accounted_bytes > self.config.parsed_memory_bytes
        {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state.parsed_entries.insert(
            cache_key,
            ParsedControlEntry {
                object,
                accounted_bytes,
                stamp,
            },
        ) {
            state.parsed_used_bytes = state
                .parsed_used_bytes
                .saturating_sub(previous.accounted_bytes);
        }
        state.parsed_used_bytes = state.parsed_used_bytes.saturating_add(accounted_bytes);
        evict_memory_to_budgets(
            &mut state,
            self.config.memory_bytes,
            self.config.parsed_memory_bytes,
        );
        Ok(())
    }

    fn admit_memory_chunk(&self, cache_key: String, bytes: Arc<[u8]>) -> TelemetryResult<()> {
        let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.config.memory_bytes == 0 || bytes_len > self.config.memory_bytes {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state
            .memory_entries
            .insert(cache_key, MemoryCacheEntry { bytes, stamp })
        {
            state.memory_used_bytes = state
                .memory_used_bytes
                .saturating_sub(u64::try_from(previous.bytes.len()).unwrap_or(u64::MAX));
        }
        state.memory_used_bytes = state.memory_used_bytes.saturating_add(bytes_len);
        evict_memory_to_budgets(
            &mut state,
            self.config.memory_bytes,
            self.config.parsed_memory_bytes,
        );
        Ok(())
    }

    fn record_cache_hit(&self) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.hits = state.hits.saturating_add(1);
        Ok(())
    }

    fn record_cache_miss(&self, bytes: u64) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.misses = state.misses.saturating_add(1);
        state.source_bytes = state.source_bytes.saturating_add(bytes);
        Ok(())
    }

    fn cache_hit(&self, cache_key: &str) -> TelemetryResult<Option<PathBuf>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        Ok(state.entries.get_mut(cache_key).map(|entry| {
            entry.stamp = stamp;
            entry.path.clone()
        }))
    }

    fn install_chunk(&self, cache_key: &str, bytes: &[u8]) -> TelemetryResult<()> {
        let framed_bytes = u64::try_from(bytes.len())
            .unwrap_or(u64::MAX)
            .saturating_add(CACHE_HEADER_BYTES as u64);
        if framed_bytes > self.config.max_bytes {
            return Ok(());
        }
        let path = self.root.join(format!("{cache_key}.chunk"));
        write_cache_chunk_atomically(&path, bytes)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        state.clock = state.clock.wrapping_add(1);
        let stamp = state.clock;
        if let Some(previous) = state.entries.insert(
            cache_key.to_owned(),
            CacheEntry {
                path,
                bytes: framed_bytes,
                stamp,
            },
        ) {
            state.used_bytes = state.used_bytes.saturating_sub(previous.bytes);
        }
        state.used_bytes = state.used_bytes.saturating_add(framed_bytes);
        evict_locked(&mut state, self.config.max_bytes)
    }

    fn remove_entry(&self, cache_key: &str) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        if let Some(entry) = state.entries.remove(cache_key) {
            state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
            remove_cache_file(&entry.path)?;
        }
        Ok(())
    }

    fn evict_to_budget(&self) -> TelemetryResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| TelemetryError::StorageIo("SSD cache state is poisoned".into()))?;
        evict_locked(&mut state, self.config.max_bytes)
    }
}

fn evict_memory_to_budgets(state: &mut CacheState, raw_budget: u64, parsed_budget: u64) {
    while state.memory_used_bytes > raw_budget {
        let Some(key) = state
            .memory_entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, _)| key.clone())
        else {
            state.memory_used_bytes = 0;
            break;
        };
        if let Some(removed) = state.memory_entries.remove(&key) {
            state.memory_used_bytes = state
                .memory_used_bytes
                .saturating_sub(u64::try_from(removed.bytes.len()).unwrap_or(u64::MAX));
        }
    }
    while state.parsed_used_bytes > parsed_budget {
        let Some(key) = state
            .parsed_entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, _)| key.clone())
        else {
            state.parsed_used_bytes = 0;
            break;
        };
        if let Some(removed) = state.parsed_entries.remove(&key) {
            state.parsed_used_bytes = state
                .parsed_used_bytes
                .saturating_sub(removed.accounted_bytes);
        }
    }
}

/// Writes selected staged block payloads as one immutable concatenated pack.
///
/// The returned entries contain exact byte extents and per-block checksums.
pub fn write_staged_payload_pack(
    catalog: &BlockCatalog,
    block_ids: &[BlockId],
    destination: impl AsRef<Path>,
) -> TelemetryResult<Vec<TierBlockEntry>> {
    if block_ids.is_empty() {
        return Err(TelemetryError::ObjectStore(
            "a payload pack requires at least one block".into(),
        ));
    }
    if block_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(TelemetryError::ObjectStore(
            "payload-pack block IDs must be strictly increasing".into(),
        ));
    }
    let first_descriptor = catalog
        .get(block_ids[0])
        .ok_or_else(|| TelemetryError::UnknownBlock(block_ids[0].get()))?;
    let destination = destination.as_ref();
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create payload-pack directory", error))?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(|error| storage_io("create staged payload pack", error))?;
    let mut offset = 0u64;
    let mut entries = Vec::with_capacity(block_ids.len());
    for &block_id in block_ids {
        let descriptor = catalog
            .get(block_id)
            .ok_or_else(|| TelemetryError::UnknownBlock(block_id.get()))?;
        if descriptor.stream_shard_id != first_descriptor.stream_shard_id
            || descriptor.topic_partition != first_descriptor.topic_partition
        {
            return Err(TelemetryError::ObjectStore(
                "one payload pack cannot cross a shard or logical partition".into(),
            ));
        }
        let payload = catalog
            .staged_payload(block_id)
            .ok_or_else(|| TelemetryError::MissingStagedPayload(block_id.get()))?;
        if u64::try_from(payload.len()).unwrap_or(u64::MAX) != descriptor.stored_bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "staged block {} length differs from its descriptor",
                block_id.get()
            )));
        }
        file.write_all(&payload)
            .map_err(|error| storage_io("write staged payload pack", error))?;
        entries.push(TierBlockEntry::from_descriptor(
            descriptor,
            offset,
            checksum_bytes(&payload),
        ));
        offset = offset
            .checked_add(descriptor.stored_bytes)
            .ok_or_else(|| TelemetryError::ObjectStore("payload-pack length overflow".into()))?;
    }
    file.sync_all()
        .map_err(|error| storage_io("sync staged payload pack", error))?;
    sync_parent(destination)?;
    Ok(entries)
}

/// Marks all local blocks in a published group as durable payload ranges.
pub fn mark_group_offloaded(
    catalog: &mut BlockCatalog,
    manifest: &TierGroupManifest,
) -> TelemetryResult<()> {
    let payload = manifest
        .artifact(TierArtifactKind::PayloadPack)
        .ok_or_else(|| TelemetryError::CorruptTier("group has no payload artifact".into()))?;

    // Validate the complete transition before mutating any block. A corrupt
    // or stale manifest must not leave a partially offloaded local catalog.
    for block in &manifest.blocks {
        let descriptor = catalog
            .get(BlockId::new(block.block_id))
            .ok_or(TelemetryError::UnknownBlock(block.block_id))?;
        let range_end = block
            .payload_offset
            .checked_add(descriptor.stored_bytes)
            .ok_or_else(|| TelemetryError::CorruptTier("payload range overflow".into()))?;
        if range_end > payload.bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "block {} exceeds payload artifact length",
                block.block_id
            )));
        }
        if descriptor.stream_shard_id.get() != manifest.shard_id
            || descriptor.topic_partition.topic_id.get().to_string() != manifest.topic_id
            || descriptor.topic_partition.partition_id.get() != manifest.partition_id
        {
            return Err(TelemetryError::CorruptTier(format!(
                "block {} belongs to another catalog namespace",
                block.block_id
            )));
        }
    }

    for block in &manifest.blocks {
        catalog.mark_offloaded_range(
            BlockId::new(block.block_id),
            payload.object_key.clone(),
            block.payload_offset,
        )?;
    }
    Ok(())
}

fn validate_source(source: &TierGroupSource) -> TelemetryResult<()> {
    if source.blocks.is_empty() || source.artifacts.is_empty() {
        return Err(TelemetryError::ObjectStore(
            "a tier group requires blocks and artifacts".into(),
        ));
    }
    if source
        .blocks
        .windows(2)
        .any(|pair| pair[0].block_id >= pair[1].block_id)
    {
        return Err(TelemetryError::ObjectStore(
            "tier group blocks must be strictly increasing".into(),
        ));
    }
    for artifact in &source.artifacts {
        validate_artifact_name(&artifact.name)?;
    }
    if source
        .artifacts
        .iter()
        .enumerate()
        .any(|(index, artifact)| {
            source.artifacts[index + 1..]
                .iter()
                .any(|other| artifact.kind == other.kind && artifact.name == other.name)
        })
    {
        return Err(TelemetryError::ObjectStore(
            "tier group artifact names must be unique per role".into(),
        ));
    }
    Ok(())
}

fn same_group_contents(left: &TierGroupManifest, right: &TierGroupManifest) -> bool {
    left.format_version == right.format_version
        && left.group_sequence == right.group_sequence
        && left.checkpoint == right.checkpoint
        && left.shard_id == right.shard_id
        && left.topic_id == right.topic_id
        && left.partition_id == right.partition_id
        && left.blocks == right.blocks
        && left.artifacts.len() == right.artifacts.len()
        && left
            .artifacts
            .iter()
            .zip(&right.artifacts)
            .all(|(left, right)| {
                left.kind == right.kind
                    && left.name == right.name
                    && left.bytes == right.bytes
                    && left.checksum_algorithm == right.checksum_algorithm
                    && left.checksum == right.checksum
            })
}

fn verify_object_metadata(
    observed: &ObjectMetadata,
    expected: &ObjectMetadata,
    context: &str,
) -> TelemetryResult<()> {
    if observed.bytes != expected.bytes || observed.content_digest != expected.content_digest {
        return Err(TelemetryError::CorruptTier(format!(
            "object store changed {context}"
        )));
    }
    Ok(())
}

fn validate_artifact(artifact: &TierArtifact) -> TelemetryResult<()> {
    validate_artifact_name(&artifact.name)?;
    validate_object_key(&artifact.object_key)?;
    if artifact.bytes == 0
        || artifact.checksum_algorithm != CHECKSUM_ALGORITHM
        || !valid_checksum(&artifact.checksum)
    {
        return Err(TelemetryError::CorruptTier(
            "group artifact metadata is invalid".into(),
        ));
    }
    Ok(())
}

fn validate_artifact_name(name: &str) -> TelemetryResult<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(TelemetryError::ObjectStore(
            "artifact names must be 1..=128 path-free ASCII characters".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_object_key(key: &str) -> TelemetryResult<()> {
    let path = Path::new(key);
    if key.is_empty()
        || key.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(TelemetryError::ObjectStore(format!(
            "unsafe object key {key:?}"
        )));
    }
    Ok(())
}

fn catalog_namespace(shard_id: ShardId, partition: TopicPartition) -> String {
    format!(
        "catalog/shard-{}/topic-{:032x}/partition-{}",
        shard_id.get(),
        partition.topic_id.get(),
        partition.partition_id.get()
    )
}

fn encode_json<T: Serialize>(value: &T, context: &str) -> TelemetryResult<Vec<u8>> {
    serde_json::to_vec(value)
        .map_err(|error| TelemetryError::CorruptTier(format!("{context} encoding failed: {error}")))
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8], context: &str) -> TelemetryResult<T> {
    serde_json::from_slice(bytes)
        .map_err(|error| TelemetryError::CorruptTier(format!("{context} decoding failed: {error}")))
}

fn ensure_control_size(bytes: usize, limit: u64, context: &str) -> TelemetryResult<()> {
    if u64::try_from(bytes).unwrap_or(u64::MAX) > limit {
        return Err(TelemetryError::ObjectStore(format!(
            "{context} exceeds configured control-object limit {limit}"
        )));
    }
    Ok(())
}

fn verify_bytes_metadata(
    bytes: &[u8],
    metadata: &ObjectMetadata,
    context: &str,
) -> TelemetryResult<()> {
    if metadata.content_digest.is_empty() {
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != metadata.bytes {
            return Err(TelemetryError::CorruptTier(format!(
                "{context} failed object-store length verification"
            )));
        }
        return Ok(());
    }
    verify_expected_object(bytes, metadata.bytes, &metadata.content_digest, context)
}

fn verify_expected_object(
    bytes: &[u8],
    expected_bytes: u64,
    expected_checksum: &str,
    context: &str,
) -> TelemetryResult<()> {
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_bytes
        || checksum_bytes(bytes) != expected_checksum
    {
        return Err(TelemetryError::CorruptTier(format!(
            "{context} failed length or BLAKE3 verification"
        )));
    }
    Ok(())
}

fn valid_checksum(checksum: &str) -> bool {
    checksum.len() == 64
        && checksum
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn metadata_for_bytes(bytes: &[u8]) -> ObjectMetadata {
    let content_digest = checksum_bytes(bytes);
    ObjectMetadata {
        bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        version_token: content_digest.clone(),
        content_digest,
    }
}

fn checksum_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

fn unix_time_millis() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    u64::try_from(millis).unwrap_or(u64::MAX)
}

pub(crate) fn hash_file(path: &Path) -> TelemetryResult<ObjectMetadata> {
    let mut file =
        File::open(path).map_err(|error| storage_io("open file for BLAKE3 hashing", error))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    let mut bytes = 0u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| storage_io("read file for BLAKE3 hashing", error))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| TelemetryError::StorageIo("file length overflow".into()))?;
    }
    Ok(ObjectMetadata {
        bytes,
        version_token: hasher.finalize().to_hex().to_string(),
        content_digest: hasher.finalize().to_hex().to_string(),
    })
}

fn metadata_for_path_if_present(path: &Path) -> TelemetryResult<Option<ObjectMetadata>> {
    match path.metadata() {
        Ok(metadata) if metadata.is_file() => hash_file(path).map(Some),
        Ok(_) => Err(TelemetryError::ObjectStore(format!(
            "object path {} is not a regular file",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(storage_io("inspect local object", error)),
    }
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create object parent directory", error))?;
    }
    let temporary = temporary_path(path);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("create temporary object", error))?;
    let result = (|| {
        file.write_all(bytes)
            .map_err(|error| storage_io("write temporary object", error))?;
        file.sync_all()
            .map_err(|error| storage_io("sync temporary object", error))?;
        fs::rename(&temporary, path)
            .map_err(|error| storage_io("publish temporary object", error))?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn copy_file_atomically(source: &Path, destination: &Path) -> TelemetryResult<ObjectMetadata> {
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| storage_io("create object parent directory", error))?;
    }
    let temporary = temporary_path(destination);
    let mut input = File::open(source).map_err(|error| storage_io("open object source", error))?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| storage_io("create temporary object", error))?;
    let result = (|| {
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0; COPY_BUFFER_BYTES];
        let mut bytes = 0u64;
        loop {
            let read = input
                .read(&mut buffer)
                .map_err(|error| storage_io("read object source", error))?;
            if read == 0 {
                break;
            }
            output
                .write_all(&buffer[..read])
                .map_err(|error| storage_io("write temporary object", error))?;
            hasher.update(&buffer[..read]);
            bytes = bytes
                .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
                .ok_or_else(|| TelemetryError::StorageIo("object length overflow".into()))?;
        }
        output
            .sync_all()
            .map_err(|error| storage_io("sync temporary object", error))?;
        fs::rename(&temporary, destination)
            .map_err(|error| storage_io("publish temporary object", error))?;
        sync_parent(destination)?;
        Ok(ObjectMetadata {
            bytes,
            version_token: hasher.finalize().to_hex().to_string(),
            content_digest: hasher.finalize().to_hex().to_string(),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    path.with_extension(format!("tmp-{}-{sequence}", std::process::id()))
}

fn sync_parent(path: &Path) -> TelemetryResult<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| storage_io("sync parent directory", error))
}

fn unlock_file(file: &File) -> TelemetryResult<()> {
    FileExt::unlock(file).map_err(|error| storage_io("unlock object-store update lock", error))
}

fn storage_io(context: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::StorageIo(format!("{context}: {error}"))
}

fn object_io(key: &str, operation: &str, error: std::io::Error) -> TelemetryError {
    TelemetryError::ObjectStore(format!("{operation} object {key}: {error}"))
}

fn write_cache_chunk_atomically(path: &Path, bytes: &[u8]) -> TelemetryResult<()> {
    let mut framed = Vec::with_capacity(bytes.len().saturating_add(CACHE_HEADER_BYTES));
    framed.extend_from_slice(CACHE_HEADER_MAGIC);
    framed.extend_from_slice(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    framed.extend_from_slice(blake3::hash(bytes).as_bytes());
    framed.extend_from_slice(bytes);
    write_bytes_atomically(path, &framed)
}

fn read_cache_chunk(path: &Path) -> TelemetryResult<Vec<u8>> {
    let framed = fs::read(path).map_err(|error| storage_io("read SSD cache chunk", error))?;
    if framed.len() < CACHE_HEADER_BYTES || &framed[..8] != CACHE_HEADER_MAGIC {
        return Err(TelemetryError::CorruptTier(
            "SSD cache chunk header is invalid".into(),
        ));
    }
    let bytes = u64::from_le_bytes(
        framed[8..16]
            .try_into()
            .map_err(|_| TelemetryError::CorruptTier("SSD cache length is invalid".into()))?,
    );
    let payload = &framed[CACHE_HEADER_BYTES..];
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) != bytes
        || blake3::hash(payload).as_bytes() != &framed[16..48]
    {
        return Err(TelemetryError::CorruptTier(
            "SSD cache chunk failed integrity verification".into(),
        ));
    }
    Ok(payload.to_vec())
}

fn evict_locked(state: &mut CacheState, max_bytes: u64) -> TelemetryResult<()> {
    while state.used_bytes > max_bytes {
        let Some((key, _)) = state
            .entries
            .iter()
            .min_by_key(|(key, entry)| (entry.stamp, *key))
            .map(|(key, entry)| (key.clone(), entry.stamp))
        else {
            state.used_bytes = 0;
            break;
        };
        let entry = state
            .entries
            .remove(&key)
            .expect("selected cache entry still exists");
        state.used_bytes = state.used_bytes.saturating_sub(entry.bytes);
        remove_cache_file(&entry.path)?;
    }
    Ok(())
}

fn remove_cache_file(path: &Path) -> TelemetryResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(storage_io("evict SSD cache chunk", error)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};

    use super::*;
    use crate::{
        CompressionCohortId, CompressionPlacementId, CompressionTemperature, DictionaryId, TraceId,
    };

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        path: PathBuf,
    }

    impl TestDirectory {
        fn new(name: &str) -> Self {
            let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "shard-telemetry-{name}-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("test directory is created");
            Self { path }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn partition() -> TopicPartition {
        TopicPartition::new(TopicId::new(91), LogicalPartitionId::new(3))
    }

    fn tier_config(groups_per_page: usize) -> ObjectTierConfig {
        ObjectTierConfig {
            groups_per_page,
            ..ObjectTierConfig::default()
        }
    }

    fn write_test_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).expect("test artifact is written");
    }

    #[test]
    fn catalog_correlation_summary_keeps_primary_and_linked_trace_matches() {
        let primary = TraceId::from_bytes([1; 16]).expect("primary trace ID is valid");
        let linked = TraceId::from_bytes([2; 16]).expect("linked trace ID is valid");
        let absent = TraceId::from_bytes([3; 16]).expect("absent trace ID is valid");
        let primary_value = u128::from_be_bytes(*primary.as_bytes());
        let linked_query = CorrelationQuery::new("tenant-a").with_trace_id(linked);
        let linked_filter = CorrelationBlockFilter::from_query(&linked_query);

        assert!(catalog_correlation_may_match(
            Some(&CorrelationBlockFilter::default()),
            Some(primary_value),
            Some(primary_value),
            &CorrelationQuery::new("tenant-a").with_trace_id(primary),
            TelemetrySignal::Traces,
        ));
        assert!(catalog_correlation_may_match(
            Some(&linked_filter),
            Some(primary_value),
            Some(primary_value),
            &linked_query,
            TelemetrySignal::Traces,
        ));
        assert!(!catalog_correlation_may_match(
            Some(&linked_filter),
            Some(primary_value),
            Some(primary_value),
            &CorrelationQuery::new("tenant-a").with_trace_id(absent),
            TelemetrySignal::Traces,
        ));
        assert!(catalog_correlation_may_match(
            Some(&linked_filter),
            Some(99),
            Some(99),
            &linked_query,
            TelemetrySignal::Metrics,
        ));
    }

    fn group_source(
        directory: &Path,
        sequence: u64,
        first_offset: u64,
        timestamp: u64,
    ) -> TierGroupSource {
        let payload = vec![u8::try_from(sequence).unwrap_or(u8::MAX); 32];
        let payload_path = directory.join(format!("group-{sequence}.payload"));
        let index_path = directory.join(format!("group-{sequence}.query-index"));
        write_test_file(&payload_path, &payload);
        write_test_file(&index_path, format!("query-index-{sequence}").as_bytes());
        TierGroupSource {
            group_sequence: sequence,
            checkpoint: TierCheckpoint {
                next_placement_sequence: sequence + 1,
                next_offset: first_offset + 10,
            },
            blocks: vec![TierBlockEntry {
                block_id: sequence,
                source_compression_cohort: 7,
                placement_id: 11,
                dictionary_id: None,
                compression_codec: "zstd".into(),
                compression_level: 1,
                first_offset,
                last_offset: first_offset + 9,
                record_count: 10,
                source_bytes: 320,
                structural_bytes: 128,
                stored_bytes: 32,
                min_timestamp_unix_nanos: timestamp,
                max_timestamp_unix_nanos: timestamp + 99,
                compression_temperature: 17,
                compression_shape_hash: 19,
                compression_temperature_variance_q8: 2,
                max_compression_temperature_deviation: 1,
                payload_offset: 0,
                payload_bytes: 32,
                payload_checksum: checksum_bytes(&payload),
                min_signal_identity: None,
                max_signal_identity: None,
                correlation_filter: None,
            }],
            artifacts: vec![
                TierArtifactSource {
                    kind: TierArtifactKind::PayloadPack,
                    name: "blocks.pack".into(),
                    path: payload_path,
                },
                TierArtifactSource {
                    kind: TierArtifactKind::QueryIndex,
                    name: "query.slogqix".into(),
                    path: index_path,
                },
            ],
        }
    }

    #[test]
    fn local_object_store_is_immutable_conditional_and_key_safe() {
        let directory = TestDirectory::new("local-object-store");
        let store = LocalObjectStore::open(&directory.path).expect("store opens");
        let first = store
            .put_bytes_if_absent("objects/one", b"first")
            .expect("immutable object is created");
        assert_eq!(
            store
                .put_bytes_if_absent("objects/one", b"first")
                .expect("identical retry succeeds"),
            first
        );
        assert!(matches!(
            store.put_bytes_if_absent("objects/one", b"different"),
            Err(TelemetryError::ObjectStore(_))
        ));
        assert!(matches!(
            store.put_bytes_if_absent("../escape", b"bad"),
            Err(TelemetryError::ObjectStore(_))
        ));

        let current = store
            .compare_and_swap("catalog/CURRENT", None, b"one")
            .expect("missing pointer is created");
        assert!(matches!(
            store.compare_and_swap("catalog/CURRENT", None, b"two"),
            Err(TelemetryError::StaleCatalog { .. })
        ));
        let next = store
            .compare_and_swap("catalog/CURRENT", Some(&current.version_token), b"two")
            .expect("matching pointer is replaced");
        assert_ne!(current.version_token, next.version_token);
        assert_eq!(current.content_digest, checksum_bytes(b"one"));
        assert_eq!(next.content_digest, checksum_bytes(b"two"));
    }

    #[test]
    fn publication_pages_prune_restart_and_reject_stale_writers() {
        let directory = TestDirectory::new("tier-publication");
        let artifact_directory = directory.path.join("sources");
        fs::create_dir_all(&artifact_directory).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("empty tier opens");
        let source0 = group_source(&artifact_directory, 0, 0, 1_000);
        let manifest0 = tier
            .publish_group(source0.clone())
            .expect("first group publishes");
        assert_eq!(tier.root().generation, 1);
        assert_eq!(tier.root().pages.len(), 1);
        assert_eq!(
            tier.publish_group(source0)
                .expect("identical last-group retry succeeds"),
            manifest0
        );
        assert_eq!(tier.root().generation, 1);

        tier.publish_group(group_source(&artifact_directory, 1, 10, 2_000))
            .expect("second group publishes");
        assert_eq!(tier.root().pages.len(), 1);
        tier.publish_group(group_source(&artifact_directory, 2, 20, 3_000))
            .expect("page rollover publishes");
        assert_eq!(tier.root().pages.len(), 2);
        assert_eq!(tier.root().generation, 3);

        let candidates = tier
            .candidate_groups(TierQueryRange {
                first_offset: Some(25),
                last_offset: Some(25),
                ..TierQueryRange::default()
            })
            .expect("offset pruning succeeds");
        assert_eq!(
            candidates
                .iter()
                .map(|entry| entry.group_sequence)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(
            !serde_json::to_string(&candidates[0])
                .expect("log catalog entry serializes")
                .contains("correlation_filter")
        );
        let loaded = tier
            .load_group(&candidates[0])
            .expect("candidate group loads");
        assert_eq!(loaded.group_sequence, 2);
        assert_eq!(
            tier.read_artifact(
                loaded
                    .artifact(TierArtifactKind::QueryIndex)
                    .expect("query index exists"),
                1024,
            )
            .expect("query index verifies"),
            b"query-index-2"
        );

        let reopened =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("published tier recovers");
        assert_eq!(reopened.root(), tier.root());

        let mut first_writer =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("first writer opens");
        let mut stale_writer =
            TelemetryObjectTier::open(store, ShardId::new(4), partition(), tier_config(2))
                .expect("stale writer opens");
        first_writer
            .publish_group(group_source(&artifact_directory, 3, 30, 4_000))
            .expect("first writer advances catalog");
        assert!(matches!(
            stale_writer.publish_group(group_source(&artifact_directory, 4, 40, 5_000)),
            Err(TelemetryError::StaleCatalog { .. })
        ));
    }

    #[test]
    fn retired_generation_waits_for_its_last_rust_lease() {
        let directory = TestDirectory::new("tier-ownership-lease");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let config = ObjectTierConfig {
            retirement_grace: std::time::Duration::ZERO,
            ..tier_config(2)
        };
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), config)
                .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
            .expect("first group publishes");
        let lease = tier.catalog_lease();
        let retired_page = lease.root().pages[0].page_key.clone();
        let current = store
            .get(&format!("{}/CURRENT", tier.namespace), POINTER_READ_LIMIT)
            .expect("CURRENT reads");
        let retired_root = decode_json::<CatalogPointer>(&current, "CURRENT")
            .expect("CURRENT decodes")
            .root_key;

        tier.publish_group(group_source(&artifacts, 1, 10, 2_000))
            .expect("second group publishes");
        assert_eq!(tier.pending_retired_objects(), 2);
        assert!(store.head(&retired_root).expect("root head").is_some());
        assert!(store.head(&retired_page).expect("page head").is_some());

        drop(lease);
        tier.reclaim_retired_objects()
            .expect("lease release reclaims");
        assert_eq!(tier.pending_retired_objects(), 0);
        assert!(store.head(&retired_root).expect("root head").is_none());
        assert!(store.head(&retired_page).expect("page head").is_none());
    }

    #[derive(Debug, Clone)]
    struct FailDeleteOnceStore {
        inner: LocalObjectStore,
        fail_delete: Arc<AtomicBool>,
    }

    impl TelemetryObjectStore for FailDeleteOnceStore {
        fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
            self.inner.put_bytes_if_absent(key, bytes)
        }

        fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
            self.inner.put_file_if_absent(key, source)
        }

        fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
            self.inner.get(key, max_bytes)
        }

        fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
            self.inner.get_range(key, range)
        }

        fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> TelemetryResult<()> {
            if !key.ends_with("/PENDING") && self.fail_delete.swap(false, Ordering::Relaxed) {
                return Err(TelemetryError::ObjectStore(
                    "injected exact-key delete failure".into(),
                ));
            }
            self.inner.delete(key)
        }

        fn compare_and_swap(
            &self,
            key: &str,
            expected_version: Option<&str>,
            bytes: &[u8],
        ) -> TelemetryResult<ObjectMetadata> {
            self.inner.compare_and_swap(key, expected_version, bytes)
        }
    }

    #[test]
    fn selected_root_replays_exact_retirements_after_delete_failure() {
        let directory = TestDirectory::new("tier-retirement-replay");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store = FailDeleteOnceStore {
            inner: LocalObjectStore::open(directory.path.join("objects"))
                .expect("object store opens"),
            fail_delete: Arc::new(AtomicBool::new(false)),
        };
        let config = ObjectTierConfig {
            retirement_grace: std::time::Duration::ZERO,
            ..tier_config(2)
        };
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), config)
                .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
            .expect("first group publishes");
        store.fail_delete.store(true, Ordering::Relaxed);
        assert!(matches!(
            tier.publish_group(group_source(&artifacts, 1, 10, 2_000)),
            Err(TelemetryError::ObjectStore(_))
        ));
        drop(tier);

        let recovered = TelemetryObjectTier::open(store, ShardId::new(4), partition(), config)
            .expect("selected root replays its exact retirement set");
        assert_eq!(recovered.root().generation, 2);
        assert_eq!(recovered.pending_retired_objects(), 0);
    }

    #[test]
    fn startup_relinquishes_every_uncommitted_transaction_key_without_listing() {
        let directory = TestDirectory::new("tier-pending-abort-replay");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("tier opens");
        let transaction_id = "0123456789abcdef0123456789abcdef".to_owned();
        let first = format!(
            "{}/transactions/{transaction_id}/groups/00000000000000000000/payload-test",
            tier.namespace
        );
        let root = format!(
            "{}/transactions/{transaction_id}/roots/root-00000000000000000001-test.json",
            tier.namespace
        );
        let transaction = CatalogTransaction {
            format_version: TIER_FORMAT_VERSION,
            transaction_id,
            target_generation: 1,
            target_root_key: root.clone(),
            reclaim_after_unix_millis: 1,
            owned_objects: vec![first.clone(), root.clone()],
        };
        let pending_key = tier.pending_key();
        store
            .compare_and_swap(
                &pending_key,
                None,
                &encode_json(&transaction, "test PENDING").expect("transaction encodes"),
            )
            .expect("PENDING is selected");
        store
            .put_bytes_if_absent(&first, b"uncommitted payload")
            .expect("first transaction object exists");
        store
            .put_bytes_if_absent(&root, b"uncommitted root")
            .expect("root transaction object exists");
        drop(tier);

        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("startup replays PENDING cleanup");
        assert!(store.head(&first).expect("first head").is_none());
        assert!(store.head(&root).expect("root head").is_none());
        assert!(store.head(&pending_key).expect("PENDING head").is_none());
    }

    #[test]
    fn startup_preserves_every_key_owned_by_an_active_writer_lease() {
        let directory = TestDirectory::new("tier-pending-active-writer");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("tier opens");
        let transaction_id = "fedcba9876543210fedcba9876543210".to_owned();
        let payload = format!(
            "{}/transactions/{transaction_id}/groups/00000000000000000000/payload-test",
            tier.namespace
        );
        let root = format!(
            "{}/transactions/{transaction_id}/roots/root-00000000000000000001-test.json",
            tier.namespace
        );
        let transaction = CatalogTransaction {
            format_version: TIER_FORMAT_VERSION,
            transaction_id,
            target_generation: 1,
            target_root_key: root.clone(),
            reclaim_after_unix_millis: u64::MAX,
            owned_objects: vec![payload.clone(), root.clone()],
        };
        let pending_key = tier.pending_key();
        store
            .compare_and_swap(
                &pending_key,
                None,
                &encode_json(&transaction, "test PENDING").expect("transaction encodes"),
            )
            .expect("PENDING is selected");
        store
            .put_bytes_if_absent(&payload, b"active payload")
            .expect("active payload exists");
        store
            .put_bytes_if_absent(&root, b"active root")
            .expect("active root exists");
        drop(tier);

        assert!(matches!(
            TelemetryObjectTier::open(
                store.clone(),
                ShardId::new(4),
                partition(),
                tier_config(2)
            ),
            Err(TelemetryError::ObjectStore(message))
                if message.contains("active writer lease")
        ));
        assert!(store.head(&payload).expect("payload head").is_some());
        assert!(store.head(&root).expect("root head").is_some());
        assert!(store.head(&pending_key).expect("PENDING head").is_some());
    }

    #[test]
    fn startup_preserves_a_committed_transaction_when_pending_cleanup_was_interrupted() {
        let directory = TestDirectory::new("tier-pending-commit-replay");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
            .expect("group publishes");
        let root = tier
            .current_root_key
            .clone()
            .expect("published catalog has a root");
        let transaction_id = root
            .strip_prefix(&format!("{}/transactions/", tier.namespace))
            .and_then(|path| path.split('/').next())
            .expect("root carries transaction identity")
            .to_owned();
        let transaction = CatalogTransaction {
            format_version: TIER_FORMAT_VERSION,
            transaction_id,
            target_generation: tier.root().generation,
            target_root_key: root.clone(),
            reclaim_after_unix_millis: u64::MAX,
            owned_objects: vec![root.clone()],
        };
        let pending_key = tier.pending_key();
        store
            .compare_and_swap(
                &pending_key,
                None,
                &encode_json(&transaction, "test PENDING").expect("transaction encodes"),
            )
            .expect("interrupted committed PENDING is restored");
        drop(tier);

        let recovered =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
                .expect("committed PENDING is recognized");
        assert_eq!(recovered.root().generation, 1);
        assert!(store.head(&root).expect("root head").is_some());
        assert!(store.head(&pending_key).expect("PENDING head").is_none());
    }

    #[test]
    fn retention_relinquishes_only_catalog_owned_exact_keys() {
        let directory = TestDirectory::new("tier-retention-ownership");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let config = ObjectTierConfig {
            retirement_grace: std::time::Duration::ZERO,
            ..tier_config(2)
        };
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), config)
                .expect("tier opens");
        for sequence in 0..4 {
            tier.publish_group(group_source(
                &artifacts,
                sequence,
                sequence * 10,
                (sequence + 1) * 1_000,
            ))
            .expect("group publishes");
        }
        let lease = tier.catalog_lease();
        let removed_manifest = tier
            .candidate_groups(TierQueryRange::default())
            .expect("groups load")[0]
            .manifest_key
            .clone();
        let report = tier
            .retain_since_timestamp(3_500)
            .expect("retention publishes");
        assert_eq!(report.retired_groups, 3);
        assert!(report.retired_objects >= 9);
        assert_eq!(
            tier.pending_retired_objects(),
            report.retired_objects as usize
        );
        assert!(
            store
                .head(&removed_manifest)
                .expect("manifest head")
                .is_some()
        );
        assert_eq!(
            tier.candidate_groups(TierQueryRange::default())
                .expect("retained groups")
                .into_iter()
                .map(|group| group.group_sequence)
                .collect::<Vec<_>>(),
            vec![3]
        );

        drop(lease);
        tier.reclaim_retired_objects().expect("retired keys delete");
        assert!(
            store
                .head(&removed_manifest)
                .expect("manifest head")
                .is_none()
        );
        let recovered = TelemetryObjectTier::open(store, ShardId::new(4), partition(), config)
            .expect("retained catalog reopens");
        assert_eq!(
            recovered.root().latest_checkpoint,
            tier.root().latest_checkpoint
        );
        assert_eq!(recovered.root().next_block_id, tier.root().next_block_id);
    }

    #[test]
    fn payload_budget_retires_oldest_complete_groups_first() {
        let directory = TestDirectory::new("tier-capacity-retention");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let config = ObjectTierConfig {
            retirement_grace: std::time::Duration::ZERO,
            ..tier_config(8)
        };
        let mut tier = TelemetryObjectTier::open(store, ShardId::new(4), partition(), config)
            .expect("tier opens");
        for sequence in 0..4 {
            tier.publish_group(group_source(
                &artifacts,
                sequence,
                sequence * 10,
                (sequence + 1) * 1_000,
            ))
            .expect("group publishes");
        }

        let report = tier
            .retain_to_payload_bytes(64)
            .expect("capacity retention");
        assert_eq!(report.retired_groups, 2);
        assert_eq!(report.retired_payload_bytes, 64);
        assert_eq!(
            tier.candidate_groups(TierQueryRange::default())
                .expect("retained groups")
                .into_iter()
                .map(|group| group.group_sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn startup_is_shallow_and_touched_pages_are_verified() {
        let directory = TestDirectory::new("lazy-verification");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store.clone(), ShardId::new(2), partition(), tier_config(2))
                .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 100))
            .expect("group publishes");
        let page_key = tier.root().pages[0].page_key.clone();
        write_test_file(&store.root().join(page_key), b"corrupt");

        let reopened =
            TelemetryObjectTier::open(store, ShardId::new(2), partition(), tier_config(2))
                .expect("startup does not scan every page or payload");
        assert!(matches!(
            reopened.candidate_groups(TierQueryRange::default()),
            Err(TelemetryError::CorruptTier(_))
        ));
    }

    fn block_descriptor(offset: u64) -> BlockDescriptor {
        BlockDescriptor {
            block_id: BlockId::new(0),
            stream_shard_id: ShardId::new(8),
            topic_partition: partition(),
            source_compression_cohort: CompressionCohortId::new(4),
            placement_id: CompressionPlacementId::new(5),
            dictionary_id: Some(DictionaryId::new(6)),
            compression_codec: CompressionCodec::Zstd,
            compression_level: 1,
            first_offset: LogicalOffset::new(offset),
            last_offset: LogicalOffset::new(offset),
            record_count: 1,
            source_bytes: 20,
            structural_bytes: 12,
            stored_bytes: 4,
            min_timestamp_unix_nanos: offset,
            max_timestamp_unix_nanos: offset,
            compression_temperature: CompressionTemperature::new(7).get(),
            compression_shape_hash: 8,
            compression_temperature_variance_q8: 0,
            max_compression_temperature_deviation: 0,
            object_key: None,
            object_offset: None,
        }
    }

    #[test]
    fn staged_blocks_become_exact_object_ranges() {
        let directory = TestDirectory::new("payload-pack");
        let mut catalog = BlockCatalog::default();
        let first = catalog.seal(block_descriptor(0), Arc::from(&b"abcd"[..]));
        let second = catalog.seal(block_descriptor(1), Arc::from(&b"efgh"[..]));
        let pack_path = directory.path.join("blocks.pack");
        let entries =
            write_staged_payload_pack(&catalog, &[first.block_id, second.block_id], &pack_path)
                .expect("payload pack is written");
        assert_eq!(fs::read(&pack_path).expect("pack reads"), b"abcdefgh");
        assert_eq!(entries[0].payload_offset, 0);
        assert_eq!(entries[1].payload_offset, 4);
        assert_eq!(entries[0].payload_checksum, checksum_bytes(b"abcd"));
        assert_eq!(entries[1].payload_checksum, checksum_bytes(b"efgh"));

        let query_path = directory.path.join("query.index");
        write_test_file(&query_path, b"index");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store, ShardId::new(8), partition(), tier_config(2))
                .expect("tier opens");
        let manifest = tier
            .publish_group(TierGroupSource {
                group_sequence: 0,
                checkpoint: TierCheckpoint {
                    next_placement_sequence: 1,
                    next_offset: 2,
                },
                blocks: entries,
                artifacts: vec![
                    TierArtifactSource {
                        kind: TierArtifactKind::PayloadPack,
                        name: "blocks.pack".into(),
                        path: pack_path,
                    },
                    TierArtifactSource {
                        kind: TierArtifactKind::QueryIndex,
                        name: "query.index".into(),
                        path: query_path,
                    },
                ],
            })
            .expect("group publishes");
        mark_group_offloaded(&mut catalog, &manifest)
            .expect("catalog is advanced to object ranges");
        assert!(catalog.staged_payload(first.block_id).is_none());
        assert!(catalog.staged_payload(second.block_id).is_none());
        assert_eq!(
            catalog
                .get(first.block_id)
                .expect("first block exists")
                .object_offset,
            Some(0)
        );
        assert_eq!(
            catalog
                .get(second.block_id)
                .expect("second block exists")
                .object_offset,
            Some(4)
        );
    }

    #[test]
    fn invalid_group_offload_is_atomic() {
        let directory = TestDirectory::new("atomic-offload");
        let mut catalog = BlockCatalog::default();
        let first = catalog.seal(block_descriptor(0), Arc::from(&b"abcd"[..]));
        let second = catalog.seal(block_descriptor(1), Arc::from(&b"efgh"[..]));
        let pack_path = directory.path.join("blocks.pack");
        let entries =
            write_staged_payload_pack(&catalog, &[first.block_id, second.block_id], &pack_path)
                .expect("payload pack is written");
        let query_path = directory.path.join("query.index");
        write_test_file(&query_path, b"index");
        let store =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier =
            TelemetryObjectTier::open(store, ShardId::new(8), partition(), tier_config(2))
                .expect("tier opens");
        let mut manifest = tier
            .publish_group(TierGroupSource {
                group_sequence: 0,
                checkpoint: TierCheckpoint {
                    next_placement_sequence: 1,
                    next_offset: 2,
                },
                blocks: entries,
                artifacts: vec![
                    TierArtifactSource {
                        kind: TierArtifactKind::PayloadPack,
                        name: "blocks.pack".into(),
                        path: pack_path,
                    },
                    TierArtifactSource {
                        kind: TierArtifactKind::QueryIndex,
                        name: "query.index".into(),
                        path: query_path,
                    },
                ],
            })
            .expect("group publishes");
        manifest.blocks[1].block_id = 999;

        assert!(matches!(
            mark_group_offloaded(&mut catalog, &manifest),
            Err(TelemetryError::UnknownBlock(999))
        ));
        for block_id in [first.block_id, second.block_id] {
            assert!(catalog.staged_payload(block_id).is_some());
            assert!(
                catalog
                    .get(block_id)
                    .expect("block remains")
                    .object_key
                    .is_none()
            );
        }
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: LocalObjectStore,
        range_reads: AtomicUsize,
    }

    impl CountingStore {
        fn new(inner: LocalObjectStore) -> Self {
            Self {
                inner,
                range_reads: AtomicUsize::new(0),
            }
        }
    }

    impl TelemetryObjectStore for CountingStore {
        fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
            self.inner.put_bytes_if_absent(key, bytes)
        }

        fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
            self.inner.put_file_if_absent(key, source)
        }

        fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
            self.inner.get(key, max_bytes)
        }

        fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
            self.range_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_range(key, range)
        }

        fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
            self.inner.head(key)
        }

        fn delete(&self, key: &str) -> TelemetryResult<()> {
            self.inner.delete(key)
        }

        fn compare_and_swap(
            &self,
            key: &str,
            expected_version: Option<&str>,
            bytes: &[u8],
        ) -> TelemetryResult<ObjectMetadata> {
            self.inner.compare_and_swap(key, expected_version, bytes)
        }
    }

    #[test]
    fn ssd_cache_reuses_ranges_and_stays_byte_bounded() {
        let directory = TestDirectory::new("ssd-cache");
        let local =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let store = CountingStore::new(local);
        store
            .put_bytes_if_absent("payload/object", b"abcdefghijklmnop")
            .expect("payload object is written");
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 2 * (CACHE_HEADER_BYTES as u64 + 4),
                chunk_bytes: 4,
                max_read_bytes: 16,
                memory_bytes: 8,
                parsed_memory_bytes: 0,
            },
        )
        .expect("cache opens");

        assert_eq!(
            cache
                .read_range(&store, "payload/object", 4..12)
                .expect("first range reads"),
            b"efghijkl"
        );
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 2);
        assert_eq!(
            cache
                .read_range(&store, "payload/object", 4..12)
                .expect("second range reads from SSD"),
            b"efghijkl"
        );
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 2);

        cache
            .read_range(&store, "payload/object", 12..16)
            .expect("third chunk is admitted and evicts the oldest");
        assert!(cache.used_bytes() <= 2 * (CACHE_HEADER_BYTES as u64 + 4));
        cache
            .read_range(&store, "payload/object", 4..8)
            .expect("evicted chunk can be fetched again");
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn ssd_cache_admits_a_published_staging_file_without_a_remote_read() {
        let directory = TestDirectory::new("ssd-cache-admit-file");
        let source = directory.path.join("payload.pack");
        let bytes = b"abcdefghijklmnop";
        write_test_file(&source, bytes);
        let checksum = checksum_bytes(bytes);
        let artifact = TierArtifact {
            kind: TierArtifactKind::PayloadPack,
            name: "payload.pack".into(),
            object_key: "remote/payload".into(),
            bytes: u64::try_from(bytes.len()).expect("length"),
            checksum_algorithm: CHECKSUM_ALGORITHM.into(),
            checksum: checksum.clone(),
        };
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 4 * (CACHE_HEADER_BYTES as u64 + 4),
                chunk_bytes: 4,
                max_read_bytes: 16,
                memory_bytes: 0,
                parsed_memory_bytes: 0,
            },
        )
        .expect("cache opens");
        cache.admit_file(&artifact, &source).expect("file admits");

        let empty_store =
            LocalObjectStore::open(directory.path.join("empty-remote")).expect("store opens");
        let cached = cache
            .read_range_with_metadata(
                &empty_store,
                &artifact.object_key,
                &ObjectMetadata {
                    bytes: artifact.bytes,
                    version_token: checksum.clone(),
                    content_digest: checksum,
                },
                0..artifact.bytes,
            )
            .expect("cache serves without the remote object");
        assert_eq!(cached, bytes);
        assert!(cache.used_bytes() <= cache.config.max_bytes);
    }

    #[test]
    fn batched_ssd_ranges_load_a_shared_chunk_once() {
        let directory = TestDirectory::new("ssd-cache-batch");
        let local =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let store = CountingStore::new(local);
        let metadata = store
            .put_bytes_if_absent("payload/object", b"abcdefghijklmnop")
            .expect("payload object is written");
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 4 * (CACHE_HEADER_BYTES as u64 + 4),
                chunk_bytes: 4,
                max_read_bytes: 16,
                memory_bytes: 16,
                parsed_memory_bytes: 0,
            },
        )
        .expect("cache opens");
        let ranges = [4..6, 6..8];

        assert_eq!(
            cache
                .read_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
                .expect("batched ranges read"),
            [b"ef".to_vec(), b"gh".to_vec()]
        );
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().source_bytes, 4);
        cache
            .read_shared_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
            .expect("batched ranges are served from verified RAM");
        assert_eq!(store.range_reads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().memory_hits, 1);
        assert!(cache.stats().memory_used_bytes <= 16);

        let shared = cache
            .read_shared_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
            .expect("shared ranges remain readable");
        assert_eq!(shared[0].as_ref(), b"ef");
        assert_eq!(shared[1].as_ref(), b"gh");
        assert!(Arc::ptr_eq(&shared[0].bytes, &shared[1].bytes));
    }

    #[test]
    fn cached_catalog_reads_do_not_revisit_object_storage() {
        let directory = TestDirectory::new("catalog-cache");
        let artifacts = directory.path.join("sources");
        fs::create_dir_all(&artifacts).expect("artifact directory is created");
        let local =
            LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
        let mut tier = TelemetryObjectTier::open(
            CountingStore::new(local),
            ShardId::new(5),
            partition(),
            tier_config(16),
        )
        .expect("tier opens");
        tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
            .expect("group publishes");
        let cache = SsdObjectCache::open(
            directory.path.join("cache"),
            SsdCacheConfig {
                max_bytes: 32 * (CACHE_HEADER_BYTES as u64 + 1_024),
                chunk_bytes: 1_024,
                max_read_bytes: 64 * 1_024,
                memory_bytes: 32 * 1_024,
                parsed_memory_bytes: 32 * 1_024,
            },
        )
        .expect("cache opens");

        let groups = tier
            .candidate_groups_cached(TierQueryRange::default(), &cache)
            .expect("catalog page loads through cache");
        let manifest = tier
            .load_group_cached(&groups[0], &cache)
            .expect("manifest loads through cache");
        let artifact = manifest
            .artifact(TierArtifactKind::QueryIndex)
            .expect("query index exists");
        assert_eq!(
            tier.read_artifact_cached(artifact, 1_024, &cache)
                .expect("artifact loads through cache"),
            b"query-index-0"
        );
        let reads_after_first_query = tier.object_store().range_reads.load(Ordering::Relaxed);
        assert!(reads_after_first_query >= 3);

        let groups = tier
            .candidate_groups_cached(TierQueryRange::default(), &cache)
            .expect("catalog page is cached");
        let manifest = tier
            .load_group_cached(&groups[0], &cache)
            .expect("manifest is cached");
        tier.read_artifact_cached(
            manifest
                .artifact(TierArtifactKind::QueryIndex)
                .expect("query index exists"),
            1_024,
            &cache,
        )
        .expect("artifact is cached");
        assert_eq!(
            tier.object_store().range_reads.load(Ordering::Relaxed),
            reads_after_first_query
        );
        let stats = cache.stats();
        assert_eq!(stats.parsed_hits, 2);
        assert_eq!(stats.parsed_entries, 2);
        assert!(stats.memory_used_bytes <= 32 * 1_024);
    }
}
