//! Durable store ownership: startup and attachment, append paths, retention, and query adapters.
//! The root keeps configuration and state; child modules own operations and their tests.

use std::cmp::{Ordering as CmpOrdering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::num::NonZeroU16;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use foldhash::HashMapExt;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
use serde::{Deserialize, Serialize};
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, PlacementSequence, ShardId, TopicId, TopicPartition,
};
use shard_stream_engine::{
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkOptions, EngineConfig, EngineError,
    StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};

use crate::analytics::{AnalyticsGroupOrder, AnalyticsGroupRow};
use crate::deletion::DeleteCatalog;
use crate::ingest_pack::decode_ingest_pack;
use crate::native_log_page::{
    PAGE_RECORD_BATCH, PagePosition, decode_page_cursor, encode_page_cursor,
    validate_page_query,
};
use crate::loki_api::{LogicalDeleteFilter, LokiApiError, LokiQueryResult, apply_logical_deletes};
use crate::rollup::MetricRollupCatalog;
use crate::storage_format::DataDirectoryLease;
use crate::{
    AnalyticsRelation, AnalyticsRow, AnalyticsScanOrder, AnalyticsScanRequest, CaseSensitivity,
    DeleteRequest, LocalObjectStore, LogMatch, LogPredicate, LogQuery, LokiEntry, LokiStore,
    MetadataField, NativeQuery, NativeQueryDirection, NativeLogPageQuery, NativeLogQueryPage, ObjectTierConfig, OtlpSinkConfig,
    QueryCursor, S3ObjectStore, S3ObjectStoreConfig, SharedTelemetryObjectStore,
    SinkObjectTierConfig, SsdCacheConfig, StoreHealth, StoreMetrics, StripeConfig,
    TelemetryService, TelemetrySinkFactory,
};

mod analytics_signals;
mod append;
mod append_helpers;
mod loki;
mod native_query;
mod remote_write;
mod retention;
mod signal_query;
mod startup;
#[cfg(test)]
mod tests;

const LOKI_TOPIC_ID: TopicId = crate::LOGS_TOPIC_ID;
const LABEL_PREFIX: &str = "resource.loki.label.";
const METADATA_PREFIX: &str = "attr.loki.metadata.";
const TENANT_FIELD: &str = "resource.loki.tenant";
const MIN_APPEND_SUBMISSION_THREADS: usize = 8;
const MAX_APPEND_SUBMISSION_THREADS: usize = 64;
const MAX_DURABLE_SINK_THREADS: usize = 256;
const REMOTE_WRITE_LOCK_SHARDS: usize = 64;

fn new_remote_write_locks() -> Box<[Mutex<()>]> {
    (0..REMOTE_WRITE_LOCK_SHARDS)
        .map(|_| Mutex::new(()))
        .collect()
}

fn remote_write_lock_index(series: crate::SeriesFingerprint) -> usize {
    (series.get() as usize) % REMOTE_WRITE_LOCK_SHARDS
}

fn apply_analytics_log_filters(mut query: LogQuery, request: &AnalyticsScanRequest) -> LogQuery {
    for term in &request.terms {
        query = query.with_term(Arc::clone(term));
    }
    for token in &request.message_tokens {
        query = query.with_predicate(LogPredicate::message_token(
            Arc::clone(token),
            CaseSensitivity::Sensitive,
        ));
    }
    for token in &request.case_insensitive_message_tokens {
        query = query.with_predicate(LogPredicate::message_token(
            Arc::clone(token),
            CaseSensitivity::Insensitive,
        ));
    }
    for field in &request.labels {
        query = query.with_field(
            format!("{LABEL_PREFIX}{}", field.key),
            Arc::clone(&field.value),
        );
    }
    for field in &request.metadata {
        query = query.with_field(
            format!("{METADATA_PREFIX}{}", field.key),
            Arc::clone(&field.value),
        );
    }
    for field in &request.attributes {
        query = query.with_field(Arc::clone(&field.key), Arc::clone(&field.value));
    }
    for field in &request.resource_attributes {
        query = query.with_field(format!("resource.{}", field.key), Arc::clone(&field.value));
    }
    for field in &request.scope_attributes {
        query = query.with_field(format!("scope.{}", field.key), Arc::clone(&field.value));
    }
    if let Some(trace_id) = request.trace_id {
        query = query.with_field("otel.trace_id", trace_id.to_string());
    }
    if let Some(span_id) = request.span_id {
        query = query.with_field("otel.span_id", span_id.to_string());
    }
    if request.predicate != LogPredicate::MatchAll {
        query = query.with_predicate(normalize_analytics_predicate(&request.predicate));
    }
    query
}

fn analytics_predicate_needs_structural_fields(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => predicates
            .iter()
            .any(analytics_predicate_needs_structural_fields),
        LogPredicate::Not(predicate) => analytics_predicate_needs_structural_fields(predicate),
        _ => false,
    }
}

fn normalize_analytics_predicate(predicate: &LogPredicate) -> LogPredicate {
    match predicate {
        LogPredicate::FieldNumeric {
            key,
            comparison,
            value,
        } if key.as_ref() == "otel.severity_number" => {
            // OTLP records store their severity in the native field, while
            // Loki pushes expose the equivalent numeric metadata field.
            LogPredicate::or(vec![
                LogPredicate::field_numeric("otel.severity_number", *comparison, *value),
                LogPredicate::field_numeric(
                    "attr.loki.metadata.severity_number",
                    *comparison,
                    *value,
                ),
            ])
        }
        LogPredicate::And(predicates) => LogPredicate::and(
            predicates
                .iter()
                .map(normalize_analytics_predicate)
                .collect(),
        ),
        LogPredicate::Or(predicates) => LogPredicate::or(
            predicates
                .iter()
                .map(normalize_analytics_predicate)
                .collect(),
        ),
        LogPredicate::Not(predicate) => {
            LogPredicate::negate(normalize_analytics_predicate(predicate))
        }
        _ => predicate.clone(),
    }
}

fn build_append_submission_pool(
    physical_stripes: u32,
    configured_threads: Option<usize>,
) -> Result<ThreadPool, LokiApiError> {
    if configured_threads
        .is_some_and(|threads| !(1..=MAX_APPEND_SUBMISSION_THREADS).contains(&threads))
    {
        return Err(LokiApiError::configuration(
            "append submission threads must be in 1..=64",
        ));
    }
    let threads = configured_threads.unwrap_or_else(|| {
        usize::try_from(physical_stripes)
            .unwrap_or(MAX_APPEND_SUBMISSION_THREADS)
            .clamp(MIN_APPEND_SUBMISSION_THREADS, MAX_APPEND_SUBMISSION_THREADS)
    });
    ThreadPoolBuilder::new()
        .num_threads(threads)
        .thread_name(|index| format!("shard-telemetry-append-{index}"))
        .build()
        .map_err(|error| {
            LokiApiError::configuration(format!("failed to build append submission pool: {error}"))
        })
}

fn durable_sink_worker_count(physical_shards: u32, configured_threads: Option<usize>) -> usize {
    configured_threads.unwrap_or_else(|| {
        usize::try_from(physical_shards)
            .unwrap_or(MAX_DURABLE_SINK_THREADS)
            .min(MAX_DURABLE_SINK_THREADS)
    })
}

fn object_tier_partitions(partition_count: u32) -> Vec<TopicPartition> {
    let mut partitions = [
        crate::LOGS_TOPIC_ID,
        crate::TRACES_TOPIC_ID,
        crate::METRICS_TOPIC_ID,
    ]
    .into_iter()
    .flat_map(|topic_id| {
        (0..partition_count)
            .map(move |partition| TopicPartition::new(topic_id, LogicalPartitionId::new(partition)))
    })
    .collect::<Vec<_>>();
    partitions.sort_unstable();
    partitions
}

/// Standalone durable ShardTelemetry configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableTelemetryConfig {
    /// Directory containing shard-stream packs, coordinator state, and index journals.
    pub data_directory: PathBuf,
    /// Optional local object-store directory used by shard-stream and ShardTelemetry.
    pub object_store_directory: Option<PathBuf>,
    /// Optional production S3 or S3-compatible compressed object tier.
    pub s3_object_store: Option<S3ObjectStoreConfig>,
    /// Retain a second raw-payload journal for faster hot-index recovery.
    ///
    /// When disabled, startup reconstructs the ephemeral hot index from the
    /// authoritative shard-stream packs and ingestion performs one durable
    /// payload write.
    pub recovery_journal: bool,
    /// Logical retention window. `None` retains records indefinitely.
    ///
    /// Queries never expose records older than this duration. Physical byte
    /// reclamation is performed by tier compaction, independently of the
    /// immediate logical cutoff.
    pub retention: Option<Duration>,
    /// Number of physical single-owner stripes.
    pub shard_count: u32,
    /// Stable tenant partitions spread across physical stripes.
    pub tenant_partitions: u32,
    /// Maximum time shard-stream may collect adjacent appends before one write and sync.
    pub append_linger: Duration,
    /// Stripe block, index, dictionary, and locality limits.
    pub stripe: StripeConfig,
    /// Maximum time a durable append may wait for indexed read visibility.
    pub indexed_ack_timeout: Duration,
}

/// Bounded memory, journal, and SSD-cache limits for one local durable store.
///
/// These limits are independent of time-based retention. Retention controls
/// which timestamps remain queryable and eligible for physical reclamation;
/// this configuration bounds the hot in-memory heads and recoverable local
/// caches while that maintenance catches up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableTelemetryLimits {
    /// Per-signal in-memory head and query limits.
    pub signals: crate::ShardTelemetryConfig,
    /// Maximum bytes retained in each recovery index journal.
    pub max_index_journal_bytes: u64,
    /// Enables the local lifetime rollup with this distinct-series bound.
    pub max_lifetime_rollup_series: Option<usize>,
    /// Maximum encoded bytes for the enabled lifetime-rollup catalog.
    pub max_lifetime_rollup_bytes: u64,
    /// SSD and parsed-memory bounds for catalogs, manifests, and indexes.
    pub control_cache: SsdCacheConfig,
    /// SSD and verified-memory bounds for compressed payload ranges.
    pub payload_cache: SsdCacheConfig,
    /// Optional compressed object payload bound for each signal partition.
    ///
    /// This applies to a local object-store backend. S3-backed stores retain
    /// their complete archive remotely and use `payload_cache.max_bytes` as
    /// the bounded local recent-data tier.
    pub max_object_payload_bytes_per_partition: Option<u64>,
    /// Optional fixed append-submission worker count. Standalone stores derive
    /// a bounded pool from stripe count; server hosts pass their runtime CPU
    /// budget explicitly when they need strict thread-per-core ownership.
    pub append_submission_threads: Option<usize>,
    /// Optional fixed durable-sink dispatcher worker count. This controls the
    /// partition-striped callback dispatchers; physical shard sinks and their
    /// index workers remain one per shard. Standalone stores default to the
    /// physical shard count.
    pub durable_sink_threads: Option<usize>,
    /// Optional fixed S3 object-store runtime worker count. Standalone stores
    /// use the adapter's four-worker default when this is unset.
    pub object_store_threads: Option<usize>,
    /// Maximum queued append records in each shard-stream shard.
    pub queue_slots_per_shard: usize,
    /// Maximum queued append payload bytes in each shard-stream shard.
    pub queue_bytes_per_shard: usize,
    /// Target raw WAL pack size before rotation.
    pub target_pack_bytes: u64,
    /// Maximum payload bytes accepted by one shard-stream append batch.
    pub max_batch_bytes: usize,
    /// Maximum payload bytes returned by one shard-stream fetch.
    pub max_fetch_bytes: usize,
}

impl Default for DurableTelemetryLimits {
    fn default() -> Self {
        Self {
            signals: crate::ShardTelemetryConfig::default(),
            max_index_journal_bytes: 64 * 1024 * 1024 * 1024,
            max_lifetime_rollup_series: None,
            max_lifetime_rollup_bytes: 512 * 1024 * 1024,
            control_cache: SsdCacheConfig {
                max_bytes: 8 * 1024 * 1024 * 1024,
                ..SsdCacheConfig::default()
            },
            payload_cache: SsdCacheConfig::default(),
            max_object_payload_bytes_per_partition: None,
            append_submission_threads: None,
            durable_sink_threads: None,
            object_store_threads: None,
            queue_slots_per_shard: 1_024,
            queue_bytes_per_shard: 128 * 1024 * 1024,
            target_pack_bytes: 8 * 1024 * 1024,
            max_batch_bytes: 64 * 1024 * 1024,
            max_fetch_bytes: 64 * 1024 * 1024,
        }
    }
}

impl DurableTelemetryConfig {
    fn validate(&self) -> Result<(), LokiApiError> {
        if self.shard_count == 0 {
            return Err(LokiApiError::configuration("shard_count must be nonzero"));
        }
        if self.tenant_partitions == 0 {
            return Err(LokiApiError::configuration(
                "tenant_partitions must be nonzero",
            ));
        }
        if self.indexed_ack_timeout.is_zero() {
            return Err(LokiApiError::configuration(
                "indexed_ack_timeout must be nonzero",
            ));
        }
        if self.retention.is_some_and(|retention| retention.is_zero()) {
            return Err(LokiApiError::configuration(
                "retention must be nonzero when configured",
            ));
        }
        if self.retention.is_some()
            && !self.recovery_journal
            && self.object_store_directory.is_none()
            && self.s3_object_store.is_none()
        {
            return Err(LokiApiError::configuration(
                "retention requires either the immutable object tier or recovery_journal so the durable index checkpoint survives log truncation",
            ));
        }
        if self.object_store_directory.is_some() && self.s3_object_store.is_some() {
            return Err(LokiApiError::configuration(
                "local and S3 object-store backends are mutually exclusive",
            ));
        }
        Ok(())
    }
}

/// Signal-native store whose acknowledged writes are durable shard-stream
/// appends and whose reads execute on the owning ShardTelemetry stripe workers.
pub struct DurableTelemetryStore {
    _data_directory_lease: DataDirectoryLease,
    engine: Arc<StreamEngine>,
    service: TelemetryService,
    append_durability: Durability,
    append_gate: Option<Arc<dyn TelemetryAppendGate>>,
    tenant_partitions: u32,
    physical_shard_count: Option<u32>,
    telemetry_router: crate::TelemetryRouter,
    ingest_stripes_per_tenant: u32,
    indexed_ack_timeout: Duration,
    max_fetch_bytes: u32,
    append_submission_pool: ThreadPool,
    next_request_id: AtomicU64,
    append_receipts: AppendReceiptCatalog,
    lifetime_rollups: Option<Mutex<MetricRollupCatalog>>,
    remote_write_append: Box<[Mutex<()>]>,
    deletes: DeleteCatalog,
    retention: Option<Duration>,
    retention_runs: AtomicU64,
    retention_advanced_offsets: AtomicU64,
    retention_failures: AtomicU64,
    object_tier_enabled: bool,
    archive_object_tier: bool,
    max_object_payload_bytes_per_partition: Option<u64>,
    source_reclaimed_offsets: AtomicU64,
    retired_object_groups: AtomicU64,
    retired_object_payload_bytes: AtomicU64,
    retired_object_keys: AtomicU64,
}

/// Product-owned admission check evaluated before a durable telemetry append.
///
/// Embedded and standalone stores leave this unset. HA hosts install a gate
/// that verifies readiness, leadership, and fencing for every routed
/// partition. Keeping the check at the store boundary ensures that native,
/// OTLP, Loki, Prometheus, and direct Rust ingestion share the same safety
/// invariant.
pub trait TelemetryAppendGate: Send + Sync + std::fmt::Debug + 'static {
    /// Returns `Ok` only when this process may append every supplied partition.
    fn check_append_partitions(
        &self,
        partitions: &[crate::NativePartitionAppend],
    ) -> Result<(), String>;
}

/// Durability required before ShardTelemetry acknowledges a WAL append.
///
/// Standalone and embedded stores use [`Leader`](Self::Leader). Private HA
/// hosts attach to their already-configured replicated shard-stream engine
/// with [`Quorum`](Self::Quorum), so a native acknowledgement cannot outrun
/// the configured in-sync replica set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TelemetryAppendDurability {
    /// Acknowledge once the local authoritative WAL has committed the batch.
    Leader,
    /// Acknowledge only after shard-stream's configured replica quorum commits.
    Quorum,
}

/// Externally owned shard-stream engine and signal-service attachment.
///
/// Private HA hosts build this after installing [`TelemetrySinkFactory`] into
/// their replicated engine and before exposing their telemetry protocol
/// listeners. The attachment cannot open a second WAL or replace the host's
/// replication, assignment, or fencing policy.
#[derive(Clone)]
pub struct TelemetryHostAttachment {
    /// Product-owned directory for delete state, retry receipts, and query
    /// metadata. It must not be shared by more than one local process.
    pub data_directory: PathBuf,
    /// The host's already-opened authoritative stream engine.
    pub engine: Arc<StreamEngine>,
    /// Query service returned by the exact sink factory installed in `engine`.
    pub service: TelemetryService,
    /// Common logical signal partition count.
    pub tenant_partitions: u32,
    /// Number of physical sink-owner stripes selected by the host.
    pub ingest_stripes_per_tenant: u32,
    /// Bound for waiting on local query-index visibility.
    pub indexed_ack_timeout: Duration,
    /// Optional logical retention window.
    pub retention: Option<Duration>,
    /// WAL durability required before an append acknowledgement.
    pub append_durability: TelemetryAppendDurability,
    /// Optional HA admission and leader-fencing check for every append path.
    pub append_gate: Option<Arc<dyn TelemetryAppendGate>>,
}

impl std::fmt::Debug for TelemetryHostAttachment {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetryHostAttachment")
            .field("data_directory", &self.data_directory)
            .field("tenant_partitions", &self.tenant_partitions)
            .field("ingest_stripes_per_tenant", &self.ingest_stripes_per_tenant)
            .field("indexed_ack_timeout", &self.indexed_ack_timeout)
            .field("retention", &self.retention)
            .field("append_durability", &self.append_durability)
            .field("append_gate_configured", &self.append_gate.is_some())
            .finish_non_exhaustive()
    }
}

impl From<TelemetryAppendDurability> for Durability {
    fn from(value: TelemetryAppendDurability) -> Self {
        match value {
            TelemetryAppendDurability::Leader => Self::Leader,
            TelemetryAppendDurability::Quorum => Self::Quorum,
        }
    }
}

const APPEND_RECEIPTS_VERSION: u8 = 1;
const MAX_NATIVE_APPEND_RECEIPTS: usize = 65_536;
const NATIVE_APPEND_RECEIPT_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// One source batch recovered from the local WAL for durable upstream offload.
#[derive(Debug, Clone)]
pub struct FetchedTelemetryBatch {
    /// Source partition from which the authoritative envelope was read.
    pub topic_partition: TopicPartition,
    /// First local durable offset covered by the envelope.
    pub first_offset: LogicalOffset,
    /// Last local durable offset covered by the envelope.
    pub last_offset: LogicalOffset,
    /// Validated signal-native envelope. Its payload remains byte-identical to
    /// the source WAL and can be sent through the native append protocol.
    pub envelope: crate::TelemetryEnvelope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAppendReceipt {
    request_id: String,
    payload_digest: String,
    recorded_at_unix_nanos: u64,
    acknowledgement: crate::NativeTelemetryAppendAck,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedAppendReceipts {
    version: u8,
    receipts: Vec<PersistedAppendReceipt>,
}

#[derive(Debug)]
struct AppendReceiptCatalog {
    directory: PathBuf,
    directory_sync: File,
    state: Mutex<AppendReceiptState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct AppendReceiptState {
    receipts: BTreeMap<String, AppendReceiptStateEntry>,
    completed_by_time: BTreeSet<(u64, String)>,
}

#[derive(Debug)]
enum AppendReceiptStateEntry {
    Pending { payload_digest: String },
    Complete(PersistedAppendReceipt),
}

enum AppendReceiptReservation {
    Existing(crate::NativeTelemetryAppendAck),
    Reserved,
}

impl AppendReceiptCatalog {
    fn open(data_directory: &Path) -> Result<Self, LokiApiError> {
        let directory = data_directory.join("native-append-receipts-v2");
        fs::create_dir_all(&directory).map_err(receipt_io_error)?;
        let mut state = AppendReceiptState::default();
        let mut found_v2_receipt = false;
        for entry in fs::read_dir(&directory).map_err(receipt_io_error)? {
            let entry = entry.map_err(receipt_io_error)?;
            let path = entry.path();
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            found_v2_receipt = true;
            let receipt = serde_json::from_slice::<PersistedAppendReceipt>(
                &fs::read(&path).map_err(receipt_io_error)?,
            )
            .map_err(|error| {
                LokiApiError::configuration(format!(
                    "native append receipt {} is invalid: {error}",
                    path.display()
                ))
            })?;
            Self::insert_recovered(&mut state, receipt)?;
        }

        // v1 kept every receipt in one ever-growing JSON document. Migrate it
        // once to independently durable, bounded v2 records without asking an
        // operator to delete the old checkpoint and risk replaying data.
        let legacy_path = data_directory.join("native-append-receipts-v1.json");
        if !found_v2_receipt && legacy_path.exists() {
            let persisted = match fs::read(&legacy_path) {
                Ok(bytes) => {
                    serde_json::from_slice::<PersistedAppendReceipts>(&bytes).map_err(|error| {
                        LokiApiError::configuration(format!(
                            "native append receipt journal {} is invalid: {error}",
                            legacy_path.display()
                        ))
                    })?
                }
                Err(error) => {
                    return Err(LokiApiError::internal(format!(
                        "native append receipt journal {} cannot be read: {error}",
                        legacy_path.display()
                    )));
                }
            };
            if persisted.version != APPEND_RECEIPTS_VERSION {
                return Err(LokiApiError::configuration(
                    "unsupported native append receipt journal version",
                ));
            }
            for receipt in persisted.receipts {
                Self::insert_recovered(&mut state, receipt)?;
            }
        }
        let catalog = Self {
            directory_sync: File::open(&directory).map_err(receipt_io_error)?,
            directory,
            state: Mutex::new(state),
            changed: Condvar::new(),
        };
        let cutoff =
            unix_nanos_now().saturating_sub(duration_to_nanos(NATIVE_APPEND_RECEIPT_RETENTION));
        let mut state = catalog
            .state
            .lock()
            .map_err(|_| LokiApiError::internal("native append receipt lock poisoned"))?;
        catalog.prune_locked(&mut state, cutoff, MAX_NATIVE_APPEND_RECEIPTS)?;
        for receipt in state.receipts.values() {
            if let AppendReceiptStateEntry::Complete(receipt) = receipt {
                let path = catalog.receipt_path(&receipt.request_id);
                if !path.exists() {
                    catalog.persist_receipt(receipt)?;
                }
            }
        }
        drop(state);
        Ok(catalog)
    }

    fn insert_recovered(
        state: &mut AppendReceiptState,
        receipt: PersistedAppendReceipt,
    ) -> Result<(), LokiApiError> {
        if receipt.request_id.len() != 32
            || receipt.payload_digest.len() != 64
            || state.receipts.contains_key(&receipt.request_id)
        {
            return Err(LokiApiError::configuration(
                "native append receipt journal contains an invalid or duplicate receipt",
            ));
        }
        state
            .completed_by_time
            .insert((receipt.recorded_at_unix_nanos, receipt.request_id.clone()));
        state.receipts.insert(
            receipt.request_id.clone(),
            AppendReceiptStateEntry::Complete(receipt),
        );
        Ok(())
    }

    fn reserve(
        &self,
        request_id: u128,
        payload_digest: &str,
    ) -> Result<AppendReceiptReservation, LokiApiError> {
        let key = request_key(request_id);
        let mut state = self
            .state
            .lock()
            .map_err(|_| LokiApiError::internal("native append receipt lock poisoned"))?;
        loop {
            match state.receipts.get(&key) {
                Some(AppendReceiptStateEntry::Complete(receipt)) => {
                    if receipt.payload_digest != payload_digest {
                        return Err(LokiApiError::bad_request(
                            "native retry ID was reused with different telemetry content",
                        ));
                    }
                    return Ok(AppendReceiptReservation::Existing(
                        receipt.acknowledgement.clone(),
                    ));
                }
                Some(AppendReceiptStateEntry::Pending {
                    payload_digest: pending_digest,
                }) => {
                    if pending_digest != payload_digest {
                        return Err(LokiApiError::bad_request(
                            "native retry ID was reused with different telemetry content",
                        ));
                    }
                    state = self.changed.wait(state).map_err(|_| {
                        LokiApiError::internal("native append receipt lock poisoned")
                    })?;
                }
                None => {
                    let cutoff = unix_nanos_now()
                        .saturating_sub(duration_to_nanos(NATIVE_APPEND_RECEIPT_RETENTION));
                    self.prune_locked(&mut state, cutoff, MAX_NATIVE_APPEND_RECEIPTS - 1)?;
                    if state.receipts.len() >= MAX_NATIVE_APPEND_RECEIPTS {
                        return Err(LokiApiError::unavailable(
                            "native append idempotency window is at capacity",
                        ));
                    }
                    state.receipts.insert(
                        key,
                        AppendReceiptStateEntry::Pending {
                            payload_digest: payload_digest.to_owned(),
                        },
                    );
                    return Ok(AppendReceiptReservation::Reserved);
                }
            }
        }
    }

    fn complete(
        &self,
        request_id: u128,
        payload_digest: String,
        acknowledgement: crate::NativeTelemetryAppendAck,
    ) -> Result<(), LokiApiError> {
        let request_id = request_key(request_id);
        let receipt = PersistedAppendReceipt {
            request_id: request_id.clone(),
            payload_digest,
            recorded_at_unix_nanos: unix_nanos_now(),
            acknowledgement,
        };
        self.persist_receipt(&receipt)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| LokiApiError::internal("native append receipt lock poisoned"))?;
        match state.receipts.remove(&request_id) {
            Some(AppendReceiptStateEntry::Pending { .. }) => {}
            Some(AppendReceiptStateEntry::Complete(_)) => {
                return Err(LokiApiError::internal(
                    "native append receipt completed more than once",
                ));
            }
            None => {
                return Err(LokiApiError::internal(
                    "native append receipt reservation disappeared",
                ));
            }
        }
        state
            .completed_by_time
            .insert((receipt.recorded_at_unix_nanos, request_id.clone()));
        state
            .receipts
            .insert(request_id, AppendReceiptStateEntry::Complete(receipt));
        self.changed.notify_all();
        Ok(())
    }

    fn abandon(&self, request_id: u128) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if matches!(
            state.receipts.get(&request_key(request_id)),
            Some(AppendReceiptStateEntry::Pending { .. })
        ) {
            state.receipts.remove(&request_key(request_id));
            self.changed.notify_all();
        }
    }

    fn retain_since(&self, cutoff_unix_nanos: u64) -> Result<(), LokiApiError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LokiApiError::internal("native append receipt lock poisoned"))?;
        self.prune_locked(&mut state, cutoff_unix_nanos, MAX_NATIVE_APPEND_RECEIPTS)
    }

    fn prune_locked(
        &self,
        state: &mut AppendReceiptState,
        cutoff_unix_nanos: u64,
        maximum_entries: usize,
    ) -> Result<(), LokiApiError> {
        while state
            .completed_by_time
            .first()
            .is_some_and(|(recorded_at, _)| {
                *recorded_at < cutoff_unix_nanos || state.completed_by_time.len() > maximum_entries
            })
        {
            let (_, request_id) = state
                .completed_by_time
                .pop_first()
                .expect("first append receipt exists");
            let Some(AppendReceiptStateEntry::Complete(receipt)) =
                state.receipts.remove(&request_id)
            else {
                return Err(LokiApiError::internal(
                    "native append receipt indexes are inconsistent",
                ));
            };
            fs::remove_file(self.receipt_path(&receipt.request_id)).map_err(receipt_io_error)?;
        }
        Ok(())
    }

    fn persist_receipt(&self, receipt: &PersistedAppendReceipt) -> Result<(), LokiApiError> {
        let encoded = serde_json::to_vec(receipt).map_err(|error| {
            LokiApiError::internal(format!("native receipt serialization failed: {error}"))
        })?;
        let path = self.receipt_path(&receipt.request_id);
        let temporary = temporary_path(&path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(receipt_io_error)?;
        file.write_all(&encoded)
            // The temporary file is atomically renamed below and the parent
            // directory is synced after the rename. The receipt bytes and
            // length are therefore the only file state that must be flushed
            // before the directory entry becomes durable.
            .and_then(|()| file.sync_data())
            .map_err(receipt_io_error)?;
        fs::rename(&temporary, &path).map_err(receipt_io_error)?;
        self.directory_sync.sync_all().map_err(receipt_io_error)?;
        Ok(())
    }

    fn receipt_path(&self, request_id: &str) -> PathBuf {
        self.directory.join(format!("{request_id}.json"))
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

fn request_key(request_id: u128) -> String {
    format!("{request_id:032x}")
}

fn unix_nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn receipt_io_error(error: std::io::Error) -> LokiApiError {
    LokiApiError::internal(format!("native append receipt journal I/O failed: {error}"))
}

/// Result of one batch-aligned physical retention pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionReport {
    /// Timestamp cutoff applied to every partition.
    pub cutoff_timestamp_unix_nanos: u64,
    /// Partitions whose durable log start advanced.
    pub advanced_partitions: u64,
    /// Logical records made eligible for pack reclamation.
    pub advanced_offsets: u64,
    /// Complete compressed groups removed from signal catalogs.
    pub retired_object_groups: u64,
    /// Compressed payload bytes removed from signal catalogs.
    pub retired_object_payload_bytes: u64,
    /// Exact object keys transferred to deferred reclamation.
    pub retired_object_keys: u64,
}

/// Result of one durable local lifetime-rollup checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LifetimeRollupReport {
    /// Distinct metric series represented by the local rollup catalog.
    pub series: usize,
    /// Raw metric points newly incorporated during this checkpoint.
    pub incorporated_points: u64,
}

impl std::fmt::Debug for DurableTelemetryStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableTelemetryStore")
            .field("tenant_partitions", &self.tenant_partitions)
            .field("ingest_stripes_per_tenant", &self.ingest_stripes_per_tenant)
            .field("indexed_ack_timeout", &self.indexed_ack_timeout)
            .field(
                "append_submission_threads",
                &self.append_submission_pool.current_num_threads(),
            )
            .finish_non_exhaustive()
    }
}

fn same_remote_write_sample_payload(
    left: &crate::DurableMetricPoint,
    right: &crate::DurableMetricPoint,
) -> bool {
    left.timestamp_unix_nanos == right.timestamp_unix_nanos
        && left.start_time_unix_nanos == right.start_time_unix_nanos
        && left.flags == right.flags
        && left.value == right.value
        && left.exemplars == right.exemplars
}

fn relevance_message_predicate_only(predicate: &LogPredicate) -> bool {
    match predicate {
        LogPredicate::MatchAll
        | LogPredicate::MatchNone
        | LogPredicate::MessagePhrase { .. }
        | LogPredicate::MessageFuzzy { .. } => true,
        LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        }
        | LogPredicate::MessageTokenRegex(_)
        | LogPredicate::MessageTokenPrefix {
            case_sensitivity: CaseSensitivity::Insensitive,
            ..
        } => true,
        LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
            predicates.iter().all(relevance_message_predicate_only)
        }
        LogPredicate::Not(predicate) => relevance_message_predicate_only(predicate),
        LogPredicate::Term(_)
        | LogPredicate::Message(_)
        | LogPredicate::MessageRegex(_)
        | LogPredicate::FieldExists(_)
        | LogPredicate::Field { .. }
        | LogPredicate::FieldIn { .. }
        | LogPredicate::FieldRegex { .. }
        | LogPredicate::FieldNumeric { .. }
        | LogPredicate::MessageToken {
            case_sensitivity: CaseSensitivity::Sensitive,
            ..
        }
        | LogPredicate::MessageTokenPrefix {
            case_sensitivity: CaseSensitivity::Sensitive,
            ..
        } => false,
    }
}

struct RelevanceHeapItem {
    score: f64,
    timestamp_unix_nanos: i64,
    offset: u64,
    row: AnalyticsRow,
}

struct MessageRelevanceHeapItem {
    score: f64,
    timestamp_unix_nanos: i64,
    offset: u64,
    partition: u32,
    message: Arc<str>,
}

impl PartialEq for RelevanceHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == CmpOrdering::Equal
            && self.timestamp_unix_nanos == other.timestamp_unix_nanos
            && self.offset == other.offset
    }
}

impl Eq for RelevanceHeapItem {}

impl PartialOrd for RelevanceHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for RelevanceHeapItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.timestamp_unix_nanos.cmp(&other.timestamp_unix_nanos))
            .then_with(|| self.offset.cmp(&other.offset))
    }
}

impl PartialEq for MessageRelevanceHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == CmpOrdering::Equal
            && self.timestamp_unix_nanos == other.timestamp_unix_nanos
            && self.offset == other.offset
    }
}

impl Eq for MessageRelevanceHeapItem {}

impl PartialOrd for MessageRelevanceHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for MessageRelevanceHeapItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.timestamp_unix_nanos.cmp(&other.timestamp_unix_nanos))
            .then_with(|| self.offset.cmp(&other.offset))
    }
}

fn log_field_maps(
    fields: &[MetadataField],
) -> (BTreeMap<String, String>, BTreeMap<String, String>) {
    let mut labels = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    for field in fields {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name.to_owned(), field.value.to_string());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            metadata.insert(name.to_owned(), field.value.to_string());
        }
    }
    (labels, metadata)
}

fn analytics_row_from_match(
    tenant: &Arc<str>,
    matched: LogMatch,
) -> Result<crate::analytics::AnalyticsRow, LokiApiError> {
    let (labels, metadata) = log_field_maps(&matched.record.fields);
    crate::analytics::log_row(tenant, &matched.record, labels, metadata)
}

fn analytics_row_and_entry(
    tenant: &Arc<str>,
    matched: LogMatch,
) -> Result<(AnalyticsRow, LokiEntry), LokiApiError> {
    let (labels, metadata) = log_field_maps(&matched.record.fields);
    let timestamp_unix_nanos = i64::try_from(matched.record.timestamp_unix_nanos)
        .map_err(|_| LokiApiError::internal("timestamp exceeds ClickHouse i64 range"))?;
    let entry = LokiEntry {
        timestamp_unix_nanos,
        labels: labels.clone(),
        line: matched.record.message.to_string(),
        structured_metadata: metadata.clone(),
    };
    let row = crate::analytics::log_row(tenant, &matched.record, labels, metadata)?;
    Ok((row, entry))
}

fn log_match_to_entry(matched: LogMatch) -> Result<LokiEntry, LokiApiError> {
    let mut labels = BTreeMap::new();
    let mut structured_metadata = BTreeMap::new();
    for field in matched.record.fields.iter() {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name.to_owned(), field.value.to_string());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            structured_metadata.insert(name.to_owned(), field.value.to_string());
        }
    }
    Ok(LokiEntry {
        timestamp_unix_nanos: i64::try_from(matched.record.timestamp_unix_nanos)
            .map_err(|_| LokiApiError::internal("timestamp exceeds Loki i64 range"))?,
        labels,
        line: matched.record.message.to_string(),
        structured_metadata,
    })
}

fn native_log_match_bytes(matched: &LogMatch) -> usize {
    let mut labels = BTreeMap::<&str, &str>::new();
    let mut metadata = BTreeMap::<&str, &str>::new();
    for field in matched.record.fields.iter() {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name, field.value.as_ref());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            metadata.insert(name, field.value.as_ref());
        }
    }
    labels
        .iter()
        .chain(metadata.iter())
        .fold(matched.record.message.len(), |bytes, (key, value)| {
            bytes.saturating_add(key.len()).saturating_add(value.len())
        })
}

fn engine_error(error: EngineError) -> LokiApiError {
    match error {
        EngineError::InvalidConfig(message) => LokiApiError::bad_request(message),
        error => LokiApiError::internal(error.to_string()),
    }
}
