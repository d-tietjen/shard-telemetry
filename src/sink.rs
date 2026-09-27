//! Durable sink ownership: factory and worker lifecycle, signal and log reads, and object-tier work.
//! The root keeps shared state and public configuration; child modules own execution paths.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::fs;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use foldhash::HashSet;
use shard_stream_core::{ShardId, TopicPartition};
use shard_stream_engine::{
    DurableAppend, DurableAppendSink, DurableAppendSinkFactory, DurableSinkApply,
    DurableSinkCheckpoint, EngineError, EngineResult,
};

use crate::analytics::RelevanceScorer;
use crate::correlation::{metric_matches_correlation, span_matches_correlation};
use crate::ingest_pack::validate_ingest_pack;
use crate::metric::{metric_exact_series_point_matches, metric_query_matches};
use crate::sink_journal::{SinkJournal, checkpoint_allows_lane_gap};
use crate::stripe::LogMessageMatch;
use crate::tier::CachedObjectRange;
use crate::trace::{TraceProjection, decode_trace_block_matching};
use crate::{
    CorrelationConfig, CorrelationIndex, CorrelationQuery, DictionaryCatalog, DurableMetricPoint,
    DurableSpan, LogMatch, LogPredicate, LogQuery, LogStripe, MetricApplyOutcome,
    MetricIngestProtocol, MetricQuery, MetricStripe, ObjectMetadata, ObjectStoreStats,
    ObjectTierConfig, RealtimeDictionaryObserver, RealtimeDictionaryTrainer, ShardTelemetryConfig,
    SharedTelemetryObjectStore, SsdCacheConfig, SsdCacheStats, SsdObjectCache, StripeConfig,
    TelemetryEnvelope, TelemetryError, TelemetryObjectTier, TelemetryRecordRef, TelemetryResult,
    TelemetryRouter, TelemetrySignal, TierArtifactKind, TierCheckpoint, TierQueryRange,
    TierRetentionReport, TraceApplyOutcome, TraceId, TraceQuery, TraceStripe, decode_metric_chunk,
    decode_signal_recovery_state, decode_trace_block, stage_signal_group,
};

mod factory;
mod indexing;
mod object_tier;
mod service_logs;
mod service_routing;
mod service_signals;
mod service_tier;
mod signal_query;
#[cfg(test)]
mod tests;
mod utilities;
mod worker;
use indexing::*;
use object_tier::*;
use signal_query::*;
use utilities::*;
use worker::*;

/// Immutable object-tier and bounded SSD-cache settings shared by sink stripes.
#[derive(Debug, Clone)]
pub struct SinkObjectTierConfig {
    /// Object-store adapter used for immutable data and catalog publication.
    pub store: SharedTelemetryObjectStore,
    /// Local crash-safe staging directory for artifacts being published.
    pub spool_directory: PathBuf,
    /// Local SSD directory reserved for catalog, manifest, and index objects.
    pub control_cache_directory: PathBuf,
    /// Local SSD directory reserved for compressed payload ranges.
    pub payload_cache_directory: PathBuf,
    /// Logical partitions whose catalogs must be opened without object listing.
    pub partitions: Vec<TopicPartition>,
    /// Immutable group and catalog bounds.
    pub tier: ObjectTierConfig,
    /// Recoverable control/index cache bounds.
    pub control_cache: SsdCacheConfig,
    /// Recoverable payload cache bounds.
    pub payload_cache: SsdCacheConfig,
    /// Admit newly published payloads and indexes to the local caches.
    ///
    /// This is enabled for S3-backed embedded stores so recent data is local
    /// immediately after upload instead of only after its first query.
    pub warm_local_cache_on_publish: bool,
}

/// Cache occupancy and object-read counters for the isolated cold tiers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObjectTierCacheStats {
    /// Catalog, manifest, recovery, and query-index cache.
    pub control: SsdCacheStats,
    /// Compressed log, trace, and metric payload cache.
    pub payload: SsdCacheStats,
}

/// Exact timestamp probes used by serialized Remote Write conflict checks.
#[derive(Debug, Clone)]
pub(crate) struct MetricTimestampQuery {
    pub(crate) tenant: Arc<str>,
    pub(crate) partition: TopicPartition,
    pub(crate) series: crate::SeriesFingerprint,
    pub(crate) timestamps: Arc<[u64]>,
}

/// Configuration for shard-telemetry's per-shard native and OTLP index sinks.
#[derive(Debug, Clone)]
pub struct OtlpSinkConfig {
    /// Hot index and dictionary-cache limits for each physical shard.
    pub stripe: StripeConfig,
    /// Per-signal partition, retention, head-memory, and query limits.
    pub signals: ShardTelemetryConfig,
    /// Bounded stripe-local links across signal metadata and labels.
    pub correlations: CorrelationConfig,
    /// Number of durable append batches waiting to be indexed per shard.
    pub queue_slots: usize,
    /// Directory containing crash-safe stripe-local sink journals.
    ///
    /// `None` is intended only for tests and embedded ephemeral operation.
    pub state_directory: Option<PathBuf>,
    /// Maximum bytes retained in each physical stripe's sink journal.
    pub max_journal_bytes: u64,
    /// Sync every journal transaction when the journal is needed to survive
    /// source-WAL reclamation. Non-retention stores batch journal barriers so
    /// the authoritative WAL remains the fast durable path.
    pub journal_sync_each_append: bool,
    /// Optional immutable object tier for bounded recovery and cold queries.
    pub object_tier: Option<SinkObjectTierConfig>,
}

impl Default for OtlpSinkConfig {
    fn default() -> Self {
        Self {
            stripe: StripeConfig::default(),
            signals: ShardTelemetryConfig::default(),
            correlations: CorrelationConfig::default(),
            queue_slots: 256,
            state_directory: None,
            max_journal_bytes: 64 * 1024 * 1024 * 1024,
            journal_sync_each_append: true,
            object_tier: None,
        }
    }
}

impl OtlpSinkConfig {
    fn validate(&self) -> TelemetryResult<()> {
        self.signals.validate()?;
        if self.correlations.max_keys == 0
            || self.correlations.max_refs_per_key == 0
            || self.correlations.max_total_refs == 0
        {
            return Err(TelemetryError::InvalidConfig(
                "correlation index bounds must be nonzero",
            ));
        }
        if self.queue_slots == 0 {
            return Err(TelemetryError::InvalidConfig(
                "log sink queue_slots must be nonzero",
            ));
        }
        if self.max_journal_bytes < 8 {
            return Err(TelemetryError::InvalidConfig(
                "log sink max_journal_bytes must fit its header",
            ));
        }
        if self.object_tier.as_ref().is_some_and(|tier| {
            tier.partitions.is_empty() || tier.partitions.windows(2).any(|pair| pair[0] >= pair[1])
        }) {
            return Err(TelemetryError::InvalidConfig(
                "object-tier partitions must be nonempty, sorted, and unique",
            ));
        }
        if self
            .object_tier
            .as_ref()
            .is_some_and(|tier| tier.control_cache_directory == tier.payload_cache_directory)
        {
            return Err(TelemetryError::InvalidConfig(
                "control and payload caches require separate directories",
            ));
        }
        Ok(())
    }
}

fn checkpoint_covers(checkpoint: DurableSinkCheckpoint, candidate: DurableSinkCheckpoint) -> bool {
    checkpoint.topic_partition == candidate.topic_partition
        && checkpoint.next_placement_sequence >= candidate.next_placement_sequence
        && checkpoint.next_offset >= candidate.next_offset
}

fn merge_recovered_checkpoint(
    checkpoints: &mut HashMap<TopicPartition, DurableSinkCheckpoint>,
    candidate: DurableSinkCheckpoint,
) -> TelemetryResult<()> {
    match checkpoints.get(&candidate.topic_partition).copied() {
        None => {
            checkpoints.insert(candidate.topic_partition, candidate);
        }
        Some(current) if checkpoint_covers(current, candidate) => {}
        Some(current) if checkpoint_covers(candidate, current) => {
            checkpoints.insert(candidate.topic_partition, candidate);
        }
        Some(_) => {
            return Err(TelemetryError::CorruptTier(
                "object-tier checkpoints are not monotonically comparable".into(),
            ));
        }
    }
    Ok(())
}

/// Builds one owner-only log index worker for every shard-stream physical shard.
///
/// The factory validates each grouped native batch or OTLP protobuf before
/// shard-stream reserves an offset range. Once the primary append is durable,
/// shard-stream delivers the batch to the matching sink, whose dedicated
/// worker owns the mutable [`LogStripe`] without a shared-map lock on the
/// indexing path.
#[derive(Debug)]
pub struct TelemetrySinkFactory {
    config: OtlpSinkConfig,
    available: Mutex<HashMap<ShardId, TelemetryStripeState>>,
    checkpoints: Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    journals: Mutex<HashMap<ShardId, Arc<SinkJournal>>>,
    query_workers: Arc<RwLock<QueryWorkerRegistry>>,
    correlation_buffers: Arc<Mutex<CorrelationBufferPool>>,
    active_log_partition_cache: Arc<Mutex<HashMap<Arc<str>, Vec<TopicPartition>>>>,
    validated_signal_cache: Arc<ValidatedSignalCache>,
    tier_caches: Option<TierCaches>,
    object_store: Option<SharedTelemetryObjectStore>,
}

const MAX_VALIDATED_SIGNAL_CACHE_ENTRIES: usize = 32;
const MAX_VALIDATED_SIGNAL_CACHE_BYTES: usize = 8 * 1024 * 1024;
const VALIDATED_SIGNAL_CACHE_SHARDS: usize = 8;
const MAX_VALIDATED_SIGNAL_CACHE_ENTRIES_PER_SHARD: usize =
    MAX_VALIDATED_SIGNAL_CACHE_ENTRIES / VALIDATED_SIGNAL_CACHE_SHARDS;
const MAX_VALIDATED_SIGNAL_CACHE_BYTES_PER_SHARD: usize =
    MAX_VALIDATED_SIGNAL_CACHE_BYTES / VALIDATED_SIGNAL_CACHE_SHARDS;
const MAX_CORRELATION_BUFFER_POOL_ENTRIES: usize = 64;
const MAX_CORRELATION_BUFFER_CAPACITY: usize = 16_384;
const MAX_FANOUT_RESULT_PREALLOC: usize = 64 * 1024;

type ProjectedQueryResponse = (ShardId, TelemetryResult<Vec<LogMatch>>);

#[derive(Clone)]
struct IndexedProjectedQuery {
    index: usize,
    query: LogQuery,
}

thread_local! {
    static TARGETED_PROJECTED_QUERY_RESPONSE: RefCell<Option<(
        SyncSender<ProjectedQueryResponse>,
        Receiver<ProjectedQueryResponse>,
    )>> = const { RefCell::new(None) };
}

fn fanout_result_capacity(limit: Option<usize>, workers: usize) -> usize {
    limit
        .map(|limit| {
            limit
                .saturating_mul(workers)
                .min(MAX_FANOUT_RESULT_PREALLOC)
        })
        .unwrap_or_default()
}

fn sort_and_limit<T>(
    values: &mut Vec<T>,
    limit: Option<usize>,
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
) {
    let Some(limit) = limit else {
        values.sort_unstable_by(compare);
        return;
    };
    if values.len() > limit {
        {
            let (selected, _, _) = values.select_nth_unstable_by(limit, &mut compare);
            selected.sort_unstable_by(&mut compare);
        }
        values.truncate(limit);
    } else {
        values.sort_unstable_by(compare);
    }
}

#[derive(Debug, Default)]
struct CorrelationBufferPool {
    buffers: Vec<Vec<TelemetryRecordRef>>,
}

impl CorrelationBufferPool {
    fn take(&mut self) -> Vec<TelemetryRecordRef> {
        self.buffers.pop().unwrap_or_default()
    }

    fn recycle(&mut self, mut buffer: Vec<TelemetryRecordRef>) {
        if buffer.capacity() > MAX_CORRELATION_BUFFER_CAPACITY
            || self.buffers.len() >= MAX_CORRELATION_BUFFER_POOL_ENTRIES
        {
            return;
        }
        buffer.clear();
        self.buffers.push(buffer);
    }
}

#[derive(Debug)]
enum ValidatedSignalPayload {
    Traces(Vec<DurableSpan>),
    Metrics(Vec<DurableMetricPoint>),
}

#[derive(Debug, Default)]
struct ValidatedSignalCacheShard {
    entries: HashMap<[u8; 32], (usize, ValidatedSignalPayload)>,
    order: VecDeque<[u8; 32]>,
    bytes: usize,
}

impl ValidatedSignalCacheShard {
    fn insert(&mut self, key: [u8; 32], payload_bytes: usize, payload: ValidatedSignalPayload) {
        if payload_bytes > MAX_VALIDATED_SIGNAL_CACHE_BYTES_PER_SHARD {
            return;
        }
        self.remove(key);
        while (self.entries.len() >= MAX_VALIDATED_SIGNAL_CACHE_ENTRIES_PER_SHARD
            || self.bytes.saturating_add(payload_bytes)
                > MAX_VALIDATED_SIGNAL_CACHE_BYTES_PER_SHARD)
            && !self.entries.is_empty()
        {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some((oldest_bytes, _)) = self.entries.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(oldest_bytes);
            }
        }
        self.bytes = self.bytes.saturating_add(payload_bytes);
        self.order.push_back(key);
        self.entries.insert(key, (payload_bytes, payload));
    }

    fn take(&mut self, key: [u8; 32]) -> Option<ValidatedSignalPayload> {
        let (payload_bytes, payload) = self.entries.remove(&key)?;
        self.bytes = self.bytes.saturating_sub(payload_bytes);
        if let Some(position) = self.order.iter().position(|queued| *queued == key) {
            self.order.remove(position);
        }
        Some(payload)
    }

    fn remove(&mut self, key: [u8; 32]) {
        if let Some((payload_bytes, _)) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(payload_bytes);
            if let Some(position) = self.order.iter().position(|queued| *queued == key) {
                self.order.remove(position);
            }
        }
    }
}

#[derive(Debug)]
struct ValidatedSignalCache {
    shards: Box<[Mutex<ValidatedSignalCacheShard>]>,
}

impl Default for ValidatedSignalCache {
    fn default() -> Self {
        Self {
            shards: (0..VALIDATED_SIGNAL_CACHE_SHARDS)
                .map(|_| Mutex::new(ValidatedSignalCacheShard::default()))
                .collect(),
        }
    }
}

impl ValidatedSignalCache {
    fn shard(&self, key: [u8; 32]) -> &Mutex<ValidatedSignalCacheShard> {
        let shard =
            usize::from(u16::from_le_bytes([key[0], key[1]])) % VALIDATED_SIGNAL_CACHE_SHARDS;
        &self.shards[shard]
    }

    fn insert(&self, key: [u8; 32], payload_bytes: usize, payload: ValidatedSignalPayload) {
        if let Ok(mut shard) = self.shard(key).lock() {
            shard.insert(key, payload_bytes, payload);
        }
    }

    fn take(&self, key: [u8; 32]) -> Option<ValidatedSignalPayload> {
        self.shard(key).lock().ok()?.take(key)
    }
}

#[derive(Debug, Default)]
struct QueryWorkerRegistry {
    by_shard: HashMap<ShardId, SyncSender<SinkCommand>>,
    ordered: Vec<(ShardId, SyncSender<SinkCommand>)>,
}

impl QueryWorkerRegistry {
    fn insert(&mut self, shard_id: ShardId, sender: SyncSender<SinkCommand>) {
        self.by_shard.insert(shard_id, sender);
        self.rebuild_ordered();
    }

    fn remove(&mut self, shard_id: ShardId) {
        self.by_shard.remove(&shard_id);
        self.rebuild_ordered();
    }

    fn rebuild_ordered(&mut self) {
        self.ordered = self
            .by_shard
            .iter()
            .map(|(shard_id, sender)| (*shard_id, sender.clone()))
            .collect();
        self.ordered.sort_unstable_by_key(|(shard_id, _)| *shard_id);
    }
}

#[derive(Debug, Clone)]
struct TierCaches {
    control: Arc<SsdObjectCache>,
    payload: Arc<SsdObjectCache>,
}

#[derive(Debug)]
struct TelemetryStripeState {
    stream_shard_id: ShardId,
    logs: LogStripe,
    traces: TraceStripe,
    metrics: MetricStripe,
    correlations: CorrelationIndex,
    log_partitions: u16,
    signal_tiers: HashMap<TelemetrySignal, SignalTierState>,
    router: TelemetryRouter,
}

#[derive(Debug)]
struct SignalTierState {
    signal: TelemetrySignal,
    tiers: HashMap<TopicPartition, TelemetryObjectTier<SharedTelemetryObjectStore>>,
    spool_directory: PathBuf,
    control_cache: Arc<SsdObjectCache>,
    payload_cache: Arc<SsdObjectCache>,
    warm_local_cache_on_publish: bool,
    config: ObjectTierConfig,
}

struct OpenedSignalTier {
    state: SignalTierState,
    checkpoints: Vec<DurableSinkCheckpoint>,
    recovery_states: Vec<Vec<u8>>,
}

impl SignalTierState {
    fn open(
        signal: TelemetrySignal,
        shard_id: ShardId,
        store: SharedTelemetryObjectStore,
        spool_directory: PathBuf,
        caches: &TierCaches,
        partitions: Vec<TopicPartition>,
        publication: (ObjectTierConfig, bool),
    ) -> TelemetryResult<Option<OpenedSignalTier>> {
        let (config, warm_local_cache_on_publish) = publication;
        if partitions.is_empty() {
            return Ok(None);
        }
        let mut tiers = HashMap::with_capacity(partitions.len());
        let mut checkpoints = Vec::new();
        let mut recovery_states = Vec::new();
        for partition in partitions {
            if partition.topic_id != signal.topic_id() {
                return Err(TelemetryError::InvalidConfig(
                    "signal object tier contains a partition from another signal",
                ));
            }
            let tier = TelemetryObjectTier::open(store.clone(), shard_id, partition, config)?;
            if let Some(checkpoint) = tier.root().latest_checkpoint {
                checkpoints.push(DurableSinkCheckpoint {
                    topic_partition: partition,
                    next_placement_sequence: shard_stream_core::PlacementSequence::new(
                        checkpoint.next_placement_sequence,
                    ),
                    next_offset: shard_stream_core::LogicalOffset::new(checkpoint.next_offset),
                });
            }
            if signal == TelemetrySignal::Metrics
                && let Some(entry) = tier.latest_group_cached(&caches.control)?
            {
                let manifest = tier.load_group_cached(&entry, &caches.control)?;
                let artifact =
                    manifest
                        .artifact(TierArtifactKind::QueryIndex)
                        .ok_or_else(|| {
                            TelemetryError::CorruptTier("metric group has no recovery index".into())
                        })?;
                let encoded = tier.read_artifact_cached(
                    artifact,
                    config.max_group_payload_bytes,
                    &caches.control,
                )?;
                recovery_states.push(decode_signal_recovery_state(
                    &encoded,
                    TelemetrySignal::Metrics,
                )?);
            }
            if tiers.insert(partition, tier).is_some() {
                return Err(TelemetryError::InvalidConfig(
                    "signal object tier contains a duplicate partition",
                ));
            }
        }
        let signal_name = match signal {
            TelemetrySignal::Traces => "traces",
            TelemetrySignal::Metrics => "metrics",
            TelemetrySignal::Logs => {
                return Err(TelemetryError::InvalidConfig(
                    "logs use the log-native object tier",
                ));
            }
        };
        Ok(Some(OpenedSignalTier {
            state: Self {
                signal,
                tiers,
                spool_directory: spool_directory
                    .join(format!("shard-{}", shard_id.get()))
                    .join(signal_name),
                control_cache: Arc::clone(&caches.control),
                payload_cache: Arc::clone(&caches.payload),
                warm_local_cache_on_publish,
                config,
            },
            checkpoints,
            recovery_states,
        }))
    }
}

struct SinkApplyCommand {
    expected: DurableSinkCheckpoint,
    appends: Vec<DurableAppend>,
    next: DurableSinkCheckpoint,
    response: SyncSender<EngineResult<DurableSinkApply>>,
}

#[allow(clippy::type_complexity)]
enum SinkCommand {
    Apply(SinkApplyCommand),
    Query {
        queries: Arc<[LogQuery]>,
        response: SyncSender<(ShardId, TelemetryResult<Vec<LogMatch>>)>,
    },
    QueryProjected {
        queries: Arc<[LogQuery]>,
        include_typed_metadata: bool,
        include_fields: bool,
        response: SyncSender<(ShardId, TelemetryResult<Vec<LogMatch>>)>,
    },
    QueryProjectedSingle {
        query: LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
        response: SyncSender<ProjectedQueryResponse>,
    },
    QueryProjectedEach {
        queries: Arc<[IndexedProjectedQuery]>,
        include_typed_metadata: bool,
        include_fields: bool,
        response: SyncSender<(ShardId, TelemetryResult<Vec<(usize, Vec<LogMatch>)>>)>,
    },
    QueryMessagesTopK {
        queries: Arc<[LogQuery]>,
        scorer: RelevanceScorer,
        limit: usize,
        response: SyncSender<(ShardId, TelemetryResult<Vec<LogMessageMatch>>)>,
    },
    QueryTraceIds {
        queries: Arc<[LogQuery]>,
        response: SyncSender<(ShardId, TelemetryResult<Vec<TraceId>>)>,
    },
    QueryTraceIdIntersection {
        outer_queries: Arc<[LogQuery]>,
        inner_queries: Arc<[LogQuery]>,
        response: SyncSender<(ShardId, TelemetryResult<Vec<TraceId>>)>,
    },
    CountQueries {
        queries: Vec<LogQuery>,
        response: SyncSender<(ShardId, TelemetryResult<u64>)>,
    },
    GroupQueries {
        queries: Vec<LogQuery>,
        keys: Vec<crate::AnalyticsGroupKey>,
        response: SyncSender<(
            ShardId,
            TelemetryResult<BTreeMap<Vec<Option<Arc<str>>>, u64>>,
        )>,
    },
    CountLogs {
        tenant: Arc<str>,
        partitions: Vec<TopicPartition>,
        response: SyncSender<(ShardId, TelemetryResult<u64>)>,
    },
    ActiveLogPartitions {
        tenant: Arc<str>,
        response: SyncSender<(ShardId, TelemetryResult<Vec<TopicPartition>>)>,
    },
    QueryTraces {
        query: TraceQuery,
        response: SyncSender<TelemetryResult<Vec<DurableSpan>>>,
    },
    QueryTraceProjected {
        query: TraceQuery,
        response: SyncSender<TelemetryResult<Vec<TraceProjection>>>,
    },
    QueryMetrics {
        query: MetricQuery,
        response: SyncSender<TelemetryResult<Vec<DurableMetricPoint>>>,
    },
    QueryMetricTimestamps {
        query: MetricTimestampQuery,
        response: SyncSender<TelemetryResult<Vec<DurableMetricPoint>>>,
    },
    Correlate {
        query: CorrelationQuery,
        buffer: Vec<TelemetryRecordRef>,
        response: SyncSender<TelemetryResult<Vec<TelemetryRecordRef>>>,
    },
    Flush {
        response: SyncSender<TelemetryResult<usize>>,
    },
    RetainObjectTier {
        cutoff_timestamp_unix_nanos: u64,
        max_payload_bytes_per_partition: Option<u64>,
        response: SyncSender<TelemetryResult<TierRetentionReport>>,
    },
    RetainedPayloadBytes {
        response: SyncSender<u64>,
    },
}

struct SinkState {
    shard_id: ShardId,
    sender: Mutex<Option<SyncSender<SinkCommand>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    query_workers: Arc<RwLock<QueryWorkerRegistry>>,
}

impl Drop for SinkState {
    fn drop(&mut self) {
        if let Ok(mut workers) = self.query_workers.write() {
            workers.remove(self.shard_id);
        }
        if let Ok(sender) = self.sender.get_mut() {
            sender.take();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

#[derive(Clone)]
struct ShardTelemetryStripeSink {
    state: Arc<SinkState>,
}

impl fmt::Debug for ShardTelemetryStripeSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardTelemetryStripeSink")
            .field("shard_id", &self.state.shard_id)
            .finish_non_exhaustive()
    }
}

impl DurableAppendSink for ShardTelemetryStripeSink {
    fn apply(
        &self,
        expected: DurableSinkCheckpoint,
        appends: &[DurableAppend],
        next: DurableSinkCheckpoint,
    ) -> EngineResult<DurableSinkApply> {
        let (response, receiver) = sync_channel(1);
        let sender = self
            .state
            .sender
            .lock()
            .map_err(|_| EngineError::CorruptState("shard-telemetry sink lock poisoned".into()))?
            .as_ref()
            .cloned()
            .ok_or(EngineError::WorkerStopped(self.state.shard_id))?;
        sender
            .send(SinkCommand::Apply(SinkApplyCommand {
                expected,
                appends: appends.to_vec(),
                next,
                response,
            }))
            .map_err(|_| EngineError::WorkerStopped(self.state.shard_id))?;
        receiver
            .recv()
            .map_err(|_| EngineError::WorkerStopped(self.state.shard_id))?
    }
}

/// Cloneable read service for the stripes owned by durable sink workers.
///
/// Global queries are sent to every active owner thread so bounded stripe
/// lookups can execute in parallel. Partition-affine queries are routed to
/// their owner before the result is merged in the deterministic order used by
/// [`crate::ShardTelemetry`].
#[derive(Debug, Clone)]
pub struct TelemetryService {
    workers: Arc<RwLock<QueryWorkerRegistry>>,
    correlation_buffers: Arc<Mutex<CorrelationBufferPool>>,
    active_log_partition_cache: Arc<Mutex<HashMap<Arc<str>, Vec<TopicPartition>>>>,
    tier_caches: Option<TierCaches>,
    object_store: Option<SharedTelemetryObjectStore>,
    router: TelemetryRouter,
}
