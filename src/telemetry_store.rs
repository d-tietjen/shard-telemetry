use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use rayon::prelude::*;
use shard_stream_core::{
    LogicalOffset, LogicalPartitionId, PlacementSequence, TopicId, TopicPartition,
};
use shard_stream_engine::{
    DurableSinkCheckpoint, DurableSinkConfig, DurableSinkOptions, EngineConfig, EngineError,
    StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability, FetchMode, FetchRequest};

use crate::deletion::DeleteCatalog;
use crate::ingest_pack::decode_ingest_pack;
use crate::loki_api::{LogicalDeleteFilter, LokiApiError, apply_logical_deletes};
use crate::storage_format::DataDirectoryLease;
use crate::{
    AnalyticsRelation, AnalyticsRow, AnalyticsScanOrder, AnalyticsScanRequest, CaseSensitivity,
    DeleteRequest, LocalObjectStore, LogMatch, LogPredicate, LogQuery, LokiEntry, LokiStore,
    NativeQuery, NativeQueryDirection, ObjectTierConfig, OtlpSinkConfig, QueryCursor,
    S3ObjectStore, S3ObjectStoreConfig, SharedTelemetryObjectStore, SinkObjectTierConfig,
    SsdCacheConfig, StoreHealth, StoreMetrics, StripeConfig, TelemetryService,
    TelemetrySinkFactory,
};

const LOKI_TOPIC_ID: TopicId = crate::LOGS_TOPIC_ID;
const LABEL_PREFIX: &str = "resource.loki.label.";
const METADATA_PREFIX: &str = "attr.loki.metadata.";
const TENANT_FIELD: &str = "resource.loki.tenant";

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
    tenant_partitions: u32,
    ingest_stripes_per_tenant: u32,
    indexed_ack_timeout: Duration,
    next_request_id: AtomicU64,
    remote_write_append: Mutex<()>,
    deletes: DeleteCatalog,
    retention: Option<Duration>,
    retention_runs: AtomicU64,
    retention_advanced_offsets: AtomicU64,
    retention_failures: AtomicU64,
    object_tier_enabled: bool,
    source_reclaimed_offsets: AtomicU64,
    retired_object_groups: AtomicU64,
    retired_object_payload_bytes: AtomicU64,
    retired_object_keys: AtomicU64,
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

impl std::fmt::Debug for DurableTelemetryStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableTelemetryStore")
            .field("tenant_partitions", &self.tenant_partitions)
            .field("ingest_stripes_per_tenant", &self.ingest_stripes_per_tenant)
            .field("indexed_ack_timeout", &self.indexed_ack_timeout)
            .finish_non_exhaustive()
    }
}

impl DurableTelemetryStore {
    /// Opens or recovers a standalone durable store.
    pub fn open(config: DurableTelemetryConfig) -> Result<Self, LokiApiError> {
        Self::open_with_object_tier_config(config, ObjectTierConfig::default())
    }

    /// Opens a store with explicit object publication and reader-lease bounds.
    pub fn open_with_object_tier_config(
        config: DurableTelemetryConfig,
        object_tier_config: ObjectTierConfig,
    ) -> Result<Self, LokiApiError> {
        config.validate()?;
        object_tier_config
            .validate()
            .map_err(|error| LokiApiError::configuration(error.to_string()))?;
        let logical_partitions =
            NonZeroU16::new(u16::try_from(config.tenant_partitions).map_err(|_| {
                LokiApiError::configuration("tenant_partitions must fit the v1 u16 routing space")
            })?)
            .expect("configuration validation rejects zero partitions");
        let physical_stripes = NonZeroU16::new(
            u16::try_from(config.shard_count.min(config.tenant_partitions)).map_err(|_| {
                LokiApiError::configuration("shard_count must fit the v1 u16 routing space")
            })?,
        )
        .expect("configuration validation rejects zero shards");
        let mut signals = crate::ShardTelemetryConfig::default();
        for signal in [&mut signals.logs, &mut signals.traces, &mut signals.metrics] {
            signal.logical_partitions = logical_partitions;
            signal.physical_stripes = physical_stripes;
        }
        let data_directory_lease = DataDirectoryLease::acquire(&config.data_directory)?;
        let deletes = DeleteCatalog::open(config.data_directory.join("delete-catalog-v1.json"))?;
        let engine_config = EngineConfig {
            data_dir: config.data_directory.join("stream"),
            // ShardTelemetry's compressed tier is authoritative after its
            // checkpoint publishes. Keeping shard-stream's raw object archive
            // as well would permanently duplicate every source byte.
            object_store_dir: None,
            shard_count: config.shard_count,
            virtual_lane_count: config.shard_count,
            replication_factor: 1,
            min_in_sync_replicas: 1,
            queue_slots_per_shard: 1_024,
            queue_bytes_per_shard: 128 * 1024 * 1024,
            target_pack_bytes: 8 * 1024 * 1024,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: 64 * 1024 * 1024,
            max_fetch_bytes: 64 * 1024 * 1024,
            append_linger: config.append_linger,
        };
        let object_store = match (
            config.object_store_directory.as_ref(),
            config.s3_object_store.clone(),
        ) {
            (Some(directory), None) => Some(SharedTelemetryObjectStore::from(
                LocalObjectStore::open(directory)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?,
            )),
            (None, Some(s3)) => Some(SharedTelemetryObjectStore::new(Arc::new(
                S3ObjectStore::open(s3)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?,
            ))),
            (None, None) => None,
            (Some(_), Some(_)) => unreachable!("configuration validation rejects two backends"),
        };
        let object_tier_enabled = object_store.is_some();
        let sink_object_tier = object_store
            .map(|store| {
                Ok::<_, crate::TelemetryError>(SinkObjectTierConfig {
                    store,
                    spool_directory: config.data_directory.join("tier-spool"),
                    control_cache_directory: config.data_directory.join("tier-control-cache"),
                    payload_cache_directory: config.data_directory.join("tier-payload-cache"),
                    partitions: object_tier_partitions(config.tenant_partitions),
                    tier: object_tier_config,
                    control_cache: SsdCacheConfig {
                        max_bytes: 8 * 1024 * 1024 * 1024,
                        ..SsdCacheConfig::default()
                    },
                    payload_cache: SsdCacheConfig::default(),
                })
            })
            .transpose()
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let sink_config = OtlpSinkConfig {
            stripe: config.stripe,
            signals,
            state_directory: config
                .recovery_journal
                .then(|| config.data_directory.join("index-journal")),
            object_tier: sink_object_tier,
            ..OtlpSinkConfig::default()
        };
        let factory = Arc::new(
            TelemetrySinkFactory::new(engine_config.shard_ids(), sink_config)
                .map_err(|error| LokiApiError::internal(error.to_string()))?,
        );
        let service = factory.service();
        let sink_options = DurableSinkOptions {
            worker_count: config.shard_count as usize,
            recovery_timeout: config.indexed_ack_timeout,
            ..DurableSinkOptions::default()
        };
        let engine = Arc::new(
            StreamEngine::open_with_durable_sink(
                engine_config,
                DurableSinkConfig::new(factory).with_options(sink_options),
            )
            .map_err(engine_error)?,
        );
        match engine.create_topic(TopicConfig {
            topic_id: LOKI_TOPIC_ID,
            partitions: config.tenant_partitions,
            shards: None,
        }) {
            Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
            Err(error) => return Err(engine_error(error)),
        }
        for topic_id in [crate::TRACES_TOPIC_ID, crate::METRICS_TOPIC_ID] {
            match engine.create_topic(TopicConfig {
                topic_id,
                partitions: config.tenant_partitions,
                shards: None,
            }) {
                Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
                Err(error) => return Err(engine_error(error)),
            }
        }
        Ok(Self {
            _data_directory_lease: data_directory_lease,
            engine,
            service,
            tenant_partitions: config.tenant_partitions,
            ingest_stripes_per_tenant: config.shard_count.min(config.tenant_partitions),
            indexed_ack_timeout: config.indexed_ack_timeout,
            next_request_id: AtomicU64::new(1),
            remote_write_append: Mutex::new(()),
            deletes,
            retention: config.retention,
            retention_runs: AtomicU64::new(0),
            retention_advanced_offsets: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
            object_tier_enabled,
            source_reclaimed_offsets: AtomicU64::new(0),
            retired_object_groups: AtomicU64::new(0),
            retired_object_payload_bytes: AtomicU64::new(0),
            retired_object_keys: AtomicU64::new(0),
        })
    }

    /// Attaches ShardTelemetry's Loki/query surface to a stream engine opened by an
    /// external HA host.
    ///
    /// The host must install the matching [`TelemetrySinkFactory`] as the
    /// engine's durable sink before recovery. This constructor never opens a
    /// second WAL and never changes the host's replication or fencing policy.
    pub fn attach(
        data_directory: PathBuf,
        engine: Arc<StreamEngine>,
        service: TelemetryService,
        tenant_partitions: u32,
        ingest_stripes_per_tenant: u32,
        indexed_ack_timeout: Duration,
        retention: Option<Duration>,
    ) -> Result<Self, LokiApiError> {
        if tenant_partitions == 0 || ingest_stripes_per_tenant == 0 {
            return Err(LokiApiError::configuration(
                "tenant and ingest stripe counts must be nonzero",
            ));
        }
        if ingest_stripes_per_tenant > tenant_partitions {
            return Err(LokiApiError::configuration(
                "ingest stripe count cannot exceed tenant partitions",
            ));
        }
        if indexed_ack_timeout.is_zero() {
            return Err(LokiApiError::configuration(
                "indexed_ack_timeout must be nonzero",
            ));
        }
        if retention.is_some_and(|retention| retention.is_zero()) {
            return Err(LokiApiError::configuration(
                "retention must be nonzero when configured",
            ));
        }
        let data_directory_lease = DataDirectoryLease::acquire(&data_directory)?;
        let deletes = DeleteCatalog::open(data_directory.join("delete-catalog-v1.json"))?;
        for topic_id in [
            LOKI_TOPIC_ID,
            crate::TRACES_TOPIC_ID,
            crate::METRICS_TOPIC_ID,
        ] {
            match engine.create_topic(TopicConfig {
                topic_id,
                partitions: tenant_partitions,
                shards: None,
            }) {
                Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
                Err(error) => return Err(engine_error(error)),
            }
        }
        let object_tier_enabled = service.object_store_stats().is_some();
        Ok(Self {
            _data_directory_lease: data_directory_lease,
            engine,
            service,
            tenant_partitions,
            ingest_stripes_per_tenant,
            indexed_ack_timeout,
            next_request_id: AtomicU64::new(1),
            remote_write_append: Mutex::new(()),
            deletes,
            retention,
            retention_runs: AtomicU64::new(0),
            retention_advanced_offsets: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
            object_tier_enabled,
            source_reclaimed_offsets: AtomicU64::new(0),
            retired_object_groups: AtomicU64::new(0),
            retired_object_payload_bytes: AtomicU64::new(0),
            retired_object_keys: AtomicU64::new(0),
        })
    }

    /// Atomically replaces one tenant's local delete view from replicated HA
    /// control state.
    ///
    /// The caller must supply only records which have already reached its
    /// cluster finality boundary. Query filtering observes the replacement
    /// only after the local catalog is durably synchronized.
    pub fn synchronize_delete_requests(
        &self,
        tenant: &str,
        requests: Vec<DeleteRequest>,
    ) -> Result<(), LokiApiError> {
        self.deletes.replace_tenant(tenant, requests)
    }

    fn tenant_partition_base(&self, tenant: &str) -> u32 {
        let hash = tenant
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        let groups = self.tenant_partitions / self.ingest_stripes_per_tenant;
        ((hash % u64::from(groups.max(1))) as u32) * self.ingest_stripes_per_tenant
    }

    fn write_partition(&self, tenant: &str, request_id: u64) -> TopicPartition {
        let partition = self.tenant_partition_base(tenant)
            + (request_id % u64::from(self.ingest_stripes_per_tenant)) as u32;
        TopicPartition::new(
            LOKI_TOPIC_ID,
            LogicalPartitionId::new(partition % self.tenant_partitions),
        )
    }

    fn tenant_partitions(&self, tenant: &str) -> Result<Vec<TopicPartition>, LokiApiError> {
        self.service
            .active_log_partitions(Arc::from(tenant))
            .map_err(|error| LokiApiError::internal(error.to_string()))
    }

    fn retention_cutoff(&self) -> Option<u64> {
        let retention = self.retention?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let cutoff = now.saturating_sub(retention.as_nanos());
        Some(u64::try_from(cutoff).unwrap_or(u64::MAX))
    }

    fn retained_query_start(&self, requested: Option<u64>) -> Option<u64> {
        match (requested, self.retention_cutoff()) {
            (Some(requested), Some(cutoff)) => Some(requested.max(cutoff)),
            (None, Some(cutoff)) => Some(cutoff),
            (requested, None) => requested,
        }
    }

    /// Advances shard-stream retention at whole append-batch boundaries.
    ///
    /// The durable sink checkpoint is an engine-level retention pin, so this
    /// cannot reclaim a source pack before its query index has applied it.
    pub fn compact_retention(&self) -> Result<RetentionReport, LokiApiError> {
        let Some(cutoff) = self.retention_cutoff() else {
            return Ok(RetentionReport::default());
        };
        let result = self.compact_retention_before(cutoff);
        self.retention_runs.fetch_add(1, Ordering::Relaxed);
        match &result {
            Ok(report) => {
                self.retention_advanced_offsets
                    .fetch_add(report.advanced_offsets, Ordering::Relaxed);
                self.retired_object_groups
                    .fetch_add(report.retired_object_groups, Ordering::Relaxed);
                self.retired_object_payload_bytes
                    .fetch_add(report.retired_object_payload_bytes, Ordering::Relaxed);
                self.retired_object_keys
                    .fetch_add(report.retired_object_keys, Ordering::Relaxed);
            }
            Err(_) => {
                self.retention_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    fn compact_retention_before(&self, cutoff: u64) -> Result<RetentionReport, LokiApiError> {
        self.flush(self.indexed_ack_timeout)?;
        let mut report = RetentionReport {
            cutoff_timestamp_unix_nanos: cutoff,
            ..RetentionReport::default()
        };
        if self.object_tier_enabled {
            let tier = self
                .service
                .retain_object_tier_since(cutoff)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            report.retired_object_groups = tier.retired_groups;
            report.retired_object_payload_bytes = tier.retired_payload_bytes;
            report.retired_object_keys = tier.retired_objects;
        }
        for partition_id in self.engine.topic_partitions(LOKI_TOPIC_ID) {
            let partition = TopicPartition::new(LOKI_TOPIC_ID, partition_id);
            let watermarks = self.engine.watermarks(partition).map_err(engine_error)?;
            let original_start = watermarks.log_start;
            let mut scan_offset = original_start;
            let mut retained_start = original_start;
            let mut reached_retained_batch = false;
            while scan_offset < watermarks.last_stable_offset && !reached_retained_batch {
                let batches = self
                    .engine
                    .fetch(FetchRequest {
                        request_id: 0,
                        topic_id: partition.topic_id,
                        partition_id: partition.partition_id,
                        start_offset: scan_offset,
                        max_bytes: 16 * 1024 * 1024,
                        mode: FetchMode::Ordered,
                    })
                    .map_err(engine_error)?;
                if batches.is_empty() {
                    break;
                }
                for batch in batches {
                    let envelope = crate::TelemetryEnvelope::decode(&batch.payload)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    if envelope.signal != crate::TelemetrySignal::Logs {
                        return Err(LokiApiError::internal(
                            "log retention encountered a non-log envelope",
                        ));
                    }
                    let records = decode_ingest_pack(&envelope.payload)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    if records
                        .iter()
                        .any(|record| record.timestamp_unix_nanos >= cutoff)
                    {
                        reached_retained_batch = true;
                        break;
                    }
                    let next = batch
                        .last_offset
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| LokiApiError::internal("retention offset exhausted"))?;
                    retained_start = LogicalOffset::new(next);
                    scan_offset = retained_start;
                }
            }
            if retained_start > original_start {
                self.engine
                    .truncate_partition(partition, retained_start)
                    .map_err(engine_error)?;
                report.advanced_partitions += 1;
                report.advanced_offsets = report
                    .advanced_offsets
                    .saturating_add(retained_start.get().saturating_sub(original_start.get()));
            }
        }
        Ok(report)
    }

    /// Appends every partition in one validated native v1 telemetry batch in parallel.
    ///
    /// The response retains request order and contains one acknowledgement per
    /// resulting partition. Any partition failure makes the request retryable;
    /// trace and metric retries resolve idempotently by durable identity.
    pub fn append_telemetry_batch(
        &self,
        batch: &crate::NativeTelemetryBatch,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        let encoded = batch
            .encode()
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let validated = crate::NativeTelemetryBatch::decode(&encoded)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        self.append_validated_telemetry_batch(&validated, wait_for_index)
    }

    /// Appends a native batch that has already been decoded and checksum
    /// validated by the native protocol server.
    pub(crate) fn append_validated_telemetry_batch(
        &self,
        batch: &crate::NativeTelemetryBatch,
        wait_for_index: bool,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        let acknowledgements = if batch.partitions.len() == 1 {
            vec![self.append_telemetry_partition(&batch.partitions[0], wait_for_index)?]
        } else {
            batch
                .partitions
                .par_iter()
                .map(|partition| self.append_telemetry_partition(partition, wait_for_index))
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(crate::NativeTelemetryAppendAck {
            partitions: acknowledgements,
        })
    }

    /// Validates and appends one complete Remote Write request under serialized
    /// same-timestamp conflict semantics.
    pub fn append_remote_write_batch(
        &self,
        batch: &crate::NativeTelemetryBatch,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        let _guard = self
            .remote_write_append
            .lock()
            .map_err(|_| LokiApiError::internal("Remote Write append lock poisoned"))?;
        let mut request_samples = BTreeMap::new();
        for partition in &batch.partitions {
            if partition.envelope.signal != crate::TelemetrySignal::Metrics
                || partition.envelope.routing_metadata.len() != 5
                || partition.envelope.routing_metadata[4]
                    != crate::MetricIngestProtocol::RemoteWrite.to_wire()
            {
                return Err(LokiApiError::bad_request(
                    "Remote Write batch contains a non-Remote-Write metric envelope",
                ));
            }
            for point in crate::decode_metric_chunk(&partition.envelope.payload)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?
            {
                let key = (point.series_fingerprint(), point.timestamp_unix_nanos);
                if let Some(existing) = request_samples.insert(key, point.clone())
                    && !same_remote_write_sample(&existing, &point)
                {
                    return Err(LokiApiError::bad_request(format!(
                        "conflicting samples for series {:032x} at {}",
                        key.0.get(),
                        key.1
                    )));
                }
                let existing = self.query_metrics(&crate::MetricQuery {
                    tenant: Arc::clone(&point.identity.tenant),
                    partition: None,
                    start_offset: None,
                    series: Some(point.series_fingerprint()),
                    name: None,
                    exact_labels: Arc::new(Vec::new()),
                    start_time_unix_nanos: Some(point.timestamp_unix_nanos),
                    end_time_unix_nanos: Some(point.timestamp_unix_nanos),
                    limit: usize::MAX,
                })?;
                if existing
                    .iter()
                    .any(|stored| !same_remote_write_sample(stored, &point))
                {
                    return Err(LokiApiError::bad_request(format!(
                        "conflicting sample for series {:032x} at {}",
                        key.0.get(),
                        key.1
                    )));
                }
            }
        }
        self.append_telemetry_batch(batch, true)
    }

    /// Executes a native trace query on the owner stripes.
    pub fn query_traces(
        &self,
        query: &crate::TraceQuery,
    ) -> Result<Vec<crate::DurableSpan>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end <= cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        self.service
            .query_traces(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))
    }

    /// Executes a native exact raw-metric query on the owner stripes.
    pub fn query_metrics(
        &self,
        query: &crate::MetricQuery,
    ) -> Result<Vec<crate::DurableMetricPoint>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end < cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        self.service
            .query_metrics(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))
    }

    /// Returns bounded cross-signal record references for exact shared
    /// trace, resource, scope, and typed-label identities.
    pub fn query_correlations(
        &self,
        query: &crate::CorrelationQuery,
    ) -> Result<Vec<crate::TelemetryRecordRef>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end < cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        self.service
            .query_correlations(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))
    }

    fn append_telemetry_partition(
        &self,
        partition: &crate::NativePartitionAppend,
        wait_for_index: bool,
    ) -> Result<crate::NativePartitionAck, LokiApiError> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let payload = partition
            .envelope
            .encode()
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let request = AppendRequest {
            request_id: u128::from(request_id),
            topic_id: partition.topic_partition.topic_id,
            partition_id: partition.topic_partition.partition_id,
            record_count: partition.envelope.item_count,
            payload: Bytes::from(payload),
            durability: Durability::Leader,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        };
        let appended = if let Some(transient_context) = &partition.transient_context {
            self.engine.append_with_durable_sink_context(
                request,
                Bytes::copy_from_slice(transient_context),
            )
        } else {
            self.engine.append(request)
        }
        .map_err(engine_error)?;
        if wait_for_index {
            let target = DurableSinkCheckpoint {
                topic_partition: partition.topic_partition,
                next_placement_sequence: PlacementSequence::new(
                    appended
                        .placement
                        .sequence
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| LokiApiError::internal("placement sequence exhausted"))?,
                ),
                next_offset: LogicalOffset::new(
                    appended
                        .last_offset
                        .get()
                        .checked_add(1)
                        .ok_or_else(|| LokiApiError::internal("logical offset exhausted"))?,
                ),
            };
            self.engine
                .wait_for_durable_sink_checkpoint(target)
                .map_err(engine_error)?;
        }
        Ok(crate::NativePartitionAck {
            topic_partition: partition.topic_partition,
            first_offset: appended.first_offset.get(),
            last_offset: appended.last_offset.get(),
        })
    }

    /// Executes a native exact-label/token query directly against the bounded
    /// stripe indexes and merges tenant partitions by timestamp.
    pub fn query_native(&self, request: &NativeQuery) -> Result<Vec<LokiEntry>, LokiApiError> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        if !delete_filter.is_empty() {
            return self.query_native_with_deletes(request, &delete_filter);
        }
        let queries = self
            .tenant_partitions(&request.tenant)?
            .into_iter()
            .map(|partition| {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(request.limit as usize)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                if request.direction == NativeQueryDirection::NewestFirst {
                    query = query.newest_first();
                }
                for (key, value) in &request.labels {
                    query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
                }
                for term in &request.terms {
                    query = query.with_term(term.as_str());
                }
                query
            })
            .collect::<Vec<_>>();
        let mut matches = self
            .service
            .query_partitions(&queries)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        matches.sort_unstable_by(|left, right| {
            let ordering = left
                .record
                .timestamp_unix_nanos
                .cmp(&right.record.timestamp_unix_nanos)
                .then_with(|| {
                    left.record
                        .record_ref
                        .offset
                        .cmp(&right.record.record_ref.offset)
                });
            match request.direction {
                NativeQueryDirection::OldestFirst => ordering,
                NativeQueryDirection::NewestFirst => ordering.reverse(),
            }
        });
        matches.truncate(request.limit as usize);
        matches.into_iter().map(log_match_to_entry).collect()
    }

    fn query_native_with_deletes(
        &self,
        request: &NativeQuery,
        delete_filter: &LogicalDeleteFilter,
    ) -> Result<Vec<LokiEntry>, LokiApiError> {
        let result_limit = request.limit as usize;
        let page_limit = result_limit.clamp(1_024, 8_192);
        let mut accepted = Vec::<(LokiEntry, u64)>::new();
        for partition in self.tenant_partitions(&request.tenant)? {
            let mut after = None;
            let mut accepted_from_partition = 0usize;
            loop {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(page_limit)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                query.after = after;
                if request.direction == NativeQueryDirection::NewestFirst {
                    query = query.newest_first();
                }
                for (key, value) in &request.labels {
                    query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
                }
                for term in &request.terms {
                    query = query.with_term(term.as_str());
                }
                let matches = self
                    .service
                    .query_partitions(std::slice::from_ref(&query))
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if matches.is_empty() {
                    break;
                }
                let returned = matches.len();
                let last = matches.last().expect("non-empty query page");
                after = Some(QueryCursor::new(
                    last.record.timestamp_unix_nanos,
                    last.record.record_ref.offset,
                ));
                for matched in matches {
                    let offset = matched.record.record_ref.offset.get();
                    let entry = log_match_to_entry(matched)?;
                    if !delete_filter.matches(&entry) {
                        accepted.push((entry, offset));
                        accepted_from_partition += 1;
                        if accepted_from_partition == result_limit {
                            break;
                        }
                    }
                }
                if accepted_from_partition == result_limit || returned < page_limit {
                    break;
                }
            }
        }
        accepted.sort_unstable_by(|(left, left_offset), (right, right_offset)| {
            let ordering = left
                .timestamp_unix_nanos
                .cmp(&right.timestamp_unix_nanos)
                .then_with(|| left_offset.cmp(right_offset));
            match request.direction {
                NativeQueryDirection::OldestFirst => ordering,
                NativeQueryDirection::NewestFirst => ordering.reverse(),
            }
        });
        accepted.truncate(result_limit);
        Ok(accepted.into_iter().map(|(entry, _)| entry).collect())
    }
}

fn same_remote_write_sample(
    left: &crate::DurableMetricPoint,
    right: &crate::DurableMetricPoint,
) -> bool {
    left.series_fingerprint() == right.series_fingerprint()
        && left.timestamp_unix_nanos == right.timestamp_unix_nanos
        && left.start_time_unix_nanos == right.start_time_unix_nanos
        && left.flags == right.flags
        && left.value == right.value
        && left.exemplars == right.exemplars
}

impl LokiStore for DurableTelemetryStore {
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        if entries.is_empty() {
            return Ok(());
        }
        let record_count = u32::try_from(entries.len())
            .map_err(|_| LokiApiError::bad_request("push contains more than u32 entries"))?;
        let routing_request = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let topic_partition = self.write_partition(tenant, routing_request);
        let envelope = crate::prepare_loki_log_envelope(tenant, entries)
            .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let acknowledgement = self.append_telemetry_partition(
            &crate::NativePartitionAppend {
                topic_partition,
                envelope,
                transient_context: None,
            },
            true,
        )?;
        debug_assert_eq!(
            acknowledgement
                .last_offset
                .saturating_sub(acknowledgement.first_offset)
                .saturating_add(1),
            u64::from(record_count)
        );
        Ok(())
    }

    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let queries = self
            .tenant_partitions(tenant)?
            .into_iter()
            .map(|partition| {
                LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_field(TENANT_FIELD, tenant)
            })
            .collect::<Vec<_>>();
        let cutoff = self.retention_cutoff();
        let queries = queries
            .into_iter()
            .map(|mut query| {
                query.start_timestamp_unix_nanos = cutoff;
                query
            })
            .collect::<Vec<_>>();
        let matches = self
            .service
            .query_partitions(&queries)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let mut entries = matches
            .into_iter()
            .map(log_match_to_entry)
            .collect::<Result<Vec<_>, _>>()?;
        apply_logical_deletes(&mut entries, &self.deletes.list(tenant)?)?;
        entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
        Ok(entries)
    }

    fn scan_analytics_arrow(
        &self,
        request: &AnalyticsScanRequest,
        schema: &SchemaRef,
        emit: &mut dyn FnMut(&RecordBatch) -> Result<(), LokiApiError>,
    ) -> Result<bool, LokiApiError> {
        request.validate()?;
        let Some(limit) = request
            .limit
            .filter(|limit| *limit <= crate::analytics::DEFAULT_SCAN_BATCH_ROWS)
        else {
            return Ok(false);
        };
        if limit == 0 {
            return Ok(true);
        }
        match request.relation {
            AnalyticsRelation::MetricPoints
                if request.series_id.is_some()
                    && request.trace_id.is_none()
                    && request.span_id.is_none()
                    && request.metadata.is_empty()
                    && request.attributes.is_empty()
                    && request.resource_attributes.is_empty()
                    && request.scope_attributes.is_empty()
                    && crate::analytics::can_direct_metric_projection(&request.columns) =>
            {
                let query = crate::MetricQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    series: request.series_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_labels: Arc::new(
                        request
                            .labels
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect(),
                    ),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request
                        .end_timestamp_unix_nanos
                        .and_then(|end| end.checked_sub(1)),
                    limit,
                };
                let points = self.query_metrics(&query)?;
                if !points.is_empty() {
                    let batch = crate::analytics::direct_metric_record_batch(
                        &points,
                        &request.columns,
                        Arc::clone(schema),
                    )?
                    .expect("direct metric projection was checked");
                    emit(&batch)?;
                }
                Ok(true)
            }
            AnalyticsRelation::Spans
                if (request.trace_id.is_some() || !request.resource_attributes.is_empty())
                    && request.labels.is_empty()
                    && request.metadata.is_empty()
                    && crate::analytics::can_direct_span_projection(&request.columns) =>
            {
                let pairs = |fields: &[crate::MetadataField]| {
                    Arc::new(
                        fields
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect::<Vec<_>>(),
                    )
                };
                let query = crate::TraceQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    trace_id: request.trace_id,
                    span_id: request.span_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_attributes: pairs(&request.attributes),
                    exact_resource_attributes: pairs(&request.resource_attributes),
                    exact_scope_attributes: pairs(&request.scope_attributes),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request.end_timestamp_unix_nanos,
                    min_duration_nanos: None,
                    limit,
                };
                let spans = self.query_traces(&query)?;
                if !spans.is_empty() {
                    let batch = crate::analytics::direct_span_record_batch(
                        &spans,
                        &request.columns,
                        Arc::clone(schema),
                    )?
                    .expect("direct span projection was checked");
                    emit(&batch)?;
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn scan_analytics_rowbinary(
        &self,
        request: &AnalyticsScanRequest,
        writer: &mut dyn std::io::Write,
    ) -> Result<bool, LokiApiError> {
        request.validate()?;
        let Some(limit) = request
            .limit
            .filter(|limit| *limit <= crate::analytics::DEFAULT_SCAN_BATCH_ROWS)
        else {
            return Ok(false);
        };
        if limit == 0 {
            return Ok(true);
        }
        match request.relation {
            AnalyticsRelation::MetricPoints
                if request.series_id.is_some()
                    && request.trace_id.is_none()
                    && request.span_id.is_none()
                    && request.metadata.is_empty()
                    && request.attributes.is_empty()
                    && request.resource_attributes.is_empty()
                    && request.scope_attributes.is_empty()
                    && crate::analytics::can_direct_metric_projection(&request.columns) =>
            {
                let query = crate::MetricQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    series: request.series_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_labels: Arc::new(
                        request
                            .labels
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect(),
                    ),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request
                        .end_timestamp_unix_nanos
                        .and_then(|end| end.checked_sub(1)),
                    limit,
                };
                let points = self.query_metrics(&query)?;
                crate::analytics::write_direct_metric_rowbinary(&points, &request.columns, writer)
            }
            AnalyticsRelation::Spans
                if (request.trace_id.is_some() || !request.resource_attributes.is_empty())
                    && request.labels.is_empty()
                    && request.metadata.is_empty()
                    && crate::analytics::can_direct_span_projection(&request.columns) =>
            {
                let pairs = |fields: &[crate::MetadataField]| {
                    Arc::new(
                        fields
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect::<Vec<_>>(),
                    )
                };
                let query = crate::TraceQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    trace_id: request.trace_id,
                    span_id: request.span_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_attributes: pairs(&request.attributes),
                    exact_resource_attributes: pairs(&request.resource_attributes),
                    exact_scope_attributes: pairs(&request.scope_attributes),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request.end_timestamp_unix_nanos,
                    min_duration_nanos: None,
                    limit,
                };
                let spans = self.query_traces(&query)?;
                crate::analytics::write_direct_span_rowbinary(&spans, &request.columns, writer)
            }
            _ => Ok(false),
        }
    }

    fn scan_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        match request.relation {
            AnalyticsRelation::Logs => {}
            AnalyticsRelation::Spans
            | AnalyticsRelation::SpanEvents
            | AnalyticsRelation::SpanLinks => {
                return self.scan_trace_analytics(request, emit);
            }
            AnalyticsRelation::MetricPoints | AnalyticsRelation::MetricExemplars => {
                return self.scan_metric_analytics(request, emit);
            }
        }
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        let index_complete_ordered_scan = request.order.is_some()
            && delete_filter.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.series_id.is_none()
            && request.name.is_none();
        if index_complete_ordered_scan {
            let queries = self
                .tenant_partitions(&request.tenant)?
                .into_iter()
                .map(|partition| {
                    let mut query = LogQuery::new(partition)
                        .sort_by_timestamp()
                        .with_limit(limit)
                        .with_field(TENANT_FIELD, request.tenant.as_ref());
                    query.start_timestamp_unix_nanos =
                        self.retained_query_start(request.start_timestamp_unix_nanos);
                    query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                    if request.order == Some(AnalyticsScanOrder::TimestampDescending) {
                        query = query.newest_first();
                    }
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
                    query
                })
                .collect::<Vec<_>>();
            let rows = self
                .service
                .query_partitions(&queries)
                .map_err(|error| LokiApiError::internal(error.to_string()))?
                .into_iter()
                .map(|matched| {
                    crate::analytics::projected_log_row(
                        &request.tenant,
                        &matched.record,
                        &request.columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !rows.is_empty() {
                emit(&rows)?;
            }
            return Ok(());
        }
        let mut emitted = 0usize;
        for partition in self.tenant_partitions(&request.tenant)? {
            let mut next_offset = None;
            loop {
                let page_limit = 8_192usize.min(limit.saturating_sub(emitted));
                if page_limit == 0 {
                    return Ok(());
                }
                let mut query = LogQuery::new(partition)
                    .with_limit(page_limit)
                    .with_field(TENANT_FIELD, request.tenant.as_ref());
                query.start_offset = next_offset.map(LogicalOffset::new);
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
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
                let matches = self
                    .service
                    .query_partitions(std::slice::from_ref(&query))
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if matches.is_empty() {
                    break;
                }
                let returned = matches.len();
                let final_offset = matches
                    .last()
                    .expect("non-empty page")
                    .record
                    .record_ref
                    .offset
                    .get();
                let rows = matches
                    .into_iter()
                    .map(|matched| analytics_row_and_entry(&request.tenant, matched))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter_map(|(row, entry)| {
                        (!delete_filter.matches(&entry)
                            && crate::analytics::row_matches(&row, request))
                        .then_some(row)
                    })
                    .collect::<Vec<_>>();
                if !rows.is_empty() {
                    emit(&rows)?;
                }
                emitted = emitted.saturating_add(rows.len());
                if emitted == limit || returned < page_limit {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        Ok(())
    }

    fn scan_analytics_cardinality(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(u64) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        let unfiltered_log_count = request.relation == AnalyticsRelation::Logs
            && request.start_timestamp_unix_nanos.is_none()
            && request.end_timestamp_unix_nanos.is_none()
            && request.terms.is_empty()
            && request.message_tokens.is_empty()
            && request.case_insensitive_message_tokens.is_empty()
            && request.labels.is_empty()
            && request.metadata.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.series_id.is_none()
            && request.name.is_none()
            && self.retention.is_none()
            && self.deletes.list(&request.tenant)?.is_empty();
        if unfiltered_log_count {
            let partitions = self.tenant_partitions(&request.tenant)?;
            let mut count = self
                .service
                .count_log_records(Arc::clone(&request.tenant), partitions)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            if let Some(limit) = request.limit {
                count = count.min(u64::try_from(limit).unwrap_or(u64::MAX));
            }
            if count > 0 {
                emit(count)?;
            }
            return Ok(());
        }

        self.scan_analytics(request, &mut |rows| {
            emit(u64::try_from(rows.len()).unwrap_or(u64::MAX))
        })
    }

    fn health(&self) -> Result<StoreHealth, LokiApiError> {
        let stats = self.engine.durable_sink_stats();
        if stats.dirty_partitions > 0 {
            return Ok(StoreHealth {
                ready: false,
                detail: Arc::from(format!(
                    "{} durable sink partitions require recovery",
                    stats.dirty_partitions
                )),
            });
        }
        let maximum_age = self
            .indexed_ack_timeout
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if stats.pending_items > 0 && stats.checkpoint_age_ms > maximum_age {
            return Ok(StoreHealth {
                ready: false,
                detail: Arc::from(format!(
                    "oldest pending index checkpoint is {} ms old",
                    stats.checkpoint_age_ms
                )),
            });
        }
        Ok(StoreHealth::default())
    }

    fn flush(&self, timeout: Duration) -> Result<(), LokiApiError> {
        self.engine.sync().map_err(engine_error)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| LokiApiError::internal("flush deadline overflow"))?;
        loop {
            let stats = self.engine.durable_sink_stats();
            if stats.dirty_partitions > 0 {
                return Err(LokiApiError::internal(format!(
                    "flush stopped with {} dirty partitions",
                    stats.dirty_partitions
                )));
            }
            if stats.pending_items == 0 && stats.pending_bytes == 0 {
                self.service
                    .flush_object_tier()
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if self.object_tier_enabled {
                    let reclaimed = self.reclaim_source_packs()?;
                    self.source_reclaimed_offsets
                        .fetch_add(reclaimed, Ordering::Relaxed);
                }
                self.engine.sync().map_err(engine_error)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LokiApiError::unavailable(format!(
                    "flush timed out with {} pending items and {} pending bytes",
                    stats.pending_items, stats.pending_bytes
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn operational_metrics(&self) -> StoreMetrics {
        let stats = self.engine.durable_sink_stats();
        StoreMetrics {
            pending_items: stats.pending_items,
            pending_bytes: stats.pending_bytes,
            checkpoint_age_ms: stats.checkpoint_age_ms,
            applied_appends: stats.applied_appends,
            retry_attempts: stats.retry_attempts,
            failed_attempts: stats.failed_attempts,
            dirty_partitions: stats.dirty_partitions,
            retained_payload_bytes: self.service.retained_payload_bytes().ok(),
            retention_runs: self.retention_runs.load(Ordering::Relaxed),
            retention_advanced_offsets: self.retention_advanced_offsets.load(Ordering::Relaxed),
            retention_failures: self.retention_failures.load(Ordering::Relaxed),
            object_store: self.service.object_store_stats(),
            source_reclaimed_offsets: self.source_reclaimed_offsets.load(Ordering::Relaxed),
            retired_object_groups: self.retired_object_groups.load(Ordering::Relaxed),
            retired_object_payload_bytes: self.retired_object_payload_bytes.load(Ordering::Relaxed),
            retired_object_keys: self.retired_object_keys.load(Ordering::Relaxed),
        }
    }

    fn create_delete(
        &self,
        tenant: &str,
        start_time: i64,
        end_time: i64,
        query: String,
        created_at: i64,
    ) -> Result<String, LokiApiError> {
        self.deletes
            .create(tenant, start_time, end_time, query, created_at)
    }

    fn delete_requests(&self, tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        self.deletes.list(tenant)
    }

    fn cancel_delete(&self, tenant: &str, request_id: &str) -> Result<bool, LokiApiError> {
        self.deletes.cancel(tenant, request_id)
    }
}

impl DurableTelemetryStore {
    fn reclaim_source_packs(&self) -> Result<u64, LokiApiError> {
        let mut reclaimed = 0u64;
        for partition in self.engine.all_partitions() {
            let Some(checkpoint) = self
                .engine
                .durable_sink_checkpoint(partition)
                .map_err(engine_error)?
            else {
                continue;
            };
            let watermarks = self.engine.watermarks(partition).map_err(engine_error)?;
            let retained_start = checkpoint.next_offset.min(watermarks.last_stable_offset);
            if retained_start <= watermarks.log_start {
                continue;
            }
            self.engine
                .truncate_partition(partition, retained_start)
                .map_err(engine_error)?;
            reclaimed = reclaimed.saturating_add(
                retained_start
                    .get()
                    .saturating_sub(watermarks.log_start.get()),
            );
        }
        Ok(reclaimed)
    }

    fn signal_partitions(&self, topic_id: TopicId) -> impl Iterator<Item = TopicPartition> {
        let count = self.tenant_partitions;
        (0..count)
            .map(move |partition| TopicPartition::new(topic_id, LogicalPartitionId::new(partition)))
    }

    fn scan_trace_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let mut emitted = 0usize;
        let router = crate::TelemetryRouter::new(
            NonZeroU16::new(u16::try_from(self.tenant_partitions).map_err(|_| {
                LokiApiError::internal("tenant partition count exceeds the routing space")
            })?)
            .ok_or_else(|| LokiApiError::internal("tenant partition count is zero"))?,
        );
        let partitions = request.trace_id.map_or_else(
            || vec![None],
            |trace_id| vec![Some(router.trace(&request.tenant, trace_id))],
        );
        let pairs = |fields: &[crate::MetadataField]| {
            Arc::new(
                fields
                    .iter()
                    .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                    .collect::<Vec<_>>(),
            )
        };
        let exact_attributes = if request.relation == AnalyticsRelation::Spans {
            pairs(&request.attributes)
        } else {
            Arc::default()
        };
        let exact_resource_attributes = pairs(&request.resource_attributes);
        let exact_scope_attributes = pairs(&request.scope_attributes);
        let span_predicates_fully_pushed = request.relation == AnalyticsRelation::Spans
            && request.labels.is_empty()
            && request.metadata.is_empty();
        for partition in partitions {
            let mut next_offset = None;
            loop {
                if emitted == limit {
                    return Ok(());
                }
                let page_limit =
                    crate::analytics::DEFAULT_SCAN_BATCH_ROWS.min(limit.saturating_sub(emitted));
                let event_relation = request.relation == AnalyticsRelation::SpanEvents;
                let query = crate::TraceQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition,
                    start_offset: next_offset.map(LogicalOffset::new),
                    trace_id: request.trace_id,
                    span_id: request.span_id,
                    name: (request.relation == AnalyticsRelation::Spans)
                        .then(|| request.name.as_ref().map(Arc::clone))
                        .flatten(),
                    exact_attributes: Arc::clone(&exact_attributes),
                    exact_resource_attributes: Arc::clone(&exact_resource_attributes),
                    exact_scope_attributes: Arc::clone(&exact_scope_attributes),
                    start_time_unix_nanos: (!event_relation)
                        .then_some(request.start_timestamp_unix_nanos)
                        .flatten(),
                    end_time_unix_nanos: (!event_relation)
                        .then_some(request.end_timestamp_unix_nanos)
                        .flatten(),
                    min_duration_nanos: None,
                    limit: page_limit,
                };
                let spans = self.query_traces(&query)?;
                if spans.is_empty() {
                    break;
                }
                let returned = spans.len();
                let final_offset = spans
                    .last()
                    .expect("non-empty trace page")
                    .record_ref
                    .offset
                    .get();
                let mut rows = Vec::with_capacity(page_limit.min(limit.saturating_sub(emitted)));
                for span in spans {
                    let candidate_rows = if span_predicates_fully_pushed {
                        vec![crate::analytics::projected_span_row(
                            &span,
                            &request.columns,
                        )?]
                    } else {
                        crate::analytics::span_rows(&span, request.relation)?
                    };
                    for row in candidate_rows {
                        if !span_predicates_fully_pushed
                            && !crate::analytics::row_matches(&row, request)
                        {
                            continue;
                        }
                        rows.push(row);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                    }
                    if emitted == limit {
                        break;
                    }
                }
                if !rows.is_empty() {
                    emit(&rows)?;
                }
                if emitted == limit || returned < page_limit || partition.is_none() {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        Ok(())
    }

    fn scan_metric_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let mut emitted = 0usize;
        let router = crate::TelemetryRouter::new(
            NonZeroU16::new(u16::try_from(self.tenant_partitions).map_err(|_| {
                LokiApiError::internal("tenant partition count exceeds the routing space")
            })?)
            .ok_or_else(|| LokiApiError::internal("tenant partition count is zero"))?,
        );
        let partitions = request.series_id.map_or_else(
            || {
                self.signal_partitions(crate::METRICS_TOPIC_ID)
                    .collect::<Vec<_>>()
            },
            |series| vec![router.metric(&request.tenant, series)],
        );
        let metric_predicates_fully_pushed = request.relation == AnalyticsRelation::MetricPoints
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.metadata.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty();
        let exact_series_single_page = metric_predicates_fully_pushed
            && request.series_id.is_some()
            && request
                .limit
                .is_some_and(|limit| limit <= crate::analytics::DEFAULT_SCAN_BATCH_ROWS);
        for partition in partitions {
            let mut next_offset = None;
            loop {
                if emitted == limit {
                    return Ok(());
                }
                let page_limit =
                    crate::analytics::DEFAULT_SCAN_BATCH_ROWS.min(limit.saturating_sub(emitted));
                let exemplar_relation = request.relation == AnalyticsRelation::MetricExemplars;
                let query = crate::MetricQuery {
                    tenant: Arc::clone(&request.tenant),
                    // A bounded exact-series scan needs no continuation cursor.
                    // Leaving the partition unset selects the timestamp-ordered,
                    // disjoint-chunk fast path instead of rebuilding offset order
                    // for every point in the series.
                    partition: (!exact_series_single_page).then_some(partition),
                    start_offset: next_offset.map(LogicalOffset::new),
                    series: request.series_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_labels: Arc::new(
                        request
                            .labels
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect(),
                    ),
                    start_time_unix_nanos: (!exemplar_relation)
                        .then_some(request.start_timestamp_unix_nanos)
                        .flatten(),
                    end_time_unix_nanos: (!exemplar_relation)
                        .then(|| {
                            request
                                .end_timestamp_unix_nanos
                                .and_then(|end| end.checked_sub(1))
                        })
                        .flatten(),
                    limit: page_limit,
                };
                let points = self.query_metrics(&query)?;
                if points.is_empty() {
                    break;
                }
                let returned = points.len();
                let final_offset = points
                    .last()
                    .expect("non-empty metric page")
                    .record_ref
                    .offset
                    .get();
                let mut rows = Vec::with_capacity(page_limit.min(limit.saturating_sub(emitted)));
                for point in points {
                    let candidate_rows = if metric_predicates_fully_pushed {
                        vec![crate::analytics::projected_metric_row(
                            &point,
                            &request.columns,
                        )?]
                    } else {
                        crate::analytics::metric_rows(&point, request.relation)?
                    };
                    for row in candidate_rows {
                        if !metric_predicates_fully_pushed
                            && !crate::analytics::row_matches(&row, request)
                        {
                            continue;
                        }
                        rows.push(row);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                    }
                    if emitted == limit {
                        break;
                    }
                }
                if !rows.is_empty() {
                    emit(&rows)?;
                }
                if emitted == limit || returned < page_limit {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        Ok(())
    }
}

fn analytics_row_and_entry(
    tenant: &Arc<str>,
    matched: LogMatch,
) -> Result<(AnalyticsRow, LokiEntry), LokiApiError> {
    let mut labels = BTreeMap::new();
    let mut metadata = BTreeMap::new();
    for field in matched.record.fields.iter() {
        if let Some(name) = field.key.as_ref().strip_prefix(LABEL_PREFIX) {
            labels.insert(name.to_owned(), field.value.to_string());
        } else if let Some(name) = field.key.as_ref().strip_prefix(METADATA_PREFIX) {
            metadata.insert(name.to_owned(), field.value.to_string());
        }
    }
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

fn engine_error(error: EngineError) -> LokiApiError {
    match error {
        EngineError::InvalidConfig(message) => LokiApiError::bad_request(message),
        error => LokiApiError::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroU16;
    use std::time::{SystemTime, UNIX_EPOCH};

    use opentelemetry_proto::tonic::{
        collector::{
            metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest,
        },
        metrics::v1::{
            Exemplar, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, exemplar,
            metric, number_data_point,
        },
        trace::v1::{ResourceSpans, ScopeSpans, Span, span},
    };
    use prost::Message;
    use shard_stream_protocol::{FetchMode, FetchRequest};

    use super::*;
    use crate::ingest_pack::{decode_ingest_pack, validate_ingest_pack};

    #[test]
    fn object_tier_catalogs_cover_every_signal_partition() {
        let partitions = object_tier_partitions(3);
        assert_eq!(partitions.len(), 9);
        for signal in [
            crate::TelemetrySignal::Logs,
            crate::TelemetrySignal::Traces,
            crate::TelemetrySignal::Metrics,
        ] {
            assert_eq!(
                partitions
                    .iter()
                    .filter(|partition| partition.topic_id == signal.topic_id())
                    .count(),
                3
            );
        }
        assert!(partitions.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn shared_durable_sink_indexes_trace_and_metric_partition_envelopes() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-signals-store-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: true,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        let decoder = crate::OtlpTelemetryDecoder;
        let router = crate::TelemetryRouter::new(NonZeroU16::new(8).unwrap());

        let trace_request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "checkout".into(),
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        events: vec![span::Event {
                            time_unix_nano: 15,
                            name: "charged".into(),
                            ..span::Event::default()
                        }],
                        links: vec![span::Link {
                            trace_id: vec![3; 16],
                            span_id: vec![4; 8],
                            ..span::Link::default()
                        }],
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let mut trace_partitions = decoder.partition_traces(
            &router,
            decoder
                .decode_traces("tenant-a", &trace_request.encode_to_vec())
                .unwrap(),
        );
        let (trace_partition, trace_events) = trace_partitions.pop_first().unwrap();

        let metric_request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "requests".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 30,
                                value: Some(number_data_point::Value::AsInt(7)),
                                exemplars: vec![Exemplar {
                                    time_unix_nano: 30,
                                    value: Some(exemplar::Value::AsInt(7)),
                                    trace_id: vec![1; 16],
                                    span_id: vec![2; 8],
                                    ..Exemplar::default()
                                }],
                                ..NumberDataPoint::default()
                            }],
                        })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        };
        let mut metric_partitions = decoder.partition_metrics(
            &router,
            decoder
                .decode_metrics("tenant-a", &metric_request.encode_to_vec())
                .unwrap(),
        );
        let (metric_partition, metric_events) = metric_partitions.pop_first().unwrap();
        let trace_id = crate::TraceId::from_bytes([1; 16]).unwrap();
        let log_partition = router.log("tenant-a", Some(trace_id), &[]);
        let log_events = vec![crate::OtlpLogEvent {
            timestamp_unix_nanos: 25,
            body: Some(crate::TelemetryValue::String(Arc::from(
                "checkout request complete",
            ))),
            message: Arc::from("checkout request complete"),
            fields: Arc::new(vec![crate::MetadataField::new(
                "otel.trace_id",
                trace_id.to_string(),
            )]),
            trace_id: Some(trace_id),
            span_id: Some(crate::SpanId::from_bytes([2; 8]).unwrap()),
            compression_cohort: crate::CompressionCohortId::new(1),
            ..crate::OtlpLogEvent::default()
        }];

        let batch = crate::NativeTelemetryBatch {
            partitions: vec![
                crate::NativePartitionAppend {
                    topic_partition: log_partition,
                    envelope: crate::prepare_log_envelope("tenant-a", &log_events).unwrap(),
                    transient_context: None,
                },
                crate::NativePartitionAppend {
                    topic_partition: trace_partition,
                    envelope: crate::prepare_trace_envelope(trace_partition, trace_events).unwrap(),
                    transient_context: None,
                },
                crate::NativePartitionAppend {
                    topic_partition: metric_partition,
                    envelope: crate::prepare_metric_envelope(metric_partition, metric_events)
                        .unwrap(),
                    transient_context: None,
                },
            ],
        };
        let acknowledgement = store.append_telemetry_batch(&batch, true).unwrap();
        assert_eq!(acknowledgement.partitions.len(), 3);
        let spans = store
            .query_traces(&crate::TraceQuery {
                tenant: Arc::from("tenant-a"),
                trace_id: Some(crate::TraceId::from_bytes([1; 16]).unwrap()),
                limit: 10,
                ..crate::TraceQuery::default()
            })
            .unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name.as_ref(), "checkout");
        let points = store
            .query_metrics(&crate::MetricQuery {
                tenant: Arc::from("tenant-a"),
                name: Some(Arc::from("requests")),
                limit: 10,
                ..crate::MetricQuery::default()
            })
            .unwrap();
        assert_eq!(points.len(), 1);
        assert_eq!(
            points[0].value,
            crate::MetricValue::Gauge(crate::NumberValue::Integer(7))
        );
        let correlated = store
            .query_correlations(
                &crate::CorrelationQuery::new("tenant-a")
                    .with_trace_id(trace_id)
                    .with_limit(10),
            )
            .unwrap();
        assert_eq!(correlated.len(), 3);
        assert!(
            [
                crate::TelemetrySignal::Logs,
                crate::TelemetrySignal::Traces,
                crate::TelemetrySignal::Metrics,
            ]
            .into_iter()
            .all(|signal| correlated.iter().any(|record| record.signal == signal))
        );
        for (relation, expected) in [
            (crate::AnalyticsRelation::Logs, 1),
            (crate::AnalyticsRelation::Spans, 1),
            (crate::AnalyticsRelation::SpanEvents, 1),
            (crate::AnalyticsRelation::SpanLinks, 1),
            (crate::AnalyticsRelation::MetricPoints, 1),
            (crate::AnalyticsRelation::MetricExemplars, 1),
        ] {
            let request = crate::AnalyticsScanRequest::for_relation("tenant-a", relation);
            let mut rows = Vec::new();
            store
                .scan_analytics(&request, &mut |batch| {
                    rows.extend_from_slice(batch);
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows.len(), expected, "{relation:?}");
            assert_eq!(
                rows.first().map(|row| row.signal.as_ref()),
                Some(relation.signal())
            );
        }
        let mut exact_trace =
            crate::AnalyticsScanRequest::for_relation("tenant-a", crate::AnalyticsRelation::Spans);
        exact_trace.trace_id = Some(trace_id);
        exact_trace.span_id = Some(crate::SpanId::from_bytes([2; 8]).unwrap());
        exact_trace.name = Some(Arc::from("checkout"));
        let mut exact_trace_rows = Vec::new();
        store
            .scan_analytics(&exact_trace, &mut |batch| {
                exact_trace_rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(exact_trace_rows.len(), 1);

        exact_trace.columns = vec![
            crate::AnalyticsColumn::Timestamp,
            crate::AnalyticsColumn::Name,
        ];
        exact_trace_rows.clear();
        store
            .scan_analytics(&exact_trace, &mut |batch| {
                exact_trace_rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(exact_trace_rows.len(), 1);
        assert_eq!(exact_trace_rows[0].name.as_deref(), Some("checkout"));
        assert!(exact_trace_rows[0].resource_attributes.is_empty());
        assert!(exact_trace_rows[0].attributes_json.is_none());
        assert!(exact_trace_rows[0].events_json.is_none());

        let mut exact_metric = crate::AnalyticsScanRequest::for_relation(
            "tenant-a",
            crate::AnalyticsRelation::MetricPoints,
        );
        exact_metric.series_id = Some(points[0].series_fingerprint());
        exact_metric.name = Some(Arc::from("requests"));
        exact_metric.limit = Some(10);
        let mut exact_metric_rows = Vec::new();
        store
            .scan_analytics(&exact_metric, &mut |batch| {
                exact_metric_rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(exact_metric_rows.len(), 1);

        exact_metric.columns = vec![
            crate::AnalyticsColumn::Timestamp,
            crate::AnalyticsColumn::ScalarInteger,
        ];
        exact_metric_rows.clear();
        store
            .scan_analytics(&exact_metric, &mut |batch| {
                exact_metric_rows.extend_from_slice(batch);
                Ok(())
            })
            .unwrap();
        assert_eq!(exact_metric_rows.len(), 1);
        assert_eq!(exact_metric_rows[0].scalar_integer, Some(7));
        assert!(exact_metric_rows[0].labels.is_empty());
        assert!(exact_metric_rows[0].value_json.is_none());

        for (relation, start, end) in [
            (crate::AnalyticsRelation::SpanEvents, 15, 16),
            (crate::AnalyticsRelation::MetricExemplars, 30, 31),
        ] {
            let mut request = crate::AnalyticsScanRequest::for_relation("tenant-a", relation);
            request.start_timestamp_unix_nanos = Some(start);
            request.end_timestamp_unix_nanos = Some(end);
            let mut rows = Vec::new();
            store
                .scan_analytics(&request, &mut |batch| {
                    rows.extend_from_slice(batch);
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows.len(), 1, "nested timestamp filter: {relation:?}");

            request.start_timestamp_unix_nanos = Some(end);
            request.end_timestamp_unix_nanos = Some(end + 1);
            rows.clear();
            store
                .scan_analytics(&request, &mut |batch| {
                    rows.extend_from_slice(batch);
                    Ok(())
                })
                .unwrap();
            assert!(rows.is_empty(), "nested timestamp residual: {relation:?}");
        }
        drop(store);
        fs::remove_dir_all(directory).expect("remove test store");
    }

    #[test]
    fn durable_store_acknowledges_and_queries_the_same_stripe_owned_record() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-loki-store-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        for timestamp in 100..103 {
            store
                .push(
                    "tenant-a",
                    vec![LokiEntry {
                        timestamp_unix_nanos: timestamp,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: format!("durable message {timestamp}"),
                        structured_metadata: BTreeMap::from([(
                            "trace_id".to_owned(),
                            format!("abc-{timestamp}"),
                        )]),
                    }],
                )
                .expect("push is durable");
        }
        let entries = store.entries("tenant-a").expect("query");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].line, "durable message 100");
        assert_eq!(entries[0].labels["app"], "api");
        assert_eq!(entries[0].structured_metadata["trace_id"], "abc-100");
        drop(store);
        let recovered = DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store recovers");
        let entries = recovered.entries("tenant-a").expect("recovered query");
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[2].line, "durable message 102");
        assert!(!directory.join("index-journal").exists());
        drop(recovered);
        fs::remove_dir_all(directory).expect("remove test store");
    }

    #[test]
    fn object_tier_flushes_queries_cold_and_recovers_without_source_replay() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-cold-recovery-{}-{nonce}",
            std::process::id()
        ));
        let object_directory = directory.join("objects");
        let config = DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: Some(object_directory.clone()),
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let store = DurableTelemetryStore::open(config.clone()).expect("store opens");
        store
            .push(
                "tenant-a",
                vec![
                    LokiEntry {
                        timestamp_unix_nanos: 100,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: "cold request completed".to_owned(),
                        structured_metadata: BTreeMap::new(),
                    },
                    LokiEntry {
                        timestamp_unix_nanos: 200,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: "cold request failed".to_owned(),
                        structured_metadata: BTreeMap::from([(
                            "code".to_owned(),
                            "500".to_owned(),
                        )]),
                    },
                ],
            )
            .expect("push");
        LokiStore::flush(&store, Duration::from_secs(30)).expect("object tier flushes");
        assert_eq!(store.operational_metrics().retained_payload_bytes, Some(0));
        assert_eq!(store.operational_metrics().source_reclaimed_offsets, 2);
        let log_partition = TopicPartition::new(LOKI_TOPIC_ID, LogicalPartitionId::new(0));
        assert_eq!(
            store
                .engine
                .watermarks(log_partition)
                .expect("log watermarks")
                .log_start,
            LogicalOffset::new(2)
        );
        let cold = store
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                terms: vec!["failed".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("cold query");
        assert_eq!(cold.len(), 1);
        assert_eq!(cold[0].line, "cold request failed");
        let mut count_request = AnalyticsScanRequest::new("tenant-a");
        count_request.columns = vec![crate::AnalyticsColumn::Offset];
        count_request.cardinality_only = true;
        let mut count = 0_u64;
        store
            .scan_analytics_cardinality(&count_request, &mut |batch_count| {
                count += batch_count;
                Ok(())
            })
            .expect("cold cardinality scan");
        assert_eq!(count, 2);
        assert!(object_directory.exists());
        drop(store);

        let recovered = DurableTelemetryStore::open(config).expect("store recovers from tier root");
        let entries = recovered.entries("tenant-a").expect("recovered cold query");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].line, "cold request completed");
        assert_eq!(entries[1].structured_metadata["code"], "500");
        let mut recovered_count = 0_u64;
        recovered
            .scan_analytics_cardinality(&count_request, &mut |batch_count| {
                recovered_count += batch_count;
                Ok(())
            })
            .expect("recovered cold cardinality scan");
        assert_eq!(recovered_count, 2);
        drop(recovered);
        fs::remove_dir_all(directory).expect("remove test store");
    }

    #[test]
    fn object_retention_removes_complete_signal_groups_without_scanning_storage() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-object-retention-{}-{nonce}",
            std::process::id()
        ));
        let config = DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: Some(directory.join("objects")),
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let store = DurableTelemetryStore::open(config.clone()).expect("store opens");
        for (timestamp, line) in [(100, "expired"), (200, "retained")] {
            store
                .push(
                    "tenant-a",
                    vec![LokiEntry {
                        timestamp_unix_nanos: timestamp,
                        labels: BTreeMap::new(),
                        line: line.into(),
                        structured_metadata: BTreeMap::new(),
                    }],
                )
                .expect("push");
            LokiStore::flush(&store, Duration::from_secs(30)).expect("group flushes");
        }
        let report = store
            .compact_retention_before(150)
            .expect("object retention publishes");
        assert_eq!(report.retired_object_groups, 1);
        assert!(report.retired_object_payload_bytes > 0);
        assert!(report.retired_object_keys >= 4);
        let matches = store
            .query_native(&NativeQuery {
                tenant: "tenant-a".into(),
                labels: BTreeMap::new(),
                terms: Vec::new(),
                start_timestamp_unix_nanos: Some(150),
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("retained query");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].line, "retained");
        drop(store);

        let recovered = DurableTelemetryStore::open(config).expect("retained store reopens");
        let matches = recovered
            .query_native(&NativeQuery {
                tenant: "tenant-a".into(),
                labels: BTreeMap::new(),
                terms: Vec::new(),
                start_timestamp_unix_nanos: Some(150),
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("recovered retained query");
        assert_eq!(matches.len(), 1);
        drop(recovered);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn logical_deletes_survive_restart_and_filter_native_and_analytical_reads() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-delete-store-{}-{nonce}",
            std::process::id()
        ));
        let config = DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let store = DurableTelemetryStore::open(config.clone()).expect("store");
        store
            .push(
                "tenant-a",
                (100..103)
                    .map(|timestamp| LokiEntry {
                        timestamp_unix_nanos: timestamp,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: format!("durable message {timestamp}"),
                        structured_metadata: BTreeMap::new(),
                    })
                    .collect(),
            )
            .expect("push");
        let request_id = store
            .create_delete(
                "tenant-a",
                1,
                200,
                "{app=\"api\"} |= \"101\"".to_owned(),
                1_000,
            )
            .expect("create delete");
        assert_eq!(request_id, "0000000000000001");
        assert_eq!(
            store
                .entries("tenant-a")
                .expect("Loki entries")
                .into_iter()
                .map(|entry| entry.timestamp_unix_nanos)
                .collect::<Vec<_>>(),
            vec![100, 102]
        );

        let native = store
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                terms: vec!["message".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("native query");
        assert_eq!(native.len(), 2);
        assert!(native.iter().all(|entry| !entry.line.ends_with("101")));

        let mut rows = Vec::new();
        store
            .scan_analytics(&AnalyticsScanRequest::new("tenant-a"), &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("analytics scan");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| {
            row.message
                .as_deref()
                .is_none_or(|message| !message.ends_with("101"))
        }));
        drop(store);

        let recovered = DurableTelemetryStore::open(config).expect("recovered store");
        assert_eq!(recovered.delete_requests("tenant-a").unwrap().len(), 1);
        assert_eq!(recovered.entries("tenant-a").unwrap().len(), 2);
        assert!(recovered.cancel_delete("tenant-a", &request_id).unwrap());
        assert_eq!(recovered.entries("tenant-a").unwrap().len(), 3);
        drop(recovered);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn retention_cutoff_is_enforced_by_loki_native_and_analytical_reads() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-retention-store-{}-{nonce}",
            std::process::id()
        ));
        let now = i64::try_from(nonce).expect("current timestamp fits i64");
        let config = DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: true,
            retention: Some(Duration::from_secs(60)),
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let store = DurableTelemetryStore::open(config.clone()).expect("store");
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: now - 120_000_000_000,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "expired message".to_owned(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push expired batch");
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: now,
                    labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                    line: "retained message".to_owned(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("push retained batch");
        let entries = store.entries("tenant-a").expect("Loki query");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].line, "retained message");

        let native = store
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::new(),
                terms: vec!["message".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("native query");
        assert_eq!(native.len(), 1);
        assert_eq!(native[0].line, "retained message");

        let mut rows = Vec::new();
        store
            .scan_analytics(&AnalyticsScanRequest::new("tenant-a"), &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("analytics scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message.as_deref(), Some("retained message"));
        let report = store.compact_retention().expect("retention compaction");
        assert_eq!(report.advanced_partitions, 1);
        assert_eq!(report.advanced_offsets, 1);
        assert_eq!(store.operational_metrics().retention_runs, 1);
        drop(store);
        let recovered = DurableTelemetryStore::open(config).expect("restart after compaction");
        assert_eq!(recovered.entries("tenant-a").unwrap().len(), 1);
        drop(recovered);
        fs::remove_dir_all(directory).expect("cleanup");
    }

    #[test]
    fn standalone_store_makes_the_stel_envelope_authoritative() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-ingest-pack-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        let entry = LokiEntry {
            timestamp_unix_nanos: 123,
            labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
            line: "request completed".to_owned(),
            structured_metadata: BTreeMap::from([("trace_id".to_owned(), "abc".to_owned())]),
        };
        store
            .push("tenant-a", vec![entry])
            .expect("Loki push succeeds");
        let batches = store
            .engine
            .fetch(FetchRequest {
                request_id: 1,
                topic_id: LOKI_TOPIC_ID,
                partition_id: LogicalPartitionId::new(0),
                start_offset: LogicalOffset::new(0),
                max_bytes: 1024 * 1024,
                mode: FetchMode::Ordered,
            })
            .expect("authoritative batch fetches");
        assert_eq!(batches.len(), 1);
        let envelope = crate::TelemetryEnvelope::decode(&batches[0].payload)
            .expect("stored STEL envelope validates");
        assert_eq!(envelope.signal, crate::TelemetrySignal::Logs);
        validate_ingest_pack(&envelope.payload, 1).expect("stored pack validates");
        let decoded = decode_ingest_pack(&envelope.payload).expect("stored pack decodes");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].timestamp_unix_nanos, 123);
        assert_eq!(decoded[0].message.as_ref(), "request completed");
        assert!(decoded[0].fields.iter().any(|field| {
            field.key.as_ref() == "resource.loki.tenant" && field.value.as_ref() == "tenant-a"
        }));
        drop(store);
        fs::remove_dir_all(directory).expect("remove test store");
    }

    #[test]
    fn durable_analytics_scan_pushes_indexable_constraints_into_stripes() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-analytics-store-{}-{nonce}",
            std::process::id()
        ));
        let store = DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens");
        store
            .push(
                "tenant-a",
                vec![
                    LokiEntry {
                        timestamp_unix_nanos: 100,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: "request completed".to_owned(),
                        structured_metadata: BTreeMap::from([(
                            "code".to_owned(),
                            "200".to_owned(),
                        )]),
                    },
                    LokiEntry {
                        timestamp_unix_nanos: 200,
                        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                        line: "request ERROR".to_owned(),
                        structured_metadata: BTreeMap::from([(
                            "code".to_owned(),
                            "500".to_owned(),
                        )]),
                    },
                ],
            )
            .expect("push");
        store
            .push(
                "tenant-a",
                vec![LokiEntry {
                    timestamp_unix_nanos: 300,
                    labels: BTreeMap::from([("app".to_owned(), "worker".to_owned())]),
                    line: "newest request".to_owned(),
                    structured_metadata: BTreeMap::new(),
                }],
            )
            .expect("second partition push");
        let mut request = AnalyticsScanRequest::new("tenant-a");
        request.start_timestamp_unix_nanos = Some(150);
        request.end_timestamp_unix_nanos = Some(250);
        request.terms.push(Arc::from("error"));
        request.labels.push(crate::MetadataField::new("app", "api"));
        request
            .metadata
            .push(crate::MetadataField::new("code", "500"));
        let mut rows = Vec::new();
        store
            .scan_analytics(&request, &mut |batch| {
                rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp_unix_nanos, 200);
        assert_eq!(rows[0].message.as_deref(), Some("request ERROR"));
        assert_eq!(rows[0].labels["app"], "api");
        assert_eq!(rows[0].metadata["code"], "500");
        let mut newest = AnalyticsScanRequest::new("tenant-a");
        newest.limit = Some(1);
        newest.order = Some(AnalyticsScanOrder::TimestampDescending);
        let mut newest_rows = Vec::new();
        store
            .scan_analytics(&newest, &mut |batch| {
                newest_rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("newest scan");
        assert_eq!(newest_rows.len(), 1);
        assert_eq!(newest_rows[0].timestamp_unix_nanos, 300);
        assert_eq!(newest_rows[0].message.as_deref(), Some("newest request"));
        newest.columns = vec![
            crate::AnalyticsColumn::Timestamp,
            crate::AnalyticsColumn::Message,
        ];
        newest_rows.clear();
        store
            .scan_analytics(&newest, &mut |batch| {
                newest_rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("projected newest scan");
        assert_eq!(newest_rows.len(), 1);
        assert_eq!(newest_rows[0].message.as_deref(), Some("newest request"));
        assert!(newest_rows[0].metadata.is_empty());
        assert!(newest_rows[0].body_json.is_none());
        let mut newest_exact_token = AnalyticsScanRequest::new("tenant-a");
        newest_exact_token.limit = Some(1);
        newest_exact_token.order = Some(AnalyticsScanOrder::TimestampDescending);
        newest_exact_token.message_tokens.push(Arc::from("ERROR"));
        let mut exact_rows = Vec::new();
        store
            .scan_analytics(&newest_exact_token, &mut |batch| {
                exact_rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("newest exact-token scan");
        assert_eq!(exact_rows.len(), 1);
        assert_eq!(exact_rows[0].timestamp_unix_nanos, 200);
        assert_eq!(exact_rows[0].message.as_deref(), Some("request ERROR"));
        let mut newest_folded_token = AnalyticsScanRequest::new("tenant-a");
        newest_folded_token.limit = Some(1);
        newest_folded_token.order = Some(AnalyticsScanOrder::TimestampDescending);
        newest_folded_token
            .case_insensitive_message_tokens
            .push(Arc::from("error"));
        let mut folded_rows = Vec::new();
        store
            .scan_analytics(&newest_folded_token, &mut |batch| {
                folded_rows.extend_from_slice(batch);
                Ok(())
            })
            .expect("newest case-insensitive token scan");
        assert_eq!(folded_rows.len(), 1);
        assert_eq!(folded_rows[0].timestamp_unix_nanos, 200);
        drop(store);
        fs::remove_dir_all(directory).expect("remove test store");
    }
}
