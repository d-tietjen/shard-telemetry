use super::*;

impl DurableTelemetryStore {
    /// Returns the common logical partition count used by this local store.
    #[must_use]
    pub const fn telemetry_partition_count(&self) -> u32 {
        self.tenant_partitions
    }

    /// Runs partition-parallel ingest work on this store's bounded append pool.
    ///
    /// Transport adapters use this for envelope preparation so their Rayon
    /// work follows the same CPU budget as the subsequent durable append.
    pub(crate) fn install_append_parallelism<OP, R>(&self, operation: OP) -> R
    where
        OP: FnOnce() -> R + Send,
        R: Send,
    {
        self.append_submission_pool.install(operation)
    }

    /// Opens or recovers a standalone durable store.
    pub fn open(config: DurableTelemetryConfig) -> Result<Self, LokiApiError> {
        Self::open_with_local_limits(config, DurableTelemetryLimits::default())
    }

    /// Opens a store with explicit bounded local memory and SSD-cache limits.
    pub fn open_with_local_limits(
        config: DurableTelemetryConfig,
        limits: DurableTelemetryLimits,
    ) -> Result<Self, LokiApiError> {
        Self::open_with_object_tier_config_and_local_limits(
            config,
            ObjectTierConfig::default(),
            limits,
        )
    }

    /// Opens a store with explicit object publication and reader-lease bounds.
    pub fn open_with_object_tier_config(
        config: DurableTelemetryConfig,
        object_tier_config: ObjectTierConfig,
    ) -> Result<Self, LokiApiError> {
        Self::open_with_object_tier_config_and_local_limits(
            config,
            object_tier_config,
            DurableTelemetryLimits::default(),
        )
    }

    /// Opens a store with explicit object-tier policy and bounded local limits.
    pub fn open_with_object_tier_config_and_local_limits(
        config: DurableTelemetryConfig,
        object_tier_config: ObjectTierConfig,
        limits: DurableTelemetryLimits,
    ) -> Result<Self, LokiApiError> {
        config.validate()?;
        if limits.append_submission_threads == Some(0)
            || limits
                .append_submission_threads
                .is_some_and(|threads| threads > MAX_APPEND_SUBMISSION_THREADS)
            || limits.durable_sink_threads == Some(0)
            || limits
                .durable_sink_threads
                .is_some_and(|threads| threads > MAX_DURABLE_SINK_THREADS)
            || limits.object_store_threads == Some(0)
            || limits
                .object_store_threads
                .is_some_and(|threads| threads > 64)
            || limits.queue_slots_per_shard == 0
            || limits.queue_bytes_per_shard == 0
            || limits.target_pack_bytes == 0
            || limits.max_batch_bytes == 0
            || limits.max_fetch_bytes == 0
        {
            return Err(LokiApiError::configuration(
                "append worker count must be 1..=64, durable sink worker count must be 1..=256, S3 worker count must be 1..=64, and queue, pack, batch, and fetch limits must be nonzero",
            ));
        }
        let max_fetch_bytes = u32::try_from(limits.max_fetch_bytes).map_err(|_| {
            LokiApiError::configuration("max_fetch_bytes must fit the v1 u32 fetch limit")
        })?;
        let append_submission_pool =
            build_append_submission_pool(config.shard_count, limits.append_submission_threads)?;
        object_tier_config
            .validate()
            .map_err(|error| LokiApiError::configuration(error.to_string()))?;
        let logical_partitions =
            NonZeroU16::new(u16::try_from(config.tenant_partitions).map_err(|_| {
                LokiApiError::configuration("tenant_partitions must fit the v1 u16 routing space")
            })?)
            .expect("configuration validation rejects zero partitions");
        let telemetry_router = crate::TelemetryRouter::new(logical_partitions);
        let physical_stripes = NonZeroU16::new(
            u16::try_from(config.shard_count.min(config.tenant_partitions)).map_err(|_| {
                LokiApiError::configuration("shard_count must fit the v1 u16 routing space")
            })?,
        )
        .expect("configuration validation rejects zero shards");
        if limits.max_lifetime_rollup_series == Some(0)
            || (limits.max_lifetime_rollup_series.is_some()
                && limits.max_lifetime_rollup_bytes == 0)
        {
            return Err(LokiApiError::configuration(
                "lifetime metric rollup series and byte limits must be nonzero when enabled",
            ));
        }
        let max_lifetime_rollup_series = limits.max_lifetime_rollup_series;
        if limits
            .max_object_payload_bytes_per_partition
            .is_some_and(|bytes| bytes == 0)
        {
            return Err(LokiApiError::configuration(
                "object payload bytes per partition must be nonzero when configured",
            ));
        }
        let max_object_payload_bytes_per_partition = limits.max_object_payload_bytes_per_partition;
        let mut signals = limits.signals;
        for signal in [&mut signals.logs, &mut signals.traces, &mut signals.metrics] {
            signal.logical_partitions = logical_partitions;
            signal.physical_stripes = physical_stripes;
            signal.retention = config.retention;
        }
        let data_directory_lease = DataDirectoryLease::acquire(&config.data_directory)?;
        let lifetime_rollups = max_lifetime_rollup_series
            .map(|max_series| {
                MetricRollupCatalog::open(
                    config
                        .data_directory
                        .join("lifetime-metric-rollups-v1.msgpack"),
                    max_series,
                    limits.max_lifetime_rollup_bytes,
                )
                .map(Mutex::new)
            })
            .transpose()
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let deletes = DeleteCatalog::open(config.data_directory.join("delete-catalog-v1.json"))?;
        let append_receipts = AppendReceiptCatalog::open(&config.data_directory)?;
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
            queue_slots_per_shard: limits.queue_slots_per_shard,
            queue_bytes_per_shard: limits.queue_bytes_per_shard,
            target_pack_bytes: limits.target_pack_bytes,
            max_pack_age: Duration::from_secs(1),
            max_batch_bytes: limits.max_batch_bytes,
            max_fetch_bytes: limits.max_fetch_bytes,
            append_linger: config.append_linger,
        };
        let archive_object_tier = config.s3_object_store.is_some();
        let object_store = match (
            config.object_store_directory.as_ref(),
            config.s3_object_store.clone(),
        ) {
            (Some(directory), None) => Some(SharedTelemetryObjectStore::from(
                LocalObjectStore::open(directory)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?,
            )),
            (None, Some(s3)) => Some(SharedTelemetryObjectStore::new(Arc::new(
                S3ObjectStore::open_with_threads(s3, limits.object_store_threads)
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
                    control_cache: limits.control_cache,
                    payload_cache: limits.payload_cache,
                    warm_local_cache_on_publish: archive_object_tier,
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
            max_journal_bytes: limits.max_index_journal_bytes,
            journal_sync_each_append: config.retention.is_some(),
            object_tier: sink_object_tier,
            ..OtlpSinkConfig::default()
        };
        let factory = Arc::new(
            TelemetrySinkFactory::new(engine_config.shard_ids(), sink_config)
                .map_err(|error| LokiApiError::internal(error.to_string()))?,
        );
        let service = factory.service();
        let sink_options = DurableSinkOptions {
            worker_count: durable_sink_worker_count(
                config.shard_count,
                limits.durable_sink_threads,
            ),
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
        match engine.create_partition_affine_topic(TopicConfig {
            topic_id: LOKI_TOPIC_ID,
            partitions: config.tenant_partitions,
            shards: None,
        }) {
            Ok(()) | Err(EngineError::TopicAlreadyExists(_)) => {}
            Err(error) => return Err(engine_error(error)),
        }
        for topic_id in [crate::TRACES_TOPIC_ID, crate::METRICS_TOPIC_ID] {
            match engine.create_partition_affine_topic(TopicConfig {
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
            append_durability: Durability::Leader,
            append_gate: None,
            tenant_partitions: config.tenant_partitions,
            physical_shard_count: Some(config.shard_count),
            telemetry_router,
            ingest_stripes_per_tenant: config.shard_count.min(config.tenant_partitions),
            indexed_ack_timeout: config.indexed_ack_timeout,
            max_fetch_bytes,
            append_submission_pool,
            next_request_id: AtomicU64::new(1),
            append_receipts,
            lifetime_rollups,
            remote_write_append: new_remote_write_locks(),
            deletes,
            retention: config.retention,
            retention_runs: AtomicU64::new(0),
            retention_advanced_offsets: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
            object_tier_enabled,
            archive_object_tier,
            max_object_payload_bytes_per_partition,
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
        Self::attach_with_durability(TelemetryHostAttachment {
            data_directory,
            engine,
            service,
            tenant_partitions,
            ingest_stripes_per_tenant,
            indexed_ack_timeout,
            retention,
            append_durability: TelemetryAppendDurability::Leader,
            append_gate: None,
        })
    }

    /// Attaches ShardTelemetry to an externally owned stream engine with the
    /// acknowledgement durability selected by that host.
    ///
    /// HA hosts must use [`TelemetryAppendDurability::Quorum`] and configure
    /// the supplied engine with their replicated transport, assignment
    /// provider, and write fence before calling this method. The store owns no
    /// WAL in this mode; it only creates the signal topics and query state on
    /// the supplied engine.
    pub fn attach_with_durability(
        attachment: TelemetryHostAttachment,
    ) -> Result<Self, LokiApiError> {
        let TelemetryHostAttachment {
            data_directory,
            engine,
            service,
            tenant_partitions,
            ingest_stripes_per_tenant,
            indexed_ack_timeout,
            retention,
            append_durability,
            append_gate,
        } = attachment;
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
        let logical_partitions =
            NonZeroU16::new(u16::try_from(tenant_partitions).map_err(|_| {
                LokiApiError::configuration("tenant_partitions must fit the v1 u16 routing space")
            })?)
            .ok_or_else(|| LokiApiError::configuration("tenant_partitions must be nonzero"))?;
        let append_submission_pool = build_append_submission_pool(ingest_stripes_per_tenant, None)?;
        let data_directory_lease = DataDirectoryLease::acquire(&data_directory)?;
        let deletes = DeleteCatalog::open(data_directory.join("delete-catalog-v1.json"))?;
        let append_receipts = AppendReceiptCatalog::open(&data_directory)?;
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
            append_durability: append_durability.into(),
            append_gate,
            tenant_partitions,
            physical_shard_count: None,
            telemetry_router: crate::TelemetryRouter::new(logical_partitions),
            ingest_stripes_per_tenant,
            indexed_ack_timeout,
            max_fetch_bytes: 16 * 1024 * 1024,
            append_submission_pool,
            next_request_id: AtomicU64::new(1),
            append_receipts,
            lifetime_rollups: None,
            remote_write_append: new_remote_write_locks(),
            deletes,
            retention,
            retention_runs: AtomicU64::new(0),
            retention_advanced_offsets: AtomicU64::new(0),
            retention_failures: AtomicU64::new(0),
            object_tier_enabled,
            archive_object_tier: false,
            max_object_payload_bytes_per_partition: None,
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

    pub(super) fn tenant_partition_base(&self, tenant: &str) -> u32 {
        let hash = tenant
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            });
        let groups = self.tenant_partitions / self.ingest_stripes_per_tenant;
        ((hash % u64::from(groups.max(1))) as u32) * self.ingest_stripes_per_tenant
    }

    pub(super) fn write_partition(&self, tenant: &str, request_id: u64) -> TopicPartition {
        let partition = self.tenant_partition_base(tenant)
            + (request_id % u64::from(self.ingest_stripes_per_tenant)) as u32;
        TopicPartition::new(
            LOKI_TOPIC_ID,
            LogicalPartitionId::new(partition % self.tenant_partitions),
        )
    }

    pub(super) fn tenant_partitions(
        &self,
        tenant: &str,
    ) -> Result<Vec<TopicPartition>, LokiApiError> {
        self.service
            .active_log_partitions(Arc::from(tenant))
            .map_err(|error| LokiApiError::internal(error.to_string()))
    }

    pub(super) fn retention_cutoff(&self) -> Option<u64> {
        let retention = self.retention?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let cutoff = now.saturating_sub(retention.as_nanos());
        Some(u64::try_from(cutoff).unwrap_or(u64::MAX))
    }

    pub(super) fn retained_query_start(&self, requested: Option<u64>) -> Option<u64> {
        match (requested, self.retention_cutoff()) {
            (Some(requested), Some(cutoff)) => Some(requested.max(cutoff)),
            (None, Some(cutoff)) => Some(cutoff),
            (requested, None) => requested,
        }
    }

    pub(super) fn standalone_owner_shard(&self, partition: TopicPartition) -> Option<ShardId> {
        self.physical_shard_count
            .map(|shard_count| ShardId::new(partition.partition_id.get() % shard_count))
    }

    pub(super) fn trace_query_owner_shard(&self, query: &crate::TraceQuery) -> Option<ShardId> {
        let partition = query.partition.or_else(|| {
            query
                .trace_id
                .map(|trace_id| self.telemetry_router.trace(&query.tenant, trace_id))
        })?;
        self.standalone_owner_shard(partition)
    }

    pub(super) fn metric_query_owner_shard(&self, query: &crate::MetricQuery) -> Option<ShardId> {
        let partition = query.partition.or_else(|| {
            query
                .series
                .map(|series| self.telemetry_router.metric(&query.tenant, series))
        })?;
        self.standalone_owner_shard(partition)
    }
}
