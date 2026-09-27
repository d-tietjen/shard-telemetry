use super::*;

impl EmbeddedTelemetryConfig {
    /// Creates an embedded runtime configuration around one durable store.
    #[must_use]
    pub fn new(store: DurableTelemetryConfig) -> Self {
        // Embedded mode still owns one append-submission and durable-sink
        // dispatcher pool. Size both from the physical shard topology so a
        // multi-shard embedded store retains the same owner parallelism as a
        // standalone server. A one-shard store keeps the existing one-thread
        // footprint.
        let worker_count = usize::try_from(store.shard_count)
            .unwrap_or(64)
            .clamp(1, 64);
        let local_limits = DurableTelemetryLimits {
            max_lifetime_rollup_series: Some(100_000),
            append_submission_threads: Some(worker_count),
            durable_sink_threads: Some(worker_count),
            queue_slots_per_shard: 64,
            queue_bytes_per_shard: 2 * 1024 * 1024,
            target_pack_bytes: 512 * 1024,
            max_batch_bytes: 1024 * 1024,
            max_fetch_bytes: 1024 * 1024,
            ..DurableTelemetryLimits::default()
        };
        Self {
            store,
            local_limits,
            object_tier: ObjectTierConfig::default(),
            max_ram_bytes: None,
            max_ssd_bytes: None,
            shutdown_flush_timeout: Duration::from_secs(30),
        }
    }

    /// Creates a one-shard, SSD-backed embedded configuration with time-based
    /// retention.
    ///
    /// The local immutable object directory lives below `data_directory`, so
    /// completed blocks can replace raw WAL packs while remaining queryable.
    /// Call [`EmbeddedTelemetryRuntime::compact_retention`] periodically to
    /// reclaim complete groups older than `retention`.
    #[must_use]
    pub fn bounded_local(data_directory: impl Into<PathBuf>, retention: Duration) -> Self {
        Self::bounded(data_directory, retention, EmbeddedEvictionPolicy::Delete)
    }

    /// Creates a one-shard embedded store with a recent local window and an
    /// explicit eviction destination.
    #[must_use]
    pub fn bounded(
        data_directory: impl Into<PathBuf>,
        retention: Duration,
        eviction: EmbeddedEvictionPolicy,
    ) -> Self {
        let data_directory = data_directory.into();
        let (object_store_directory, s3_object_store) = match eviction {
            EmbeddedEvictionPolicy::Delete => (Some(data_directory.join("objects")), None),
            EmbeddedEvictionPolicy::OffloadToS3(s3) => (None, Some(s3)),
        };
        let mut config = Self::new(DurableTelemetryConfig {
            object_store_directory,
            data_directory,
            s3_object_store,
            recovery_journal: false,
            retention: Some(retention),
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        });
        if config.store.s3_object_store.is_none() {
            // Embedded mode has one process and the tier already protects
            // active readers with generation leases, so no cross-process grace
            // window is required before deleting expired local objects.
            config.object_tier.retirement_grace = Duration::ZERO;
        }
        config
    }

    /// Sets a total bound for storage-engine telemetry state in RAM.
    ///
    /// Two thirds is divided across log, trace, and metric heads on every
    /// physical stripe, one twelfth is reserved for compression dictionaries,
    /// and the remainder is divided across verified payload and control caches.
    #[must_use]
    pub fn with_max_ram_bytes(mut self, max_bytes: u64) -> Self {
        self.max_ram_bytes = Some(max_bytes);
        let stripes = u64::from(
            self.store
                .shard_count
                .min(self.store.tenant_partitions)
                .max(1),
        );
        let physical_shards = u64::from(self.store.shard_count.max(1));
        let queue_total = (max_bytes / 16).max(1);
        self.local_limits.queue_bytes_per_shard =
            usize_from_u64((queue_total / physical_shards).max(1));
        self.local_limits.queue_slots_per_shard = self
            .local_limits
            .queue_slots_per_shard
            .min(self.local_limits.queue_bytes_per_shard)
            .max(1);
        self.local_limits.max_batch_bytes = self
            .local_limits
            .max_batch_bytes
            .min((self.local_limits.queue_bytes_per_shard / 2).max(1));
        self.local_limits.max_fetch_bytes = self
            .local_limits
            .max_fetch_bytes
            .min(self.local_limits.max_batch_bytes)
            .max(1);
        self.local_limits.target_pack_bytes = self
            .local_limits
            .target_pack_bytes
            .min(u64::try_from(self.local_limits.max_batch_bytes).unwrap_or(u64::MAX))
            .max(1);
        let component_bytes = max_bytes.saturating_sub(
            u64::try_from(self.local_limits.queue_bytes_per_shard)
                .unwrap_or(u64::MAX)
                .saturating_mul(physical_shards),
        );
        let head_total = component_bytes.saturating_mul(2) / 3;
        let head_per_stripe = head_total / stripes;
        let logs = (head_per_stripe / 8).max(1);
        let traces = (head_per_stripe / 4).max(1);
        let metrics = head_per_stripe
            .saturating_sub(logs)
            .saturating_sub(traces)
            .max(1);
        self.local_limits.signals.logs.head_memory_bytes_per_stripe = usize_from_u64(logs);
        self.local_limits
            .signals
            .traces
            .head_memory_bytes_per_stripe = usize_from_u64(traces);
        self.local_limits
            .signals
            .metrics
            .head_memory_bytes_per_stripe = usize_from_u64(metrics);

        let dictionary_per_stripe = (component_bytes / 12 / stripes).max(1);
        self.store.stripe.dictionary_cache_bytes = usize_from_u64(dictionary_per_stripe);
        let charged_heads =
            stripes.saturating_mul(logs.saturating_add(traces).saturating_add(metrics));
        let charged_dictionaries = stripes.saturating_mul(dictionary_per_stripe);
        let cache_total = component_bytes
            .saturating_sub(charged_heads)
            .saturating_sub(charged_dictionaries);
        let control_total = cache_total / 4;
        self.local_limits.control_cache.memory_bytes = control_total.saturating_mul(3) / 4;
        self.local_limits.control_cache.parsed_memory_bytes = control_total / 4;
        self.local_limits.payload_cache.memory_bytes = cache_total.saturating_sub(control_total);
        self.local_limits.payload_cache.parsed_memory_bytes = 0;
        self
    }

    /// Sets the steady-state local data budget and derives cache/group limits.
    ///
    /// Local-delete mode reserves three quarters for immutable payloads, one
    /// eighth for caches, one sixteenth for lifetime rollups, and the remainder
    /// for catalog/filesystem overhead. S3 mode assigns the remaining managed
    /// budget to local caches because authoritative immutable objects are remote.
    #[must_use]
    pub fn with_max_ssd_bytes(mut self, max_bytes: u64) -> Self {
        self.max_ssd_bytes = Some(max_bytes);
        let archive = self.store.s3_object_store.is_some();
        let rollup_bytes = (max_bytes / 16).max(1);
        self.local_limits.max_lifetime_rollup_bytes = rollup_bytes;
        let cache_and_objects = max_bytes.saturating_sub(rollup_bytes);
        let (control_bytes, payload_cache_bytes, object_bytes) = if archive {
            let control = (cache_and_objects / 8).max(1);
            (control, cache_and_objects.saturating_sub(control).max(1), 0)
        } else {
            let cache = (max_bytes / 16).max(1);
            (cache, cache, max_bytes.saturating_mul(3) / 4)
        };
        configure_cache_ssd(&mut self.local_limits.control_cache, control_bytes);
        configure_cache_ssd(&mut self.local_limits.payload_cache, payload_cache_bytes);
        if archive {
            self.local_limits.max_object_payload_bytes_per_partition = None;
        } else {
            let catalogs = u64::from(self.store.tenant_partitions).saturating_mul(3);
            let per_partition = (object_bytes / catalogs.max(1)).max(1);
            self.local_limits.max_object_payload_bytes_per_partition = Some(per_partition);
            self.object_tier.max_group_payload_bytes =
                self.object_tier.max_group_payload_bytes.min(per_partition);
            self.object_tier.target_group_payload_bytes = self
                .object_tier
                .target_group_payload_bytes
                .min((self.object_tier.max_group_payload_bytes / 2).max(1));
        }
        self
    }

    /// Sets both embedded storage-state budgets.
    #[must_use]
    pub fn with_storage_budgets(self, max_ram_bytes: u64, max_ssd_bytes: u64) -> Self {
        self.with_max_ram_bytes(max_ram_bytes)
            .with_max_ssd_bytes(max_ssd_bytes)
    }

    /// Replaces the complete bounded local-storage policy.
    #[must_use]
    pub fn with_local_limits(mut self, limits: DurableTelemetryLimits) -> Self {
        self.local_limits = limits;
        self
    }

    /// Sets the hot in-memory head limit for logs, traces, and metrics on each
    /// physical stripe.
    #[must_use]
    pub fn with_head_memory_bytes_per_stripe(
        mut self,
        logs: usize,
        traces: usize,
        metrics: usize,
    ) -> Self {
        self.local_limits.signals.logs.head_memory_bytes_per_stripe = logs;
        self.local_limits
            .signals
            .traces
            .head_memory_bytes_per_stripe = traces;
        self.local_limits
            .signals
            .metrics
            .head_memory_bytes_per_stripe = metrics;
        self
    }

    /// Sets independent SSD-cache policies for metadata/index objects and
    /// compressed payload ranges.
    #[must_use]
    pub fn with_ssd_caches(
        mut self,
        control_cache: SsdCacheConfig,
        payload_cache: SsdCacheConfig,
    ) -> Self {
        self.local_limits.control_cache = control_cache;
        self.local_limits.payload_cache = payload_cache;
        self
    }

    /// Sets the bounded shutdown flush timeout.
    #[must_use]
    pub const fn with_shutdown_flush_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_flush_timeout = timeout;
        self
    }

    pub(super) fn validate(&self) -> Result<(), LokiApiError> {
        if self.shutdown_flush_timeout.is_zero() {
            return Err(LokiApiError::configuration(
                "embedded shutdown_flush_timeout must be nonzero",
            ));
        }
        if self.max_ram_bytes == Some(0) || self.max_ssd_bytes == Some(0) {
            return Err(LokiApiError::configuration(
                "embedded RAM and SSD budgets must be nonzero",
            ));
        }
        if let Some(max_bytes) = self.max_ram_bytes {
            let stripes = u64::from(
                self.store
                    .shard_count
                    .min(self.store.tenant_partitions)
                    .max(1),
            );
            let heads = [
                self.local_limits.signals.logs.head_memory_bytes_per_stripe,
                self.local_limits
                    .signals
                    .traces
                    .head_memory_bytes_per_stripe,
                self.local_limits
                    .signals
                    .metrics
                    .head_memory_bytes_per_stripe,
            ]
            .into_iter()
            .map(|bytes| u64::try_from(bytes).unwrap_or(u64::MAX))
            .fold(0_u64, u64::saturating_add)
            .saturating_mul(stripes);
            let caches = self
                .local_limits
                .control_cache
                .memory_bytes
                .saturating_add(self.local_limits.control_cache.parsed_memory_bytes)
                .saturating_add(self.local_limits.payload_cache.memory_bytes)
                .saturating_add(self.local_limits.payload_cache.parsed_memory_bytes);
            let dictionaries = u64::try_from(self.store.stripe.dictionary_cache_bytes)
                .unwrap_or(u64::MAX)
                .saturating_mul(stripes);
            let queues = u64::try_from(self.local_limits.queue_bytes_per_shard)
                .unwrap_or(u64::MAX)
                .saturating_mul(u64::from(self.store.shard_count.max(1)));
            if heads
                .saturating_add(dictionaries)
                .saturating_add(caches)
                .saturating_add(queues)
                > max_bytes
            {
                return Err(LokiApiError::configuration(
                    "embedded component RAM limits exceed max_ram_bytes",
                ));
            }
        }
        if let Some(max_bytes) = self.max_ssd_bytes {
            let caches = self
                .local_limits
                .control_cache
                .max_bytes
                .saturating_add(self.local_limits.payload_cache.max_bytes);
            let objects = self
                .local_limits
                .max_object_payload_bytes_per_partition
                .unwrap_or(0)
                .saturating_mul(u64::from(self.store.tenant_partitions).saturating_mul(3));
            let rollup = self
                .local_limits
                .max_lifetime_rollup_series
                .map_or(0, |_| self.local_limits.max_lifetime_rollup_bytes);
            if caches.saturating_add(objects).saturating_add(rollup) > max_bytes {
                return Err(LokiApiError::configuration(
                    "embedded component SSD limits exceed max_ssd_bytes",
                ));
            }
        }
        Ok(())
    }
}
