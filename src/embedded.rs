use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::{
    DurableMetricPoint, DurableTelemetryConfig, DurableTelemetryLimits, DurableTelemetryStore,
    LokiApiError, LokiStore, NativeTelemetryAppendAck, ObjectTierConfig, OtlpLogEvent,
    OtlpSpanEvent, RetentionReport, S3ObjectStoreConfig, SsdCacheConfig, StripeConfig,
};

fn usize_from_u64(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn configure_cache_ssd(config: &mut SsdCacheConfig, max_bytes: u64) {
    config.max_bytes = max_bytes.max(1);
    config.chunk_bytes = config.chunk_bytes.min(config.max_bytes).max(1);
    config.max_read_bytes = config.max_read_bytes.max(config.chunk_bytes);
}

/// Fate of raw records that leave an embedded node's recent local window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbeddedEvictionPolicy {
    /// Permanently delete expired or over-budget immutable groups.
    Delete,
    /// Commit immutable groups to S3, then keep only an LRU-bounded local cache.
    OffloadToS3(S3ObjectStoreConfig),
}

/// Lifecycle configuration for a one-process embedded ShardTelemetry runtime.
#[derive(Debug, Clone)]
pub struct EmbeddedTelemetryConfig {
    /// Durable local WAL, index, and object-tier configuration.
    pub store: DurableTelemetryConfig,
    /// Bounded hot-memory, journal, and SSD-cache limits.
    pub local_limits: DurableTelemetryLimits,
    /// Immutable group sizing used for local deletion or S3 publication.
    pub object_tier: ObjectTierConfig,
    /// Optional bound for engine-controlled resident telemetry state.
    ///
    /// Transient request decoding and query result allocations are outside this
    /// storage-state budget.
    pub max_ram_bytes: Option<u64>,
    /// Optional bound for local immutable payload and SSD-cache bytes.
    ///
    /// Filesystem metadata and one in-flight WAL/spool group may temporarily
    /// exceed this steady-state data budget.
    pub max_ssd_bytes: Option<u64>,
    /// Upper bound applied when draining the embedded store during shutdown.
    pub shutdown_flush_timeout: Duration,
}

impl EmbeddedTelemetryConfig {
    /// Creates an embedded runtime configuration around one durable store.
    #[must_use]
    pub fn new(store: DurableTelemetryConfig) -> Self {
        let local_limits = DurableTelemetryLimits {
            max_lifetime_rollup_series: Some(100_000),
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
        let head_total = max_bytes.saturating_mul(2) / 3;
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

        let dictionary_per_stripe = (max_bytes / 12 / stripes).max(1);
        self.store.stripe.dictionary_cache_bytes = usize_from_u64(dictionary_per_stripe);
        let charged_heads =
            stripes.saturating_mul(logs.saturating_add(traces).saturating_add(metrics));
        let charged_dictionaries = stripes.saturating_mul(dictionary_per_stripe);
        let cache_total = max_bytes
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

    fn validate(&self) -> Result<(), LokiApiError> {
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
            if heads.saturating_add(dictionaries).saturating_add(caches) > max_bytes {
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

/// Observable lifecycle state of [`EmbeddedTelemetryRuntime`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum EmbeddedTelemetryState {
    /// The durable store is recovering local state.
    Recovering = 0,
    /// Recovery succeeded; exporters may now be attached before traffic starts.
    AwaitingAttachment = 1,
    /// Ingestion and query consumers may use the runtime.
    Ready = 2,
    /// The runtime is draining and does not accept new attachment work.
    Draining = 3,
    /// The durable store has been flushed and closed by its owner.
    Stopped = 4,
}

impl EmbeddedTelemetryState {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::AwaitingAttachment,
            2 => Self::Ready,
            3 => Self::Draining,
            4 => Self::Stopped,
            _ => Self::Recovering,
        }
    }
}

/// One-owner embedded runtime suitable for a local node telemetry deployment.
///
/// Opening the runtime performs all WAL, index, catalog, and metric-accumulator
/// recovery synchronously. Call [`Self::mark_ready`] only after the host has
/// attached its exporters, then call [`Self::drain`] before dropping the last
/// runtime reference during shutdown.
#[derive(Debug)]
pub struct EmbeddedTelemetryRuntime {
    store: Arc<DurableTelemetryStore>,
    shutdown_flush_timeout: Duration,
    state: AtomicU8,
    /// Serializes exporter attachment, readiness, and drain transitions. The
    /// atomic state remains cheap for steady-state ingestion checks, while this
    /// lock makes lifecycle transitions linearizable.
    lifecycle_gate: Mutex<()>,
    /// Coordinates direct in-process producers with shutdown. A producer holds
    /// a shared lease across the complete durable append; shutdown first marks
    /// the runtime draining, then takes the exclusive lease before flushing.
    ingestion_gate: RwLock<()>,
}

impl EmbeddedTelemetryRuntime {
    /// Opens and recovers the local durable store. The data directory remains
    /// exclusively owned until this runtime and all store clones are dropped.
    pub fn open(config: EmbeddedTelemetryConfig) -> Result<Self, LokiApiError> {
        config.validate()?;
        let runtime = Self {
            store: Arc::new(
                DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
                    config.store,
                    config.object_tier,
                    config.local_limits,
                )?,
            ),
            shutdown_flush_timeout: config.shutdown_flush_timeout,
            state: AtomicU8::new(EmbeddedTelemetryState::Recovering as u8),
            lifecycle_gate: Mutex::new(()),
            ingestion_gate: RwLock::new(()),
        };
        runtime.state.store(
            EmbeddedTelemetryState::AwaitingAttachment as u8,
            Ordering::Release,
        );
        Ok(runtime)
    }

    /// Runs an exporter-attachment operation while the runtime is not yet ready.
    ///
    /// A failed operation leaves the runtime unavailable, so callers cannot
    /// accidentally start a partially attached embedded node.
    pub fn attach<T>(
        &self,
        attach: impl FnOnce() -> Result<T, LokiApiError>,
    ) -> Result<T, LokiApiError> {
        let _transition = self
            .lifecycle_gate
            .lock()
            .map_err(|_| LokiApiError::internal("embedded telemetry lifecycle gate poisoned"))?;
        if self.state() != EmbeddedTelemetryState::AwaitingAttachment {
            return Err(LokiApiError::configuration(
                "embedded telemetry accepts exporter attachment only before readiness",
            ));
        }
        attach()
    }

    /// Marks the recovered, fully attached runtime ready for host traffic.
    pub fn mark_ready(&self) -> Result<(), LokiApiError> {
        let _transition = self
            .lifecycle_gate
            .lock()
            .map_err(|_| LokiApiError::internal("embedded telemetry lifecycle gate poisoned"))?;
        self.state
            .compare_exchange(
                EmbeddedTelemetryState::AwaitingAttachment as u8,
                EmbeddedTelemetryState::Ready as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| {
                LokiApiError::configuration("embedded telemetry is not awaiting attachment")
            })?;
        Ok(())
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub fn state(&self) -> EmbeddedTelemetryState {
        EmbeddedTelemetryState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Queries the recovered embedded log index without exposing a writable
    /// store handle that could bypass this runtime's drain gate.
    pub fn query_native(
        &self,
        query: &crate::NativeQuery,
    ) -> Result<Vec<crate::LokiEntry>, LokiApiError> {
        self.store.query_native(query)
    }

    /// Queries the recovered embedded trace index without exposing writable
    /// ingestion APIs outside the lifecycle gate.
    pub fn query_traces(
        &self,
        query: &crate::TraceQuery,
    ) -> Result<Vec<crate::DurableSpan>, LokiApiError> {
        self.store.query_traces(query)
    }

    /// Queries the recovered embedded metric index without exposing writable
    /// ingestion APIs outside the lifecycle gate.
    pub fn query_metrics(
        &self,
        query: &crate::MetricQuery,
    ) -> Result<Vec<DurableMetricPoint>, LokiApiError> {
        self.store.query_metrics(query)
    }

    /// Returns durable lifetime metric outcomes without reading evicted raw
    /// records or contacting the configured object tier.
    pub fn query_lifetime_metric_rollups(
        &self,
        tenant: &str,
        name: Option<&str>,
    ) -> Result<Vec<crate::LifetimeMetricRollup>, LokiApiError> {
        self.store.query_lifetime_metric_rollups(tenant, name)
    }

    /// Directly appends normalized logs to this process's embedded store.
    ///
    /// This is an in-process fast path: it performs signal routing and the
    /// required WAL-envelope encoding, but does not create a native protocol
    /// frame, serialize a native batch, open a socket, or decode the same data
    /// again. Call it from a bounded exporter worker, not from an application
    /// logging call site.
    pub fn append_log_events(
        &self,
        tenant: &str,
        events: Vec<OtlpLogEvent>,
        wait_for_index: bool,
    ) -> Result<NativeTelemetryAppendAck, LokiApiError> {
        let _lease = self.begin_ingest()?;
        self.store.append_log_events(tenant, events, wait_for_index)
    }

    /// Directly appends normalized spans to this process's embedded store.
    ///
    /// The spans have already passed OTLP validation. This path routes them
    /// by trace identity and writes their durable envelopes without a local
    /// protocol round trip. Invoke it from a bounded exporter worker rather
    /// than an application tracing call site.
    pub fn append_trace_events(
        &self,
        events: Vec<OtlpSpanEvent>,
        wait_for_index: bool,
    ) -> Result<NativeTelemetryAppendAck, LokiApiError> {
        let _lease = self.begin_ingest()?;
        self.store.append_trace_events(events, wait_for_index)
    }

    /// Directly appends normalized metrics to this process's embedded store.
    ///
    /// The metric points retain their native labels, histograms, resource, and
    /// scope identity without an OTLP or native-protocol round trip. It is the
    /// intended target for periodic `fast-telemetry` snapshot export; counter
    /// and histogram updates themselves must remain independent of this call.
    pub fn append_metric_point(
        &self,
        point: DurableMetricPoint,
        wait_for_index: bool,
    ) -> Result<NativeTelemetryAppendAck, LokiApiError> {
        let _lease = self.begin_ingest()?;
        self.store.append_metric_point(point, wait_for_index)
    }

    /// Directly appends normalized metrics to this process's embedded store.
    ///
    /// The metric points retain their native labels, histograms, resource, and
    /// scope identity without an OTLP or native-protocol round trip. It is the
    /// intended target for periodic `fast-telemetry` snapshot export; counter
    /// and histogram updates themselves must remain independent of this call.
    pub fn append_metric_points(
        &self,
        points: Vec<DurableMetricPoint>,
        wait_for_index: bool,
    ) -> Result<NativeTelemetryAppendAck, LokiApiError> {
        let _lease = self.begin_ingest()?;
        self.store.append_metric_points(points, wait_for_index)
    }

    /// Flushes pending local blocks and reclaims complete WAL/object groups
    /// older than the configured retention window.
    ///
    /// Logical queries enforce the cutoff immediately. Hosts should call this
    /// periodically (for example, once per minute for a 5–60 minute window)
    /// so physical SSD usage tracks that logical window.
    pub fn compact_retention(&self) -> Result<RetentionReport, LokiApiError> {
        let _lease = self.begin_ingest()?;
        self.store.compact_retention()
    }

    /// Stops new host work and flushes every current durable boundary.
    ///
    /// The caller remains responsible for stopping its producers before this
    /// method, because producer ownership is intentionally outside this generic
    /// runtime.
    pub fn drain(&self) -> Result<(), LokiApiError> {
        let _transition = self
            .lifecycle_gate
            .lock()
            .map_err(|_| LokiApiError::internal("embedded telemetry lifecycle gate poisoned"))?;
        let previous = self
            .state
            .swap(EmbeddedTelemetryState::Draining as u8, Ordering::AcqRel);
        if EmbeddedTelemetryState::from_u8(previous) == EmbeddedTelemetryState::Stopped {
            return Ok(());
        }
        let _ingestion = self
            .ingestion_gate
            .write()
            .map_err(|_| LokiApiError::internal("embedded telemetry ingestion gate poisoned"))?;
        LokiStore::flush(self.store.as_ref(), self.shutdown_flush_timeout)?;
        self.state
            .store(EmbeddedTelemetryState::Stopped as u8, Ordering::Release);
        Ok(())
    }

    fn begin_ingest(&self) -> Result<std::sync::RwLockReadGuard<'_, ()>, LokiApiError> {
        let lease = self
            .ingestion_gate
            .read()
            .map_err(|_| LokiApiError::internal("embedded telemetry ingestion gate poisoned"))?;
        if self.state() != EmbeddedTelemetryState::Ready {
            return Err(LokiApiError::configuration(
                "embedded telemetry is not ready to accept direct ingestion",
            ));
        }
        Ok(lease)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    use super::*;
    use crate::{
        NativeQuery, NativeQueryDirection, OtlpTelemetryDecoder, ResourceContext, StripeConfig,
        TelemetryValue, TraceId, TraceQuery,
    };

    #[test]
    fn embedded_budget_builder_bounds_local_and_s3_managed_storage() {
        let local = EmbeddedTelemetryConfig::bounded_local(
            std::env::temp_dir().join("shard-telemetry-unused-budget-config"),
            Duration::from_secs(5 * 60),
        )
        .with_storage_budgets(256 * 1024 * 1024, 1024 * 1024 * 1024);
        local.validate().expect("local budgets validate");
        assert!(
            local
                .local_limits
                .max_object_payload_bytes_per_partition
                .is_some()
        );
        assert_eq!(local.object_tier.retirement_grace, Duration::ZERO);

        let archived = EmbeddedTelemetryConfig::bounded(
            std::env::temp_dir().join("shard-telemetry-unused-s3-budget-config"),
            Duration::from_secs(60 * 60),
            EmbeddedEvictionPolicy::OffloadToS3(S3ObjectStoreConfig {
                bucket: "telemetry-archive".into(),
                prefix: "device-a".into(),
                region: Some("us-east-1".into()),
                endpoint: None,
                allow_http: false,
                virtual_hosted_style: false,
            }),
        )
        .with_storage_budgets(256 * 1024 * 1024, 1024 * 1024 * 1024);
        archived.validate().expect("S3 budgets validate");
        assert!(archived.store.object_store_directory.is_none());
        assert!(archived.store.s3_object_store.is_some());
        assert!(
            archived
                .local_limits
                .max_object_payload_bytes_per_partition
                .is_none()
        );
        assert_eq!(
            archived.local_limits.control_cache.max_bytes
                + archived.local_limits.payload_cache.max_bytes
                + archived.local_limits.max_lifetime_rollup_bytes,
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn direct_embedded_log_path_is_lifecycle_checked_and_avoids_the_native_listener() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "shard-telemetry-embedded-direct-{}-{nonce}",
            std::process::id()
        ));
        let runtime =
            EmbeddedTelemetryRuntime::open(EmbeddedTelemetryConfig::new(DurableTelemetryConfig {
                data_directory: directory.clone(),
                object_store_directory: None,
                s3_object_store: None,
                recovery_journal: false,
                retention: None,
                shard_count: 1,
                tenant_partitions: 1,
                append_linger: Duration::from_micros(250),
                stripe: StripeConfig::default(),
                indexed_ack_timeout: Duration::from_secs(30),
            }))
            .expect("runtime opens");
        let event = OtlpLogEvent {
            timestamp_unix_nanos: 42,
            message: Arc::from("embedded fast path"),
            body: Some(TelemetryValue::String(Arc::from("embedded fast path"))),
            resource: Arc::new(ResourceContext::default()),
            ..OtlpLogEvent::default()
        };

        assert!(
            runtime
                .append_log_events("tenant-a", vec![event.clone()], true)
                .is_err()
        );
        runtime.mark_ready().expect("runtime ready");
        let acknowledgement = runtime
            .append_log_events("tenant-a", vec![event.clone()], true)
            .expect("direct append");
        assert_eq!(acknowledgement.partitions.len(), 1);
        let logs = runtime
            .query_native(&NativeQuery {
                tenant: "tenant-a".to_owned(),
                labels: BTreeMap::new(),
                terms: vec!["fast".to_owned()],
                start_timestamp_unix_nanos: None,
                end_timestamp_unix_nanos: None,
                limit: 10,
                direction: NativeQueryDirection::OldestFirst,
            })
            .expect("query");
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].line, "embedded fast path");

        let trace_id = TraceId::from_bytes([7; 16]).expect("trace ID");
        let trace_request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.as_bytes().to_vec(),
                        span_id: vec![8; 8],
                        name: "embedded direct span".into(),
                        start_time_unix_nano: 50,
                        end_time_unix_nano: 60,
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let spans = OtlpTelemetryDecoder
            .decode_traces("tenant-a", &trace_request.encode_to_vec())
            .expect("spans decode");
        let acknowledgement = runtime
            .append_trace_events(spans, true)
            .expect("direct trace append");
        assert_eq!(acknowledgement.partitions.len(), 1);
        let traces = runtime
            .query_traces(&TraceQuery {
                tenant: Arc::from("tenant-a"),
                trace_id: Some(trace_id),
                limit: 10,
                ..TraceQuery::default()
            })
            .expect("trace query");
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].name.as_ref(), "embedded direct span");

        runtime.drain().expect("drain");
        assert!(
            runtime
                .append_log_events("tenant-a", vec![event], true)
                .is_err()
        );
        drop(runtime);
        std::fs::remove_dir_all(directory).expect("cleanup");
    }
}
