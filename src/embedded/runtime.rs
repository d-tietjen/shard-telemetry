use super::*;

impl EmbeddedTelemetryRuntime {
    /// Opens and recovers the local durable store. The data directory remains
    /// exclusively owned until this runtime and all store clones are dropped.
    pub fn open(config: EmbeddedTelemetryConfig) -> Result<Self, LokiApiError> {
        config.validate()?;
        let data_directory = config.store.data_directory.clone();
        let max_ssd_bytes = config.max_ssd_bytes;
        let max_lifetime_rollup_bytes = config.local_limits.max_lifetime_rollup_bytes;
        let runtime = Self {
            store: Arc::new(
                DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
                    config.store,
                    config.object_tier,
                    config.local_limits,
                )?,
            ),
            data_directory,
            max_ssd_bytes,
            max_lifetime_rollup_bytes,
            shutdown_flush_timeout: config.shutdown_flush_timeout,
            last_successful_maintenance_unix_seconds: AtomicU64::new(0),
            maintenance_running: Arc::new(AtomicBool::new(false)),
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

    /// Returns complete-directory, rollup, backlog, and maintenance health.
    ///
    /// Directory bytes include logical lengths and physical allocation for all
    /// regular files. Directory-entry metadata remains platform-specific; use
    /// [`crate::EmbeddedUsageLedger`] when a hard single-file product-usage
    /// quota is required.
    pub fn storage_health(&self) -> Result<EmbeddedStorageHealth, LokiApiError> {
        let (data_directory_bytes, allocated_data_directory_bytes) =
            directory_file_bytes(&self.data_directory)?;
        let accounted_data_directory_bytes =
            data_directory_bytes.max(allocated_data_directory_bytes);
        let (lifetime_rollup_series, lifetime_rollup_bytes) =
            self.store.lifetime_rollup_storage()?;
        let metrics = self.store.operational_metrics();
        let last_maintenance = self
            .last_successful_maintenance_unix_seconds
            .load(Ordering::Relaxed);
        Ok(EmbeddedStorageHealth {
            state: self.state(),
            data_directory_bytes,
            allocated_data_directory_bytes,
            max_ssd_bytes: self.max_ssd_bytes,
            ssd_headroom_bytes: self
                .max_ssd_bytes
                .map(|maximum| maximum.saturating_sub(accounted_data_directory_bytes)),
            ssd_budget_exceeded: self
                .max_ssd_bytes
                .is_some_and(|maximum| accounted_data_directory_bytes > maximum),
            lifetime_rollup_bytes,
            max_lifetime_rollup_bytes: self.max_lifetime_rollup_bytes,
            lifetime_rollup_series,
            backlog_items: metrics.pending_items,
            backlog_bytes: metrics.pending_bytes,
            retained_payload_bytes: metrics.retained_payload_bytes,
            retention_runs: metrics.retention_runs,
            retention_failures: metrics.retention_failures,
            last_successful_maintenance_unix_seconds: (last_maintenance != 0)
                .then_some(last_maintenance),
        })
    }

    /// Starts built-in periodic export-independent retention maintenance.
    ///
    /// The worker waits while the runtime is not ready and exits when its
    /// handle is dropped, explicitly shut down, or the runtime is dropped.
    pub fn spawn_maintenance(
        self: &Arc<Self>,
        interval: Duration,
    ) -> Result<EmbeddedMaintenanceWorker, LokiApiError> {
        if interval.is_zero() {
            return Err(LokiApiError::configuration(
                "embedded maintenance interval must be nonzero",
            ));
        }
        self.maintenance_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                LokiApiError::configuration("embedded maintenance worker is already running")
            })?;
        let runtime = Arc::downgrade(self);
        let running = Arc::clone(&self.maintenance_running);
        let (shutdown, receiver) = mpsc::channel();
        let thread = match std::thread::Builder::new()
            .name("shard-telemetry-maintenance".into())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(interval) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
                        Err(RecvTimeoutError::Timeout) => {}
                    }
                    let Some(runtime) = runtime.upgrade() else {
                        break;
                    };
                    if runtime.state() == EmbeddedTelemetryState::Ready {
                        let _ = runtime.compact_retention();
                    }
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                running.store(false, Ordering::Release);
                return Err(LokiApiError::configuration(format!(
                    "failed to start embedded maintenance worker: {error}",
                )));
            }
        };
        Ok(EmbeddedMaintenanceWorker {
            shutdown: Some(shutdown),
            thread: Some(thread),
            running,
        })
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
        let report = self.store.compact_retention()?;
        self.last_successful_maintenance_unix_seconds
            .store(unix_seconds_now(), Ordering::Relaxed);
        Ok(report)
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
