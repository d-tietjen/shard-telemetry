use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use crate::{
    DurableMetricPoint, DurableTelemetryConfig, DurableTelemetryStore, LokiApiError, LokiStore,
    NativeTelemetryAppendAck, OtlpLogEvent, OtlpSpanEvent,
};

/// Lifecycle configuration for a one-process embedded ShardTelemetry runtime.
#[derive(Debug, Clone)]
pub struct EmbeddedTelemetryConfig {
    /// Durable local WAL, index, and object-tier configuration.
    pub store: DurableTelemetryConfig,
    /// Upper bound applied when draining the embedded store during shutdown.
    pub shutdown_flush_timeout: Duration,
}

impl EmbeddedTelemetryConfig {
    /// Creates an embedded runtime configuration around one durable store.
    #[must_use]
    pub fn new(store: DurableTelemetryConfig) -> Self {
        Self {
            store,
            shutdown_flush_timeout: Duration::from_secs(30),
        }
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
            store: Arc::new(DurableTelemetryStore::open(config.store)?),
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
