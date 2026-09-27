//! Embedded ownership: configuration, runtime lifecycle, and filesystem accounting live in `embedded/`.
mod config;
mod filesystem;
mod runtime;
use filesystem::*;
#[cfg(test)]
mod tests;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
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

/// Public storage and maintenance snapshot for an embedded runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddedStorageHealth {
    /// Current embedded lifecycle state.
    pub state: EmbeddedTelemetryState,
    /// Logical bytes in every regular file below the owned data directory.
    pub data_directory_bytes: u64,
    /// Physical filesystem blocks assigned to regular files below the owned
    /// data directory. On platforms without block accounting this equals
    /// `data_directory_bytes`.
    pub allocated_data_directory_bytes: u64,
    /// Configured managed SSD budget, when present.
    pub max_ssd_bytes: Option<u64>,
    /// Remaining managed SSD headroom based on the greater of logical and
    /// allocated regular-file bytes from the complete directory scan.
    pub ssd_headroom_bytes: Option<u64>,
    /// Whether complete logical or allocated directory bytes exceed
    /// `max_ssd_bytes`.
    pub ssd_budget_exceeded: bool,
    /// Persisted lifetime-rollup file bytes.
    pub lifetime_rollup_bytes: u64,
    /// Configured lifetime-rollup file byte cap.
    pub max_lifetime_rollup_bytes: u64,
    /// Distinct metric series represented in the lifetime rollup.
    pub lifetime_rollup_series: usize,
    /// Durable sink items waiting for index application.
    pub backlog_items: u64,
    /// Durable sink bytes waiting for index application.
    pub backlog_bytes: u64,
    /// Source payload bytes retained in the shard-stream WAL.
    pub retained_payload_bytes: Option<u64>,
    /// Completed retention maintenance passes.
    pub retention_runs: u64,
    /// Failed retention maintenance passes.
    pub retention_failures: u64,
    /// Last successful runtime-driven maintenance wall-clock second.
    pub last_successful_maintenance_unix_seconds: Option<u64>,
}

/// Owned periodic maintenance task for an [`EmbeddedTelemetryRuntime`].
///
/// Dropping this handle requests shutdown and joins its thread. Hosts should
/// retain it until before draining the runtime.
#[derive(Debug)]
pub struct EmbeddedMaintenanceWorker {
    shutdown: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
}

impl EmbeddedMaintenanceWorker {
    /// Stops the maintenance task and waits for its thread to exit.
    pub fn shutdown(mut self) -> Result<(), LokiApiError> {
        self.stop()
    }

    fn stop(&mut self) -> Result<(), LokiApiError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let result = thread
                .join()
                .map_err(|_| LokiApiError::internal("embedded maintenance worker thread panicked"));
            self.running.store(false, Ordering::Release);
            result?;
        }
        Ok(())
    }
}

impl Drop for EmbeddedMaintenanceWorker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
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
    data_directory: PathBuf,
    max_ssd_bytes: Option<u64>,
    max_lifetime_rollup_bytes: u64,
    shutdown_flush_timeout: Duration,
    last_successful_maintenance_unix_seconds: AtomicU64,
    maintenance_running: Arc<AtomicBool>,
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
