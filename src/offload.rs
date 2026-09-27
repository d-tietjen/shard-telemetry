//! Offload ownership: checkpoint journal, transfer worker, and retry helpers live in `offload/`.
mod helpers;
mod journal;
mod worker;
use helpers::*;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fs2::FileExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    DurableTelemetryStore, NativePartitionAppend, NativeTelemetryBatch, ShardTelemetryClient,
    TelemetryEnvelope, TelemetrySignal, decode_metric_chunk,
};

const OFFLOAD_CHECKPOINT_VERSION: u8 = 3;
const DEFAULT_MAX_FETCH_BYTES: u32 = 16 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_PARTITIONS: usize = 1;
const DEFAULT_IDLE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_CONSECUTIVE_FAILURES: usize = 8;

/// Durable state for one upstream-offloaded local WAL partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffloadCheckpoint {
    /// Local source signal partition.
    pub topic_partition: TopicPartition,
    /// First source offset not yet acknowledged by the upstream server.
    pub next_offset: LogicalOffset,
}

/// Configuration for a generic background upstream offloader.
#[derive(Debug, Clone)]
pub struct UpstreamOffloadConfig {
    /// Crash-safe local progress journal. It should be placed below the local
    /// telemetry data directory, not on an ephemeral filesystem.
    pub checkpoint_path: PathBuf,
    /// Stable identity of the node-local WAL source.
    ///
    /// This value scopes deterministic native retry IDs, so it must remain
    /// unchanged across restarts while `checkpoint_path` is retained. Use a
    /// node UUID or another deployment identity that is unique among all
    /// nodes forwarding to the same central ShardTelemetry service.
    pub source_id: Arc<str>,
    /// Maximum source WAL bytes fetched in one offload round per partition.
    pub max_fetch_bytes: u32,
    /// Maximum independent source partitions advancing concurrently.
    ///
    /// Order is always preserved within one physical source partition. This
    /// bound only permits separate partitions to fetch and append in parallel.
    pub max_in_flight_partitions: usize,
    /// Signals eligible for forwarding to the upstream service.
    ///
    /// Local retention remains independent of this selection. For example, an
    /// embedded node can retain logs and traces locally while forwarding only
    /// metrics to its regional ShardTelemetry deployment.
    pub signals: BTreeSet<TelemetrySignal>,
    /// Optional allow-list of metric names eligible for upstream forwarding.
    ///
    /// Metric envelopes are forwarded unchanged, so every metric point in an
    /// eligible envelope must match this allow-list. The direct embedded path
    /// writes one canonical series per envelope; a mixed envelope fails closed
    /// rather than silently forwarding an unselected metric.
    pub metric_names: Option<BTreeSet<Arc<str>>>,
}

impl UpstreamOffloadConfig {
    /// Creates a bounded offload configuration.
    #[must_use]
    pub fn new(checkpoint_path: PathBuf, source_id: impl Into<Arc<str>>) -> Self {
        Self {
            checkpoint_path,
            source_id: source_id.into(),
            max_fetch_bytes: DEFAULT_MAX_FETCH_BYTES,
            max_in_flight_partitions: DEFAULT_MAX_IN_FLIGHT_PARTITIONS,
            signals: [
                TelemetrySignal::Logs,
                TelemetrySignal::Traces,
                TelemetrySignal::Metrics,
            ]
            .into_iter()
            .collect(),
            metric_names: None,
        }
    }

    /// Changes the maximum bytes read from one source partition in a round.
    #[must_use]
    pub const fn with_max_fetch_bytes(mut self, max_fetch_bytes: u32) -> Self {
        self.max_fetch_bytes = max_fetch_bytes;
        self
    }

    /// Changes the maximum number of independent source partitions processed
    /// in parallel. Set this no higher than the upstream client's configured
    /// connection pool for network parallelism.
    #[must_use]
    pub const fn with_max_in_flight_partitions(mut self, max_in_flight_partitions: usize) -> Self {
        self.max_in_flight_partitions = max_in_flight_partitions;
        self
    }

    /// Replaces the set of signal types eligible for upstream forwarding.
    ///
    /// The selected set must not be empty. Use separate embedded stores when
    /// labeled-series routing policies require independent failure domains or
    /// retention windows.
    #[must_use]
    pub fn with_signals(mut self, signals: impl IntoIterator<Item = TelemetrySignal>) -> Self {
        self.signals = signals.into_iter().collect();
        self
    }

    /// Forwards only metric envelopes whose metric name is in `metric_names`.
    ///
    /// The filter applies only when metrics are selected by [`Self::with_signals`].
    /// It does not alter local ingestion, query visibility, retention, or
    /// object-tier offload. Use separate local stores for policies that need to
    /// route one labeled series differently from another series of the same
    /// metric name.
    #[must_use]
    pub fn with_metric_names<I, S>(mut self, metric_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.metric_names = Some(metric_names.into_iter().map(Into::into).collect());
        self
    }

    fn validate(&self) -> Result<(), OffloadError> {
        if self.checkpoint_path.as_os_str().is_empty() {
            return Err(OffloadError::new(
                "upstream offload checkpoint path must not be empty",
            ));
        }
        if self.source_id.is_empty() {
            return Err(OffloadError::new(
                "upstream offload source ID must not be empty",
            ));
        }
        if self.max_fetch_bytes == 0 {
            return Err(OffloadError::new(
                "upstream offload max_fetch_bytes must be nonzero",
            ));
        }
        if self.max_in_flight_partitions == 0 {
            return Err(OffloadError::new(
                "upstream offload max_in_flight_partitions must be nonzero",
            ));
        }
        if self.signals.is_empty() {
            return Err(OffloadError::new(
                "upstream offload must select at least one telemetry signal",
            ));
        }
        if let Some(metric_names) = &self.metric_names {
            if metric_names.is_empty() {
                return Err(OffloadError::new(
                    "upstream metric offload name filter must not be empty",
                ));
            }
            if !self.signals.contains(&TelemetrySignal::Metrics) {
                return Err(OffloadError::new(
                    "upstream metric offload name filter requires metrics to be selected",
                ));
            }
            if metric_names.iter().any(|name| name.is_empty()) {
                return Err(OffloadError::new(
                    "upstream metric offload names must not be empty",
                ));
            }
        }
        Ok(())
    }
}

/// Scheduling and retry bounds for [`UpstreamOffloader::run_until`].
#[derive(Debug, Clone, Copy)]
pub struct UpstreamOffloadLoopConfig {
    /// Wait after a successful round which found no local work.
    pub idle_interval: Duration,
    /// Wait after a retryable local fetch or upstream append failure.
    pub retry_interval: Duration,
    /// Consecutive failed rounds allowed before the worker returns an error.
    pub max_consecutive_failures: usize,
}

impl UpstreamOffloadLoopConfig {
    /// Creates a bounded background-loop configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            idle_interval: DEFAULT_IDLE_INTERVAL,
            retry_interval: DEFAULT_RETRY_INTERVAL,
            max_consecutive_failures: DEFAULT_MAX_CONSECUTIVE_FAILURES,
        }
    }

    /// Changes the wait used when the local WAL has no eligible data.
    #[must_use]
    pub const fn with_idle_interval(mut self, idle_interval: Duration) -> Self {
        self.idle_interval = idle_interval;
        self
    }

    /// Changes the delay before a failed offload round is retried.
    #[must_use]
    pub const fn with_retry_interval(mut self, retry_interval: Duration) -> Self {
        self.retry_interval = retry_interval;
        self
    }

    /// Changes the maximum number of consecutive retryable failures.
    #[must_use]
    pub const fn with_max_consecutive_failures(mut self, max_consecutive_failures: usize) -> Self {
        self.max_consecutive_failures = max_consecutive_failures;
        self
    }

    fn validate(self) -> Result<(), OffloadError> {
        if self.idle_interval.is_zero() || self.retry_interval.is_zero() {
            return Err(OffloadError::new(
                "upstream offload loop intervals must be nonzero",
            ));
        }
        if self.max_consecutive_failures == 0 {
            return Err(OffloadError::new(
                "upstream offload max_consecutive_failures must be nonzero",
            ));
        }
        Ok(())
    }
}

impl Default for UpstreamOffloadLoopConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-round store-and-forward outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OffloadReport {
    /// Source partitions inspected during the round.
    pub scanned_partitions: usize,
    /// Local durable WAL batches acknowledged by the upstream service.
    pub offloaded_batches: usize,
    /// Local durable records covered by the acknowledged batches.
    pub offloaded_records: u64,
    /// Source offsets advanced in the persisted checkpoint journal.
    pub advanced_offsets: u64,
    /// Durable checkpoint-journal writes completed during the round.
    pub checkpoint_writes: usize,
    /// Batches deliberately retained locally by the configured metric filter.
    pub skipped_batches: usize,
    /// Records deliberately retained locally by the configured metric filter.
    pub skipped_records: u64,
}

/// Cumulative outcome returned when a background offload loop is stopped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OffloadLoopReport {
    /// Successful calls to [`UpstreamOffloader::offload_once`].
    pub completed_rounds: u64,
    /// Failed offload rounds retried before shutdown.
    pub failed_rounds: u64,
    /// Source partitions inspected across successful rounds.
    pub scanned_partitions: u64,
    /// Local durable WAL batches acknowledged by the upstream service.
    pub offloaded_batches: u64,
    /// Local durable records covered by acknowledged batches.
    pub offloaded_records: u64,
    /// Source offsets advanced in the persisted checkpoint journal.
    pub advanced_offsets: u64,
    /// Durable checkpoint-journal writes completed across successful rounds.
    pub checkpoint_writes: u64,
    /// Batches deliberately retained locally by the configured metric filter.
    pub skipped_batches: u64,
    /// Records deliberately retained locally by the configured metric filter.
    pub skipped_records: u64,
}

impl OffloadLoopReport {
    fn record(&mut self, report: OffloadReport) {
        self.completed_rounds = self.completed_rounds.saturating_add(1);
        self.scanned_partitions = self
            .scanned_partitions
            .saturating_add(u64::try_from(report.scanned_partitions).unwrap_or(u64::MAX));
        self.offloaded_batches = self
            .offloaded_batches
            .saturating_add(u64::try_from(report.offloaded_batches).unwrap_or(u64::MAX));
        self.offloaded_records = self
            .offloaded_records
            .saturating_add(report.offloaded_records);
        self.advanced_offsets = self
            .advanced_offsets
            .saturating_add(report.advanced_offsets);
        self.checkpoint_writes = self
            .checkpoint_writes
            .saturating_add(u64::try_from(report.checkpoint_writes).unwrap_or(u64::MAX));
        self.skipped_batches = self
            .skipped_batches
            .saturating_add(u64::try_from(report.skipped_batches).unwrap_or(u64::MAX));
        self.skipped_records = self.skipped_records.saturating_add(report.skipped_records);
    }
}

/// Error returned by the background store-and-forward pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffloadError {
    message: Arc<str>,
}

impl OffloadError {
    fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for OffloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for OffloadError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedCheckpoint {
    topic_id: u128,
    partition_id: u32,
    next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedCheckpoints {
    version: u8,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    retry_namespace: Option<String>,
    checkpoints: Vec<PersistedCheckpoint>,
}

#[derive(Debug, Clone)]
enum RetryNamespace {
    LegacyV1,
    Source(Arc<str>),
}

#[derive(Debug)]
struct CheckpointJournal {
    path: PathBuf,
    source_id: Arc<str>,
    retry_namespace: RetryNamespace,
    offsets: BTreeMap<TopicPartition, LogicalOffset>,
    // Held for the lifetime of the offloader so one checkpoint path has one
    // owner across processes and across independently constructed workers.
    _exclusive_lock: File,
}

/// Generic background store-and-forward uploader for an embedded local store.
///
/// Call [`Self::run_until`] from a dedicated background task, or use
/// [`Self::offload_once`] when the host owns its own scheduler. The local WAL
/// stays authoritative: a checkpoint moves only after the upstream native
/// server has acknowledged the corresponding envelope. Source-byte reclamation
/// remains controlled by the local store's retention policy, so an offload
/// outage cannot delete acknowledged local telemetry.
#[derive(Debug)]
pub struct UpstreamOffloader {
    store: Arc<DurableTelemetryStore>,
    client: Arc<ShardTelemetryClient>,
    max_fetch_bytes: u32,
    max_in_flight_partitions: usize,
    signals: BTreeSet<TelemetrySignal>,
    metric_names: Option<BTreeSet<Arc<str>>>,
    checkpoints: Mutex<CheckpointJournal>,
    run_gate: AsyncMutex<()>,
}
