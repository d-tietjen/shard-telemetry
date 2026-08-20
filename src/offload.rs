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

impl CheckpointJournal {
    fn open(path: PathBuf, source_id: Arc<str>) -> Result<Self, OffloadError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(journal_io_error)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path.with_extension("lock"))
            .map_err(journal_io_error)?;
        lock.try_lock_exclusive().map_err(|error| {
            OffloadError::new(format!(
                "upstream offload checkpoint is already owned by another worker: {error}"
            ))
        })?;
        let persisted = match fs::read(&path) {
            Ok(encoded) => {
                serde_json::from_slice::<PersistedCheckpoints>(&encoded).map_err(|error| {
                    OffloadError::new(format!(
                        "invalid upstream offload checkpoint journal: {error}"
                    ))
                })?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => PersistedCheckpoints {
                version: OFFLOAD_CHECKPOINT_VERSION,
                source_id: Some(source_id.to_string()),
                retry_namespace: Some(source_retry_namespace(&source_id)),
                checkpoints: Vec::new(),
            },
            Err(error) => return Err(journal_io_error(error)),
        };
        let retry_namespace = match persisted.version {
            1 => RetryNamespace::LegacyV1,
            2 => {
                validate_persisted_source(persisted.source_id.as_deref(), &source_id)?;
                RetryNamespace::Source(Arc::clone(&source_id))
            }
            OFFLOAD_CHECKPOINT_VERSION => {
                validate_persisted_source(persisted.source_id.as_deref(), &source_id)?;
                match persisted.retry_namespace.as_deref() {
                    Some("legacy-v1") => RetryNamespace::LegacyV1,
                    Some(namespace) if namespace == source_retry_namespace(&source_id) => {
                        RetryNamespace::Source(Arc::clone(&source_id))
                    }
                    _ => {
                        return Err(OffloadError::new(
                            "upstream offload checkpoint journal retry namespace is invalid",
                        ));
                    }
                }
            }
            version => {
                return Err(OffloadError::new(format!(
                    "unsupported upstream offload checkpoint journal version {version}",
                )));
            }
        };
        let mut offsets = BTreeMap::new();
        for checkpoint in persisted.checkpoints {
            let topic_partition = TopicPartition::new(
                TopicId::new(checkpoint.topic_id),
                LogicalPartitionId::new(checkpoint.partition_id),
            );
            if offsets
                .insert(topic_partition, LogicalOffset::new(checkpoint.next_offset))
                .is_some()
            {
                return Err(OffloadError::new(
                    "upstream offload checkpoint journal contains duplicate partitions",
                ));
            }
        }
        Ok(Self {
            path,
            source_id,
            retry_namespace,
            offsets,
            _exclusive_lock: lock,
        })
    }

    fn next(&self, partition: TopicPartition) -> Option<LogicalOffset> {
        self.offsets.get(&partition).copied()
    }

    fn advance(
        &mut self,
        partition: TopicPartition,
        next_offset: LogicalOffset,
    ) -> Result<(), OffloadError> {
        let previous = self.offsets.insert(partition, next_offset);
        if previous.is_some_and(|previous| previous > next_offset) {
            return Err(OffloadError::new(
                "upstream offload checkpoint attempted to move backward",
            ));
        }
        self.persist()
    }

    fn snapshot(&self) -> Vec<OffloadCheckpoint> {
        self.offsets
            .iter()
            .map(|(topic_partition, next_offset)| OffloadCheckpoint {
                topic_partition: *topic_partition,
                next_offset: *next_offset,
            })
            .collect()
    }

    fn retry_namespace(&self) -> RetryNamespace {
        self.retry_namespace.clone()
    }

    fn persist(&self) -> Result<(), OffloadError> {
        let encoded = serde_json::to_vec(&PersistedCheckpoints {
            version: OFFLOAD_CHECKPOINT_VERSION,
            source_id: Some(self.source_id.to_string()),
            retry_namespace: Some(match &self.retry_namespace {
                RetryNamespace::LegacyV1 => "legacy-v1".to_owned(),
                RetryNamespace::Source(source_id) => source_retry_namespace(source_id),
            }),
            checkpoints: self
                .offsets
                .iter()
                .map(|(topic_partition, next_offset)| PersistedCheckpoint {
                    topic_id: topic_partition.topic_id.get(),
                    partition_id: topic_partition.partition_id.get(),
                    next_offset: next_offset.get(),
                })
                .collect(),
        })
        .map_err(|error| {
            OffloadError::new(format!(
                "upstream offload checkpoint serialization failed: {error}"
            ))
        })?;
        let temporary = temporary_path(&self.path);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(journal_io_error)?;
        file.write_all(&encoded)
            .and_then(|()| file.sync_all())
            .map_err(journal_io_error)?;
        fs::rename(&temporary, &self.path).map_err(journal_io_error)?;
        File::open(self.path.parent().unwrap_or_else(|| Path::new(".")))
            .and_then(|directory| directory.sync_all())
            .map_err(journal_io_error)
    }
}

fn validate_persisted_source(
    persisted_source: Option<&str>,
    configured_source: &str,
) -> Result<(), OffloadError> {
    match persisted_source {
        Some(source) if source == configured_source => Ok(()),
        Some(_) => Err(OffloadError::new(
            "upstream offload checkpoint journal source ID does not match configuration",
        )),
        None => Err(OffloadError::new(
            "upstream offload checkpoint journal is missing source ID",
        )),
    }
}

fn source_retry_namespace(source_id: &str) -> String {
    format!("source:{source_id}")
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

impl UpstreamOffloader {
    /// Opens or recovers the progress journal for one local store and upstream client.
    pub fn open(
        store: Arc<DurableTelemetryStore>,
        client: Arc<ShardTelemetryClient>,
        config: UpstreamOffloadConfig,
    ) -> Result<Self, OffloadError> {
        config.validate()?;
        let UpstreamOffloadConfig {
            checkpoint_path,
            source_id,
            max_fetch_bytes,
            max_in_flight_partitions,
            signals,
            metric_names,
        } = config;
        Ok(Self {
            store,
            client,
            max_fetch_bytes,
            max_in_flight_partitions,
            signals,
            metric_names,
            checkpoints: Mutex::new(CheckpointJournal::open(checkpoint_path, source_id)?),
            run_gate: AsyncMutex::new(()),
        })
    }

    /// Returns the recovered durable progress checkpoints.
    pub fn checkpoints(&self) -> Result<Vec<OffloadCheckpoint>, OffloadError> {
        self.checkpoints
            .lock()
            .map(|journal| journal.snapshot())
            .map_err(|_| OffloadError::new("upstream offload checkpoint lock poisoned"))
    }

    /// Offloads at most one bounded WAL fetch from every local signal partition.
    ///
    /// One successful fetch advances its source checkpoint with one durable
    /// journal write. If an append later in that fetch fails, the journal stays
    /// at its prior boundary and the next run safely reuses deterministic retry
    /// IDs for every previously acknowledged append in that fetch.
    pub async fn offload_once(&self) -> Result<OffloadReport, OffloadError> {
        let _round = self.run_gate.lock().await;
        let mut report = OffloadReport::default();
        let mut in_flight = FuturesUnordered::new();
        let mut first_error = None;
        for partition in self.store.telemetry_partitions() {
            if !self
                .signals
                .iter()
                .any(|signal| signal.topic_id() == partition.topic_id)
            {
                continue;
            }
            in_flight.push(self.offload_partition(partition));
            if in_flight.len() >= self.max_in_flight_partitions
                && let Some(result) = in_flight.next().await
            {
                record_partition_result(&mut report, &mut first_error, result);
                if first_error.is_some() {
                    break;
                }
            }
        }
        while let Some(result) = in_flight.next().await {
            record_partition_result(&mut report, &mut first_error, result);
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(report)
    }

    async fn offload_partition(
        &self,
        partition: TopicPartition,
    ) -> Result<OffloadReport, OffloadError> {
        let mut report = OffloadReport {
            scanned_partitions: 1,
            ..OffloadReport::default()
        };
        let start_offset = self.start_offset(partition)?;
        let store = Arc::clone(&self.store);
        let max_fetch_bytes = self.max_fetch_bytes;
        let batches = tokio::task::spawn_blocking(move || {
            store.fetch_telemetry_batches(partition, start_offset, max_fetch_bytes)
        })
        .await
        .map_err(|error| {
            OffloadError::new(format!("upstream offload fetch worker failed: {error}"))
        })?
        .map_err(|error| OffloadError::new(error.to_string()))?;
        let mut checkpoint = None;
        for batch in batches {
            if batch.topic_partition != partition {
                return Err(OffloadError::new(
                    "upstream offload fetch returned a batch from another partition",
                ));
            }
            let record_count = batch
                .last_offset
                .get()
                .saturating_sub(batch.first_offset.get())
                .saturating_add(1);
            let should_forward = self.should_forward_envelope(&batch.envelope)?;
            if should_forward {
                let native_batch = NativeTelemetryBatch {
                    partitions: vec![NativePartitionAppend {
                        topic_partition: batch.topic_partition,
                        envelope: batch.envelope,
                        transient_context: None,
                    }],
                };
                let retry_id = retry_id(
                    self.retry_namespace()?,
                    batch.topic_partition,
                    batch.first_offset,
                    batch.last_offset,
                    &native_batch,
                )?;
                self.client
                    .append_with_request_id(&native_batch, retry_id)
                    .await
                    .map_err(|error| OffloadError::new(error.to_string()))?;
                report.offloaded_batches = report.offloaded_batches.saturating_add(1);
                report.offloaded_records = report.offloaded_records.saturating_add(record_count);
            } else {
                report.skipped_batches = report.skipped_batches.saturating_add(1);
                report.skipped_records = report.skipped_records.saturating_add(record_count);
            }
            let next = batch
                .last_offset
                .get()
                .checked_add(1)
                .ok_or_else(|| OffloadError::new("upstream offload source offset exhausted"))?;
            checkpoint = Some(LogicalOffset::new(next));
            report.advanced_offsets = report.advanced_offsets.saturating_add(record_count);
        }
        if let Some(checkpoint) = checkpoint {
            self.advance(partition, checkpoint)?;
            report.checkpoint_writes = report.checkpoint_writes.saturating_add(1);
        }
        Ok(report)
    }

    /// Repeatedly offloads local WAL data until `shutdown` resolves.
    ///
    /// Successful rounds with work continue immediately to drain a bounded
    /// backlog. Idle rounds wait for `idle_interval`; retryable local or
    /// upstream errors wait for `retry_interval` and stop after the configured
    /// consecutive-failure limit. Stopping cancels the next round or wait; a
    /// possibly in-flight native append remains safe because its retry ID is
    /// deterministic and the checkpoint advances only after acknowledgement.
    pub async fn run_until<F>(
        &self,
        config: UpstreamOffloadLoopConfig,
        shutdown: F,
    ) -> Result<OffloadLoopReport, OffloadError>
    where
        F: Future<Output = ()> + Send,
    {
        config.validate()?;
        tokio::pin!(shutdown);
        let mut report = OffloadLoopReport::default();
        let mut consecutive_failures = 0_usize;
        loop {
            let delay = tokio::select! {
                biased;
                () = &mut shutdown => return Ok(report),
                round = self.offload_once() => match round {
                    Ok(round) => {
                        let has_backlog = round.advanced_offsets != 0;
                        report.record(round);
                        consecutive_failures = 0;
                        if has_backlog {
                            None
                        } else {
                            Some(config.idle_interval)
                        }
                    }
                    Err(error) => {
                        report.failed_rounds = report.failed_rounds.saturating_add(1);
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures >= config.max_consecutive_failures {
                            return Err(error);
                        }
                        Some(config.retry_interval)
                    }
                },
            };
            let Some(delay) = delay else {
                continue;
            };
            tokio::select! {
                biased;
                () = &mut shutdown => return Ok(report),
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    fn start_offset(&self, partition: TopicPartition) -> Result<LogicalOffset, OffloadError> {
        let local_start = self
            .store
            .telemetry_partition_start_offset(partition)
            .map_err(|error| OffloadError::new(error.to_string()))?;
        let checkpoint = self
            .checkpoints
            .lock()
            .map_err(|_| OffloadError::new("upstream offload checkpoint lock poisoned"))?
            .next(partition);
        Ok(checkpoint.map_or(local_start, |checkpoint| checkpoint.max(local_start)))
    }

    fn advance(
        &self,
        partition: TopicPartition,
        next_offset: LogicalOffset,
    ) -> Result<(), OffloadError> {
        self.checkpoints
            .lock()
            .map_err(|_| OffloadError::new("upstream offload checkpoint lock poisoned"))?
            .advance(partition, next_offset)
    }

    fn retry_namespace(&self) -> Result<RetryNamespace, OffloadError> {
        self.checkpoints
            .lock()
            .map_err(|_| OffloadError::new("upstream offload checkpoint lock poisoned"))
            .map(|journal| journal.retry_namespace())
    }

    fn should_forward_envelope(&self, envelope: &TelemetryEnvelope) -> Result<bool, OffloadError> {
        let Some(metric_names) = &self.metric_names else {
            return Ok(true);
        };
        if envelope.signal != TelemetrySignal::Metrics {
            return Ok(true);
        }
        let points = decode_metric_chunk(&envelope.payload)
            .map_err(|error| OffloadError::new(error.to_string()))?;
        if points.len() != envelope.item_count as usize {
            return Err(OffloadError::new(
                "metric offload filter decoded a count different from its envelope",
            ));
        }
        let mut selected = false;
        let mut unselected = false;
        for point in points {
            if metric_names.contains(&point.identity.name) {
                selected = true;
            } else {
                unselected = true;
            }
        }
        if selected && unselected {
            return Err(OffloadError::new(
                "metric offload filter requires every envelope to have one selection outcome",
            ));
        }
        Ok(selected)
    }
}

fn record_partition_result(
    report: &mut OffloadReport,
    first_error: &mut Option<OffloadError>,
    result: Result<OffloadReport, OffloadError>,
) {
    match result {
        Ok(partition) => {
            report.scanned_partitions = report
                .scanned_partitions
                .saturating_add(partition.scanned_partitions);
            report.offloaded_batches = report
                .offloaded_batches
                .saturating_add(partition.offloaded_batches);
            report.offloaded_records = report
                .offloaded_records
                .saturating_add(partition.offloaded_records);
            report.advanced_offsets = report
                .advanced_offsets
                .saturating_add(partition.advanced_offsets);
            report.checkpoint_writes = report
                .checkpoint_writes
                .saturating_add(partition.checkpoint_writes);
            report.skipped_batches = report
                .skipped_batches
                .saturating_add(partition.skipped_batches);
            report.skipped_records = report
                .skipped_records
                .saturating_add(partition.skipped_records);
        }
        Err(error) if first_error.is_none() => *first_error = Some(error),
        Err(_) => {}
    }
}

fn retry_id(
    namespace: RetryNamespace,
    partition: TopicPartition,
    first_offset: LogicalOffset,
    last_offset: LogicalOffset,
    batch: &NativeTelemetryBatch,
) -> Result<u128, OffloadError> {
    let payload = batch
        .encode_native_append()
        .map_err(|error| OffloadError::new(error.to_string()))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"shard-telemetry-upstream-offload-v1\0");
    if let RetryNamespace::Source(source_id) = namespace {
        hasher.update(source_id.as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&partition.topic_id.get().to_le_bytes());
    hasher.update(&partition.partition_id.get().to_le_bytes());
    hasher.update(&first_offset.get().to_le_bytes());
    hasher.update(&last_offset.get().to_le_bytes());
    hasher.update(&payload);
    let digest = hasher.finalize();
    Ok(u128::from_le_bytes(
        digest.as_bytes()[..16]
            .try_into()
            .expect("BLAKE3 digest contains sixteen bytes"),
    ))
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn journal_io_error(error: std::io::Error) -> OffloadError {
    OffloadError::new(format!(
        "upstream offload checkpoint journal I/O failed: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest,
        metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
            number_data_point,
        },
    };
    use prost::Message;
    use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        DurableTelemetryConfig, LokiEntry, LokiStore, METRICS_TOPIC_ID, MetricQuery,
        NativeClientConfig, NativeServerConfig, OtlpTelemetryDecoder, StripeConfig, serve_native,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upstream_offload_persists_progress_only_after_native_acknowledgement() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "shard-telemetry-upstream-offload-{}-{nonce}",
            std::process::id()
        ));
        let source_directory = root.join("source");
        let destination_directory = root.join("destination");
        let config = |data_directory| DurableTelemetryConfig {
            data_directory,
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        };
        let source = Arc::new(
            DurableTelemetryStore::open(config(source_directory.clone())).expect("source"),
        );
        let destination = Arc::new(
            DurableTelemetryStore::open(config(destination_directory)).expect("destination"),
        );
        let expected = LokiEntry {
            timestamp_unix_nanos: 100,
            labels: BTreeMap::from([("node".to_owned(), "node-a".to_owned())]),
            line: "store and forward".to_owned(),
            structured_metadata: BTreeMap::new(),
        };
        LokiStore::push(source.as_ref(), "tenant-a", vec![expected.clone()])
            .expect("source append");
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server_destination = Arc::clone(&destination);
        let server = tokio::spawn(async move {
            serve_native(
                listener,
                server_destination,
                NativeServerConfig::default(),
                async {
                    let _ = stopped.await;
                },
            )
            .await
        });
        let client =
            Arc::new(ShardTelemetryClient::new(NativeClientConfig::new(address)).expect("client"));
        let metrics_only = UpstreamOffloader::open(
            Arc::clone(&source),
            Arc::clone(&client),
            UpstreamOffloadConfig::new(
                source_directory.join("metrics-only-offload-v1.json"),
                "test-node-a",
            )
            .with_signals([TelemetrySignal::Metrics]),
        )
        .expect("metrics-only offloader");
        let skipped = metrics_only.offload_once().await.expect("filtered offload");
        assert_eq!(skipped.scanned_partitions, 1);
        assert_eq!(skipped.offloaded_batches, 0);
        assert!(
            metrics_only
                .checkpoints()
                .expect("filtered checkpoints")
                .is_empty()
        );
        assert!(
            LokiStore::entries(destination.as_ref(), "tenant-a")
                .expect("destination remains empty")
                .is_empty()
        );

        let metric_request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "node_cpu_seconds_total".into(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: 200,
                                    value: Some(number_data_point::Value::AsInt(7)),
                                    ..NumberDataPoint::default()
                                }],
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            name: "eden_local_only".into(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: 201,
                                    value: Some(number_data_point::Value::AsInt(3)),
                                    ..NumberDataPoint::default()
                                }],
                            })),
                            ..Metric::default()
                        },
                    ],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        };
        let metric_points = OtlpTelemetryDecoder
            .decode_metrics("tenant-a", &metric_request.encode_to_vec())
            .expect("metric decode")
            .into_iter()
            .map(|event| {
                event.into_durable(
                    ShardId::new(0),
                    TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                    LogicalOffset::new(0),
                )
            })
            .collect::<Vec<_>>();
        source
            .append_metric_points(metric_points, true)
            .expect("source metric append");

        let selected_metrics = UpstreamOffloader::open(
            Arc::clone(&source),
            Arc::clone(&client),
            UpstreamOffloadConfig::new(
                source_directory.join("selected-metrics-offload-v1.json"),
                "test-node-a",
            )
            .with_signals([TelemetrySignal::Metrics])
            .with_metric_names(["node_cpu_seconds_total"]),
        )
        .expect("selected-metrics offloader");
        let selected = selected_metrics
            .offload_once()
            .await
            .expect("selected metric offload");
        assert_eq!(selected.scanned_partitions, 1);
        assert_eq!(selected.offloaded_batches, 1);
        assert_eq!(selected.offloaded_records, 1);
        assert_eq!(selected.skipped_batches, 1);
        assert_eq!(selected.skipped_records, 1);
        assert_eq!(selected.advanced_offsets, 2);
        assert_eq!(selected.checkpoint_writes, 1);
        assert_eq!(
            selected_metrics
                .checkpoints()
                .expect("metric checkpoints")
                .len(),
            1
        );
        assert_eq!(
            destination
                .query_metrics(&MetricQuery {
                    tenant: Arc::from("tenant-a"),
                    name: Some(Arc::from("node_cpu_seconds_total")),
                    limit: 10,
                    ..MetricQuery::default()
                })
                .expect("selected metric query")
                .len(),
            1
        );
        assert!(
            destination
                .query_metrics(&MetricQuery {
                    tenant: Arc::from("tenant-a"),
                    name: Some(Arc::from("eden_local_only")),
                    limit: 10,
                    ..MetricQuery::default()
                })
                .expect("unselected metric query")
                .is_empty()
        );
        drop(selected_metrics);
        let recovered_selected_metrics = UpstreamOffloader::open(
            Arc::clone(&source),
            Arc::clone(&client),
            UpstreamOffloadConfig::new(
                source_directory.join("selected-metrics-offload-v1.json"),
                "test-node-a",
            )
            .with_signals([TelemetrySignal::Metrics])
            .with_metric_names(["node_cpu_seconds_total"]),
        )
        .expect("recovered selected-metrics offloader");
        let recovered_selected = recovered_selected_metrics
            .offload_once()
            .await
            .expect("recovered selected metric offload");
        assert_eq!(recovered_selected.advanced_offsets, 0);
        assert_eq!(recovered_selected.offloaded_batches, 0);
        assert_eq!(recovered_selected.skipped_batches, 0);
        assert_eq!(recovered_selected.checkpoint_writes, 0);

        let offloader = Arc::new(
            UpstreamOffloader::open(
                Arc::clone(&source),
                Arc::clone(&client),
                UpstreamOffloadConfig::new(
                    source_directory.join("upstream-offload-v1.json"),
                    "test-node-a",
                ),
            )
            .expect("offloader"),
        );
        let (offload_stop, offload_stopped) = tokio::sync::oneshot::channel();
        let loop_offloader = Arc::clone(&offloader);
        let offload_loop = tokio::spawn(async move {
            loop_offloader
                .run_until(
                    UpstreamOffloadLoopConfig::new()
                        .with_idle_interval(Duration::from_millis(5))
                        .with_retry_interval(Duration::from_millis(5)),
                    async {
                        let _ = offload_stopped.await;
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if LokiStore::entries(destination.as_ref(), "tenant-a").expect("destination query")
                    == vec![expected.clone()]
                    && offloader.checkpoints().expect("checkpoints").len() == 1
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background offload completes");
        offload_stop.send(()).expect("stop offload loop");
        let report = offload_loop
            .await
            .expect("offload loop joins")
            .expect("offload loop succeeds");
        assert_eq!(report.failed_rounds, 0);
        assert_eq!(
            LokiStore::entries(destination.as_ref(), "tenant-a").expect("destination query"),
            vec![expected.clone()]
        );
        assert_eq!(offloader.checkpoints().expect("checkpoints").len(), 1);

        // Both sources start at the same local WAL offset and emit the exact
        // same envelope. Their distinct stable source IDs must prevent the
        // central native receipt catalog from treating the second node as a
        // retry of the first node's append.
        let second_source_directory = root.join("second-source");
        let second_source = Arc::new(
            DurableTelemetryStore::open(config(second_source_directory.clone()))
                .expect("second source"),
        );
        LokiStore::push(second_source.as_ref(), "tenant-a", vec![expected.clone()])
            .expect("second source append");
        let second_offloader = UpstreamOffloader::open(
            Arc::clone(&second_source),
            client,
            UpstreamOffloadConfig::new(
                second_source_directory.join("upstream-offload-v2.json"),
                "test-node-b",
            ),
        )
        .expect("second offloader");
        let second_report = second_offloader
            .offload_once()
            .await
            .expect("second offload");
        assert_eq!(second_report.offloaded_batches, 1);
        assert_eq!(second_report.offloaded_records, 1);
        assert_eq!(
            LokiStore::entries(destination.as_ref(), "tenant-a")
                .expect("both sources reach destination")
                .len(),
            2
        );

        stop.send(()).expect("stop");
        server
            .await
            .expect("server joins")
            .expect("server succeeds");
        drop(source);
        drop(second_source);
        drop(destination);
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn metric_name_filter_is_explicit_and_requires_metrics() {
        assert!(
            UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "")
                .validate()
                .is_err()
        );
        assert!(
            UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
                .with_metric_names(std::iter::empty::<&str>())
                .validate()
                .is_err()
        );
        assert!(
            UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
                .with_signals([TelemetrySignal::Logs])
                .with_metric_names(["node_cpu_seconds_total"])
                .validate()
                .is_err()
        );
        assert!(
            UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
                .with_max_in_flight_partitions(0)
                .validate()
                .is_err()
        );
        assert!(
            UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
                .with_signals([TelemetrySignal::Metrics])
                .with_metric_names(["node_cpu_seconds_total"])
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn checkpoint_journal_binds_the_stable_source_identity() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "shard-telemetry-offload-source-id-{}-{nonce}",
            std::process::id()
        ));
        let path = root.join("offload-v2.json");
        let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
        let mut journal =
            CheckpointJournal::open(path.clone(), Arc::from("node-a")).expect("open journal");
        journal
            .advance(partition, LogicalOffset::new(1))
            .expect("persist checkpoint");
        assert!(CheckpointJournal::open(path.clone(), Arc::from("node-a")).is_err());
        drop(journal);
        let reopened = CheckpointJournal::open(path.clone(), Arc::from("node-a"))
            .expect("same source reopens after prior owner exits");
        drop(reopened);
        assert!(CheckpointJournal::open(path, Arc::from("node-b")).is_err());

        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn v1_checkpoint_journal_migrates_with_its_legacy_retry_namespace() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "shard-telemetry-offload-v1-migration-{}-{nonce}",
            std::process::id()
        ));
        let path = root.join("offload.json");
        std::fs::create_dir_all(&root).expect("directory");
        std::fs::write(
            &path,
            serde_json::to_vec(&PersistedCheckpoints {
                version: 1,
                source_id: None,
                retry_namespace: None,
                checkpoints: vec![PersistedCheckpoint {
                    topic_id: 9,
                    partition_id: 2,
                    next_offset: 11,
                }],
            })
            .expect("legacy journal"),
        )
        .expect("write legacy journal");
        let partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(2));
        let mut journal =
            CheckpointJournal::open(path.clone(), Arc::from("node-a")).expect("migrate v1 journal");
        assert!(matches!(
            journal.retry_namespace(),
            RetryNamespace::LegacyV1
        ));
        assert_eq!(journal.next(partition), Some(LogicalOffset::new(11)));
        journal
            .advance(partition, LogicalOffset::new(12))
            .expect("persist migration");
        drop(journal);
        let persisted: PersistedCheckpoints =
            serde_json::from_slice(&std::fs::read(&path).expect("read migrated journal"))
                .expect("decode migrated journal");
        assert_eq!(persisted.version, OFFLOAD_CHECKPOINT_VERSION);
        assert_eq!(persisted.source_id.as_deref(), Some("node-a"));
        assert_eq!(persisted.retry_namespace.as_deref(), Some("legacy-v1"));
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
