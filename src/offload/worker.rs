use super::*;

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
