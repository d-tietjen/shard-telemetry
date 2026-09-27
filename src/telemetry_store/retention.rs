use super::*;

impl DurableTelemetryStore {
    /// Incorporates every not-yet-checkpointed metric WAL point into the
    /// crash-safe local lifetime rollup catalog.
    ///
    /// Source WAL/object reclamation calls this first. If rollup persistence
    /// fails or its configured series bound is exhausted, reclamation fails
    /// closed and the raw source remains available.
    pub fn checkpoint_lifetime_rollups(&self) -> Result<LifetimeRollupReport, LokiApiError> {
        let Some(lifetime_rollups) = &self.lifetime_rollups else {
            return Ok(LifetimeRollupReport::default());
        };
        let mut catalog = lifetime_rollups
            .lock()
            .map_err(|_| LokiApiError::internal("lifetime metric rollup lock poisoned"))?;
        // Work on a private generation. A series-cap, decode, or persistence
        // failure must not leave partially accumulated in-memory state that a
        // retry would count twice.
        let mut staged = catalog.clone();
        staged.clear_pending_report();
        for partition in self.signal_partitions(crate::METRICS_TOPIC_ID) {
            let watermarks = self.engine.watermarks(partition).map_err(engine_error)?;
            let mut next = match staged.checkpoint(partition) {
                Some(checkpoint) if checkpoint < watermarks.log_start => {
                    return Err(LokiApiError::internal(format!(
                        "metric rollup checkpoint {} precedes retained WAL start {} for {partition:?}",
                        checkpoint.get(),
                        watermarks.log_start.get()
                    )));
                }
                Some(checkpoint) => checkpoint,
                None => watermarks.log_start,
            };
            while next < watermarks.last_stable_offset {
                let batches =
                    self.fetch_telemetry_batches(partition, next, self.max_fetch_bytes)?;
                if batches.is_empty() {
                    break;
                }
                for batch in batches {
                    let points = crate::decode_metric_chunk(&batch.envelope.payload)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    let encoded_points = batch
                        .last_offset
                        .get()
                        .saturating_sub(batch.first_offset.get())
                        .saturating_add(1);
                    if u64::try_from(points.len()).unwrap_or(u64::MAX) != encoded_points {
                        return Err(LokiApiError::internal(
                            "metric rollup WAL offsets disagree with decoded point count",
                        ));
                    }
                    staged
                        .apply_batch(partition, batch.first_offset, points)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    next =
                        LogicalOffset::new(batch.last_offset.get().checked_add(1).ok_or_else(
                            || LokiApiError::internal("metric rollup offset exhausted"),
                        )?);
                }
            }
        }
        let incorporated_points = staged
            .persist()
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let series = staged.len();
        *catalog = staged;
        Ok(LifetimeRollupReport {
            series,
            incorporated_points,
        })
    }

    /// Returns locally persisted lifetime metric outcomes without reading raw
    /// object-tier data.
    pub fn query_lifetime_metric_rollups(
        &self,
        tenant: &str,
        name: Option<&str>,
    ) -> Result<Vec<crate::LifetimeMetricRollup>, LokiApiError> {
        self.checkpoint_lifetime_rollups()?;
        self.lifetime_rollups
            .as_ref()
            .ok_or_else(|| LokiApiError::configuration("lifetime metric rollups are not enabled"))?
            .lock()
            .map(|catalog| catalog.query(tenant, name))
            .map_err(|_| LokiApiError::internal("lifetime metric rollup lock poisoned"))
    }

    pub(crate) fn lifetime_rollup_storage(&self) -> Result<(usize, u64), LokiApiError> {
        let Some(catalog) = &self.lifetime_rollups else {
            return Ok((0, 0));
        };
        let catalog = catalog
            .lock()
            .map_err(|_| LokiApiError::internal("lifetime metric rollup lock poisoned"))?;
        Ok((
            catalog.len(),
            catalog
                .persisted_bytes()
                .map_err(|error| LokiApiError::internal(error.to_string()))?,
        ))
    }

    /// Advances shard-stream retention at whole append-batch boundaries.
    ///
    /// The durable sink checkpoint is an engine-level retention pin, so this
    /// cannot reclaim a source pack before its query index has applied it.
    pub fn compact_retention(&self) -> Result<RetentionReport, LokiApiError> {
        let cutoff = self.retention_cutoff();
        if cutoff.is_none()
            && (self.archive_object_tier || self.max_object_payload_bytes_per_partition.is_none())
        {
            return Ok(RetentionReport::default());
        }
        let cutoff = cutoff.unwrap_or(0);
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
        if result.is_ok() && cutoff > 0 {
            self.append_receipts.retain_since(cutoff)?;
        }
        result
    }

    pub(super) fn compact_retention_before(
        &self,
        cutoff: u64,
    ) -> Result<RetentionReport, LokiApiError> {
        self.flush(self.indexed_ack_timeout)?;
        let mut report = RetentionReport {
            cutoff_timestamp_unix_nanos: cutoff,
            ..RetentionReport::default()
        };
        if self.object_tier_enabled && !self.archive_object_tier {
            let tier = self
                .service
                .retain_object_tier(cutoff, self.max_object_payload_bytes_per_partition)
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
                        max_bytes: self.max_fetch_bytes,
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
}
