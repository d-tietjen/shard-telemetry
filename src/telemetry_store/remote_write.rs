use super::*;

impl DurableTelemetryStore {
    /// Validates and appends one complete Remote Write request under serialized
    /// same-timestamp conflict semantics.
    pub fn append_remote_write_batch(
        &self,
        batch: &crate::NativeTelemetryBatch,
    ) -> Result<crate::NativeTelemetryAppendAck, LokiApiError> {
        self.check_append_partitions(&batch.partitions)?;
        let mut request_samples = foldhash::HashMap::with_capacity(
            batch
                .partitions
                .iter()
                .map(|partition| partition.envelope.item_count as usize)
                .sum(),
        );
        let mut request_order = Vec::with_capacity(request_samples.capacity());
        let mut lock_indices = BTreeSet::new();
        for partition in &batch.partitions {
            if partition.envelope.signal != crate::TelemetrySignal::Metrics
                || partition.envelope.routing_metadata.len() != 5
                || partition.envelope.routing_metadata[4]
                    != crate::MetricIngestProtocol::RemoteWrite.to_wire()
            {
                return Err(LokiApiError::bad_request(
                    "Remote Write batch contains a non-Remote-Write metric envelope",
                ));
            }
            let points = crate::decode_metric_chunk(&partition.envelope.payload)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
            let series = points
                .first()
                .map(crate::DurableMetricPoint::series_fingerprint)
                .ok_or_else(|| LokiApiError::bad_request("Remote Write metric chunk is empty"))?;
            for point in points {
                let key = (
                    partition.topic_partition,
                    series,
                    point.timestamp_unix_nanos,
                );
                if let Some(existing) = request_samples.get(&key) {
                    if !same_remote_write_sample_payload(existing, &point) {
                        return Err(LokiApiError::bad_request(format!(
                            "conflicting samples for series {:032x} at {}",
                            key.1.get(),
                            key.2
                        )));
                    }
                    continue;
                }
                lock_indices.insert(remote_write_lock_index(series));
                request_order.push(key);
                request_samples.insert(key, point);
            }
        }

        // Conflict checks and the durable append must share the same locks.
        // Acquire every lock in index order so batches spanning multiple lock
        // shards cannot deadlock with another request acquiring the same set.
        let _guards = lock_indices
            .into_iter()
            .map(|index| {
                self.remote_write_append[index]
                    .lock()
                    .map_err(|_| LokiApiError::internal("Remote Write append lock poisoned"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut timestamp_queries = foldhash::HashMap::<
            (TopicPartition, crate::SeriesFingerprint),
            Vec<u64>,
        >::with_capacity(request_samples.len());
        for &(topic_partition, series, timestamp) in &request_order {
            timestamp_queries
                .entry((topic_partition, series))
                .or_default()
                .push(timestamp);
        }
        let retention_cutoff = self.retention_cutoff();
        for timestamps in timestamp_queries.values_mut() {
            timestamps.sort_unstable();
            timestamps.dedup();
            if let Some(cutoff) = retention_cutoff {
                timestamps.retain(|timestamp| *timestamp >= cutoff);
            }
        }

        // Probe each series once. The stripe query filters to the exact
        // timestamp set, so a sparse request does not materialize unrelated
        // points from the surrounding range.
        let mut existing_samples = foldhash::HashMap::with_capacity(request_samples.len());
        for ((topic_partition, series), timestamps) in timestamp_queries {
            let Some(first_timestamp) = timestamps.first().copied() else {
                continue;
            };
            let point = request_samples
                .get(&(topic_partition, series, first_timestamp))
                .expect("timestamp query only contains inserted samples");
            let metric_query = crate::MetricQuery {
                tenant: Arc::clone(&point.identity.tenant),
                // The envelope already carries the exact logical
                // partition used for this Remote Write append. Keeping
                // it here avoids fanning every conflict probe across all
                // tenant partitions and owner stripes.
                partition: Some(topic_partition),
                series: Some(series),
                start_time_unix_nanos: timestamps.first().copied(),
                end_time_unix_nanos: timestamps.last().copied(),
                limit: usize::MAX,
                ..crate::MetricQuery::default()
            };
            let exact_query = crate::sink::MetricTimestampQuery {
                tenant: Arc::clone(&point.identity.tenant),
                partition: topic_partition,
                series,
                timestamps: Arc::from(timestamps.into_boxed_slice()),
            };
            let existing = if let Some(shard_id) = self.metric_query_owner_shard(&metric_query) {
                self.service
                    .query_metric_timestamps_on_shard(shard_id, &exact_query)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?
            } else {
                self.service
                    .query_metric_timestamps(&exact_query)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?
            };
            for stored in existing {
                existing_samples.insert(
                    (topic_partition, series, stored.timestamp_unix_nanos),
                    stored,
                );
            }
        }

        for (topic_partition, series, timestamp) in request_order {
            let point = request_samples
                .get(&(topic_partition, series, timestamp))
                .expect("request order only contains inserted samples");
            if existing_samples
                .get(&(topic_partition, series, timestamp))
                .is_some_and(|stored| !same_remote_write_sample_payload(stored, point))
            {
                return Err(LokiApiError::bad_request(format!(
                    "conflicting sample for series {:032x} at {}",
                    series.get(),
                    timestamp
                )));
            }
        }
        // Remote Write has already validated the metric envelope and applied
        // its serialized conflict checks above. Re-encoding and decoding the
        // native batch here would repeat the wire validation pass.
        self.append_validated_telemetry_batch(batch, true)
    }
}
