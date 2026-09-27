use super::*;

impl TelemetryService {
    /// Returns shared cold-tier cache occupancy and object-read counters.
    #[must_use]
    pub fn object_tier_cache_stats(&self) -> Option<ObjectTierCacheStats> {
        self.tier_caches
            .as_ref()
            .map(|caches| ObjectTierCacheStats {
                control: caches.control.stats(),
                payload: caches.payload.stats(),
            })
    }

    /// Returns object-store request, transfer, deletion, and failure counters.
    #[must_use]
    pub fn object_store_stats(&self) -> Option<ObjectStoreStats> {
        self.object_store
            .as_ref()
            .map(SharedTelemetryObjectStore::stats)
    }

    /// Fans a partition-local query across all active physical stripes.
    pub fn query_all(&self, query: &LogQuery) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partitions(std::slice::from_ref(query))
    }

    /// Fans a native trace query across all owner stripes and merges by trace/start/offset.
    pub fn query_traces(&self, query: &TraceQuery) -> TelemetryResult<Vec<DurableSpan>> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryTraces {
                    query: query.clone(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a trace query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut spans = Vec::with_capacity(fanout_result_capacity(
            Some(query.limit.max(1)),
            responses.len(),
        ));
        for (shard_id, receiver) in responses {
            spans.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying traces"
                ))
            })??);
        }
        sort_and_limit(&mut spans, Some(query.limit.max(1)), |left, right| {
            if query.partition.is_some() {
                left.record_ref.offset.cmp(&right.record_ref.offset)
            } else {
                (
                    left.trace_id,
                    left.start_time_unix_nanos,
                    left.record_ref.offset,
                )
                    .cmp(&(
                        right.trace_id,
                        right.start_time_unix_nanos,
                        right.record_ref.offset,
                    ))
            }
        });
        Ok(spans)
    }

    /// Executes a bounded trace scan in deterministic stripe order without
    /// paying the all-stripe top-k merge cost.
    ///
    /// This is valid only for callers that do not request a global ordering,
    /// such as an analytical scan whose evaluator performs any later sort.
    pub(crate) fn query_traces_unordered(
        &self,
        query: &TraceQuery,
    ) -> TelemetryResult<Vec<DurableSpan>> {
        let limit = query.limit.max(1);
        let mut workers = self.worker_senders()?;
        let mut spans = Vec::with_capacity(limit);
        if workers.is_empty() {
            return Ok(spans);
        }
        let (first_shard_id, first_sender) = workers.remove(0);
        let (first_response, first_receiver) = sync_channel(1);
        let mut first_query = query.clone();
        first_query.limit = limit;
        first_sender
            .send(SinkCommand::QueryTraces {
                query: first_query,
                response: first_response,
            })
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {first_shard_id} stopped before accepting an unordered trace query"
                ))
            })?;
        spans.extend(first_receiver.recv().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(format!(
                "stripe {first_shard_id} stopped while querying unordered traces"
            ))
        })??);
        if spans.len() >= limit {
            spans.truncate(limit);
            return Ok(spans);
        }

        let remaining = limit.saturating_sub(spans.len());
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let mut stripe_query = query.clone();
            stripe_query.limit = remaining;
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryTraces {
                    query: stripe_query,
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting an unordered trace query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        for (shard_id, receiver) in responses {
            spans.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying unordered traces"
                ))
            })??);
        }
        spans.truncate(limit);
        Ok(spans)
    }

    /// Executes a bounded unordered span scan while retaining only scalar
    /// projection fields for resource-filtered analytical queries.
    pub(crate) fn query_traces_projected_unordered(
        &self,
        query: &TraceQuery,
    ) -> TelemetryResult<Vec<TraceProjection>> {
        let limit = query.limit.max(1);
        let mut workers = self.worker_senders()?;
        let mut spans = Vec::with_capacity(limit);
        if workers.is_empty() {
            return Ok(spans);
        }
        let (first_shard_id, first_sender) = workers.remove(0);
        let (first_response, first_receiver) = sync_channel(1);
        let mut first_query = query.clone();
        first_query.limit = limit;
        first_sender
            .send(SinkCommand::QueryTraceProjected {
                query: first_query,
                response: first_response,
            })
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {first_shard_id} stopped before accepting an unordered projected trace query"
                ))
            })?;
        spans.extend(first_receiver.recv().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(format!(
                "stripe {first_shard_id} stopped while querying unordered projected traces"
            ))
        })??);
        if spans.len() >= limit {
            spans.truncate(limit);
            return Ok(spans);
        }

        let remaining = limit.saturating_sub(spans.len());
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let mut stripe_query = query.clone();
            stripe_query.limit = remaining;
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryTraceProjected {
                    query: stripe_query,
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting an unordered projected trace query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        for (shard_id, receiver) in responses {
            spans.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying unordered projected traces"
                ))
            })??);
        }
        spans.truncate(limit);
        Ok(spans)
    }

    /// Executes a partition-affine trace query on its owning worker.
    pub(crate) fn query_traces_on_shard(
        &self,
        shard_id: ShardId,
        query: &TraceQuery,
    ) -> TelemetryResult<Vec<DurableSpan>> {
        let sender = self.worker_sender(shard_id)?;
        let (response, receiver) = sync_channel(1);
        sender
            .send(SinkCommand::QueryTraces {
                query: query.clone(),
                response,
            })
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped before accepting a targeted trace query"
                ))
            })?;
        receiver.recv().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(format!(
                "stripe {shard_id} stopped while executing a targeted trace query"
            ))
        })?
    }

    pub(crate) fn query_traces_projected_on_shard(
        &self,
        shard_id: ShardId,
        query: &TraceQuery,
    ) -> TelemetryResult<Vec<TraceProjection>> {
        let sender = self.worker_sender(shard_id)?;
        let (response, receiver) = sync_channel(1);
        sender
            .send(SinkCommand::QueryTraceProjected {
                query: query.clone(),
                response,
            })
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped before accepting a targeted projected trace query"
                ))
            })?;
        receiver.recv().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(format!(
                "stripe {shard_id} stopped while executing a targeted projected trace query"
            ))
        })?
    }

    /// Fans a native raw metric query across all owner stripes and merges by time/offset.
    pub fn query_metrics(&self, query: &MetricQuery) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryMetrics {
                    query: query.clone(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a metric query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut points = Vec::with_capacity(fanout_result_capacity(
            Some(query.limit.max(1)),
            responses.len(),
        ));
        for (shard_id, receiver) in responses {
            points.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying metrics"
                ))
            })??);
        }
        sort_and_limit(&mut points, Some(query.limit.max(1)), |left, right| {
            if query.partition.is_some() {
                left.record_ref.offset.cmp(&right.record_ref.offset)
            } else {
                (left.timestamp_unix_nanos, left.record_ref.offset)
                    .cmp(&(right.timestamp_unix_nanos, right.record_ref.offset))
            }
        });
        Ok(points)
    }

    /// Executes a partition-affine metric query on its owning worker.
    pub(crate) fn query_metrics_on_shard(
        &self,
        shard_id: ShardId,
        query: &MetricQuery,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let sender = self.worker_sender(shard_id)?;
        let (response, receiver) = sync_channel(1);
        sender
            .send(SinkCommand::QueryMetrics {
                query: query.clone(),
                response,
            })
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped before accepting a targeted metric query"
                ))
            })?;
        receiver.recv().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(format!(
                "stripe {shard_id} stopped while executing a targeted metric query"
            ))
        })?
    }

    /// Fans an exact timestamp metric probe across owner stripes.
    pub(crate) fn query_metric_timestamps(
        &self,
        query: &MetricTimestampQuery,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            sender
                .send(SinkCommand::QueryMetricTimestamps {
                    query: query.clone(),
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting an exact metric probe"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut points = Vec::new();
        for (shard_id, receiver) in responses {
            points.extend(receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while executing an exact metric probe"
                ))
            })??);
        }
        points.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
        Ok(points)
    }

    /// Executes an exact timestamp metric probe on one owner stripe.
    pub(crate) fn query_metric_timestamps_on_shard(
        &self,
        shard_id: ShardId,
        query: &MetricTimestampQuery,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let sender = self.worker_sender(shard_id)?;
        let (response, receiver) = sync_channel(1);
        sender
            .send(SinkCommand::QueryMetricTimestamps {
                query: query.clone(),
                response,
            })
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped before accepting an exact metric probe"
                ))
            })?;
        receiver.recv().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(format!(
                "stripe {shard_id} stopped while executing an exact metric probe"
            ))
        })?
    }

    /// Connects logs, spans, and metric exemplars through exact trace,
    /// resource, scope, and typed-label identities.
    pub fn query_correlations(
        &self,
        query: &CorrelationQuery,
    ) -> TelemetryResult<Vec<TelemetryRecordRef>> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let workers = self.worker_senders()?;
        let mut responses = Vec::with_capacity(workers.len());
        for (shard_id, sender) in workers {
            let (response, receiver) = sync_channel(1);
            let buffer = self
                .correlation_buffers
                .lock()
                .map(|mut pool| pool.take())
                .unwrap_or_default();
            sender
                .send(SinkCommand::Correlate {
                    query: query.clone(),
                    buffer,
                    response,
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a correlation query"
                    ))
                })?;
            responses.push((shard_id, receiver));
        }
        let mut refs: Option<Vec<TelemetryRecordRef>> = None;
        for (shard_id, receiver) in responses {
            let worker_refs = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while querying correlations"
                ))
            })??;
            if let Some(refs) = refs.as_mut() {
                refs.extend(worker_refs.iter().copied());
                if let Ok(mut pool) = self.correlation_buffers.lock() {
                    pool.recycle(worker_refs);
                }
            } else {
                refs = Some(worker_refs);
            }
        }
        let mut refs = refs.unwrap_or_default();
        refs.sort_unstable();
        refs.dedup();
        if let Some(after) = query.after {
            refs.retain(|record| *record > after);
        }
        refs.truncate(query.limit);
        Ok(refs)
    }
}
