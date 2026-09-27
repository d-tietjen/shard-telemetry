use super::*;

impl DurableTelemetryStore {
    pub(super) fn reclaim_source_packs(&self) -> Result<u64, LokiApiError> {
        let mut reclaimed = 0u64;
        for partition in self.engine.all_partitions() {
            let Some(checkpoint) = self
                .engine
                .durable_sink_checkpoint(partition)
                .map_err(engine_error)?
            else {
                continue;
            };
            let watermarks = self.engine.watermarks(partition).map_err(engine_error)?;
            let retained_start = checkpoint.next_offset.min(watermarks.last_stable_offset);
            if retained_start <= watermarks.log_start {
                continue;
            }
            self.engine
                .truncate_partition(partition, retained_start)
                .map_err(engine_error)?;
            reclaimed = reclaimed.saturating_add(
                retained_start
                    .get()
                    .saturating_sub(watermarks.log_start.get()),
            );
        }
        Ok(reclaimed)
    }

    pub(super) fn signal_partitions(
        &self,
        topic_id: TopicId,
    ) -> impl Iterator<Item = TopicPartition> {
        let count = self.tenant_partitions;
        (0..count)
            .map(move |partition| TopicPartition::new(topic_id, LogicalPartitionId::new(partition)))
    }

    pub(super) fn scan_trace_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let mut emitted = 0usize;
        let router = crate::TelemetryRouter::new(
            NonZeroU16::new(u16::try_from(self.tenant_partitions).map_err(|_| {
                LokiApiError::internal("tenant partition count exceeds the routing space")
            })?)
            .ok_or_else(|| LokiApiError::internal("tenant partition count is zero"))?,
        );
        let partitions = request.trace_id.map_or_else(
            || vec![None],
            |trace_id| vec![Some(router.trace(&request.tenant, trace_id))],
        );
        let pairs = |fields: &[crate::MetadataField]| {
            Arc::new(
                fields
                    .iter()
                    .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                    .collect::<Vec<_>>(),
            )
        };
        let exact_attributes = if request.relation == AnalyticsRelation::Spans {
            pairs(&request.attributes)
        } else {
            Arc::default()
        };
        let exact_resource_attributes = pairs(&request.resource_attributes);
        let exact_scope_attributes = pairs(&request.scope_attributes);
        let span_predicates_fully_pushed = request.relation == AnalyticsRelation::Spans
            && request.labels.is_empty()
            && request.metadata.is_empty();
        let projected_resource_scan = span_predicates_fully_pushed
            && request.trace_id.is_none()
            && !request.resource_attributes.is_empty()
            && crate::analytics::can_direct_span_projection(&request.columns);
        for partition in partitions {
            let mut next_offset = None;
            loop {
                if emitted == limit {
                    return Ok(());
                }
                let page_limit =
                    crate::analytics::DEFAULT_SCAN_BATCH_ROWS.min(limit.saturating_sub(emitted));
                let event_relation = request.relation == AnalyticsRelation::SpanEvents;
                let query = crate::TraceQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition,
                    start_offset: next_offset.map(LogicalOffset::new),
                    trace_id: request.trace_id,
                    span_id: request.span_id,
                    name: (request.relation == AnalyticsRelation::Spans)
                        .then(|| request.name.as_ref().map(Arc::clone))
                        .flatten(),
                    exact_attributes: Arc::clone(&exact_attributes),
                    exact_resource_attributes: Arc::clone(&exact_resource_attributes),
                    exact_scope_attributes: Arc::clone(&exact_scope_attributes),
                    start_time_unix_nanos: (!event_relation)
                        .then_some(request.start_timestamp_unix_nanos)
                        .flatten(),
                    end_time_unix_nanos: (!event_relation)
                        .then_some(request.end_timestamp_unix_nanos)
                        .flatten(),
                    min_duration_nanos: None,
                    limit: page_limit,
                };
                let targeted_shard = self.physical_shard_count.and_then(|shard_count| {
                    partition
                        .map(|partition| ShardId::new(partition.partition_id.get() % shard_count))
                });
                if projected_resource_scan {
                    let spans = if let Some(shard_id) = targeted_shard.or(self
                        .service
                        .trace_query_owner_shard(&query)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?)
                    {
                        self.service
                            .query_traces_projected_on_shard(shard_id, &query)
                            .map_err(|error| LokiApiError::internal(error.to_string()))?
                    } else {
                        self.service
                            .query_traces_projected_unordered(&query)
                            .map_err(|error| LokiApiError::internal(error.to_string()))?
                    };
                    let returned = spans.len();
                    let final_offset = spans.last().map(|span| span.record_ref.offset.get());
                    let mut rows =
                        Vec::with_capacity(page_limit.min(limit.saturating_sub(emitted)));
                    for span in spans {
                        rows.push(crate::analytics::projected_trace_row(
                            &request.tenant,
                            &span,
                            &request.columns,
                        )?);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                    }
                    if !rows.is_empty() {
                        emit(&rows)?;
                    }
                    if emitted == limit || returned < page_limit || partition.is_none() {
                        break;
                    }
                    let Some(final_offset) = final_offset else {
                        break;
                    };
                    let Some(start) = final_offset.checked_add(1) else {
                        break;
                    };
                    next_offset = Some(start);
                    continue;
                }
                let spans = if let Some(shard_id) = targeted_shard.or(self
                    .service
                    .trace_query_owner_shard(&query)
                    .map_err(|error| LokiApiError::internal(error.to_string()))?)
                {
                    self.service
                        .query_traces_on_shard(shard_id, &query)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                } else if request.order.is_none() {
                    self.query_traces_unordered(&query)?
                } else {
                    self.query_traces(&query)?
                };
                if spans.is_empty() {
                    break;
                }
                let returned = spans.len();
                let final_offset = spans
                    .last()
                    .expect("non-empty trace page")
                    .record_ref
                    .offset
                    .get();
                let mut rows = Vec::with_capacity(page_limit.min(limit.saturating_sub(emitted)));
                for span in spans {
                    if span_predicates_fully_pushed {
                        rows.push(crate::analytics::projected_span_row(
                            &span,
                            &request.columns,
                        )?);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                        continue;
                    }
                    let candidate_rows = crate::analytics::span_rows(&span, request.relation)?;
                    for row in candidate_rows {
                        if !crate::analytics::row_matches(&row, request) {
                            continue;
                        }
                        rows.push(row);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                    }
                    if emitted == limit {
                        break;
                    }
                }
                if !rows.is_empty() {
                    emit(&rows)?;
                }
                if emitted == limit || returned < page_limit || partition.is_none() {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        Ok(())
    }

    pub(super) fn scan_metric_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let mut emitted = 0usize;
        let router = crate::TelemetryRouter::new(
            NonZeroU16::new(u16::try_from(self.tenant_partitions).map_err(|_| {
                LokiApiError::internal("tenant partition count exceeds the routing space")
            })?)
            .ok_or_else(|| LokiApiError::internal("tenant partition count is zero"))?,
        );
        let partitions = request.series_id.map_or_else(
            || {
                self.signal_partitions(crate::METRICS_TOPIC_ID)
                    .collect::<Vec<_>>()
            },
            |series| vec![router.metric(&request.tenant, series)],
        );
        let metric_predicates_fully_pushed = request.relation == AnalyticsRelation::MetricPoints
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.metadata.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty();
        let exact_series_single_page = metric_predicates_fully_pushed
            && request.series_id.is_some()
            && request
                .limit
                .is_some_and(|limit| limit <= crate::analytics::DEFAULT_SCAN_BATCH_ROWS);
        for partition in partitions {
            let mut next_offset = None;
            loop {
                if emitted == limit {
                    return Ok(());
                }
                let page_limit =
                    crate::analytics::DEFAULT_SCAN_BATCH_ROWS.min(limit.saturating_sub(emitted));
                let exemplar_relation = request.relation == AnalyticsRelation::MetricExemplars;
                let query = crate::MetricQuery {
                    tenant: Arc::clone(&request.tenant),
                    // A bounded exact-series scan needs no continuation cursor.
                    // Leaving the partition unset selects the timestamp-ordered,
                    // disjoint-chunk fast path instead of rebuilding offset order
                    // for every point in the series.
                    partition: (!exact_series_single_page).then_some(partition),
                    start_offset: next_offset.map(LogicalOffset::new),
                    series: request.series_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_labels: Arc::new(
                        request
                            .labels
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect(),
                    ),
                    start_time_unix_nanos: (!exemplar_relation)
                        .then_some(request.start_timestamp_unix_nanos)
                        .flatten(),
                    end_time_unix_nanos: (!exemplar_relation)
                        .then(|| {
                            request
                                .end_timestamp_unix_nanos
                                .and_then(|end| end.checked_sub(1))
                        })
                        .flatten(),
                    limit: page_limit,
                };
                let points = if let Some(shard_count) = self.physical_shard_count
                    && request.series_id.is_some()
                {
                    self.service
                        .query_metrics_on_shard(
                            ShardId::new(partition.partition_id.get() % shard_count),
                            &query,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                } else {
                    self.query_metrics(&query)?
                };
                if points.is_empty() {
                    break;
                }
                let returned = points.len();
                let final_offset = points
                    .last()
                    .expect("non-empty metric page")
                    .record_ref
                    .offset
                    .get();
                let mut rows = Vec::with_capacity(page_limit.min(limit.saturating_sub(emitted)));
                for point in points {
                    if metric_predicates_fully_pushed {
                        rows.push(crate::analytics::projected_metric_row(
                            &point,
                            &request.columns,
                        )?);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                        continue;
                    }
                    let candidate_rows = crate::analytics::metric_rows(&point, request.relation)?;
                    for row in candidate_rows {
                        if !crate::analytics::row_matches(&row, request) {
                            continue;
                        }
                        rows.push(row);
                        emitted = emitted.saturating_add(1);
                        if rows.len() == crate::analytics::DEFAULT_SCAN_BATCH_ROWS {
                            emit(&rows)?;
                            rows.clear();
                        }
                        if emitted == limit {
                            break;
                        }
                    }
                    if emitted == limit {
                        break;
                    }
                }
                if !rows.is_empty() {
                    emit(&rows)?;
                }
                if emitted == limit || returned < page_limit {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        Ok(())
    }
}
