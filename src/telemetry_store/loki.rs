use super::*;

impl DurableTelemetryStore {
    fn query_loki_range(
        &self,
        tenant: &str,
        selector: &crate::loki_api::LogSelector,
        start_timestamp_unix_nanos: i64,
        end_timestamp_unix_nanos: i64,
        limit: usize,
        newest_first: bool,
    ) -> Result<LokiQueryResult, LokiApiError> {
        if limit == 0 || end_timestamp_unix_nanos < 0 {
            return Ok(LokiQueryResult::default());
        }
        let partitions = self.tenant_partitions(tenant)?;
        if partitions.is_empty() {
            return Ok(LokiQueryResult::default());
        }
        let start = u64::try_from(start_timestamp_unix_nanos).unwrap_or_default();
        // Loki range bounds are inclusive. The native log query uses an
        // exclusive upper bound, so widen the converted end by one tick.
        let end = u64::try_from(end_timestamp_unix_nanos)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let start = self.retained_query_start(Some(start));
        let delete_requests = self.deletes.list(tenant)?;
        let exact_selector = selector.is_exact_label_only();
        let bounded_candidates = exact_selector && delete_requests.is_empty();
        let indexed_line_predicate = selector.indexed_line_predicate();
        let mut per_partition_limit = limit.div_ceil(partitions.len()).max(1);
        let mut lines_processed = 0usize;
        let mut bytes_processed = 0usize;
        loop {
            let queries = partitions
                .iter()
                .copied()
                .map(|partition| {
                    let mut query = LogQuery::new(partition)
                        .sort_by_timestamp()
                        .with_field(TENANT_FIELD, tenant);
                    if bounded_candidates {
                        query = query.with_limit(per_partition_limit);
                    }
                    query.start_timestamp_unix_nanos = start;
                    query.end_timestamp_unix_nanos = Some(end);
                    if newest_first {
                        query = query.newest_first();
                    }
                    for (key, value) in selector.exact_label_matchers() {
                        query = query.with_field(format!("{LABEL_PREFIX}{key}"), value);
                    }
                    if let Some(predicate) = &indexed_line_predicate {
                        query = query.with_predicate(predicate.clone());
                    }
                    query
                })
                .collect::<Vec<_>>();
            let partition_matches = self
                .service
                .query_partitions_projected_each_with_fields(&queries, false, true)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            let saturated = bounded_candidates
                && partition_matches
                    .iter()
                    .any(|matches| matches.len() >= per_partition_limit);
            let matches = partition_matches.into_iter().flatten().inspect(|matched| {
                lines_processed = lines_processed.saturating_add(1);
                bytes_processed = bytes_processed.saturating_add(matched.record.message.len());
            });
            let mut entries = matches
                .map(log_match_to_entry)
                .collect::<Result<Vec<_>, _>>()?;
            if !delete_requests.is_empty() {
                apply_logical_deletes(&mut entries, &delete_requests)?;
            }
            let mut entries = if exact_selector {
                entries
            } else {
                entries
                    .into_iter()
                    .filter_map(|entry| selector.process(entry))
                    .collect::<Vec<_>>()
            };
            entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
            if newest_first {
                entries.reverse();
            }
            entries.truncate(limit);
            if entries.len() >= limit || !saturated {
                return Ok(LokiQueryResult {
                    entries,
                    lines_processed,
                    bytes_processed,
                });
            }
            let next_limit = per_partition_limit.saturating_mul(2);
            if next_limit == per_partition_limit {
                return Ok(LokiQueryResult {
                    entries,
                    lines_processed,
                    bytes_processed,
                });
            }
            per_partition_limit = next_limit;
        }
    }
}

impl LokiStore for DurableTelemetryStore {
    fn push(&self, tenant: &str, entries: Vec<LokiEntry>) -> Result<(), LokiApiError> {
        if entries.is_empty() {
            return Ok(());
        }
        let record_count = u32::try_from(entries.len())
            .map_err(|_| LokiApiError::bad_request("push contains more than u32 entries"))?;
        let routing_request = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let topic_partition = self.write_partition(tenant, routing_request);
        let (envelope, transient_context) =
            crate::signal_ingest::prepare_loki_log_envelope_with_context(tenant, entries)
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let acknowledgement = self.append_telemetry_partition(
            &crate::NativePartitionAppend {
                topic_partition,
                envelope,
                transient_context: Some(transient_context),
            },
            true,
        )?;
        debug_assert_eq!(
            acknowledgement
                .last_offset
                .saturating_sub(acknowledgement.first_offset)
                .saturating_add(1),
            u64::from(record_count)
        );
        Ok(())
    }

    fn entries(&self, tenant: &str) -> Result<Vec<LokiEntry>, LokiApiError> {
        let queries = self
            .tenant_partitions(tenant)?
            .into_iter()
            .map(|partition| {
                LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_field(TENANT_FIELD, tenant)
            })
            .collect::<Vec<_>>();
        let cutoff = self.retention_cutoff();
        let queries = queries
            .into_iter()
            .map(|mut query| {
                query.start_timestamp_unix_nanos = cutoff;
                query
            })
            .collect::<Vec<_>>();
        let matches = self
            .service
            // Loki listings only need the message, timestamp, and structural
            // fields used to reconstruct labels and structured metadata.
            // Avoid cloning typed OTLP bodies and signal context for every
            // result in an unbounded listing.
            .query_partitions_projected_with_fields(&queries, false, true)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let mut entries = matches
            .into_iter()
            .map(log_match_to_entry)
            .collect::<Result<Vec<_>, _>>()?;
        apply_logical_deletes(&mut entries, &self.deletes.list(tenant)?)?;
        entries.sort_unstable_by_key(|entry| entry.timestamp_unix_nanos);
        Ok(entries)
    }

    fn query_range(
        &self,
        tenant: &str,
        expression: &str,
        start_timestamp_unix_nanos: i64,
        end_timestamp_unix_nanos: i64,
        limit: usize,
        newest_first: bool,
    ) -> Result<LokiQueryResult, LokiApiError> {
        let selector = crate::loki_api::parse_log_query(expression)?;
        self.query_loki_range(
            tenant,
            &selector,
            start_timestamp_unix_nanos,
            end_timestamp_unix_nanos,
            limit,
            newest_first,
        )
    }

    fn scan_analytics_arrow(
        &self,
        request: &AnalyticsScanRequest,
        schema: &SchemaRef,
        emit: &mut dyn FnMut(&RecordBatch) -> Result<(), LokiApiError>,
    ) -> Result<bool, LokiApiError> {
        request.validate()?;
        let Some(limit) = request
            .limit
            .filter(|limit| *limit <= crate::analytics::DEFAULT_SCAN_BATCH_ROWS)
        else {
            return Ok(false);
        };
        if limit == 0 {
            return Ok(true);
        }
        match request.relation {
            AnalyticsRelation::MetricPoints
                if request.series_id.is_some()
                    && request.trace_id.is_none()
                    && request.span_id.is_none()
                    && request.metadata.is_empty()
                    && request.attributes.is_empty()
                    && request.resource_attributes.is_empty()
                    && request.scope_attributes.is_empty()
                    && crate::analytics::can_direct_metric_projection(&request.columns) =>
            {
                let query = crate::MetricQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    series: request.series_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_labels: Arc::new(
                        request
                            .labels
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect(),
                    ),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request
                        .end_timestamp_unix_nanos
                        .and_then(|end| end.checked_sub(1)),
                    limit,
                };
                let points = self.query_metrics(&query)?;
                if !points.is_empty() {
                    let batch = crate::analytics::direct_metric_record_batch(
                        &points,
                        &request.columns,
                        Arc::clone(schema),
                    )?
                    .expect("direct metric projection was checked");
                    emit(&batch)?;
                }
                Ok(true)
            }
            AnalyticsRelation::Spans
                if (request.trace_id.is_some() || !request.resource_attributes.is_empty())
                    && request.labels.is_empty()
                    && request.metadata.is_empty()
                    && crate::analytics::can_direct_span_projection(&request.columns) =>
            {
                let pairs = |fields: &[crate::MetadataField]| {
                    Arc::new(
                        fields
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect::<Vec<_>>(),
                    )
                };
                let query = crate::TraceQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    trace_id: request.trace_id,
                    span_id: request.span_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_attributes: pairs(&request.attributes),
                    exact_resource_attributes: pairs(&request.resource_attributes),
                    exact_scope_attributes: pairs(&request.scope_attributes),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request.end_timestamp_unix_nanos,
                    min_duration_nanos: None,
                    limit,
                };
                let spans = if request.order.is_none() && request.trace_id.is_none() {
                    self.query_traces_unordered(&query)?
                } else {
                    self.query_traces(&query)?
                };
                if !spans.is_empty() {
                    let batch = crate::analytics::direct_span_record_batch(
                        &spans,
                        &request.columns,
                        Arc::clone(schema),
                    )?
                    .expect("direct span projection was checked");
                    emit(&batch)?;
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn scan_analytics_rowbinary(
        &self,
        request: &AnalyticsScanRequest,
        writer: &mut dyn std::io::Write,
    ) -> Result<bool, LokiApiError> {
        request.validate()?;
        let Some(limit) = request
            .limit
            .filter(|limit| *limit <= crate::analytics::DEFAULT_SCAN_BATCH_ROWS)
        else {
            return Ok(false);
        };
        if limit == 0 {
            return Ok(true);
        }
        match request.relation {
            AnalyticsRelation::MetricPoints
                if request.series_id.is_some()
                    && request.trace_id.is_none()
                    && request.span_id.is_none()
                    && request.metadata.is_empty()
                    && request.attributes.is_empty()
                    && request.resource_attributes.is_empty()
                    && request.scope_attributes.is_empty()
                    && crate::analytics::can_direct_metric_projection(&request.columns) =>
            {
                let query = crate::MetricQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    series: request.series_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_labels: Arc::new(
                        request
                            .labels
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect(),
                    ),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request
                        .end_timestamp_unix_nanos
                        .and_then(|end| end.checked_sub(1)),
                    limit,
                };
                let points = self.query_metrics(&query)?;
                crate::analytics::write_direct_metric_rowbinary(&points, &request.columns, writer)
            }
            AnalyticsRelation::Spans
                if (request.trace_id.is_some() || !request.resource_attributes.is_empty())
                    && request.labels.is_empty()
                    && request.metadata.is_empty()
                    && crate::analytics::can_direct_span_projection(&request.columns) =>
            {
                let pairs = |fields: &[crate::MetadataField]| {
                    Arc::new(
                        fields
                            .iter()
                            .map(|field| (Arc::clone(&field.key), Arc::clone(&field.value)))
                            .collect::<Vec<_>>(),
                    )
                };
                let query = crate::TraceQuery {
                    tenant: Arc::clone(&request.tenant),
                    partition: None,
                    start_offset: None,
                    trace_id: request.trace_id,
                    span_id: request.span_id,
                    name: request.name.as_ref().map(Arc::clone),
                    exact_attributes: pairs(&request.attributes),
                    exact_resource_attributes: pairs(&request.resource_attributes),
                    exact_scope_attributes: pairs(&request.scope_attributes),
                    start_time_unix_nanos: request.start_timestamp_unix_nanos,
                    end_time_unix_nanos: request.end_timestamp_unix_nanos,
                    min_duration_nanos: None,
                    limit,
                };
                let spans = if request.order.is_none() {
                    self.query_traces_unordered(&query)?
                } else {
                    self.query_traces(&query)?
                };
                crate::analytics::write_direct_span_rowbinary(&spans, &request.columns, writer)
            }
            _ => Ok(false),
        }
    }

    fn scan_analytics(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        match request.relation {
            AnalyticsRelation::Logs => {}
            AnalyticsRelation::Spans
            | AnalyticsRelation::SpanEvents
            | AnalyticsRelation::SpanLinks => {
                return self.scan_trace_analytics(request, emit);
            }
            AnalyticsRelation::MetricPoints | AnalyticsRelation::MetricExemplars => {
                return self.scan_metric_analytics(request, emit);
            }
        }
        if request.order == Some(AnalyticsScanOrder::RelevanceDescending) {
            return self.scan_analytics_relevance(request, emit);
        }
        let limit = request.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(());
        }
        let include_typed_metadata =
            crate::analytics::log_columns_need_typed_metadata(&request.columns)
                || !request.attributes.is_empty();
        let include_fields = crate::analytics::log_columns_need_structural_fields(&request.columns)
            || analytics_predicate_needs_structural_fields(&request.predicate)
            || (!request.attributes.is_empty()
                && (!request.labels.is_empty() || !request.metadata.is_empty()))
            || request.series_id.is_some()
            || request.name.is_some();
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        let index_complete_log_scan = delete_filter.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.series_id.is_none()
            && request.name.is_none()
            && (request.order.is_some() || request.trace_id.is_some() || request.limit.is_some());
        if index_complete_log_scan {
            let partitions = if let Some(trace_id) = request.trace_id {
                let router = crate::TelemetryRouter::new(
                    NonZeroU16::new(u16::try_from(self.tenant_partitions).map_err(|_| {
                        LokiApiError::internal("tenant partition count exceeds the routing space")
                    })?)
                    .ok_or_else(|| LokiApiError::internal("tenant partition count is zero"))?,
                );
                vec![router.log(&request.tenant, Some(trace_id), &[])]
            } else {
                self.tenant_partitions(&request.tenant)?
            };
            let queries = partitions
                .into_iter()
                .map(|partition| {
                    let mut query =
                        LogQuery::new(partition).with_field(TENANT_FIELD, request.tenant.as_ref());
                    if request.order.is_some() {
                        query = query.sort_by_timestamp();
                    }
                    // SQL leaves the row order unspecified when ORDER BY is
                    // absent. For a bounded unordered page, newest-first
                    // selection lets tiered frames stop at the first useful
                    // time groups instead of decoding the entire window.
                    if request.order.is_none() && request.trace_id.is_none() {
                        query = query.sort_by_timestamp().newest_first();
                    }
                    if let Some(limit) = request.limit {
                        query = query.with_limit(limit);
                    }
                    query.start_timestamp_unix_nanos =
                        self.retained_query_start(request.start_timestamp_unix_nanos);
                    query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                    query = apply_analytics_log_filters(query, request);
                    if request.order == Some(AnalyticsScanOrder::TimestampDescending) {
                        query = query.newest_first();
                    }
                    query
                })
                .collect::<Vec<_>>();
            let mut matches = if request.trace_id.is_some() {
                let partition = queries
                    .first()
                    .expect("trace-routed log scan always builds one query")
                    .topic_partition;
                if let Some(shard_count) = self.physical_shard_count {
                    self.service
                        .query_partition_projected_on_shard_with_fields(
                            ShardId::new(partition.partition_id.get() % shard_count),
                            &queries[0],
                            include_typed_metadata,
                            include_fields,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                } else {
                    self.service
                        .query_partitions_projected_each_with_fields(
                            &queries,
                            include_typed_metadata,
                            include_fields,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                        .into_iter()
                        .flatten()
                        .collect()
                }
            } else if request.order.is_none() {
                // An unordered bounded scan only needs enough rows to fill the
                // global page. Asking every partition for the full limit can
                // decode tens of thousands of rows that are immediately
                // discarded below. Start with an even per-partition budget and
                // grow it only when a partition was saturated before the page
                // filled, preserving the same arbitrary-order semantics.
                let mut per_partition_limit = request
                    .limit
                    .unwrap_or(limit)
                    .div_ceil(queries.len().max(1))
                    .max(1);
                loop {
                    let bounded_queries = queries
                        .iter()
                        .cloned()
                        .map(|query| query.with_limit(per_partition_limit))
                        .collect::<Vec<_>>();
                    let matches = self
                        .service
                        .query_partitions_projected_unordered_with_fields(
                            &bounded_queries,
                            include_typed_metadata,
                            include_fields,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?;
                    if matches.len() >= limit || per_partition_limit >= limit {
                        break matches;
                    }
                    let mut counts = BTreeMap::<TopicPartition, usize>::new();
                    for matched in &matches {
                        *counts
                            .entry(matched.record.record_ref.topic_partition)
                            .or_default() += 1;
                    }
                    if !counts.values().any(|count| *count >= per_partition_limit) {
                        break matches;
                    }
                    per_partition_limit = per_partition_limit.saturating_mul(2).min(limit);
                }
            } else {
                self.service
                    .query_partitions_projected_with_fields(
                        &queries,
                        include_typed_metadata,
                        include_fields,
                    )
                    .map_err(|error| LokiApiError::internal(error.to_string()))?
            };
            if request.order.is_none() && request.trace_id.is_none() {
                matches.sort_unstable_by_key(|matched| {
                    (
                        matched.record.record_ref.topic_partition,
                        matched.record.record_ref.offset,
                    )
                });
                matches.truncate(limit);
            }
            let rows = matches
                .into_iter()
                .map(|matched| {
                    crate::analytics::projected_log_row(
                        &request.tenant,
                        &matched.record,
                        &request.columns,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !rows.is_empty() {
                emit(&rows)?;
            }
            return Ok(());
        }

        // An unordered, bounded scan with no logical deletes can batch all
        // partition queries into one owner-worker command. This keeps the
        // post-filter semantics below while avoiding one channel round trip
        // per logical partition on the common analytical path.
        if delete_filter.is_empty()
            && request.order.is_none()
            && request.trace_id.is_none()
            && let Some(request_limit) = request.limit
        {
            let mut active_partitions = self.tenant_partitions(&request.tenant)?;
            let mut per_partition_limit = request_limit
                .div_ceil(active_partitions.len().max(1))
                .max(1);
            let mut rows =
                Vec::with_capacity(request_limit.min(crate::analytics::DEFAULT_SCAN_BATCH_ROWS));
            let mut seen = HashSet::new();
            loop {
                let queries = active_partitions
                    .iter()
                    .copied()
                    .map(|partition| {
                        let mut query = LogQuery::new(partition)
                            .with_field(TENANT_FIELD, request.tenant.as_ref());
                        query.start_timestamp_unix_nanos =
                            self.retained_query_start(request.start_timestamp_unix_nanos);
                        query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                        apply_analytics_log_filters(query, request).with_limit(per_partition_limit)
                    })
                    .collect::<Vec<_>>();
                let partition_matches = self
                    .service
                    .query_partitions_projected_each_with_fields(
                        &queries,
                        include_typed_metadata,
                        include_fields,
                    )
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                let mut saturated_partitions = Vec::new();
                for (partition, matches) in active_partitions.iter().zip(partition_matches) {
                    if matches.len() >= per_partition_limit {
                        saturated_partitions.push(*partition);
                    }
                    for matched in matches {
                        let record_key = (
                            matched.record.record_ref.topic_partition,
                            matched.record.record_ref.offset.get(),
                        );
                        if !seen.insert(record_key) {
                            continue;
                        }
                        if request.attributes.is_empty() {
                            rows.push(crate::analytics::projected_log_row(
                                &request.tenant,
                                &matched.record,
                                &request.columns,
                            )?);
                        } else {
                            let row = analytics_row_from_match(&request.tenant, matched)?;
                            if !crate::analytics::row_matches(&row, request) {
                                continue;
                            }
                            rows.push(row);
                        }
                        if rows.len() == request_limit {
                            break;
                        }
                    }
                    if rows.len() == request_limit {
                        break;
                    }
                }
                if rows.len() == request_limit
                    || saturated_partitions.is_empty()
                    || per_partition_limit >= request_limit
                {
                    for batch in rows.chunks(crate::analytics::DEFAULT_SCAN_BATCH_ROWS) {
                        emit(batch)?;
                    }
                    return Ok(());
                }
                active_partitions = saturated_partitions;
                per_partition_limit = per_partition_limit.saturating_mul(2).min(request_limit);
            }
        }

        // The fallback pages materialize full rows and apply row_matches, so
        // retain every lane that residual filtering can inspect. The bounded
        // fast path above can omit these lanes because LogQuery already
        // verified its exact pushdown filters before projection.
        let post_filter_include_typed_metadata = include_typed_metadata
            || request.trace_id.is_some()
            || request.span_id.is_some()
            || request.series_id.is_some()
            || request.name.is_some()
            || !request.attributes.is_empty()
            || !request.resource_attributes.is_empty()
            || !request.scope_attributes.is_empty()
            || analytics_predicate_needs_structural_fields(&request.predicate);
        let post_filter_include_fields =
            include_fields || !request.labels.is_empty() || !request.metadata.is_empty();
        let mut emitted = 0usize;
        let mut ordered_rows = Vec::new();
        let partitions = if let Some(trace_id) = request.trace_id {
            let router = crate::TelemetryRouter::new(
                NonZeroU16::new(u16::try_from(self.tenant_partitions).map_err(|_| {
                    LokiApiError::internal("tenant partition count exceeds the routing space")
                })?)
                .ok_or_else(|| LokiApiError::internal("tenant partition count is zero"))?,
            );
            vec![router.log(&request.tenant, Some(trace_id), &[])]
        } else {
            self.tenant_partitions(&request.tenant)?
        };
        for partition in partitions {
            let mut next_offset = None;
            loop {
                let page_limit = if request.order.is_some() {
                    8_192
                } else {
                    8_192usize.min(limit.saturating_sub(emitted))
                };
                if page_limit == 0 {
                    return Ok(());
                }
                let mut query = LogQuery::new(partition)
                    .with_limit(page_limit)
                    .with_field(TENANT_FIELD, request.tenant.as_ref());
                query.start_offset = next_offset.map(LogicalOffset::new);
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                query = apply_analytics_log_filters(query, request);
                let matches = if let Some(shard_id) = self.standalone_owner_shard(partition) {
                    self.service
                        .query_partition_projected_on_shard_with_fields(
                            shard_id,
                            &query,
                            post_filter_include_typed_metadata,
                            post_filter_include_fields,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                } else {
                    self.service
                        .query_partitions_projected_with_fields(
                            std::slice::from_ref(&query),
                            post_filter_include_typed_metadata,
                            post_filter_include_fields,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                };
                if matches.is_empty() {
                    break;
                }
                let returned = matches.len();
                let final_offset = matches
                    .last()
                    .expect("non-empty page")
                    .record
                    .record_ref
                    .offset
                    .get();
                let rows = if delete_filter.is_empty() {
                    matches
                        .into_iter()
                        .map(|matched| analytics_row_from_match(&request.tenant, matched))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .filter(|row| crate::analytics::row_matches(row, request))
                        .collect::<Vec<_>>()
                } else {
                    matches
                        .into_iter()
                        .map(|matched| analytics_row_and_entry(&request.tenant, matched))
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .filter_map(|(row, entry)| {
                            (!delete_filter.matches(&entry)
                                && crate::analytics::row_matches(&row, request))
                            .then_some(row)
                        })
                        .collect::<Vec<_>>()
                };
                if let Some(order) = request.order {
                    ordered_rows.extend(rows);
                    ordered_rows.sort_unstable_by(|left: &AnalyticsRow, right| {
                        let order_by_timestamp =
                            (left.timestamp_unix_nanos, left.partition, left.offset).cmp(&(
                                right.timestamp_unix_nanos,
                                right.partition,
                                right.offset,
                            ));
                        if order == AnalyticsScanOrder::TimestampDescending {
                            order_by_timestamp.reverse()
                        } else {
                            order_by_timestamp
                        }
                    });
                    ordered_rows.truncate(limit);
                } else {
                    if !rows.is_empty() {
                        emit(&rows)?;
                    }
                    emitted = emitted.saturating_add(rows.len());
                }
                if (request.order.is_none() && emitted == limit) || returned < page_limit {
                    break;
                }
                let Some(start) = final_offset.checked_add(1) else {
                    break;
                };
                next_offset = Some(start);
            }
        }
        if !ordered_rows.is_empty() {
            emit(&ordered_rows)?;
        }
        Ok(())
    }

    fn scan_analytics_relevance(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        let limit = request
            .limit
            .ok_or_else(|| LokiApiError::bad_request("relevance order requires a limit"))?;
        if limit == 0 {
            return Ok(());
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        let queries = self
            .tenant_partitions(&request.tenant)?
            .into_iter()
            .map(|partition| {
                let mut query =
                    LogQuery::new(partition).with_field(TENANT_FIELD, request.tenant.as_ref());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                apply_analytics_log_filters(query, request)
            })
            .collect::<Vec<_>>();
        let include_typed_metadata =
            crate::analytics::log_columns_need_typed_metadata(&request.columns)
                || !request.attributes.is_empty()
                || !request.resource_attributes.is_empty()
                || !request.scope_attributes.is_empty()
                || request.trace_id.is_some()
                || request.span_id.is_some();
        let include_fields = crate::analytics::log_columns_need_structural_fields(&request.columns)
            || analytics_predicate_needs_structural_fields(&request.predicate)
            || !request.attributes.is_empty()
            || !request.resource_attributes.is_empty()
            || !request.scope_attributes.is_empty()
            || request.series_id.is_some()
            || request.name.is_some();
        let needs_post_filter = !delete_filter.is_empty()
            || !request.attributes.is_empty()
            || !request.resource_attributes.is_empty()
            || !request.scope_attributes.is_empty()
            || request.series_id.is_some()
            || request.name.is_some();
        let message_only = !needs_post_filter
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.columns.iter().all(|column| {
                matches!(
                    column,
                    crate::AnalyticsColumn::Timestamp
                        | crate::AnalyticsColumn::Message
                        | crate::AnalyticsColumn::Score
                )
            })
            && relevance_message_predicate_only(&request.predicate);
        let relevance_scorer = crate::analytics::RelevanceScorer::from_request(request);
        let mut rows = if message_only {
            let mut top =
                BinaryHeap::with_capacity(limit.min(crate::analytics::DEFAULT_SCAN_BATCH_ROWS));
            let matches = self
                .service
                .query_partitions_messages_top_k_unordered(&queries, &relevance_scorer, limit)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            let mut visit = |matched: &crate::stripe::LogMessageMatch,
                             score: f64|
             -> Result<(), LokiApiError> {
                let timestamp_unix_nanos =
                    i64::try_from(matched.timestamp_unix_nanos).map_err(|_| {
                        LokiApiError::internal("timestamp exceeds ClickHouse i64 range")
                    })?;
                let offset = matched.record_ref.offset.get();
                let partition = matched.record_ref.topic_partition.partition_id.get();
                let belongs_in_top = top.len() < limit
                    || top.peek().is_some_and(
                        |Reverse(worst): &Reverse<MessageRelevanceHeapItem>| {
                            score
                                .total_cmp(&worst.score)
                                .then_with(|| timestamp_unix_nanos.cmp(&worst.timestamp_unix_nanos))
                                .then_with(|| offset.cmp(&worst.offset))
                                == CmpOrdering::Greater
                        },
                    );
                if belongs_in_top {
                    let item = MessageRelevanceHeapItem {
                        score,
                        timestamp_unix_nanos,
                        offset,
                        partition,
                        message: matched.message_arc(),
                    };
                    if top.len() == limit {
                        top.pop();
                    }
                    top.push(Reverse(item));
                }
                Ok(())
            };
            crate::stripe::for_each_message_match_score(&matches, &relevance_scorer, &mut visit)?;
            top.into_iter()
                .map(|Reverse(item)| {
                    let mut row = AnalyticsRow::empty(
                        Arc::clone(&request.tenant),
                        "logs",
                        u64::try_from(item.timestamp_unix_nanos)
                            .expect("validated message timestamp is non-negative"),
                        item.partition,
                        item.offset,
                    )?;
                    row.message = Some(item.message);
                    row.score = Some(item.score);
                    Ok(row)
                })
                .collect::<Result<Vec<_>, LokiApiError>>()?
        } else {
            let mut top =
                BinaryHeap::with_capacity(limit.min(crate::analytics::DEFAULT_SCAN_BATCH_ROWS));
            let mut push_row = |mut row: AnalyticsRow| {
                row.score =
                    Some(relevance_scorer.score(row.message.as_deref().unwrap_or_default()));
                let item = RelevanceHeapItem {
                    score: row.score.unwrap_or_default(),
                    timestamp_unix_nanos: row.timestamp_unix_nanos,
                    offset: row.offset,
                    row,
                };
                if top.len() < limit {
                    top.push(Reverse(item));
                } else if top.peek().is_some_and(|Reverse(worst)| item > *worst) {
                    top.pop();
                    top.push(Reverse(item));
                }
            };
            let matches = self
                .service
                .query_partitions_projected_unordered_with_fields(
                    &queries,
                    include_typed_metadata,
                    include_fields,
                )
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            for matched in matches {
                let row = if needs_post_filter {
                    if delete_filter.is_empty() {
                        let row = analytics_row_from_match(&request.tenant, matched)?;
                        if !crate::analytics::row_matches(&row, request) {
                            continue;
                        }
                        row
                    } else {
                        let (row, entry) = analytics_row_and_entry(&request.tenant, matched)?;
                        if delete_filter.matches(&entry)
                            || !crate::analytics::row_matches(&row, request)
                        {
                            continue;
                        }
                        row
                    }
                } else {
                    crate::analytics::projected_log_row(
                        &request.tenant,
                        &matched.record,
                        &request.columns,
                    )?
                };
                push_row(row);
            }
            top.into_iter()
                .map(|Reverse(item)| item.row)
                .collect::<Vec<_>>()
        };
        rows.sort_unstable_by(|left, right| {
            right
                .score
                .unwrap_or_default()
                .partial_cmp(&left.score.unwrap_or_default())
                .unwrap_or(CmpOrdering::Equal)
                .then_with(|| right.timestamp_unix_nanos.cmp(&left.timestamp_unix_nanos))
                .then_with(|| right.offset.cmp(&left.offset))
        });
        rows.truncate(limit);
        for batch in rows.chunks(crate::analytics::DEFAULT_SCAN_BATCH_ROWS) {
            emit(batch)?;
        }
        Ok(())
    }

    fn scan_analytics_distinct_trace_cardinality(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(u64) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        let can_use_projected_ids = delete_filter.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.series_id.is_none()
            && request.name.is_none();
        if !can_use_projected_ids {
            return crate::loki_api::scan_distinct_trace_cardinality_by_rows(self, request, emit);
        }

        let mut outer_request = request.clone();
        let join_service = outer_request.trace_join_service.take();
        outer_request.cardinality_only = false;
        outer_request.distinct_trace_id = false;
        outer_request.columns = vec![crate::AnalyticsColumn::TraceId];

        let queries_for = |scan_request: &AnalyticsScanRequest| {
            self.tenant_partitions(&scan_request.tenant)?
                .into_iter()
                .map(|partition| {
                    let mut query = LogQuery::new(partition)
                        .with_field(TENANT_FIELD, scan_request.tenant.as_ref());
                    query.start_timestamp_unix_nanos =
                        self.retained_query_start(scan_request.start_timestamp_unix_nanos);
                    query.end_timestamp_unix_nanos = scan_request.end_timestamp_unix_nanos;
                    Ok(apply_analytics_log_filters(query, scan_request))
                })
                .collect::<Result<Vec<_>, LokiApiError>>()
        };

        let outer_queries = queries_for(&outer_request)?;
        if let Some(service) = join_service {
            let mut inner_request = outer_request;
            inner_request.predicate = LogPredicate::MatchAll;
            inner_request.predicate_any = false;
            inner_request.terms.clear();
            inner_request.message_tokens.clear();
            inner_request.case_insensitive_message_tokens.clear();
            inner_request.labels = vec![crate::MetadataField::new("service_name", service)];
            let inner_queries = queries_for(&inner_request)?;
            let trace_ids = self
                .service
                .query_partitions_trace_ids_intersection_unordered(&outer_queries, &inner_queries)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            emit(u64::try_from(trace_ids.len()).unwrap_or(u64::MAX))
        } else {
            let trace_ids = self
                .service
                .query_partitions_trace_ids_unordered(&outer_queries)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            let distinct = trace_ids.into_iter().collect::<HashSet<_>>();
            emit(u64::try_from(distinct.len()).unwrap_or(u64::MAX))
        }
    }

    fn scan_analytics_cardinality(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(u64) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        if request.trace_join_service.is_some() || request.distinct_trace_id {
            return self.scan_analytics_distinct_trace_cardinality(request, emit);
        }
        let unfiltered_log_count = request.relation == AnalyticsRelation::Logs
            && request.start_timestamp_unix_nanos.is_none()
            && request.end_timestamp_unix_nanos.is_none()
            && request.terms.is_empty()
            && request.message_tokens.is_empty()
            && request.case_insensitive_message_tokens.is_empty()
            && request.predicate == LogPredicate::MatchAll
            && request.labels.is_empty()
            && request.metadata.is_empty()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.series_id.is_none()
            && request.name.is_none()
            && self.retention.is_none()
            && self.deletes.list(&request.tenant)?.is_empty();
        if unfiltered_log_count {
            let partitions = self.tenant_partitions(&request.tenant)?;
            let mut count = self
                .service
                .count_log_records(Arc::clone(&request.tenant), partitions)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            if let Some(limit) = request.limit {
                count = count.min(u64::try_from(limit).unwrap_or(u64::MAX));
            }
            if count > 0 {
                emit(count)?;
            }
            return Ok(());
        }

        let indexed_filtered_log_count = request.relation == AnalyticsRelation::Logs
            && request.limit.is_none()
            && request.order.is_none()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.series_id.is_none()
            && request.name.is_none()
            && self.retention.is_none()
            && self.deletes.list(&request.tenant)?.is_empty();
        if indexed_filtered_log_count {
            let queries = self
                .tenant_partitions(&request.tenant)?
                .into_iter()
                .map(|partition| {
                    let mut query =
                        LogQuery::new(partition).with_field(TENANT_FIELD, request.tenant.as_ref());
                    query.start_timestamp_unix_nanos = request.start_timestamp_unix_nanos;
                    query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                    apply_analytics_log_filters(query, request)
                })
                .collect::<Vec<_>>();
            let count = self
                .service
                .count_queries(&queries)
                .map_err(|error| LokiApiError::internal(error.to_string()))?;
            if count > 0 {
                emit(count)?;
            }
            return Ok(());
        }

        self.scan_analytics(request, &mut |rows| {
            emit(u64::try_from(rows.len()).unwrap_or(u64::MAX))
        })
    }

    fn scan_analytics_grouped(
        &self,
        request: &AnalyticsScanRequest,
        emit: &mut dyn FnMut(&[AnalyticsGroupRow]) -> Result<(), LokiApiError>,
    ) -> Result<(), LokiApiError> {
        request.validate()?;
        let fast_path = request.relation == AnalyticsRelation::Logs
            && request.limit.is_none()
            && request.order.is_none()
            && request.attributes.is_empty()
            && request.resource_attributes.is_empty()
            && request.scope_attributes.is_empty()
            && request.trace_id.is_none()
            && request.span_id.is_none()
            && request.series_id.is_none()
            && request.name.is_none()
            && self.retention.is_none()
            && self.deletes.list(&request.tenant)?.is_empty();
        if !fast_path {
            return crate::analytics::group_analytics_rows(self, request, emit);
        }
        let partitions = self.tenant_partitions(&request.tenant)?;
        let queries = partitions
            .into_iter()
            .map(|partition| {
                let mut query =
                    LogQuery::new(partition).with_field(TENANT_FIELD, request.tenant.as_ref());
                query.start_timestamp_unix_nanos = request.start_timestamp_unix_nanos;
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                apply_analytics_log_filters(query, request)
            })
            .collect::<Vec<_>>();
        let groups = self
            .service
            .group_queries(&queries, &request.group_by)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let mut grouped = groups
            .into_iter()
            .map(|(keys, count)| AnalyticsGroupRow { keys, count })
            .collect::<Vec<_>>();
        if request.group_order == AnalyticsGroupOrder::CountDescending {
            grouped.sort_unstable_by(|left, right| {
                right
                    .count
                    .cmp(&left.count)
                    .then_with(|| left.keys.cmp(&right.keys))
            });
        }
        if let Some(limit) = request.group_limit {
            grouped.truncate(limit);
        }
        if !grouped.is_empty() {
            emit(&grouped)?;
        }
        Ok(())
    }

    fn health(&self) -> Result<StoreHealth, LokiApiError> {
        let stats = self.engine.durable_sink_stats();
        if stats.dirty_partitions > 0 {
            return Ok(StoreHealth {
                ready: false,
                detail: Arc::from(format!(
                    "{} durable sink partitions require recovery",
                    stats.dirty_partitions
                )),
            });
        }
        let maximum_age = self
            .indexed_ack_timeout
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        if stats.pending_items > 0 && stats.checkpoint_age_ms > maximum_age {
            return Ok(StoreHealth {
                ready: false,
                detail: Arc::from(format!(
                    "oldest pending index checkpoint is {} ms old",
                    stats.checkpoint_age_ms
                )),
            });
        }
        Ok(StoreHealth::default())
    }

    fn flush(&self, timeout: Duration) -> Result<(), LokiApiError> {
        self.engine.sync().map_err(engine_error)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| LokiApiError::internal("flush deadline overflow"))?;
        loop {
            let stats = self.engine.durable_sink_stats();
            if stats.dirty_partitions > 0 {
                return Err(LokiApiError::internal(format!(
                    "flush stopped with {} dirty partitions",
                    stats.dirty_partitions
                )));
            }
            if stats.pending_items == 0 && stats.pending_bytes == 0 {
                self.checkpoint_lifetime_rollups()?;
                self.service
                    .flush_object_tier()
                    .map_err(|error| LokiApiError::internal(error.to_string()))?;
                if self.object_tier_enabled {
                    let reclaimed = self.reclaim_source_packs()?;
                    self.source_reclaimed_offsets
                        .fetch_add(reclaimed, Ordering::Relaxed);
                }
                self.engine.sync().map_err(engine_error)?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(LokiApiError::unavailable(format!(
                    "flush timed out with {} pending items and {} pending bytes",
                    stats.pending_items, stats.pending_bytes
                )));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn operational_metrics(&self) -> StoreMetrics {
        let stats = self.engine.durable_sink_stats();
        StoreMetrics {
            pending_items: stats.pending_items,
            pending_bytes: stats.pending_bytes,
            checkpoint_age_ms: stats.checkpoint_age_ms,
            applied_appends: stats.applied_appends,
            retry_attempts: stats.retry_attempts,
            failed_attempts: stats.failed_attempts,
            dirty_partitions: stats.dirty_partitions,
            retained_payload_bytes: self.service.retained_payload_bytes().ok(),
            retention_runs: self.retention_runs.load(Ordering::Relaxed),
            retention_advanced_offsets: self.retention_advanced_offsets.load(Ordering::Relaxed),
            retention_failures: self.retention_failures.load(Ordering::Relaxed),
            object_store: self.service.object_store_stats(),
            source_reclaimed_offsets: self.source_reclaimed_offsets.load(Ordering::Relaxed),
            retired_object_groups: self.retired_object_groups.load(Ordering::Relaxed),
            retired_object_payload_bytes: self.retired_object_payload_bytes.load(Ordering::Relaxed),
            retired_object_keys: self.retired_object_keys.load(Ordering::Relaxed),
        }
    }

    fn create_delete(
        &self,
        tenant: &str,
        start_time: i64,
        end_time: i64,
        query: String,
        created_at: i64,
    ) -> Result<String, LokiApiError> {
        self.deletes
            .create(tenant, start_time, end_time, query, created_at)
    }

    fn delete_requests(&self, tenant: &str) -> Result<Vec<DeleteRequest>, LokiApiError> {
        self.deletes.list(tenant)
    }

    fn cancel_delete(&self, tenant: &str, request_id: &str) -> Result<bool, LokiApiError> {
        self.deletes.cancel(tenant, request_id)
    }
}
