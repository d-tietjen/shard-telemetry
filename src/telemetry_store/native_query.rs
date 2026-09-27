use super::*;

impl DurableTelemetryStore {
    /// Executes a native exact-label/token query directly against the bounded
    /// stripe indexes and merges tenant partitions by timestamp.
    pub fn query_native(&self, request: &NativeQuery) -> Result<Vec<LokiEntry>, LokiApiError> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        if !delete_filter.is_empty() {
            return self.query_native_with_deletes(request, &delete_filter);
        }
        self.query_native_indexed_matches(request)?
            .into_iter()
            .map(log_match_to_entry)
            .collect()
    }

    /// Returns projected native-query matches when the query needs no
    /// post-filtering. The native server can encode these shared records
    /// directly, avoiding per-result Loki label and metadata maps.
    pub(crate) fn query_native_projected(
        &self,
        request: &NativeQuery,
    ) -> Result<Option<Vec<LogMatch>>, LokiApiError> {
        if request.limit == 0 {
            return Ok(Some(Vec::new()));
        }
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.tenant)?)?;
        if !delete_filter.is_empty() {
            return Ok(None);
        }
        self.query_native_indexed_matches(request).map(Some)
    }

    pub(super) fn query_native_indexed_matches(
        &self,
        request: &NativeQuery,
    ) -> Result<Vec<LogMatch>, LokiApiError> {
        let queries = self
            .tenant_partitions(&request.tenant)?
            .into_iter()
            .map(|partition| {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(request.limit as usize)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                if request.direction == NativeQueryDirection::NewestFirst {
                    query = query.newest_first();
                }
                for (key, value) in &request.labels {
                    query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
                }
                for term in &request.terms {
                    query = query.with_term(term.as_str());
                }
                query
            })
            .collect::<Vec<_>>();
        // Every logical partition has one deterministic stripe owner. Route
        // each query directly to that owner instead of broadcasting the full
        // tenant fan-out to every stripe and making each worker discard the
        // partitions it does not own.
        let matches = self
            .service
            .query_partitions_projected_with_fields(&queries, false, true)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        Ok(matches)
    }

    pub(super) fn query_native_with_deletes(
        &self,
        request: &NativeQuery,
        delete_filter: &LogicalDeleteFilter,
    ) -> Result<Vec<LokiEntry>, LokiApiError> {
        let result_limit = request.limit as usize;
        let page_limit = result_limit.clamp(1_024, 8_192);
        let mut accepted = Vec::<(LokiEntry, u64)>::new();
        for partition in self.tenant_partitions(&request.tenant)? {
            let mut after = None;
            let mut accepted_from_partition = 0usize;
            loop {
                let mut query = LogQuery::new(partition)
                    .sort_by_timestamp()
                    .with_limit(page_limit)
                    .with_field(TENANT_FIELD, request.tenant.as_str());
                query.start_timestamp_unix_nanos =
                    self.retained_query_start(request.start_timestamp_unix_nanos);
                query.end_timestamp_unix_nanos = request.end_timestamp_unix_nanos;
                query.after = after;
                if request.direction == NativeQueryDirection::NewestFirst {
                    query = query.newest_first();
                }
                for (key, value) in &request.labels {
                    query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
                }
                for term in &request.terms {
                    query = query.with_term(term.as_str());
                }
                let matches = if let Some(shard_id) = self.standalone_owner_shard(partition) {
                    self.service
                        .query_partition_projected_on_shard(shard_id, &query, false)
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                } else {
                    self.service
                        .query_partitions_projected_each_with_fields(
                            std::slice::from_ref(&query),
                            false,
                            true,
                        )
                        .map_err(|error| LokiApiError::internal(error.to_string()))?
                        .into_iter()
                        .flatten()
                        .collect()
                };
                if matches.is_empty() {
                    break;
                }
                let returned = matches.len();
                let last = matches.last().expect("non-empty query page");
                after = Some(QueryCursor::new(
                    last.record.timestamp_unix_nanos,
                    last.record.record_ref.offset,
                ));
                for matched in matches {
                    let offset = matched.record.record_ref.offset.get();
                    let entry = log_match_to_entry(matched)?;
                    if !delete_filter.matches(&entry) {
                        accepted.push((entry, offset));
                        accepted_from_partition += 1;
                        if accepted_from_partition == result_limit {
                            break;
                        }
                    }
                }
                if accepted_from_partition == result_limit || returned < page_limit {
                    break;
                }
            }
        }
        accepted.sort_unstable_by(|(left, left_offset), (right, right_offset)| {
            let ordering = left
                .timestamp_unix_nanos
                .cmp(&right.timestamp_unix_nanos)
                .then_with(|| left_offset.cmp(right_offset));
            match request.direction {
                NativeQueryDirection::OldestFirst => ordering,
                NativeQueryDirection::NewestFirst => ordering.reverse(),
            }
        });
        accepted.truncate(result_limit);
        Ok(accepted.into_iter().map(|(entry, _)| entry).collect())
    }
}
