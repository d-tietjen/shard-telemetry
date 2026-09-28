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

    /// Returns a byte-bounded native log page. Candidate lookup retains at
    /// most one small projected batch per partition; Loki entries are copied
    /// only after their size fits the caller's remaining byte budget.
    pub fn query_native_page(&self, request: &NativeLogPageQuery) -> Result<NativeLogQueryPage, LokiApiError> {
        let query_bytes = validate_page_query(request).map_err(|error| LokiApiError::bad_request(error.to_string()))?;
        let cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_page_cursor(&query_bytes, cursor).map_err(|error| LokiApiError::bad_request(error.to_string())))
            .transpose()?;
        let partitions = self.tenant_partitions(&request.query.tenant)?;
        if cursor.is_some_and(|cursor| !partitions.iter().any(|partition| partition.partition_id.get() == cursor.partition)) {
            return Err(LokiApiError::bad_request("native log page cursor partition is invalid"));
        }
        let batch_limit = (request.query.limit as usize).min(PAGE_RECORD_BATCH).min(request.max_bytes as usize);
        let mut queries = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let mut query = LogQuery::new(partition)
                .sort_by_timestamp()
                .with_limit(batch_limit)
                .with_field(TENANT_FIELD, request.query.tenant.as_str());
            query.start_timestamp_unix_nanos = self.retained_query_start(request.query.start_timestamp_unix_nanos);
            query.end_timestamp_unix_nanos = request.query.end_timestamp_unix_nanos;
            if request.query.direction == NativeQueryDirection::NewestFirst {
                query = query.newest_first();
            }
            for (key, value) in &request.query.labels {
                query = query.with_field(format!("{LABEL_PREFIX}{key}"), value.as_str());
            }
            for term in &request.query.terms {
                query = query.with_term(term.as_str());
            }
            if let Some(cursor) = cursor {
                let partition_id = partition.partition_id.get();
                match request.query.direction {
                    NativeQueryDirection::OldestFirst if partition_id < cursor.partition => {
                        let next = cursor.timestamp.saturating_add(1);
                        query.start_timestamp_unix_nanos = Some(query.start_timestamp_unix_nanos.map_or(next, |start| start.max(next)));
                    }
                    NativeQueryDirection::OldestFirst if partition_id > cursor.partition => {
                        query.start_timestamp_unix_nanos =
                            Some(query.start_timestamp_unix_nanos.map_or(cursor.timestamp, |start| start.max(cursor.timestamp)));
                    }
                    NativeQueryDirection::NewestFirst if partition_id > cursor.partition => {
                        query.end_timestamp_unix_nanos =
                            Some(query.end_timestamp_unix_nanos.map_or(cursor.timestamp, |end| end.min(cursor.timestamp)));
                    }
                    NativeQueryDirection::NewestFirst if partition_id < cursor.partition => {
                        let next = cursor.timestamp.saturating_add(1);
                        query.end_timestamp_unix_nanos = Some(query.end_timestamp_unix_nanos.map_or(next, |end| end.min(next)));
                    }
                    _ => {
                        query.after = Some(QueryCursor::new(cursor.timestamp, cursor.offset));
                    }
                }
            }
            queries.push(query);
        }
        let partition_matches = self
            .service
            .query_partitions_projected_each_with_fields(&queries, false, true)
            .map_err(|error| LokiApiError::internal(error.to_string()))?;
        let potentially_more = partition_matches.iter().any(|matches| matches.len() == batch_limit);
        let mut matches = partition_matches.into_iter().flatten().collect::<Vec<_>>();
        matches.sort_unstable_by(|left, right| {
            let left_key = (
                left.record.timestamp_unix_nanos,
                left.record.record_ref.topic_partition.partition_id.get(),
                left.record.record_ref.offset,
            );
            let right_key = (
                right.record.timestamp_unix_nanos,
                right.record.record_ref.topic_partition.partition_id.get(),
                right.record.record_ref.offset,
            );
            match request.query.direction {
                NativeQueryDirection::OldestFirst => left_key.cmp(&right_key),
                NativeQueryDirection::NewestFirst => right_key.cmp(&left_key),
            }
        });
        let delete_filter = LogicalDeleteFilter::compile(&self.deletes.list(&request.query.tenant)?)?;
        let mut entries = Vec::new();
        let mut bytes = 0_usize;
        let mut last_scanned = None;
        let mut examined = 0_usize;
        for matched in matches.iter().take(batch_limit) {
            let record = &matched.record;
            let position = PagePosition {
                timestamp: record.timestamp_unix_nanos,
                partition: record.record_ref.topic_partition.partition_id.get(),
                offset: record.record_ref.offset,
            };
            // Compute the exact projected Loki size without copying the line
            // or field strings. Logical deletes still need one entry at a
            // time for their selector, never an unbounded result vector.
            let entry_bytes = native_log_match_bytes(matched);
            if delete_filter.is_empty() && entry_bytes > request.max_bytes as usize {
                return Err(LokiApiError::bad_request("one native log entry exceeds the page byte limit"));
            }
            if delete_filter.is_empty() && bytes.saturating_add(entry_bytes) > request.max_bytes as usize {
                break;
            }
            let entry = log_match_to_entry(matched.clone())?;
            if !delete_filter.is_empty() && delete_filter.matches(&entry) {
                last_scanned = Some(position);
                examined += 1;
                continue;
            }
            if entry_bytes > request.max_bytes as usize {
                return Err(LokiApiError::bad_request("one native log entry exceeds the page byte limit"));
            }
            if bytes.saturating_add(entry_bytes) > request.max_bytes as usize {
                break;
            }
            bytes += entry_bytes;
            entries.push(entry);
            last_scanned = Some(position);
            examined += 1;
            if entries.len() == request.query.limit as usize {
                break;
            }
        }
        let next_cursor = last_scanned
            .filter(|_| examined < matches.len() || potentially_more)
            .map(|position| encode_page_cursor(&query_bytes, position));
        Ok(NativeLogQueryPage { tenant: request.query.tenant.clone(), entries, next_cursor })
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
