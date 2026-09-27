use super::*;

impl TraceStripe {
    /// Creates a stripe-local trace head with production idle and late windows.
    pub fn new(head_budget_bytes: usize) -> TelemetryResult<Self> {
        if head_budget_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "trace head budget must be nonzero",
            ));
        }
        Ok(Self {
            head_budget_bytes,
            head_bytes: 0,
            idle_nanos: DEFAULT_TRACE_IDLE_NANOS,
            late_grace_nanos: DEFAULT_LATE_TRACE_NANOS,
            traces: HashMap::new(),
            recently_sealed: HashMap::new(),
            recent_order: VecDeque::new(),
            recently_sealed_bytes: 0,
            sealed_blocks: HashMap::new(),
            pending_blocks: Vec::new(),
            directory: TraceDirectory::default(),
            next_block_id: 1,
            analytical_resources: RefCell::new(HashMap::new()),
            analytical_resource_postings: HashMap::new(),
            analytical_winners: HashMap::new(),
            analytical_index_dirty: Cell::new(false),
            analytical_index_bytes: 0,
            analytical_index_budget_bytes: head_budget_bytes / 2,
            analytical_index_complete: true,
            resource_id_cache: std::iter::repeat_with(|| None)
                .take(RESOURCE_ID_CACHE_ENTRIES)
                .collect(),
        })
    }

    /// Applies one durable span with deterministic retry/conflict semantics.
    pub fn apply(
        &mut self,
        span: DurableSpan,
        append_time_nanos: u64,
    ) -> TelemetryResult<TraceApplyOutcome> {
        self.apply_ref(&span, append_time_nanos)
    }

    /// Applies a borrowed durable span, cloning it only when it is accepted
    /// into the stripe head. Transport paths use this to avoid cloning
    /// duplicate or obsolete retries before conflict resolution.
    pub fn apply_ref(
        &mut self,
        span: &DurableSpan,
        append_time_nanos: u64,
    ) -> TelemetryResult<TraceApplyOutcome> {
        if span.record_ref.signal != TelemetrySignal::Traces {
            return Err(TelemetryError::InvalidBlockEncoding(
                "non-trace record applied to trace stripe",
            ));
        }
        let key = (Arc::clone(&span.tenant), span.trace_id);
        let span_id = span.span_id;
        let estimated = span.estimated_head_bytes();
        if estimated > self.head_budget_bytes {
            return Err(TelemetryError::RecordTooLarge);
        }
        self.expire_recent(append_time_nanos);
        let hot_has_span = self
            .traces
            .get(&key)
            .is_some_and(|trace| trace.spans.contains_key(&span.span_id));
        let mut replaces_recent = false;
        if !hot_has_span
            && let Some(recent) = self.recently_sealed.get_mut(&key)
            && let Some(existing) = recent.spans.get(&span.span_id)
        {
            if same_span_payload(existing, span) {
                recent.retries = recent.retries.saturating_add(1);
                return Ok(TraceApplyOutcome::Duplicate);
            }
            recent.conflicts = recent.conflicts.saturating_add(1);
            if existing.record_ref.offset >= span.record_ref.offset {
                return Ok(TraceApplyOutcome::Obsolete);
            }
            let previous = existing.estimated_head_bytes();
            recent.spans.remove(&span.span_id);
            recent.bytes = recent.bytes.saturating_sub(previous);
            self.recently_sealed_bytes = self.recently_sealed_bytes.saturating_sub(previous);
            replaces_recent = true;
        }
        if self.resident_state_bytes().saturating_add(estimated) > self.head_budget_bytes {
            self.seal_idle(append_time_nanos)?;
        }
        self.evict_recent_to_fit(estimated);
        if self.resident_state_bytes().saturating_add(estimated) > self.head_budget_bytes {
            return Err(TelemetryError::InvalidConfig(
                "trace head memory budget exhausted",
            ));
        }
        let first_sealed_nanos = self
            .recently_sealed
            .get(&key)
            .map(|recent| recent.first_sealed_nanos);
        let outcome = {
            let trace = self.traces.entry(key).or_insert_with(|| HotTrace {
                spans: BTreeMap::new(),
                bytes: 0,
                last_append_nanos: append_time_nanos,
                first_sealed_nanos,
                conflicts: 0,
                retries: 0,
            });
            trace.last_append_nanos = append_time_nanos;

            match trace.spans.get(&span_id) {
                Some(existing) if same_span_payload(existing, span) => {
                    trace.retries = trace.retries.saturating_add(1);
                    TraceApplyOutcome::Duplicate
                }
                Some(existing) if existing.record_ref.offset >= span.record_ref.offset => {
                    trace.conflicts = trace.conflicts.saturating_add(1);
                    TraceApplyOutcome::Obsolete
                }
                Some(existing) => {
                    let previous = existing.estimated_head_bytes();
                    trace.conflicts = trace.conflicts.saturating_add(1);
                    trace.bytes = trace
                        .bytes
                        .saturating_sub(previous)
                        .saturating_add(estimated);
                    self.head_bytes = self
                        .head_bytes
                        .saturating_sub(previous)
                        .saturating_add(estimated);
                    trace.spans.insert(span_id, span.clone());
                    TraceApplyOutcome::Replaced
                }
                None => {
                    trace.bytes = trace.bytes.saturating_add(estimated);
                    self.head_bytes = self.head_bytes.saturating_add(estimated);
                    trace.spans.insert(span_id, span.clone());
                    if replaces_recent {
                        TraceApplyOutcome::Replaced
                    } else {
                        TraceApplyOutcome::Inserted
                    }
                }
            }
        };
        if matches!(
            outcome,
            TraceApplyOutcome::Inserted | TraceApplyOutcome::Replaced
        ) {
            self.update_analytical_index(span);
        }
        Ok(outcome)
    }

    pub(super) fn resource_id_for(&mut self, resource: &Arc<ResourceContext>) -> ResourceContextId {
        let pointer = Arc::as_ptr(resource) as usize;
        let slot = pointer.wrapping_mul(0x9e37_79b9_7f4a_7c15) & (RESOURCE_ID_CACHE_ENTRIES - 1);
        if let Some(cached) = &self.resource_id_cache[slot]
            && Arc::ptr_eq(&cached.resource, resource)
        {
            return cached.id;
        }
        let id = resource.id();
        self.resource_id_cache[slot] = Some(CachedResourceIdentity {
            resource: Arc::clone(resource),
            id,
        });
        id
    }

    pub(super) fn update_analytical_index(&mut self, span: &DurableSpan) {
        if !self.analytical_index_complete {
            return;
        }
        let identity = (span.trace_id, span.span_id);
        let resource_id = self.resource_id_for(&span.resource);
        let is_new_winner = self
            .analytical_winners
            .get(&span.tenant)
            .is_none_or(|winners| !winners.contains_key(&identity));
        let entry_bytes = std::mem::size_of::<DurableSpan>()
            .saturating_add(128)
            .saturating_add(usize::from(is_new_winner).saturating_mul(96));
        if self.analytical_index_bytes.saturating_add(entry_bytes)
            > self.analytical_index_budget_bytes
            || self.resident_state_bytes().saturating_add(entry_bytes) > self.head_budget_bytes
        {
            self.analytical_resources.get_mut().clear();
            self.analytical_resource_postings.clear();
            self.analytical_winners.clear();
            self.analytical_index_bytes = 0;
            self.analytical_index_complete = false;
            return;
        }
        self.analytical_winners
            .entry(Arc::clone(&span.tenant))
            .or_default()
            .insert(identity, span.record_ref.offset);
        let resource_key = (Arc::clone(&span.tenant), resource_id);
        let mut inserted_resource = false;
        {
            let resources = self
                .analytical_resources
                .get_mut()
                .entry(resource_key.clone())
                .or_default();
            if let Some(bucket) = resources.iter_mut().find(|bucket| {
                Arc::ptr_eq(&bucket.resource, &span.resource)
                    || bucket.resource.as_ref() == span.resource.as_ref()
            }) {
                bucket.spans.push(span.clone());
            } else {
                resources.push(AnalyticalResourceBucket {
                    resource: Arc::clone(&span.resource),
                    spans: vec![span.clone()],
                });
                inserted_resource = true;
            }
        }
        if inserted_resource {
            for attribute in span.resource.attributes.iter() {
                let Some(value) = render_resource_attribute_value(attribute.value.as_ref()) else {
                    continue;
                };
                self.analytical_resource_postings
                    .entry((Arc::clone(&span.tenant), Arc::clone(&attribute.key), value))
                    .or_default()
                    .push(resource_key.clone());
            }
        }
        self.analytical_index_dirty.set(true);
        self.analytical_index_bytes = self.analytical_index_bytes.saturating_add(entry_bytes);
    }

    pub(super) fn resident_state_bytes(&self) -> usize {
        self.head_bytes
            .saturating_add(self.recently_sealed_bytes)
            .saturating_add(self.analytical_index_bytes)
    }

    pub(super) fn expire_recent(&mut self, now_nanos: u64) {
        while self.recent_order.front().is_some_and(|(sealed_at, _)| {
            now_nanos.saturating_sub(*sealed_at) > self.late_grace_nanos
        }) {
            let (sealed_at, key) = self.recent_order.pop_front().expect("front was checked");
            if self
                .recently_sealed
                .get(&key)
                .is_some_and(|recent| recent.last_sealed_nanos == sealed_at)
                && let Some(recent) = self.recently_sealed.remove(&key)
            {
                self.recently_sealed_bytes =
                    self.recently_sealed_bytes.saturating_sub(recent.bytes);
            }
        }
    }

    pub(super) fn evict_recent_to_fit(&mut self, additional_bytes: usize) {
        while self.resident_state_bytes().saturating_add(additional_bytes) > self.head_budget_bytes
        {
            let Some((sealed_at, key)) = self.recent_order.pop_front() else {
                break;
            };
            if self
                .recently_sealed
                .get(&key)
                .is_some_and(|recent| recent.last_sealed_nanos == sealed_at)
                && let Some(recent) = self.recently_sealed.remove(&key)
            {
                self.recently_sealed_bytes =
                    self.recently_sealed_bytes.saturating_sub(recent.bytes);
            }
        }
    }

    pub(super) fn remember_recent(
        &mut self,
        key: &(Arc<str>, TraceId),
        spans: &[DurableSpan],
        first_sealed_nanos: u64,
        now_nanos: u64,
    ) {
        let recent =
            self.recently_sealed
                .entry(key.clone())
                .or_insert_with(|| RecentlySealedTrace {
                    spans: BTreeMap::new(),
                    bytes: 0,
                    first_sealed_nanos,
                    last_sealed_nanos: now_nanos,
                    conflicts: 0,
                    retries: 0,
                });
        let previous_bytes = recent.bytes;
        recent.first_sealed_nanos = recent.first_sealed_nanos.min(first_sealed_nanos);
        recent.last_sealed_nanos = now_nanos;
        for span in spans {
            let replace = recent
                .spans
                .get(&span.span_id)
                .is_none_or(|existing| existing.record_ref.offset < span.record_ref.offset);
            if replace {
                if let Some(existing) = recent.spans.insert(span.span_id, span.clone()) {
                    recent.bytes = recent.bytes.saturating_sub(existing.estimated_head_bytes());
                }
                recent.bytes = recent.bytes.saturating_add(span.estimated_head_bytes());
            }
        }
        self.recently_sealed_bytes = self
            .recently_sealed_bytes
            .saturating_sub(previous_bytes)
            .saturating_add(recent.bytes);
        self.recent_order.push_back((now_nanos, key.clone()));
        self.evict_recent_to_fit(0);
    }

    /// Seals traces idle for 30 seconds and returns immutable trace blocks.
    pub fn seal_idle(&mut self, now_nanos: u64) -> TelemetryResult<Vec<Vec<u8>>> {
        let ready = self
            .traces
            .iter()
            .filter_map(|(key, trace)| {
                (now_nanos.saturating_sub(trace.last_append_nanos) >= self.idle_nanos)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        self.seal_keys(ready, now_nanos)
    }

    /// Seals every hot fragment belonging to one logical trace partition.
    pub(crate) fn seal_partition(
        &mut self,
        partition: TopicPartition,
        now_nanos: u64,
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        let ready = self
            .traces
            .iter()
            .filter_map(|(key, trace)| {
                trace
                    .spans
                    .values()
                    .next()
                    .is_some_and(|span| span.record_ref.topic_partition == partition)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        self.seal_keys(ready, now_nanos)
    }

    pub(super) fn seal_keys(
        &mut self,
        mut ready: Vec<(Arc<str>, TraceId)>,
        now_nanos: u64,
    ) -> TelemetryResult<Vec<Vec<u8>>> {
        ready.sort_unstable();
        let mut by_partition = BTreeMap::<TopicPartition, Vec<PreparedTrace>>::new();
        for key in ready {
            let trace = self.traces.remove(&key).expect("selected trace exists");
            self.head_bytes = self.head_bytes.saturating_sub(trace.bytes);
            let continued_recent_trace = trace.first_sealed_nanos.is_some();
            let first_sealed_nanos = trace.first_sealed_nanos.unwrap_or(now_nanos);
            let spans = trace.spans.into_values().collect::<Vec<_>>();
            self.remember_recent(&key, &spans, first_sealed_nanos, now_nanos);
            let summary_spans = if continued_recent_trace {
                self.recently_sealed
                    .get(&key)
                    .expect("continued trace was remembered")
                    .spans
                    .values()
                    .cloned()
                    .collect()
            } else {
                spans.clone()
            };
            let partition = spans[0].record_ref.topic_partition;
            by_partition
                .entry(partition)
                .or_default()
                .push(PreparedTrace {
                    spans,
                    summary_spans,
                    replaces_summary: continued_recent_trace,
                    source_bytes: trace.bytes,
                });
        }
        let mut blocks = Vec::new();
        for traces in by_partition.into_values() {
            let mut group = Vec::new();
            let mut group_bytes = 0usize;
            for trace in traces {
                if !group.is_empty()
                    && group_bytes.saturating_add(trace.source_bytes)
                        > TARGET_TRACE_BLOCK_SOURCE_BYTES
                {
                    blocks.push(self.seal_prepared_block(std::mem::take(&mut group))?);
                    group_bytes = 0;
                }
                group_bytes = group_bytes.saturating_add(trace.source_bytes);
                group.push(trace);
            }
            if !group.is_empty() {
                blocks.push(self.seal_prepared_block(group)?);
            }
        }
        Ok(blocks)
    }

    pub(super) fn seal_prepared_block(
        &mut self,
        traces: Vec<PreparedTrace>,
    ) -> TelemetryResult<Vec<u8>> {
        let block_id = self.next_block_id;
        self.next_block_id = self.next_block_id.saturating_add(1);
        let span_count = traces.iter().map(|trace| trace.spans.len()).sum();
        let mut spans = Vec::with_capacity(span_count);
        let mut summaries = Vec::with_capacity(traces.len());
        for trace in traces {
            spans.extend(trace.spans);
            summaries.push((trace.summary_spans, trace.replaces_summary));
        }
        let block = encode_trace_block(&spans)?;
        for (summary_spans, replaces_summary) in summaries {
            let summary = summarize_trace(&summary_spans, block_id)?;
            if replaces_summary {
                self.directory.publish_current(summary);
            } else {
                self.directory.publish(summary);
            }
        }
        let payload = Arc::<[u8]>::from(block.clone());
        let first_offset = spans
            .iter()
            .map(|span| span.record_ref.offset.get())
            .min()
            .expect("sealed trace block is nonempty");
        let last_offset = spans
            .iter()
            .map(|span| span.record_ref.offset.get())
            .max()
            .expect("sealed trace block is nonempty");
        let min_timestamp_unix_nanos = spans
            .iter()
            .map(|span| span.start_time_unix_nanos)
            .min()
            .expect("sealed trace block is nonempty");
        let max_timestamp_unix_nanos = spans
            .iter()
            .map(|span| span.end_time_unix_nanos().unwrap_or(u64::MAX))
            .max()
            .expect("sealed trace block is nonempty");
        let min_signal_identity = spans
            .iter()
            .map(|span| u128::from_be_bytes(*span.trace_id.as_bytes()))
            .min()
            .expect("sealed trace block is nonempty");
        let max_signal_identity = spans
            .iter()
            .map(|span| u128::from_be_bytes(*span.trace_id.as_bytes()))
            .max()
            .expect("sealed trace block is nonempty");
        self.pending_blocks.push(SignalTierPayload {
            resident_id: block_id,
            topic_partition: spans[0].record_ref.topic_partition,
            min_signal_identity,
            max_signal_identity,
            first_offset,
            last_offset,
            record_count: u32::try_from(spans.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            payload: Arc::clone(&payload),
            correlation_filter: CorrelationBlockFilter::for_spans(&spans),
        });
        self.sealed_blocks.insert(block_id, payload);
        Ok(block)
    }

    pub(crate) fn pending_partition(&self, partition: TopicPartition) -> Vec<SignalTierPayload> {
        self.pending_blocks
            .iter()
            .filter(|payload| payload.topic_partition == partition)
            .cloned()
            .collect()
    }

    pub(crate) fn release_published_blocks(&mut self, resident_ids: &[u64]) {
        self.pending_blocks
            .retain(|payload| !resident_ids.contains(&payload.resident_id));
        self.sealed_blocks
            .retain(|block_id, _| !resident_ids.contains(block_id));
    }

    pub(crate) fn retained_payload_bytes(&self) -> u64 {
        self.sealed_blocks
            .values()
            .map(|payload| u64::try_from(payload.len()).unwrap_or(u64::MAX))
            .sum()
    }

    pub(super) fn analytical_resource_candidates(
        &self,
        query: &TraceQuery,
    ) -> Option<Vec<AnalyticalResourceKey>> {
        let mut candidates = None::<HashMap<AnalyticalResourceKey, ()>>;
        for (attribute_key, expected_value) in query.exact_resource_attributes.iter() {
            let postings = self.analytical_resource_postings.get(&(
                Arc::clone(&query.tenant),
                Arc::clone(attribute_key),
                Arc::clone(expected_value),
            ))?;
            let mut next = HashMap::new();
            for resource_key in postings {
                if candidates
                    .as_ref()
                    .is_none_or(|existing| existing.contains_key(resource_key))
                {
                    next.insert(resource_key.clone(), ());
                }
            }
            candidates = Some(next);
        }
        Some(
            candidates
                .unwrap_or_default()
                .into_keys()
                .collect::<Vec<_>>(),
        )
    }

    /// Returns the immutable summary directory.
    #[must_use]
    pub const fn directory(&self) -> &TraceDirectory {
        &self.directory
    }

    /// Queries current hot spans after trace/time pushdown.
    pub fn query(&self, query: &TraceQuery) -> TelemetryResult<Vec<DurableSpan>> {
        let limit = query.limit.max(1);
        if let Some(trace_id) = query.trace_id {
            return self.query_exact_trace(query, trace_id, limit);
        }
        if let Some(spans) = self.query_analytical_resource_index(query, limit) {
            return Ok(spans);
        }
        let mut winners = HashMap::<(TraceId, SpanId), DurableSpan>::new();
        for payload in self.sealed_blocks.values() {
            for span in decode_trace_block_matching(payload, query)? {
                retain_newest_span(&mut winners, span);
            }
        }
        for span in self
            .traces
            .iter()
            .filter(|((tenant, trace_id), _)| {
                tenant.as_ref() == query.tenant.as_ref()
                    && query
                        .trace_id
                        .is_none_or(|requested| requested == *trace_id)
            })
            .flat_map(|(_, trace)| trace.spans.values())
            .filter(|span| trace_query_matches(query, span))
            .cloned()
        {
            retain_newest_span(&mut winners, span);
        }
        let mut spans = winners
            .into_values()
            .filter(|span| trace_query_cursor_matches(query, span))
            .collect::<Vec<_>>();
        if query.partition.is_some() {
            if spans.len() > limit {
                spans.select_nth_unstable_by_key(limit - 1, |span| span.record_ref.offset);
                spans.truncate(limit);
            }
            spans.sort_unstable_by_key(|span| span.record_ref.offset);
        } else {
            if spans.len() > limit {
                spans.select_nth_unstable_by_key(limit - 1, |span| {
                    (
                        span.trace_id,
                        span.start_time_unix_nanos,
                        span.record_ref.offset,
                    )
                });
                spans.truncate(limit);
            }
            spans.sort_unstable_by_key(|span| {
                (
                    span.trace_id,
                    span.start_time_unix_nanos,
                    span.record_ref.offset,
                )
            });
        }
        Ok(spans)
    }

    /// Executes a resource-filtered analytical query without cloning full
    /// span payloads when the caller only needs scalar span columns.
    pub(crate) fn query_projected(
        &self,
        query: &TraceQuery,
    ) -> TelemetryResult<Vec<TraceProjection>> {
        if query.trace_id.is_some()
            || query.partition.is_some()
            || query.exact_resource_attributes.is_empty()
            || !self.analytical_index_complete
        {
            return self
                .query(query)
                .map(|spans| spans.iter().map(TraceProjection::from_span).collect());
        }
        let Some(resource_keys) = self.analytical_resource_candidates(query) else {
            return self
                .query(query)
                .map(|spans| spans.iter().map(TraceProjection::from_span).collect());
        };
        let limit = query.limit.max(1);
        let mut resources = self.analytical_resources.borrow_mut();
        if self.analytical_index_dirty.replace(false) {
            for buckets in resources.values_mut() {
                for bucket in buckets {
                    bucket.spans.sort_unstable_by_key(|span| {
                        (
                            span.trace_id,
                            span.start_time_unix_nanos,
                            span.span_id,
                            span.record_ref.offset,
                        )
                    });
                }
            }
        }
        let mut spans = Vec::with_capacity(limit.min(1_024));
        let span_filter_is_match_all = trace_projected_span_filter_is_match_all(query);
        for resource_key in resource_keys {
            let Some(buckets) = resources.get(&resource_key) else {
                continue;
            };
            let tenant = &resource_key.0;
            let Some(winners) = self.analytical_winners.get(tenant) else {
                continue;
            };
            for bucket in buckets {
                let remaining = limit.saturating_sub(spans.len());
                if remaining == 0 {
                    break;
                }
                if !rendered_attributes_match(
                    &bucket.resource.attributes,
                    &query.exact_resource_attributes,
                ) {
                    continue;
                }
                spans.extend(
                    bucket
                        .spans
                        .iter()
                        .filter(|span| {
                            winners.get(&(span.trace_id, span.span_id))
                                == Some(&span.record_ref.offset)
                                && (span_filter_is_match_all
                                    || (trace_query_matches_without_tenant_resource(query, span)
                                        && trace_query_cursor_matches(query, span)))
                        })
                        .take(remaining)
                        .map(TraceProjection::from_span),
                );
            }
        }
        if spans.len() > limit {
            spans.select_nth_unstable_by_key(limit - 1, |span| {
                (
                    span.trace_id,
                    span.start_time_unix_nanos,
                    span.record_ref.offset,
                )
            });
            spans.truncate(limit);
        }
        spans.sort_unstable_by_key(|span| {
            (
                span.trace_id,
                span.start_time_unix_nanos,
                span.record_ref.offset,
            )
        });
        Ok(spans)
    }

    pub(super) fn query_analytical_resource_index(
        &self,
        query: &TraceQuery,
        limit: usize,
    ) -> Option<Vec<DurableSpan>> {
        if !self.analytical_index_complete
            || query.partition.is_some()
            || query.exact_resource_attributes.is_empty()
        {
            return None;
        }
        let resource_keys = self.analytical_resource_candidates(query)?;
        let mut resources = self.analytical_resources.borrow_mut();
        if self.analytical_index_dirty.replace(false) {
            for buckets in resources.values_mut() {
                for bucket in buckets {
                    bucket.spans.sort_unstable_by_key(|span| {
                        (
                            span.trace_id,
                            span.start_time_unix_nanos,
                            span.span_id,
                            span.record_ref.offset,
                        )
                    });
                }
            }
        }
        let mut spans = Vec::with_capacity(limit.min(1_024));
        let span_filter_is_match_all = trace_projected_span_filter_is_match_all(query);
        for resource_key in resource_keys {
            let Some(buckets) = resources.get(&resource_key) else {
                continue;
            };
            let tenant = &resource_key.0;
            let Some(winners) = self.analytical_winners.get(tenant) else {
                continue;
            };
            for bucket in buckets {
                let remaining = limit.saturating_sub(spans.len());
                if remaining == 0 {
                    break;
                }
                if !rendered_attributes_match(
                    &bucket.resource.attributes,
                    &query.exact_resource_attributes,
                ) {
                    continue;
                }
                spans.extend(
                    bucket
                        .spans
                        .iter()
                        .filter(|span| {
                            winners.get(&(span.trace_id, span.span_id))
                                == Some(&span.record_ref.offset)
                                && (span_filter_is_match_all
                                    || (trace_query_matches_without_tenant_resource(query, span)
                                        && trace_query_cursor_matches(query, span)))
                        })
                        .take(remaining)
                        .cloned(),
                );
            }
        }
        if spans.len() > limit {
            spans.select_nth_unstable_by_key(limit - 1, |span| {
                (
                    span.trace_id,
                    span.start_time_unix_nanos,
                    span.record_ref.offset,
                )
            });
            spans.truncate(limit);
        }
        spans.sort_unstable_by_key(|span| {
            (
                span.trace_id,
                span.start_time_unix_nanos,
                span.record_ref.offset,
            )
        });
        Some(spans)
    }

    pub(super) fn query_exact_trace(
        &self,
        query: &TraceQuery,
        trace_id: TraceId,
        limit: usize,
    ) -> TelemetryResult<Vec<DurableSpan>> {
        let key = (Arc::clone(&query.tenant), trace_id);
        let has_visible_sealed_fragment = self.directory.entries.get(&key).is_some_and(|summary| {
            summary
                .block_fragments
                .iter()
                .any(|block_id| self.sealed_blocks.contains_key(block_id))
        });
        if !has_visible_sealed_fragment {
            let Some(trace) = self.traces.get(&key) else {
                return Ok(Vec::new());
            };
            let mut spans = trace
                .spans
                .values()
                .filter(|span| trace_query_matches(query, span))
                .filter(|span| trace_query_cursor_matches(query, span))
                .cloned()
                .collect::<Vec<_>>();
            if query.partition.is_some() {
                spans.sort_unstable_by_key(|span| span.record_ref.offset);
            } else {
                spans.sort_unstable_by_key(|span| {
                    (span.start_time_unix_nanos, span.record_ref.offset)
                });
            }
            spans.truncate(limit);
            return Ok(spans);
        }
        let mut winners = BTreeMap::<SpanId, DurableSpan>::new();
        if let Some(summary) = self.directory.entries.get(&key) {
            for block_id in summary.block_fragments.iter() {
                let Some(payload) = self.sealed_blocks.get(block_id) else {
                    continue;
                };
                for span in decode_trace_block_matching(payload, query)? {
                    retain_exact_trace_span(&mut winners, span);
                }
            }
        }
        if let Some(trace) = self.traces.get(&key) {
            for span in trace
                .spans
                .values()
                .filter(|span| trace_query_matches(query, span))
            {
                retain_exact_trace_span(&mut winners, span.clone());
            }
        }
        let mut spans = winners
            .into_values()
            .filter(|span| trace_query_cursor_matches(query, span))
            .collect::<Vec<_>>();
        if query.partition.is_some() {
            spans.sort_unstable_by_key(|span| span.record_ref.offset);
        } else {
            spans.sort_unstable_by_key(|span| (span.start_time_unix_nanos, span.record_ref.offset));
        }
        spans.truncate(limit);
        Ok(spans)
    }

    /// Returns current mutable and late-fragment state bytes.
    #[must_use]
    pub fn head_bytes(&self) -> usize {
        self.resident_state_bytes()
    }

    /// Returns the configured late-fragment compaction window.
    #[must_use]
    pub const fn late_grace_nanos(&self) -> u64 {
        self.late_grace_nanos
    }
}
