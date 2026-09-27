use super::*;

impl CorrelationIndex {
    /// Creates a bounded stripe-local index.
    #[must_use]
    pub fn new(config: CorrelationConfig) -> Self {
        Self {
            config,
            tenants: HashMap::new(),
            postings: HashMap::with_capacity(config.max_keys.min(4_096)),
            resource_pointer_ids: std::iter::repeat_with(|| None)
                .take(CONTEXT_ID_CACHE_ENTRIES)
                .collect(),
            resource_ids: std::iter::repeat_with(|| None)
                .take(CONTEXT_ID_CACHE_ENTRIES)
                .collect(),
            scope_pointer_ids: std::iter::repeat_with(|| None)
                .take(CONTEXT_ID_CACHE_ENTRIES)
                .collect(),
            scope_ids: std::iter::repeat_with(|| None)
                .take(CONTEXT_ID_CACHE_ENTRIES)
                .collect(),
            attribute_pointer_ids: std::iter::repeat_with(|| None)
                .take(ATTRIBUTE_ID_CACHE_ENTRIES)
                .collect(),
            attribute_ids: std::iter::repeat_with(|| None)
                .take(ATTRIBUTE_ID_CACHE_ENTRIES)
                .collect(),
            refs: 0,
            dropped_postings: 0,
        }
    }

    /// Indexes one durable log without retaining its body or metadata values.
    pub fn index_log(&mut self, tenant: &str, log: &DurableLog) {
        let Some(tenant_id) = self.tenant_id(tenant) else {
            return;
        };
        if let Some(trace_id) = log.trace_id {
            self.insert(
                tenant_id,
                CorrelationKey::Trace(trace_id),
                log.record_ref,
                log.timestamp_unix_nanos,
            );
        }
        self.index_contexts(
            tenant_id,
            log.record_ref,
            log.timestamp_unix_nanos,
            &log.resource,
            &log.scope,
        );
        self.index_attribute_set(
            tenant_id,
            log.record_ref,
            log.timestamp_unix_nanos,
            &log.attributes,
        );
    }

    /// Indexes one durable span, including event and link metadata.
    pub fn index_span(&mut self, span: &DurableSpan) {
        let tenant = span.tenant.as_ref();
        let Some(tenant_id) = self.tenant_id(tenant) else {
            return;
        };
        self.insert(
            tenant_id,
            CorrelationKey::Trace(span.trace_id),
            span.record_ref,
            span.start_time_unix_nanos,
        );
        for link in span.links.iter() {
            self.insert(
                tenant_id,
                CorrelationKey::Trace(link.trace_id),
                span.record_ref,
                span.start_time_unix_nanos,
            );
            self.index_attribute_set(
                tenant_id,
                span.record_ref,
                span.start_time_unix_nanos,
                &link.attributes,
            );
        }
        self.index_contexts(
            tenant_id,
            span.record_ref,
            span.start_time_unix_nanos,
            &span.resource,
            &span.scope,
        );
        self.index_attribute_set(
            tenant_id,
            span.record_ref,
            span.start_time_unix_nanos,
            &span.attributes,
        );
        for event in span.events.iter() {
            self.index_attribute_set(
                tenant_id,
                span.record_ref,
                span.start_time_unix_nanos,
                &event.attributes,
            );
        }
    }

    /// Indexes one metric point and connects exemplar trace IDs directly.
    pub fn index_metric(&mut self, point: &DurableMetricPoint) {
        let tenant = point.identity.tenant.as_ref();
        let Some(tenant_id) = self.tenant_id(tenant) else {
            return;
        };
        for exemplar in point.exemplars.iter() {
            if let Some(trace_id) = exemplar.trace_id {
                self.insert(
                    tenant_id,
                    CorrelationKey::Trace(trace_id),
                    point.record_ref,
                    point.timestamp_unix_nanos,
                );
            }
            self.index_attribute_set(
                tenant_id,
                point.record_ref,
                point.timestamp_unix_nanos,
                &exemplar.filtered_attributes,
            );
        }
        self.index_contexts(
            tenant_id,
            point.record_ref,
            point.timestamp_unix_nanos,
            &point.identity.resource,
            &point.identity.scope,
        );
        self.index_attribute_set(
            tenant_id,
            point.record_ref,
            point.timestamp_unix_nanos,
            &point.identity.point_attributes,
        );
        self.index_attribute_set(
            tenant_id,
            point.record_ref,
            point.timestamp_unix_nanos,
            &point.metadata,
        );
    }

    /// Returns the deterministic intersection of every requested posting.
    #[must_use]
    pub fn query(&self, query: &CorrelationQuery) -> Vec<TelemetryRecordRef> {
        let mut selected = Vec::new();
        self.query_into(query, &mut selected);
        selected
    }

    /// Fills a reusable output buffer with the deterministic posting
    /// intersection. The buffer is cleared before results are written.
    pub fn query_into(&self, query: &CorrelationQuery, selected: &mut Vec<TelemetryRecordRef>) {
        selected.clear();
        if query.limit == 0 {
            return;
        }
        let Some(tenant_id) = self.tenants.get(query.tenant.as_ref()).copied() else {
            return;
        };
        let mut key_iter = query_keys(query);
        let Some(first_key) = key_iter.next() else {
            return;
        };
        let first = self
            .postings
            .get(&(tenant_id, first_key))
            .map(Vec::as_slice)
            .unwrap_or_default();
        let Some(second_key) = key_iter.next() else {
            query_single_posting(query, first, selected);
            return;
        };
        let second = self
            .postings
            .get(&(tenant_id, second_key))
            .map(Vec::as_slice)
            .unwrap_or_default();
        if key_iter.next().is_none() {
            query_two_postings(query, first, second, selected);
            return;
        }

        let mut keys = Vec::with_capacity(3 + query.attributes.len());
        keys.push(first_key);
        keys.push(second_key);
        keys.extend(key_iter);
        let mut lists = keys
            .into_iter()
            .map(|key| {
                self.postings
                    .get(&(tenant_id, key))
                    .map(Vec::as_slice)
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        lists.sort_unstable_by_key(|list| list.len());
        let Some(first) = lists.first() else {
            return;
        };
        let start = query.after.map_or(0, |after| {
            first.partition_point(|posting| posting.record_ref <= after)
        });
        selected.reserve(query.limit.min(first.len().saturating_sub(start)));
        let mut cursors = lists[1..]
            .iter()
            .map(|incoming| {
                query.after.map_or(0, |after| {
                    incoming.partition_point(|posting| posting.record_ref <= after)
                })
            })
            .collect::<Vec<_>>();
        'candidate: for posting in &first[start..] {
            let record = posting.record_ref;
            if !correlation_posting_matches(query, posting) {
                continue;
            }
            for (incoming, cursor) in lists[1..].iter().zip(&mut cursors) {
                while incoming
                    .get(*cursor)
                    .is_some_and(|current| current.record_ref < record)
                {
                    *cursor += 1;
                }
                let Some(current) = incoming.get(*cursor) else {
                    return;
                };
                if current.record_ref != record {
                    continue 'candidate;
                }
            }
            selected.push(record);
            if selected.len() == query.limit {
                break;
            }
        }
    }

    /// Drops expired bounded navigation postings without touching durable data.
    pub fn retain_since_timestamp(&mut self, cutoff_timestamp_unix_nanos: u64) {
        let mut retained = 0usize;
        self.postings.retain(|_, postings| {
            postings.retain(|posting| posting.timestamp_unix_nanos >= cutoff_timestamp_unix_nanos);
            retained = retained.saturating_add(postings.len());
            !postings.is_empty()
        });
        self.refs = retained;
    }

    /// Returns current bounds and drop diagnostics.
    #[must_use]
    pub fn stats(&self) -> CorrelationStats {
        CorrelationStats {
            keys: self.postings.len(),
            refs: self.refs,
            dropped_postings: self.dropped_postings,
        }
    }

    fn index_contexts(
        &mut self,
        tenant_id: u32,
        record_ref: TelemetryRecordRef,
        timestamp_unix_nanos: u64,
        resource: &Arc<ResourceContext>,
        scope: &Arc<ScopeContext>,
    ) {
        let resource_id = self.resource_id(resource);
        let scope_id = self.scope_id(scope);
        self.insert(
            tenant_id,
            CorrelationKey::Resource(resource_id),
            record_ref,
            timestamp_unix_nanos,
        );
        self.insert(
            tenant_id,
            CorrelationKey::Scope(scope_id),
            record_ref,
            timestamp_unix_nanos,
        );
        self.index_attribute_set(
            tenant_id,
            record_ref,
            timestamp_unix_nanos,
            &resource.attributes,
        );
        self.index_attribute_set(
            tenant_id,
            record_ref,
            timestamp_unix_nanos,
            &scope.attributes,
        );
    }

    fn index_attribute_set(
        &mut self,
        tenant_id: u32,
        record_ref: TelemetryRecordRef,
        timestamp_unix_nanos: u64,
        attributes: &Arc<Vec<TelemetryAttribute>>,
    ) {
        for id in self.attribute_ids(attributes).iter().copied() {
            self.insert(
                tenant_id,
                CorrelationKey::Attribute(id),
                record_ref,
                timestamp_unix_nanos,
            );
        }
    }

    fn resource_id(&mut self, context: &Arc<ResourceContext>) -> ResourceContextId {
        let pointer_slot = pointer_cache_slot(Arc::as_ptr(context), CONTEXT_ID_CACHE_ENTRIES);
        if let Some(cached) = &self.resource_pointer_ids[pointer_slot]
            && Arc::ptr_eq(&cached.context, context)
        {
            return cached.id;
        }
        let hash = identity_hash(context.as_ref());
        let slot = hash as usize & (CONTEXT_ID_CACHE_ENTRIES - 1);
        if let Some(cached) = &self.resource_ids[slot]
            && cached.hash == hash
            && (Arc::ptr_eq(&cached.context, context)
                || cached.context.as_ref() == context.as_ref())
        {
            self.resource_pointer_ids[pointer_slot] = Some(CachedResourceId {
                hash,
                context: Arc::clone(context),
                id: cached.id,
            });
            return cached.id;
        }
        let id = context.id();
        self.resource_pointer_ids[pointer_slot] = Some(CachedResourceId {
            hash,
            context: Arc::clone(context),
            id,
        });
        self.resource_ids[slot] = Some(CachedResourceId {
            hash,
            context: Arc::clone(context),
            id,
        });
        id
    }

    fn scope_id(&mut self, context: &Arc<ScopeContext>) -> ScopeContextId {
        let pointer_slot = pointer_cache_slot(Arc::as_ptr(context), CONTEXT_ID_CACHE_ENTRIES);
        if let Some(cached) = &self.scope_pointer_ids[pointer_slot]
            && Arc::ptr_eq(&cached.context, context)
        {
            return cached.id;
        }
        let hash = identity_hash(context.as_ref());
        let slot = hash as usize & (CONTEXT_ID_CACHE_ENTRIES - 1);
        if let Some(cached) = &self.scope_ids[slot]
            && cached.hash == hash
            && (Arc::ptr_eq(&cached.context, context)
                || cached.context.as_ref() == context.as_ref())
        {
            self.scope_pointer_ids[pointer_slot] = Some(CachedScopeId {
                hash,
                context: Arc::clone(context),
                id: cached.id,
            });
            return cached.id;
        }
        let id = context.id();
        self.scope_pointer_ids[pointer_slot] = Some(CachedScopeId {
            hash,
            context: Arc::clone(context),
            id,
        });
        self.scope_ids[slot] = Some(CachedScopeId {
            hash,
            context: Arc::clone(context),
            id,
        });
        id
    }

    fn attribute_ids(
        &mut self,
        attributes: &Arc<Vec<TelemetryAttribute>>,
    ) -> Arc<[AttributeFingerprint]> {
        let pointer_slot = pointer_cache_slot(Arc::as_ptr(attributes), ATTRIBUTE_ID_CACHE_ENTRIES);
        if let Some(cached) = &self.attribute_pointer_ids[pointer_slot]
            && Arc::ptr_eq(&cached.attributes, attributes)
        {
            return Arc::clone(&cached.ids);
        }
        let hash = identity_hash(attributes.as_ref());
        let slot = hash as usize & (ATTRIBUTE_ID_CACHE_ENTRIES - 1);
        if let Some(cached) = &self.attribute_ids[slot]
            && cached.hash == hash
            && (Arc::ptr_eq(&cached.attributes, attributes)
                || cached.attributes.as_ref() == attributes.as_ref())
        {
            self.attribute_pointer_ids[pointer_slot] = Some(CachedAttributeIds {
                hash,
                attributes: Arc::clone(attributes),
                ids: Arc::clone(&cached.ids),
            });
            return Arc::clone(&cached.ids);
        }
        let ids = attributes
            .iter()
            .map(TelemetryAttribute::fingerprint)
            .collect::<Arc<[_]>>();
        self.attribute_pointer_ids[pointer_slot] = Some(CachedAttributeIds {
            hash,
            attributes: Arc::clone(attributes),
            ids: Arc::clone(&ids),
        });
        self.attribute_ids[slot] = Some(CachedAttributeIds {
            hash,
            attributes: Arc::clone(attributes),
            ids: Arc::clone(&ids),
        });
        ids
    }

    pub(super) fn tenant_id(&mut self, tenant: &str) -> Option<u32> {
        if let Some(tenant_id) = self.tenants.get(tenant).copied() {
            return Some(tenant_id);
        }
        if self.postings.len() >= self.config.max_keys {
            self.dropped_postings = self.dropped_postings.saturating_add(1);
            return None;
        }
        let tenant_id = u32::try_from(self.tenants.len()).ok()?;
        self.tenants.insert(Arc::from(tenant), tenant_id);
        Some(tenant_id)
    }

    pub(super) fn insert(
        &mut self,
        tenant_id: u32,
        key: CorrelationKey,
        record_ref: TelemetryRecordRef,
        timestamp_unix_nanos: u64,
    ) {
        let lookup = (tenant_id, key);
        let refs = if self.postings.len() < self.config.max_keys {
            self.postings.entry(lookup).or_default()
        } else if let Some(refs) = self.postings.get_mut(&lookup) {
            refs
        } else {
            self.dropped_postings = self.dropped_postings.saturating_add(1);
            return;
        };
        if refs
            .last()
            .is_some_and(|posting| posting.record_ref == record_ref)
        {
            return;
        }
        if refs.len() >= self.config.max_refs_per_key || self.refs >= self.config.max_total_refs {
            self.dropped_postings = self.dropped_postings.saturating_add(1);
            return;
        }
        let posting = CorrelationPosting {
            record_ref,
            timestamp_unix_nanos,
        };
        if refs.last().is_none_or(|last| last.record_ref < record_ref) {
            refs.push(posting);
        } else {
            match refs.binary_search_by_key(&record_ref, |posting| posting.record_ref) {
                Ok(_) => return,
                Err(position) => refs.insert(position, posting),
            }
        }
        self.refs += 1;
    }
}

#[inline]
pub(super) fn pointer_cache_slot<T>(pointer: *const T, entries: usize) -> usize {
    let address = pointer as usize;
    (address ^ (address >> 12) ^ (address >> 24)) & (entries - 1)
}

pub(super) fn identity_hash(value: &impl Hash) -> u64 {
    foldhash::fast::FixedState::with_seed(0x5348_4152_4443_4f52).hash_one(value)
}
