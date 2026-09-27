use super::*;

impl CorrelationBlockFilter {
    #[cfg(test)]
    pub(crate) fn from_query(query: &CorrelationQuery) -> Self {
        let mut filter = Self::default();
        for key in query_keys(query) {
            filter.insert(&query.tenant, key);
        }
        filter
    }

    /// Merges another immutable filter into this one without introducing
    /// false negatives. Used to summarize blocks into catalog groups/pages.
    pub(crate) fn union_assign(&mut self, other: &Self) {
        for (target, source) in self.bits.iter_mut().zip(other.bits) {
            *target |= source;
        }
    }

    /// Builds a filter for one immutable trace block.
    #[must_use]
    pub fn for_spans(spans: &[DurableSpan]) -> Self {
        let mut filter = Self::default();
        let mut last_resource = None;
        let mut last_scope = None;
        let mut last_attributes = None;
        for span in spans {
            let tenant = Arc::clone(&span.tenant);
            let resource_key = (Arc::clone(&tenant), Arc::as_ptr(&span.resource));
            if last_resource.as_ref() != Some(&resource_key) {
                filter.insert(&tenant, CorrelationKey::Resource(span.resource_id()));
                filter.insert_attributes(&tenant, &span.resource.attributes);
                last_resource = Some(resource_key);
            }
            let scope_key = (Arc::clone(&tenant), Arc::as_ptr(&span.scope));
            if last_scope.as_ref() != Some(&scope_key) {
                filter.insert(&tenant, CorrelationKey::Scope(span.scope_id()));
                filter.insert_attributes(&tenant, &span.scope.attributes);
                last_scope = Some(scope_key);
            }
            let attribute_key = (Arc::clone(&tenant), Arc::as_ptr(&span.attributes));
            if last_attributes.as_ref() != Some(&attribute_key) {
                filter.insert_attributes(&tenant, &span.attributes);
                last_attributes = Some(attribute_key);
            }
            for event in span.events.iter() {
                filter.insert_attributes(&tenant, &event.attributes);
            }
            for link in span.links.iter() {
                filter.insert(&tenant, CorrelationKey::Trace(link.trace_id));
                filter.insert_attributes(&tenant, &link.attributes);
            }
        }
        filter
    }

    /// Builds a filter for one immutable metric chunk.
    #[must_use]
    pub fn for_metrics(points: &[DurableMetricPoint]) -> Self {
        let mut filter = Self::default();
        let mut traces = HashSet::new();
        let mut attribute_sets = HashSet::new();
        if let Some(point) = points.first() {
            let tenant = point.identity.tenant.as_ref();
            filter.insert(
                tenant,
                CorrelationKey::Resource(point.identity.resource_id()),
            );
            filter.insert(tenant, CorrelationKey::Scope(point.identity.scope_id()));
            filter.insert_attributes(tenant, &point.identity.resource.attributes);
            filter.insert_attributes(tenant, &point.identity.scope.attributes);
            filter.insert_attributes(tenant, &point.identity.point_attributes);
        }
        for point in points {
            let tenant = Arc::clone(&point.identity.tenant);
            if !point.metadata.is_empty() {
                filter.insert_attribute_set_once(&tenant, &point.metadata, &mut attribute_sets);
            }
            for exemplar in point.exemplars.iter() {
                if let Some(trace_id) = exemplar.trace_id
                    && traces.insert((Arc::clone(&tenant), trace_id))
                {
                    filter.insert(&tenant, CorrelationKey::Trace(trace_id));
                }
                filter.insert_attribute_set_once(
                    &tenant,
                    &exemplar.filtered_attributes,
                    &mut attribute_sets,
                );
            }
        }
        filter
    }

    /// Returns whether this block can satisfy all exact identities in a query.
    #[must_use]
    pub fn may_match(&self, query: &CorrelationQuery) -> bool {
        query_keys(query).all(|key| self.contains(&query.tenant, key))
    }

    /// Tests a trace block whose primary trace IDs are represented exactly by
    /// the catalog range. The Bloom filter only needs linked trace IDs.
    #[must_use]
    pub fn may_match_trace_block(
        &self,
        query: &CorrelationQuery,
        min_trace_id: u128,
        max_trace_id: u128,
    ) -> bool {
        query_keys(query).all(|key| match key {
            CorrelationKey::Trace(trace_id) => {
                let value = u128::from_be_bytes(*trace_id.as_bytes());
                (value >= min_trace_id && value <= max_trace_id)
                    || self.contains(&query.tenant, key)
            }
            _ => self.contains(&query.tenant, key),
        })
    }

    fn insert(&mut self, tenant: &str, key: CorrelationKey) {
        for bit in correlation_filter_bits(tenant, key) {
            self.bits[bit / 64] |= 1_u64 << (bit % 64);
        }
    }

    fn contains(&self, tenant: &str, key: CorrelationKey) -> bool {
        correlation_filter_bits(tenant, key)
            .into_iter()
            .all(|bit| self.bits[bit / 64] & (1_u64 << (bit % 64)) != 0)
    }

    fn insert_attributes(&mut self, tenant: &str, attributes: &[TelemetryAttribute]) {
        for attribute in attributes {
            self.insert(tenant, CorrelationKey::Attribute(attribute.fingerprint()));
        }
    }

    fn insert_attribute_set_once(
        &mut self,
        tenant: &Arc<str>,
        attributes: &Arc<Vec<TelemetryAttribute>>,
        seen: &mut HashSet<(Arc<str>, *const Vec<TelemetryAttribute>)>,
    ) {
        if seen.insert((Arc::clone(tenant), Arc::as_ptr(attributes))) {
            self.insert_attributes(tenant, attributes);
        }
    }
}
