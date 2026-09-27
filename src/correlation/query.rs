use super::*;

pub(super) fn query_keys(query: &CorrelationQuery) -> impl Iterator<Item = CorrelationKey> + '_ {
    query
        .trace_id
        .map(CorrelationKey::Trace)
        .into_iter()
        .chain(query.resource_id.map(CorrelationKey::Resource))
        .chain(query.scope_id.map(CorrelationKey::Scope))
        .chain(
            query
                .attributes
                .iter()
                .copied()
                .map(CorrelationKey::Attribute),
        )
}

pub(super) fn correlation_posting_matches(
    query: &CorrelationQuery,
    posting: &CorrelationPosting,
) -> bool {
    query
        .signal
        .is_none_or(|signal| posting.record_ref.signal == signal)
        && query
            .start_time_unix_nanos
            .is_none_or(|start| posting.timestamp_unix_nanos >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| posting.timestamp_unix_nanos <= end)
}

pub(super) fn query_single_posting(
    query: &CorrelationQuery,
    postings: &[CorrelationPosting],
    selected: &mut Vec<TelemetryRecordRef>,
) {
    let start = query.after.map_or(0, |after| {
        postings.partition_point(|posting| posting.record_ref <= after)
    });
    selected.reserve(query.limit.min(postings.len().saturating_sub(start)));
    if query.signal.is_none()
        && query.start_time_unix_nanos.is_none()
        && query.end_time_unix_nanos.is_none()
    {
        selected.extend(
            postings[start..]
                .iter()
                .take(query.limit)
                .map(|posting| posting.record_ref),
        );
        return;
    }
    for posting in &postings[start..] {
        if correlation_posting_matches(query, posting) {
            selected.push(posting.record_ref);
            if selected.len() == query.limit {
                break;
            }
        }
    }
}

pub(super) fn query_two_postings(
    query: &CorrelationQuery,
    left: &[CorrelationPosting],
    right: &[CorrelationPosting],
    selected: &mut Vec<TelemetryRecordRef>,
) {
    let (first, incoming) = if left.len() <= right.len() {
        (left, right)
    } else {
        (right, left)
    };
    let start = query.after.map_or(0, |after| {
        first.partition_point(|posting| posting.record_ref <= after)
    });
    let mut incoming_cursor = query.after.map_or(0, |after| {
        incoming.partition_point(|posting| posting.record_ref <= after)
    });
    selected.reserve(query.limit.min(first.len().saturating_sub(start)));
    if query.signal.is_none()
        && query.start_time_unix_nanos.is_none()
        && query.end_time_unix_nanos.is_none()
    {
        for posting in &first[start..] {
            let record = posting.record_ref;
            while incoming
                .get(incoming_cursor)
                .is_some_and(|current| current.record_ref < record)
            {
                incoming_cursor += 1;
            }
            if incoming
                .get(incoming_cursor)
                .is_none_or(|current| current.record_ref != record)
            {
                continue;
            }
            selected.push(record);
            if selected.len() == query.limit {
                break;
            }
        }
        return;
    }
    for posting in &first[start..] {
        if !correlation_posting_matches(query, posting) {
            continue;
        }
        let record = posting.record_ref;
        while incoming
            .get(incoming_cursor)
            .is_some_and(|current| current.record_ref < record)
        {
            incoming_cursor += 1;
        }
        if incoming
            .get(incoming_cursor)
            .is_none_or(|current| current.record_ref != record)
        {
            continue;
        }
        selected.push(record);
        if selected.len() == query.limit {
            break;
        }
    }
}

pub(super) fn correlation_filter_bits(tenant: &str, key: CorrelationKey) -> [usize; 4] {
    let (tag, value) = match key {
        CorrelationKey::Trace(value) => (0_u64, u128::from_be_bytes(*value.as_bytes())),
        CorrelationKey::Resource(value) => (1, value.get()),
        CorrelationKey::Scope(value) => (2, value.get()),
        CorrelationKey::Attribute(value) => (3, value.get()),
    };
    let tenant_hash = tenant
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
        });
    let low = value as u64;
    let high = (value >> 64) as u64;
    let first = mix_filter_hash(low ^ tenant_hash ^ tag.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let stride = mix_filter_hash(
        high ^ tenant_hash.rotate_left(29) ^ tag.wrapping_mul(0xd6e8_feb8_6659_fd93),
    ) | 1;
    let bit_count = CORRELATION_FILTER_WORDS * 64;
    let mut bits = [0; CORRELATION_FILTER_HASHES];
    for (index, bit) in bits.iter_mut().enumerate() {
        *bit = first.wrapping_add((index as u64).wrapping_mul(stride)) as usize % bit_count;
    }
    bits
}

const fn mix_filter_hash(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub(crate) fn span_matches_correlation(query: &CorrelationQuery, span: &DurableSpan) -> bool {
    if span.tenant != query.tenant {
        return false;
    }
    let mut observed = Vec::new();
    visit_span_keys(span, |_, key| observed.push(key));
    query_keys(query).all(|key| observed.contains(&key))
}

pub(crate) fn metric_matches_correlation(
    query: &CorrelationQuery,
    point: &DurableMetricPoint,
) -> bool {
    if point.identity.tenant != query.tenant {
        return false;
    }
    let mut observed = Vec::new();
    visit_metric_keys(point, |_, key| observed.push(key));
    query_keys(query).all(|key| observed.contains(&key))
}

pub(super) fn visit_span_keys(span: &DurableSpan, mut visit: impl FnMut(&str, CorrelationKey)) {
    let tenant = span.tenant.as_ref();
    visit(tenant, CorrelationKey::Trace(span.trace_id));
    visit(tenant, CorrelationKey::Resource(span.resource_id()));
    visit(tenant, CorrelationKey::Scope(span.scope_id()));
    visit_attributes(tenant, span.resource.attributes.iter(), &mut visit);
    visit_attributes(tenant, span.scope.attributes.iter(), &mut visit);
    visit_attributes(tenant, span.attributes.iter(), &mut visit);
    for event in span.events.iter() {
        visit_attributes(tenant, event.attributes.iter(), &mut visit);
    }
    for link in span.links.iter() {
        visit(tenant, CorrelationKey::Trace(link.trace_id));
        visit_attributes(tenant, link.attributes.iter(), &mut visit);
    }
}

pub(super) fn visit_metric_keys(
    point: &DurableMetricPoint,
    mut visit: impl FnMut(&str, CorrelationKey),
) {
    let tenant = point.identity.tenant.as_ref();
    visit(
        tenant,
        CorrelationKey::Resource(point.identity.resource_id()),
    );
    visit(tenant, CorrelationKey::Scope(point.identity.scope_id()));
    visit_attributes(
        tenant,
        point.identity.resource.attributes.iter(),
        &mut visit,
    );
    visit_attributes(tenant, point.identity.scope.attributes.iter(), &mut visit);
    visit_attributes(tenant, point.identity.point_attributes.iter(), &mut visit);
    visit_attributes(tenant, point.metadata.iter(), &mut visit);
    for exemplar in point.exemplars.iter() {
        if let Some(trace_id) = exemplar.trace_id {
            visit(tenant, CorrelationKey::Trace(trace_id));
        }
        visit_attributes(tenant, exemplar.filtered_attributes.iter(), &mut visit);
    }
}

pub(super) fn visit_attributes<'a>(
    tenant: &str,
    attributes: impl Iterator<Item = &'a TelemetryAttribute>,
    visit: &mut impl FnMut(&str, CorrelationKey),
) {
    for attribute in attributes {
        visit(tenant, CorrelationKey::Attribute(attribute.fingerprint()));
    }
}
