use super::*;

pub(super) fn update_accumulator(head: &mut SeriesHead, point: &DurableMetricPoint) {
    let MetricKind::Sum {
        temporality,
        monotonic: _,
    } = point.identity.kind
    else {
        return;
    };
    let MetricValue::Sum(value) = point.value else {
        return;
    };
    if temporality == 1 {
        head.cumulative = match (head.cumulative, value) {
            (Some(NumberValue::Integer(left)), NumberValue::Integer(right)) => {
                left.checked_add(right).map(NumberValue::Integer)
            }
            (Some(NumberValue::DoubleBits(left)), NumberValue::DoubleBits(right)) => Some(
                NumberValue::from_f64(f64::from_bits(left) + f64::from_bits(right)),
            ),
            (None, value) => Some(value),
            _ => {
                head.reset_generation = head.reset_generation.saturating_add(1);
                Some(value)
            }
        };
    } else {
        head.cumulative = Some(value);
    }
}

pub(super) fn retain_metric_winner(
    winners: &mut BTreeMap<(SeriesFingerprint, u64), DurableMetricPoint>,
    series: SeriesFingerprint,
    point: DurableMetricPoint,
) {
    let key = (series, point.timestamp_unix_nanos);
    if winners
        .get(&key)
        .is_none_or(|winner| winner.record_ref.offset < point.record_ref.offset)
    {
        winners.insert(key, point);
    }
}

pub(super) fn retain_exact_metric_winner(
    winners: &mut BTreeMap<u64, DurableMetricPoint>,
    point: DurableMetricPoint,
) {
    let key = point.timestamp_unix_nanos;
    if winners
        .get(&key)
        .is_none_or(|winner| winner.record_ref.offset < point.record_ref.offset)
    {
        winners.insert(key, point);
    }
}

pub(super) fn metric_identity_matches(query: &MetricQuery, identity: &MetricIdentity) -> bool {
    identity.tenant == query.tenant
        && query
            .name
            .as_ref()
            .is_none_or(|name| name.as_ref() == identity.name.as_ref())
        && query.exact_labels.iter().all(|(name, value)| {
            prometheus_string_labels(identity)
                .iter()
                .any(|(observed_name, observed_value)| {
                    observed_name.as_ref() == name.as_ref()
                        && observed_value.as_ref() == value.as_ref()
                })
        })
}

pub(super) fn metric_point_time_matches(query: &MetricQuery, point: &DurableMetricPoint) -> bool {
    query
        .start_time_unix_nanos
        .is_none_or(|start| point.timestamp_unix_nanos >= start)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| point.timestamp_unix_nanos <= end)
}

#[inline]
pub(crate) fn metric_exact_series_point_matches(
    query: &MetricQuery,
    point: &DurableMetricPoint,
) -> bool {
    query
        .partition
        .is_none_or(|partition| partition == point.record_ref.topic_partition)
        && metric_point_time_matches(query, point)
}

pub(super) fn timestamps_intersect_chunk(timestamps: &[u64], chunk: &SealedMetricChunk) -> bool {
    timestamps
        .get(timestamps.partition_point(|timestamp| *timestamp < chunk.min_timestamp_unix_nanos))
        .is_some_and(|timestamp| *timestamp <= chunk.max_timestamp_unix_nanos)
}

pub(crate) fn metric_query_matches(query: &MetricQuery, point: &DurableMetricPoint) -> bool {
    query
        .partition
        .is_none_or(|partition| partition == point.record_ref.topic_partition)
        && metric_identity_matches(query, &point.identity)
        && metric_point_time_matches(query, point)
}

pub(super) fn metric_query_cursor_matches(query: &MetricQuery, point: &DurableMetricPoint) -> bool {
    query
        .start_offset
        .is_none_or(|offset| point.record_ref.offset >= offset)
}

/// Returns Prometheus-visible string labels for one canonical series.
#[must_use]
pub fn prometheus_string_labels(identity: &MetricIdentity) -> Vec<(Arc<str>, Arc<str>)> {
    let mut labels = BTreeMap::<Arc<str>, Arc<str>>::new();
    for attribute in identity.resource.attributes.iter() {
        if let Some(crate::TelemetryValue::String(value)) = &attribute.value {
            labels.insert(Arc::clone(&attribute.key), Arc::clone(value));
        }
    }
    for attribute in identity.point_attributes.iter() {
        if let Some(crate::TelemetryValue::String(value)) = &attribute.value {
            labels.insert(Arc::clone(&attribute.key), Arc::clone(value));
        }
    }
    labels.into_iter().collect()
}
