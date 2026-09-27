use super::*;

pub(super) fn query_trace_stripe(
    stripe: &TelemetryStripeState,
    query: &TraceQuery,
) -> TelemetryResult<Vec<DurableSpan>> {
    let Some(state) = stripe.signal_tiers.get(&TelemetrySignal::Traces) else {
        return stripe.traces.query(query);
    };
    let mut storage_query = query.clone();
    storage_query.start_offset = None;
    let mut winners = BTreeMap::new();
    for span in stripe.traces.query(&storage_query)? {
        winners.insert(
            (Arc::clone(&span.tenant), span.trace_id, span.span_id),
            span,
        );
    }
    let partitions = if let Some(partition) = query.partition {
        vec![partition]
    } else {
        query.trace_id.map_or_else(
            || state.tiers.keys().copied().collect::<Vec<_>>(),
            |trace_id| vec![stripe.router.trace(&query.tenant, trace_id)],
        )
    };
    let identity = query
        .trace_id
        .map(|trace_id| u128::from_be_bytes(*trace_id.as_bytes()));
    for payload in read_signal_tier_payloads(
        state,
        &partitions,
        TierQueryRange {
            min_timestamp_unix_nanos: query.start_time_unix_nanos,
            max_timestamp_unix_nanos: query.end_time_unix_nanos,
            signal_identity: identity,
            ..TierQueryRange::default()
        },
        None,
    )? {
        for span in decode_trace_block_matching(payload.as_ref(), &storage_query)? {
            let key = (Arc::clone(&span.tenant), span.trace_id, span.span_id);
            if winners.get(&key).is_none_or(|existing: &DurableSpan| {
                existing.record_ref.offset < span.record_ref.offset
            }) {
                winners.insert(key, span);
            }
        }
    }
    let mut spans = winners
        .into_values()
        .filter(|span| {
            query
                .start_offset
                .is_none_or(|offset| span.record_ref.offset >= offset)
        })
        .collect::<Vec<_>>();
    if query.partition.is_some() {
        spans.sort_unstable_by_key(|span| span.record_ref.offset);
    } else {
        spans.sort_unstable_by_key(|span| {
            (
                span.trace_id,
                span.start_time_unix_nanos,
                span.record_ref.offset,
            )
        });
    }
    spans.truncate(query.limit.max(1));
    Ok(spans)
}

pub(super) fn query_trace_projected_stripe(
    stripe: &TelemetryStripeState,
    query: &TraceQuery,
) -> TelemetryResult<Vec<TraceProjection>> {
    if stripe.signal_tiers.contains_key(&TelemetrySignal::Traces) {
        return query_trace_stripe(stripe, query)
            .map(|spans| spans.iter().map(TraceProjection::from_span).collect());
    }
    stripe.traces.query_projected(query)
}

pub(super) fn query_metric_timestamps_stripe(
    stripe: &TelemetryStripeState,
    query: &MetricTimestampQuery,
) -> TelemetryResult<Vec<DurableMetricPoint>> {
    let Some(first_timestamp) = query.timestamps.first().copied() else {
        return Ok(Vec::new());
    };
    let last_timestamp = query
        .timestamps
        .last()
        .copied()
        .expect("nonempty timestamp query has a last timestamp");
    let storage_query = MetricQuery {
        tenant: Arc::clone(&query.tenant),
        partition: Some(query.partition),
        series: Some(query.series),
        start_time_unix_nanos: Some(first_timestamp),
        end_time_unix_nanos: Some(last_timestamp),
        limit: usize::MAX,
        ..MetricQuery::default()
    };
    let mut winners = BTreeMap::<u64, DurableMetricPoint>::new();
    for point in stripe
        .metrics
        .query_exact_timestamps(&storage_query, &query.timestamps)?
    {
        winners.insert(point.timestamp_unix_nanos, point);
    }

    let Some(state) = stripe.signal_tiers.get(&TelemetrySignal::Metrics) else {
        return Ok(winners.into_values().collect());
    };
    let partitions = [query.partition];
    if partitions.iter().all(|partition| {
        state
            .tiers
            .get(partition)
            .is_none_or(|tier| tier.root().pages.is_empty())
    }) {
        return Ok(winners.into_values().collect());
    }
    for payload in read_signal_tier_payloads(
        state,
        &partitions,
        TierQueryRange {
            min_timestamp_unix_nanos: Some(first_timestamp),
            max_timestamp_unix_nanos: Some(last_timestamp),
            signal_identity: Some(query.series.get()),
            ..TierQueryRange::default()
        },
        None,
    )? {
        let points = decode_metric_chunk(payload.as_ref())?;
        if points
            .first()
            .is_none_or(|point| point.series_fingerprint() != query.series)
        {
            continue;
        }
        for point in points.into_iter().filter(|point| {
            point.record_ref.topic_partition == query.partition
                && query
                    .timestamps
                    .binary_search(&point.timestamp_unix_nanos)
                    .is_ok()
        }) {
            if winners
                .get(&point.timestamp_unix_nanos)
                .is_none_or(|existing| existing.record_ref.offset < point.record_ref.offset)
            {
                winners.insert(point.timestamp_unix_nanos, point);
            }
        }
    }
    Ok(winners.into_values().collect())
}

pub(super) fn query_metric_stripe(
    stripe: &TelemetryStripeState,
    query: &MetricQuery,
) -> TelemetryResult<Vec<DurableMetricPoint>> {
    let mut storage_query = query.clone();
    storage_query.start_offset = None;
    let resident = stripe.metrics.query(&storage_query)?;
    let Some(state) = stripe.signal_tiers.get(&TelemetrySignal::Metrics) else {
        if query.start_offset.is_none() {
            return Ok(resident);
        }
        return Ok(finalize_metric_query(resident, query));
    };
    let partitions = if let Some(partition) = query.partition {
        vec![partition]
    } else {
        query.series.map_or_else(
            || state.tiers.keys().copied().collect::<Vec<_>>(),
            |series| vec![stripe.router.metric(&query.tenant, series)],
        )
    };
    if partitions.iter().all(|partition| {
        state
            .tiers
            .get(partition)
            .is_none_or(|tier| tier.root().pages.is_empty())
    }) {
        if query.start_offset.is_none() {
            return Ok(resident);
        }
        return Ok(finalize_metric_query(resident, query));
    }
    let mut winners = BTreeMap::new();
    for point in resident {
        winners.insert(
            (
                query.series.unwrap_or_else(|| point.series_fingerprint()),
                point.timestamp_unix_nanos,
            ),
            point,
        );
    }
    for payload in read_signal_tier_payloads(
        state,
        &partitions,
        TierQueryRange {
            min_timestamp_unix_nanos: query.start_time_unix_nanos,
            max_timestamp_unix_nanos: query.end_time_unix_nanos,
            signal_identity: query.series.map(crate::SeriesFingerprint::get),
            ..TierQueryRange::default()
        },
        None,
    )? {
        let points = decode_metric_chunk(payload.as_ref())?;
        let chunk_series = points.first().map(DurableMetricPoint::series_fingerprint);
        if let Some(series) = query.series
            && chunk_series.is_some_and(|chunk_series| chunk_series != series)
        {
            continue;
        }
        if let Some(series) = query.series {
            // A metric tier payload is encoded from one series. The first
            // point check above verifies that invariant before the hot loop,
            // so avoid recomputing the full identity and label predicate for
            // every decoded point.
            for point in points
                .into_iter()
                .filter(|point| metric_exact_series_point_matches(&storage_query, point))
            {
                let key = (series, point.timestamp_unix_nanos);
                if winners
                    .get(&key)
                    .is_none_or(|existing: &DurableMetricPoint| {
                        existing.record_ref.offset < point.record_ref.offset
                    })
                {
                    winners.insert(key, point);
                }
            }
        } else {
            for point in points
                .into_iter()
                .filter(|point| metric_query_matches(&storage_query, point))
            {
                let key = (
                    chunk_series.expect("metric tier payload is nonempty"),
                    point.timestamp_unix_nanos,
                );
                if winners
                    .get(&key)
                    .is_none_or(|existing: &DurableMetricPoint| {
                        existing.record_ref.offset < point.record_ref.offset
                    })
                {
                    winners.insert(key, point);
                }
            }
        }
    }
    Ok(finalize_metric_query(
        winners.into_values().collect(),
        query,
    ))
}

pub(super) fn finalize_metric_query(
    mut points: Vec<DurableMetricPoint>,
    query: &MetricQuery,
) -> Vec<DurableMetricPoint> {
    points.retain(|point| {
        query
            .start_offset
            .is_none_or(|offset| point.record_ref.offset >= offset)
    });
    if query.partition.is_some() {
        points.sort_unstable_by_key(|point| point.record_ref.offset);
    } else {
        points.sort_unstable_by_key(|point| (point.timestamp_unix_nanos, point.record_ref.offset));
    }
    points.truncate(query.limit.max(1));
    points
}

pub(super) fn query_correlation_stripe(
    stripe: &TelemetryStripeState,
    query: &CorrelationQuery,
    mut refs: Vec<TelemetryRecordRef>,
) -> TelemetryResult<Vec<TelemetryRecordRef>> {
    refs.clear();
    if query.limit == 0
        || (query.trace_id.is_none()
            && query.resource_id.is_none()
            && query.scope_id.is_none()
            && query.attributes.is_empty())
    {
        return Ok(refs);
    }
    stripe.correlations.query_into(query, &mut refs);
    if query
        .signal
        .is_none_or(|signal| signal == TelemetrySignal::Logs)
        && query
            .after
            .is_none_or(|after| after.signal <= TelemetrySignal::Logs)
        && query.attributes.len() == query.labels.len()
        && (query.trace_id.is_some()
            || query.resource_id.is_some()
            || query.scope_id.is_some()
            || !query.labels.is_empty())
    {
        let partitions = if let Some(trace_id) = query.trace_id {
            vec![stripe.router.log(&query.tenant, Some(trace_id), &[])]
        } else {
            (0..stripe.log_partitions)
                .map(|partition| {
                    TopicPartition::new(
                        TelemetrySignal::Logs.topic_id(),
                        shard_stream_core::LogicalPartitionId::new(u32::from(partition)),
                    )
                })
                .collect()
        };
        let label_predicates = query
            .labels
            .iter()
            .map(|(key, value)| {
                LogPredicate::or(
                    [
                        key.to_string(),
                        format!("resource.{key}"),
                        format!("scope.{key}"),
                        format!("attr.{key}"),
                        format!("resource.loki.label.{key}"),
                        format!("attr.loki.metadata.{key}"),
                    ]
                    .into_iter()
                    .map(|field| LogPredicate::field_equals(field, Arc::clone(value)))
                    .collect(),
                )
            })
            .collect::<Vec<_>>();
        for partition in partitions {
            if query.after.is_some_and(|after| {
                after.signal == TelemetrySignal::Logs && partition < after.topic_partition
            }) {
                continue;
            }
            let mut log_query = LogQuery::new(partition)
                .where_predicate(LogPredicate::and(label_predicates.clone()))
                .with_limit(query.limit);
            log_query.start_timestamp_unix_nanos = query.start_time_unix_nanos;
            log_query.end_timestamp_unix_nanos = query.end_time_unix_nanos;
            if let Some(after) = query.after.filter(|after| {
                after.signal == TelemetrySignal::Logs && after.topic_partition == partition
            }) {
                let Some(start_offset) = after.offset.get().checked_add(1) else {
                    continue;
                };
                log_query.start_offset = Some(shard_stream_core::LogicalOffset::new(start_offset));
            }
            if let Some(trace_id) = query.trace_id {
                log_query = log_query.with_field("otel.trace_id", trace_id.to_string());
            }
            if let Some(resource_id) = query.resource_id {
                log_query = log_query.with_field("otel.resource.id", resource_id.to_string());
            }
            if let Some(scope_id) = query.scope_id {
                log_query = log_query.with_field("otel.scope.id", scope_id.to_string());
            }
            refs.extend(stripe.logs.query_refs(&log_query));
        }
    }
    for signal in [TelemetrySignal::Traces, TelemetrySignal::Metrics] {
        if query.signal.is_some_and(|requested| requested != signal)
            || query.after.is_some_and(|after| after.signal > signal)
        {
            continue;
        }
        let Some(state) = stripe.signal_tiers.get(&signal) else {
            continue;
        };
        let mut partitions = state.tiers.keys().copied().collect::<Vec<_>>();
        partitions.sort_unstable();
        for payload in read_signal_tier_payloads(
            state,
            &partitions,
            TierQueryRange {
                min_timestamp_unix_nanos: query.start_time_unix_nanos,
                max_timestamp_unix_nanos: query.end_time_unix_nanos,
                ..TierQueryRange::default()
            },
            Some(query),
        )? {
            match signal {
                TelemetrySignal::Traces => refs.extend(
                    decode_trace_block(payload.as_ref())?
                        .into_iter()
                        .filter(|span| {
                            correlation_time_matches(query, span.start_time_unix_nanos)
                                && span_matches_correlation(query, span)
                        })
                        .map(|span| span.record_ref),
                ),
                TelemetrySignal::Metrics => refs.extend(
                    decode_metric_chunk(payload.as_ref())?
                        .into_iter()
                        .filter(|point| {
                            correlation_time_matches(query, point.timestamp_unix_nanos)
                                && metric_matches_correlation(query, point)
                        })
                        .map(|point| point.record_ref),
                ),
                TelemetrySignal::Logs => unreachable!("loop contains only cold native signals"),
            }
            refs.sort_unstable();
            refs.dedup();
            if let Some(after) = query.after {
                refs.retain(|record| *record > after);
            }
            refs.truncate(query.limit);
        }
    }
    refs.sort_unstable();
    refs.dedup();
    if let Some(after) = query.after {
        refs.retain(|record| *record > after);
    }
    refs.truncate(query.limit);
    Ok(refs)
}
