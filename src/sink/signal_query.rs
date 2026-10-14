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
    let mut budget = crate::query_work::QueryWorkBudget::new(stripe.native_query_work);
    for span in stripe.traces.query(&storage_query)? {
        budget.replace(
            None,
            &span,
            std::mem::size_of::<(Arc<str>, TraceId, crate::SpanId)>(),
        )?;
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
    for_each_signal_tier_payload(
        state,
        &partitions,
        TierQueryRange {
            min_timestamp_unix_nanos: query.start_time_unix_nanos,
            // Native trace bounds match span end against the lower bound
            // and span start against the upper bound. A retained crossing
            // span can match even when those bounds do not form an ordinary
            // point-timestamp interval. Decode enforces the original bounds.
            max_timestamp_unix_nanos: query.end_time_unix_nanos.filter(|end| {
                query
                    .start_time_unix_nanos
                    .is_none_or(|start| *end >= start)
            }),
            signal_identity: identity,
            ..TierQueryRange::default()
        },
        None,
        stripe.native_query_work,
        |payload| {
            for span in decode_trace_block_with_work_limits(
                payload,
                Some(&storage_query),
                stripe.native_query_work,
            )? {
                let key = (Arc::clone(&span.tenant), span.trace_id, span.span_id);
                check_owner_winners(stripe, winners.len(), !winners.contains_key(&key))?;
                if winners.get(&key).is_none_or(|existing: &DurableSpan| {
                    existing.record_ref.offset < span.record_ref.offset
                }) {
                    budget.replace(
                        winners.get(&key),
                        &span,
                        std::mem::size_of::<(Arc<str>, TraceId, crate::SpanId)>(),
                    )?;
                    winners.insert(key, span);
                }
            }
            Ok(())
        },
    )?;
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
    let mut budget = crate::query_work::QueryWorkBudget::new(stripe.native_query_work);
    for point in stripe
        .metrics
        .query_exact_timestamps(&storage_query, &query.timestamps)?
    {
        budget.replace(None, &point, std::mem::size_of::<u64>())?;
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
    for_each_signal_tier_payload(
        state,
        &partitions,
        TierQueryRange {
            min_timestamp_unix_nanos: Some(first_timestamp),
            max_timestamp_unix_nanos: Some(last_timestamp),
            signal_identity: Some(query.series.get()),
            ..TierQueryRange::default()
        },
        None,
        stripe.native_query_work,
        |payload| {
            let points = decode_metric_chunk_with_work_limits(payload, stripe.native_query_work)?;
            if points
                .first()
                .is_none_or(|point| point.series_fingerprint() != query.series)
            {
                return Ok(());
            }
            for point in points.into_iter().filter(|point| {
                point.record_ref.topic_partition == query.partition
                    && query
                        .timestamps
                        .binary_search(&point.timestamp_unix_nanos)
                        .is_ok()
            }) {
                check_owner_winners(
                    stripe,
                    winners.len(),
                    !winners.contains_key(&point.timestamp_unix_nanos),
                )?;
                if winners
                    .get(&point.timestamp_unix_nanos)
                    .is_none_or(|existing| existing.record_ref.offset < point.record_ref.offset)
                {
                    budget.replace(
                        winners.get(&point.timestamp_unix_nanos),
                        &point,
                        std::mem::size_of::<u64>(),
                    )?;
                    winners.insert(point.timestamp_unix_nanos, point);
                }
            }
            Ok(())
        },
    )?;
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
    let mut budget = crate::query_work::QueryWorkBudget::new(stripe.native_query_work);
    for point in resident {
        budget.replace(
            None,
            &point,
            std::mem::size_of::<(crate::SeriesFingerprint, u64)>(),
        )?;
        winners.insert(
            (
                query.series.unwrap_or_else(|| point.series_fingerprint()),
                point.timestamp_unix_nanos,
            ),
            point,
        );
    }
    for_each_signal_tier_payload(
        state,
        &partitions,
        TierQueryRange {
            min_timestamp_unix_nanos: query.start_time_unix_nanos,
            max_timestamp_unix_nanos: query.end_time_unix_nanos,
            signal_identity: query.series.map(crate::SeriesFingerprint::get),
            ..TierQueryRange::default()
        },
        None,
        stripe.native_query_work,
        |payload| {
            let points = decode_metric_chunk_with_work_limits(payload, stripe.native_query_work)?;
            let chunk_series = points.first().map(DurableMetricPoint::series_fingerprint);
            if let Some(series) = query.series
                && chunk_series.is_some_and(|chunk_series| chunk_series != series)
            {
                return Ok(());
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
                    check_owner_winners(stripe, winners.len(), !winners.contains_key(&key))?;
                    if winners
                        .get(&key)
                        .is_none_or(|existing: &DurableMetricPoint| {
                            existing.record_ref.offset < point.record_ref.offset
                        })
                    {
                        budget.replace(
                            winners.get(&key),
                            &point,
                            std::mem::size_of::<(crate::SeriesFingerprint, u64)>(),
                        )?;
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
                    check_owner_winners(stripe, winners.len(), !winners.contains_key(&key))?;
                    if winners
                        .get(&key)
                        .is_none_or(|existing: &DurableMetricPoint| {
                            existing.record_ref.offset < point.record_ref.offset
                        })
                    {
                        budget.replace(
                            winners.get(&key),
                            &point,
                            std::mem::size_of::<(crate::SeriesFingerprint, u64)>(),
                        )?;
                        winners.insert(key, point);
                    }
                }
            }
            Ok(())
        },
    )?;
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
    signal_cutoffs: [u64; 3],
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
    stripe
        .correlations
        .query_into_with_retention(query, signal_cutoffs, &mut refs);
    refs.sort_unstable();
    refs.dedup();
    if let Some(after) = query.after {
        refs.retain(|record| *record > after);
    }
    let mut ref_budget = crate::query_work::QueryWorkBudget::new(stripe.native_query_work);
    for record in &refs {
        ref_budget.replace(None, record, 0)?;
    }
    if stripe.correlations.is_disabled()
        && query
            .signal
            .is_none_or(|signal| signal == TelemetrySignal::Logs)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| end >= signal_cutoffs[0])
        && query
            .after
            .is_none_or(|after| after.signal <= TelemetrySignal::Logs)
    {
        query_typed_log_correlations(stripe, query, signal_cutoffs[0], &mut refs, &mut ref_budget)?;
    }
    if query
        .signal
        .is_none_or(|signal| signal == TelemetrySignal::Logs)
        && query
            .end_time_unix_nanos
            .is_none_or(|end| end >= signal_cutoffs[0])
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
                .with_field("resource.loki.tenant", Arc::clone(&query.tenant))
                .where_predicate(LogPredicate::and(label_predicates.clone()))
                .with_limit(query.limit.min(64));
            log_query.start_timestamp_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .unwrap_or(0)
                    .max(signal_cutoffs[0]),
            );
            log_query.end_timestamp_unix_nanos =
                query.end_time_unix_nanos.and_then(|end| end.checked_add(1));
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
            loop {
                let page = stripe.logs.query_refs(&log_query);
                let last = page.last().copied();
                let saturated = page.len() == log_query.limit.unwrap_or(64);
                for record in page {
                    push_correlation_ref(
                        &mut refs,
                        query,
                        record,
                        stripe.native_query_work,
                        &mut ref_budget,
                    )?;
                }
                let Some(next) = last
                    .and_then(|last| last.offset.get().checked_add(1))
                    .filter(|_| saturated)
                else {
                    break;
                };
                log_query.start_offset = Some(shard_stream_core::LogicalOffset::new(next));
            }
        }
    }
    for signal in [TelemetrySignal::Traces, TelemetrySignal::Metrics] {
        if query.signal.is_some_and(|requested| requested != signal)
            || query.after.is_some_and(|after| after.signal > signal)
            || (signal == TelemetrySignal::Metrics
                && query
                    .end_time_unix_nanos
                    .is_some_and(|end| end < signal_cutoffs[2]))
        {
            continue;
        }
        if stripe.correlations.is_disabled() {
            query_resident_native_correlations(
                stripe,
                query,
                signal,
                signal_cutoffs,
                &mut refs,
                &mut ref_budget,
            )?;
        }
        let Some(state) = stripe.signal_tiers.get(&signal) else {
            continue;
        };
        let mut partitions = state.tiers.keys().copied().collect::<Vec<_>>();
        partitions.sort_unstable();
        let retained_start = query.start_time_unix_nanos.unwrap_or(0).max(match signal {
            TelemetrySignal::Traces => signal_cutoffs[1],
            TelemetrySignal::Metrics => signal_cutoffs[2],
            TelemetrySignal::Logs => signal_cutoffs[0],
        });
        // A span can start before the retained window and finish inside it.
        // When the caller's upper start-time bound predates the retention
        // cutoff, omit that coarse bound and enforce it on decoded spans.
        let coarse_end = query
            .end_time_unix_nanos
            .filter(|end| *end >= retained_start);
        for_each_signal_tier_payload(
            state,
            &partitions,
            TierQueryRange {
                min_timestamp_unix_nanos: Some(retained_start),
                max_timestamp_unix_nanos: coarse_end,
                ..TierQueryRange::default()
            },
            Some(query),
            stripe.native_query_work,
            |payload| {
                match signal {
                    TelemetrySignal::Traces => {
                        for span in decode_trace_block_with_work_limits(
                            payload,
                            None,
                            stripe.native_query_work,
                        )?
                        .into_iter()
                        .filter(|span| {
                            correlation_record_is_retained(
                                TelemetrySignal::Traces,
                                span.end_time_unix_nanos().unwrap_or(u64::MAX),
                                signal_cutoffs,
                            ) && correlation_time_matches(query, span.start_time_unix_nanos)
                                && span_matches_correlation(query, span)
                        }) {
                            push_correlation_ref(
                                &mut refs,
                                query,
                                span.record_ref,
                                stripe.native_query_work,
                                &mut ref_budget,
                            )?;
                        }
                    }
                    TelemetrySignal::Metrics => {
                        for point in
                            decode_metric_chunk_with_work_limits(payload, stripe.native_query_work)?
                                .into_iter()
                                .filter(|point| {
                                    correlation_record_is_retained(
                                        TelemetrySignal::Metrics,
                                        point.timestamp_unix_nanos,
                                        signal_cutoffs,
                                    ) && correlation_time_matches(query, point.timestamp_unix_nanos)
                                        && metric_matches_correlation(query, point)
                                })
                        {
                            push_correlation_ref(
                                &mut refs,
                                query,
                                point.record_ref,
                                stripe.native_query_work,
                                &mut ref_budget,
                            )?;
                        }
                    }
                    TelemetrySignal::Logs => unreachable!("loop contains only cold native signals"),
                }
                Ok(())
            },
        )?;
    }
    refs.sort_unstable();
    refs.dedup();
    if let Some(after) = query.after {
        refs.retain(|record| *record > after);
    }
    refs.truncate(query.limit);
    Ok(refs)
}

/// Scans bounded offset pages because typed identities must match before limit.
fn query_typed_log_correlations(
    stripe: &TelemetryStripeState,
    query: &CorrelationQuery,
    cutoff: u64,
    refs: &mut Vec<TelemetryRecordRef>,
    ref_budget: &mut crate::query_work::QueryWorkBudget,
) -> TelemetryResult<()> {
    const RECORDS_PER_PAGE: usize = 64;
    for partition in 0..stripe.log_partitions {
        let partition = TopicPartition::new(
            TelemetrySignal::Logs.topic_id(),
            shard_stream_core::LogicalPartitionId::new(u32::from(partition)),
        );
        if query
            .after
            .is_some_and(|after| after.topic_partition > partition)
        {
            continue;
        }
        let mut log_query = LogQuery::new(partition)
            .with_field("resource.loki.tenant", Arc::clone(&query.tenant))
            .with_limit(RECORDS_PER_PAGE);
        log_query.start_timestamp_unix_nanos =
            Some(query.start_time_unix_nanos.unwrap_or(0).max(cutoff));
        log_query.end_timestamp_unix_nanos =
            query.end_time_unix_nanos.and_then(|end| end.checked_add(1));
        if let Some(after) = query
            .after
            .filter(|after| after.topic_partition == partition)
        {
            let Some(next) = after.offset.get().checked_add(1) else {
                continue;
            };
            log_query.start_offset = Some(shard_stream_core::LogicalOffset::new(next));
        }
        loop {
            let page = stripe.logs.query_checked(&log_query)?;
            let Some(last) = page.last() else {
                break;
            };
            let next = last.record.record_ref.offset.get().checked_add(1);
            let saturated = page.len() == RECORDS_PER_PAGE;
            for matched in page {
                if log_matches_correlation(query, &matched.record) {
                    push_correlation_ref(
                        refs,
                        query,
                        matched.record.record_ref,
                        stripe.native_query_work,
                        ref_budget,
                    )?;
                    if refs.len() == query.limit {
                        return Ok(());
                    }
                }
            }
            let Some(next) = next.filter(|_| saturated) else {
                break;
            };
            log_query.start_offset = Some(shard_stream_core::LogicalOffset::new(next));
        }
    }
    Ok(())
}

/// Native heads provide exact identities when optional navigation is disabled.
fn query_resident_native_correlations(
    stripe: &TelemetryStripeState,
    query: &CorrelationQuery,
    signal: TelemetrySignal,
    cutoffs: [u64; 3],
    refs: &mut Vec<TelemetryRecordRef>,
    ref_budget: &mut crate::query_work::QueryWorkBudget,
) -> TelemetryResult<()> {
    match signal {
        TelemetrySignal::Traces => {
            let native = TraceQuery {
                tenant: Arc::clone(&query.tenant),
                start_time_unix_nanos: Some(
                    query.start_time_unix_nanos.unwrap_or(0).max(cutoffs[1]),
                ),
                end_time_unix_nanos: query.end_time_unix_nanos.and_then(|end| end.checked_add(1)),
                limit: usize::MAX,
                ..TraceQuery::default()
            };
            for span in stripe.traces.query(&native)?.into_iter().filter(|span| {
                correlation_record_is_retained(
                    signal,
                    span.end_time_unix_nanos().unwrap_or(u64::MAX),
                    cutoffs,
                ) && correlation_time_matches(query, span.start_time_unix_nanos)
                    && span_matches_correlation(query, span)
            }) {
                push_correlation_ref(
                    refs,
                    query,
                    span.record_ref,
                    stripe.native_query_work,
                    ref_budget,
                )?;
            }
        }
        TelemetrySignal::Metrics => {
            let native = MetricQuery {
                tenant: Arc::clone(&query.tenant),
                start_time_unix_nanos: Some(
                    query.start_time_unix_nanos.unwrap_or(0).max(cutoffs[2]),
                ),
                end_time_unix_nanos: query.end_time_unix_nanos,
                limit: usize::MAX,
                ..MetricQuery::default()
            };
            for point in stripe.metrics.query(&native)?.into_iter().filter(|point| {
                correlation_record_is_retained(signal, point.timestamp_unix_nanos, cutoffs)
                    && correlation_time_matches(query, point.timestamp_unix_nanos)
                    && metric_matches_correlation(query, point)
            }) {
                push_correlation_ref(
                    refs,
                    query,
                    point.record_ref,
                    stripe.native_query_work,
                    ref_budget,
                )?;
            }
        }
        TelemetrySignal::Logs => unreachable!("resident native loop contains traces and metrics"),
    }
    refs.sort_unstable();
    refs.dedup();
    if let Some(after) = query.after {
        refs.retain(|record| *record > after);
    }
    refs.truncate(query.limit);
    Ok(())
}

fn check_owner_winners(
    stripe: &TelemetryStripeState,
    current: usize,
    new: bool,
) -> TelemetryResult<()> {
    if let Some(work) = stripe.native_query_work {
        work.winners(current.saturating_add(usize::from(new)))?;
    }
    Ok(())
}

/// Keep the smallest exact references even when later groups contain earlier offsets.
fn push_correlation_ref(
    refs: &mut Vec<TelemetryRecordRef>,
    query: &CorrelationQuery,
    record: TelemetryRecordRef,
    work: Option<crate::NativeQueryWorkLimits>,
    budget: &mut crate::query_work::QueryWorkBudget,
) -> TelemetryResult<()> {
    if query.after.is_some_and(|after| record <= after) {
        return Ok(());
    }
    let Err(position) = refs.binary_search(&record) else {
        return Ok(());
    };
    if position >= query.limit {
        return Ok(());
    }
    if refs.len() == query.limit {
        budget.replace(refs.last(), &record, 0)?;
        refs.pop();
    } else if let Some(work) = work {
        work.winners(refs.len().saturating_add(1))?;
        budget.replace(None, &record, 0)?;
    }
    refs.insert(position, record);
    Ok(())
}
