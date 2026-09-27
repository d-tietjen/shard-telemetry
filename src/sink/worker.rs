use super::*;

pub(super) fn run_sink_worker(
    mut stripe: TelemetryStripeState,
    checkpoints: Arc<Mutex<HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    journal: Option<Arc<SinkJournal>>,
    active_log_partition_cache: Arc<Mutex<HashMap<Arc<str>, Vec<TopicPartition>>>>,
    validated_signal_cache: Arc<ValidatedSignalCache>,
    receiver: Receiver<SinkCommand>,
) {
    let mut apply_failure_reported = false;
    while let Ok(command) = receiver.recv() {
        match command {
            SinkCommand::Apply(command) => {
                let result = apply_durable_appends(
                    &mut stripe,
                    &checkpoints,
                    journal.as_deref(),
                    &validated_signal_cache,
                    command.expected,
                    &command.appends,
                    command.next,
                );
                if let Err(error) = &result {
                    if !apply_failure_reported {
                        eprintln!(
                            "shard-telemetry stripe {} durable apply failed and will be retried: {error}",
                            stripe.stream_shard_id
                        );
                    }
                    apply_failure_reported = true;
                } else {
                    apply_failure_reported = false;
                }
                if result.is_ok()
                    && command
                        .appends
                        .iter()
                        .any(|append| append.topic_partition().topic_id == crate::LOGS_TOPIC_ID)
                    && let Ok(mut cache) = active_log_partition_cache.lock()
                {
                    cache.clear();
                }
                let _ = command.response.send(result);
            }
            SinkCommand::Query { queries, response } => {
                let result = stripe.logs.query_partitions_checked(&queries);
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::QueryProjected {
                queries,
                include_typed_metadata,
                include_fields,
                response,
            } => {
                let result = stripe.logs.query_partitions_checked_projected_with_fields(
                    &queries,
                    include_typed_metadata,
                    include_fields,
                );
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::QueryProjectedSingle {
                query,
                include_typed_metadata,
                include_fields,
                response,
            } => {
                let result = stripe.logs.query_partitions_checked_projected_with_fields(
                    std::slice::from_ref(&query),
                    include_typed_metadata,
                    include_fields,
                );
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::QueryProjectedEach {
                queries,
                include_typed_metadata,
                include_fields,
                response,
            } => {
                let indexed_queries = queries
                    .iter()
                    .map(|indexed| indexed.index)
                    .collect::<Vec<_>>();
                let query_refs = queries
                    .iter()
                    .map(|indexed| &indexed.query)
                    .collect::<Vec<_>>();
                let result = stripe
                    .logs
                    .query_partition_refs_checked_projected_each_with_fields(
                        &query_refs,
                        include_typed_metadata,
                        include_fields,
                    )
                    .map(|matches| indexed_queries.into_iter().zip(matches).collect());
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::QueryMessagesTopK {
                queries,
                scorer,
                limit,
                response,
            } => {
                let result = stripe
                    .logs
                    .query_partitions_checked_messages_top_k(&queries, &scorer, limit);
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::QueryTraceIds { queries, response } => {
                let result = stripe.logs.query_partitions_checked_trace_ids(&queries);
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::QueryTraceIdIntersection {
                outer_queries,
                inner_queries,
                response,
            } => {
                let result = (|| {
                    let outer = stripe
                        .logs
                        .query_partitions_checked_trace_ids(&outer_queries)?
                        .into_iter()
                        .collect::<HashSet<_>>();
                    let inner = stripe
                        .logs
                        .query_partitions_checked_trace_ids(&inner_queries)?
                        .into_iter()
                        .collect::<HashSet<_>>();
                    Ok::<Vec<_>, TelemetryError>(
                        outer
                            .into_iter()
                            .filter(|trace_id| inner.contains(trace_id))
                            .collect(),
                    )
                })();
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::CountQueries { queries, response } => {
                let result = stripe.logs.count_query_partitions_checked(&queries);
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::GroupQueries {
                queries,
                keys,
                response,
            } => {
                let result = stripe.logs.group_query_partitions_checked(&queries, &keys);
                let _ = response.send((stripe.stream_shard_id, result));
            }
            SinkCommand::CountLogs {
                tenant,
                partitions,
                response,
            } => {
                let _ = response.send((
                    stripe.stream_shard_id,
                    stripe.logs.count_tenant_records(&tenant, &partitions),
                ));
            }
            SinkCommand::ActiveLogPartitions { tenant, response } => {
                let _ = response.send((
                    stripe.stream_shard_id,
                    stripe.logs.tenant_partitions(&tenant),
                ));
            }
            SinkCommand::QueryTraces { query, response } => {
                let _ = response.send(query_trace_stripe(&stripe, &query));
            }
            SinkCommand::QueryTraceProjected { query, response } => {
                let _ = response.send(query_trace_projected_stripe(&stripe, &query));
            }
            SinkCommand::QueryMetrics { query, response } => {
                let _ = response.send(query_metric_stripe(&stripe, &query));
            }
            SinkCommand::QueryMetricTimestamps { query, response } => {
                let _ = response.send(query_metric_timestamps_stripe(&stripe, &query));
            }
            SinkCommand::Correlate {
                query,
                buffer,
                response,
            } => {
                let _ = response.send(query_correlation_stripe(&stripe, &query, buffer));
            }
            SinkCommand::Flush { response } => {
                let result = flush_object_tiers(&mut stripe, &checkpoints);
                let _ = response.send(result);
            }
            SinkCommand::RetainObjectTier {
                cutoff_timestamp_unix_nanos,
                max_payload_bytes_per_partition,
                response,
            } => {
                let _ = response.send(retain_object_tiers(
                    &mut stripe,
                    cutoff_timestamp_unix_nanos,
                    max_payload_bytes_per_partition,
                ));
            }
            SinkCommand::RetainedPayloadBytes { response } => {
                let bytes = stripe
                    .logs
                    .retained_payload_bytes()
                    .saturating_add(stripe.traces.retained_payload_bytes())
                    .saturating_add(stripe.metrics.retained_payload_bytes());
                let _ = response.send(bytes);
            }
        }
    }
}
