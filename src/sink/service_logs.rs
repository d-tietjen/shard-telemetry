use super::*;

impl TelemetryService {
    pub(crate) fn query_partitions(&self, queries: &[LogQuery]) -> TelemetryResult<Vec<LogMatch>> {
        let Some(ordering_query) = queries.first() else {
            return Ok(Vec::new());
        };
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let shared_queries: Arc<[LogQuery]> = Arc::from(queries.to_vec());

        let (response, receiver) = sync_channel(worker_count);
        for (shard_id, sender) in workers {
            sender
                .send(SinkCommand::Query {
                    queries: Arc::clone(&shared_queries),
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a query"
                    ))
                })?;
        }

        let mut matches =
            Vec::with_capacity(fanout_result_capacity(ordering_query.limit, worker_count));
        for _ in 0..worker_count {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while executing a query".to_string(),
                )
            })?;
            let worker_matches = worker_result?;
            matches.reserve(worker_matches.len());
            matches.extend(worker_matches);
        }
        sort_and_limit(&mut matches, ordering_query.limit, |left, right| {
            ordering_query
                .compare(&left.record, &right.record)
                .then_with(|| {
                    left.record
                        .stream_shard_id
                        .cmp(&right.record.stream_shard_id)
                })
        });
        Ok(matches)
    }

    pub(crate) fn query_partitions_projected_with_fields(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let Some(ordering_query) = queries.first() else {
            return Ok(Vec::new());
        };
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let mut queries_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        for query in queries {
            let owner = usize::try_from(query.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            queries_by_worker[owner].push(query.clone());
        }
        let (response, receiver) = sync_channel(worker_count);
        let mut dispatched_workers = 0usize;
        for ((shard_id, sender), worker_queries) in workers.into_iter().zip(queries_by_worker) {
            if worker_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            let worker_queries: Arc<[LogQuery]> = Arc::from(worker_queries);
            sender
                .send(SinkCommand::QueryProjected {
                    queries: worker_queries,
                    include_typed_metadata,
                    include_fields,
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a projected query"
                    ))
                })?;
        }

        let mut matches = Vec::with_capacity(fanout_result_capacity(
            ordering_query.limit,
            dispatched_workers,
        ));
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while executing a projected query".to_string(),
                )
            })?;
            let worker_matches = worker_result?;
            matches.reserve(worker_matches.len());
            matches.extend(worker_matches);
        }
        sort_and_limit(&mut matches, ordering_query.limit, |left, right| {
            ordering_query
                .compare(&left.record, &right.record)
                .then_with(|| {
                    left.record
                        .stream_shard_id
                        .cmp(&right.record.stream_shard_id)
                })
        });
        Ok(matches)
    }

    pub(crate) fn query_partitions_projected_unordered_with_fields(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        Ok(self
            .query_partitions_projected_each_with_fields(
                queries,
                include_typed_metadata,
                include_fields,
            )?
            .into_iter()
            .flatten()
            .collect())
    }

    pub(crate) fn query_partitions_projected_each_with_fields(
        &self,
        queries: &[LogQuery],
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<Vec<LogMatch>>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let (response, receiver) = sync_channel(worker_count);
        let mut queries_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<IndexedProjectedQuery>>>();
        for (index, query) in queries.iter().enumerate() {
            let owner = usize::try_from(query.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            queries_by_worker[owner].push(IndexedProjectedQuery {
                index,
                query: query.clone(),
            });
        }
        let mut dispatched_workers = 0usize;
        for ((shard_id, sender), worker_queries) in workers.into_iter().zip(queries_by_worker) {
            if worker_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            let worker_queries: Arc<[IndexedProjectedQuery]> = Arc::from(worker_queries);
            sender
                .send(SinkCommand::QueryProjectedEach {
                    queries: worker_queries,
                    include_typed_metadata,
                    include_fields,
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting an independent projected query"
                    ))
                })?;
        }
        let mut matches = queries.iter().map(|_| Vec::new()).collect::<Vec<_>>();
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while executing an independent projected query".to_string(),
                )
            })?;
            let worker_matches = worker_result?;
            for (index, worker) in worker_matches {
                let combined = matches.get_mut(index).ok_or_else(|| {
                    TelemetryError::QueryWorkerUnavailable(
                        "query worker returned an invalid independent result index".into(),
                    )
                })?;
                combined.extend(worker);
            }
        }
        Ok(matches)
    }

    pub(crate) fn query_partitions_messages_top_k_unordered(
        &self,
        queries: &[LogQuery],
        scorer: &RelevanceScorer,
        limit: usize,
    ) -> TelemetryResult<Vec<LogMessageMatch>> {
        if queries.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let mut queries_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        for query in queries {
            let owner = usize::try_from(query.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            queries_by_worker[owner].push(query.clone());
        }
        let (response, receiver) = sync_channel(worker_count);
        let mut dispatched_workers = 0usize;
        for ((shard_id, sender), worker_queries) in workers.into_iter().zip(queries_by_worker) {
            if worker_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            sender
                .send(SinkCommand::QueryMessagesTopK {
                    queries: Arc::from(worker_queries),
                    scorer: scorer.clone(),
                    limit,
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a top-k message query"
                    ))
                })?;
        }
        let capacity = fanout_result_capacity(Some(limit), dispatched_workers);
        let mut matches = Vec::with_capacity(capacity);
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while executing a top-k message query".into(),
                )
            })?;
            matches.extend(worker_result?);
        }
        Ok(matches)
    }

    /// Executes an unordered log query while decoding only matching trace IDs.
    pub(crate) fn query_partitions_trace_ids_unordered(
        &self,
        queries: &[LogQuery],
    ) -> TelemetryResult<Vec<TraceId>> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let mut queries_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        for query in queries {
            let owner = usize::try_from(query.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            queries_by_worker[owner].push(query.clone());
        }
        let (response, receiver) = sync_channel(worker_count);
        let mut dispatched_workers = 0usize;
        for ((shard_id, sender), worker_queries) in workers.into_iter().zip(queries_by_worker) {
            if worker_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            sender
                .send(SinkCommand::QueryTraceIds {
                    queries: Arc::from(worker_queries),
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a trace ID query"
                    ))
                })?;
        }
        let capacity = fanout_result_capacity(None, dispatched_workers);
        let mut trace_ids = Vec::with_capacity(capacity);
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while executing a trace ID query".into(),
                )
            })?;
            trace_ids.extend(worker_result?);
        }
        Ok(trace_ids)
    }

    pub(crate) fn query_partitions_trace_ids_intersection_unordered(
        &self,
        outer_queries: &[LogQuery],
        inner_queries: &[LogQuery],
    ) -> TelemetryResult<Vec<TraceId>> {
        if outer_queries.is_empty() || inner_queries.is_empty() {
            return Ok(Vec::new());
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let same_partitions = outer_queries.len() == inner_queries.len()
            && outer_queries
                .iter()
                .zip(inner_queries)
                .all(|(outer, inner)| outer.topic_partition == inner.topic_partition);
        if !same_partitions {
            let shared_outer_queries: Arc<[LogQuery]> = Arc::from(outer_queries.to_vec());
            let shared_inner_queries: Arc<[LogQuery]> = Arc::from(inner_queries.to_vec());
            let (response, receiver) = sync_channel(worker_count);
            for (shard_id, sender) in workers {
                sender
                    .send(SinkCommand::QueryTraceIdIntersection {
                        outer_queries: Arc::clone(&shared_outer_queries),
                        inner_queries: Arc::clone(&shared_inner_queries),
                        response: response.clone(),
                    })
                    .map_err(|_| {
                        TelemetryError::QueryWorkerUnavailable(format!(
                            "stripe {shard_id} stopped before accepting a trace ID intersection query"
                        ))
                    })?;
            }
            let capacity = fanout_result_capacity(None, worker_count);
            let mut trace_ids = Vec::with_capacity(capacity);
            for _ in 0..worker_count {
                let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(
                        "a stripe stopped while executing a trace ID intersection query".into(),
                    )
                })?;
                trace_ids.extend(worker_result?);
            }
            return Ok(trace_ids);
        }
        let mut outer_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        let mut inner_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        for (outer, inner) in outer_queries.iter().zip(inner_queries) {
            let owner = usize::try_from(outer.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            outer_by_worker[owner].push(outer.clone());
            inner_by_worker[owner].push(inner.clone());
        }
        let (response, receiver) = sync_channel(worker_count);
        let mut dispatched_workers = 0usize;
        for (((shard_id, sender), outer_queries), inner_queries) in workers
            .into_iter()
            .zip(outer_by_worker)
            .zip(inner_by_worker)
        {
            if outer_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            sender
                .send(SinkCommand::QueryTraceIdIntersection {
                    outer_queries: Arc::from(outer_queries),
                    inner_queries: Arc::from(inner_queries),
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a trace ID intersection query"
                    ))
                })?;
        }
        let capacity = fanout_result_capacity(None, dispatched_workers);
        let mut trace_ids = Vec::with_capacity(capacity);
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while executing a trace ID intersection query".into(),
                )
            })?;
            trace_ids.extend(worker_result?);
        }
        Ok(trace_ids)
    }

    /// Executes a single partition-affine projected log query on its owner.
    pub(crate) fn query_partition_projected_on_shard(
        &self,
        shard_id: ShardId,
        query: &LogQuery,
        include_typed_metadata: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        self.query_partition_projected_on_shard_with_fields(
            shard_id,
            query,
            include_typed_metadata,
            true,
        )
    }

    pub(crate) fn query_partition_projected_on_shard_with_fields(
        &self,
        shard_id: ShardId,
        query: &LogQuery,
        include_typed_metadata: bool,
        include_fields: bool,
    ) -> TelemetryResult<Vec<LogMatch>> {
        let sender = self.worker_sender(shard_id)?;
        TARGETED_PROJECTED_QUERY_RESPONSE.with(|slot| {
            let mut slot = slot.borrow_mut();
            if slot.is_none() {
                *slot = Some(sync_channel(1));
            }
            let (response_sender, receiver) = slot
                .as_ref()
                .expect("targeted projected query response channel was initialized");
            sender
                .send(SinkCommand::QueryProjectedSingle {
                    query: query.clone(),
                    include_typed_metadata,
                    include_fields,
                    response: response_sender.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a targeted projected query"
                    ))
                })?;
            let (_response_shard_id, result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(format!(
                    "stripe {shard_id} stopped while executing a targeted projected query"
                ))
            })?;
            result
        })
    }

    /// Counts exact tenant-bound log appends across every owner stripe without
    /// materializing compressed payloads.
    pub(crate) fn count_log_records(
        &self,
        tenant: Arc<str>,
        partitions: Vec<TopicPartition>,
    ) -> TelemetryResult<u64> {
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let (response, receiver) = sync_channel(worker_count);
        for (shard_id, sender) in workers {
            sender
                .send(SinkCommand::CountLogs {
                    tenant: Arc::clone(&tenant),
                    partitions: partitions.clone(),
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a log count"
                    ))
                })?;
        }
        let mut total = 0_u64;
        for _ in 0..worker_count {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while counting logs".into(),
                )
            })?;
            let count = worker_result?;
            total = total
                .checked_add(count)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
        Ok(total)
    }

    /// Counts exact matches across every owner stripe without constructing
    /// normalized log rows.
    pub(crate) fn count_queries(&self, queries: &[LogQuery]) -> TelemetryResult<u64> {
        if queries.is_empty() {
            return Ok(0);
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let mut queries_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        for query in queries {
            let owner = usize::try_from(query.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            queries_by_worker[owner].push(query.clone());
        }
        let (response, receiver) = sync_channel(worker_count);
        let mut dispatched_workers = 0usize;
        for ((shard_id, sender), worker_queries) in workers.into_iter().zip(queries_by_worker) {
            if worker_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            sender
                .send(SinkCommand::CountQueries {
                    queries: worker_queries,
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a count query"
                    ))
                })?;
        }
        let mut total = 0_u64;
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while counting query matches".into(),
                )
            })?;
            let count = worker_result?;
            total = total
                .checked_add(count)
                .ok_or(TelemetryError::RecordTooLarge)?;
        }
        Ok(total)
    }

    /// Counts grouped matches across every owner stripe without materializing
    /// normalized log rows.
    pub(crate) fn group_queries(
        &self,
        queries: &[LogQuery],
        keys: &[crate::AnalyticsGroupKey],
    ) -> TelemetryResult<BTreeMap<Vec<Option<Arc<str>>>, u64>> {
        if queries.is_empty() || keys.is_empty() {
            return Ok(BTreeMap::new());
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let mut queries_by_worker = (0..worker_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LogQuery>>>();
        for query in queries {
            let owner = usize::try_from(query.topic_partition.partition_id.get())
                .unwrap_or_default()
                % worker_count;
            queries_by_worker[owner].push(query.clone());
        }
        let (response, receiver) = sync_channel(worker_count);
        let mut dispatched_workers = 0usize;
        for ((shard_id, sender), worker_queries) in workers.into_iter().zip(queries_by_worker) {
            if worker_queries.is_empty() {
                continue;
            }
            dispatched_workers += 1;
            sender
                .send(SinkCommand::GroupQueries {
                    queries: worker_queries,
                    keys: keys.to_vec(),
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before accepting a grouped query"
                    ))
                })?;
        }
        let mut total = BTreeMap::<Vec<Option<Arc<str>>>, u64>::new();
        for _ in 0..dispatched_workers {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while counting grouped matches".into(),
                )
            })?;
            for (key, count) in worker_result? {
                let total_count = total.entry(key).or_default();
                *total_count = total_count
                    .checked_add(count)
                    .ok_or(TelemetryError::RecordTooLarge)?;
            }
        }
        Ok(total)
    }

    /// Resolves the exact logical log partitions currently containing a
    /// tenant across hot, compressed, and object-tier stripe state.
    pub(crate) fn active_log_partitions(
        &self,
        tenant: Arc<str>,
    ) -> TelemetryResult<Vec<TopicPartition>> {
        let mut cache = self.active_log_partition_cache.lock().map_err(|_| {
            TelemetryError::QueryWorkerUnavailable(
                "active log partition cache lock poisoned".into(),
            )
        })?;
        if let Some(partitions) = cache.get(&tenant) {
            return Ok(partitions.clone());
        }
        let workers = self.worker_senders()?;
        let worker_count = workers.len();
        let (response, receiver) = sync_channel(worker_count);
        for (shard_id, sender) in workers {
            sender
                .send(SinkCommand::ActiveLogPartitions {
                    tenant: Arc::clone(&tenant),
                    response: response.clone(),
                })
                .map_err(|_| {
                    TelemetryError::QueryWorkerUnavailable(format!(
                        "stripe {shard_id} stopped before resolving active log partitions"
                    ))
                })?;
        }

        let mut partitions = Vec::new();
        for _ in 0..worker_count {
            let (_shard_id, worker_result) = receiver.recv().map_err(|_| {
                TelemetryError::QueryWorkerUnavailable(
                    "a stripe stopped while resolving active log partitions".into(),
                )
            })?;
            partitions.extend(worker_result?);
        }
        partitions.sort_unstable();
        partitions.dedup();
        cache.insert(tenant, partitions.clone());
        Ok(partitions)
    }
}
