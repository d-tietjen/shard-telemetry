use super::*;

impl TelemetryService {
    pub(super) fn worker_senders(
        &self,
    ) -> TelemetryResult<Vec<(ShardId, SyncSender<SinkCommand>)>> {
        let workers = self
            .workers
            .read()
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable("query registry lock is poisoned".into())
            })?
            .ordered
            .clone();
        if workers.is_empty() {
            return Err(TelemetryError::QueryWorkerUnavailable(
                "no stripe workers are active".into(),
            ));
        }
        Ok(workers)
    }

    pub(super) fn worker_sender(
        &self,
        shard_id: ShardId,
    ) -> TelemetryResult<SyncSender<SinkCommand>> {
        self.workers
            .read()
            .map_err(|_| {
                TelemetryError::QueryWorkerUnavailable("query registry lock is poisoned".into())
            })?
            .by_shard
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| {
                TelemetryError::QueryWorkerUnavailable(format!("stripe {shard_id} is not active"))
            })
    }

    pub(super) fn owner_shard_for_partition(
        &self,
        partition: TopicPartition,
    ) -> TelemetryResult<ShardId> {
        let workers = self.worker_senders()?;
        let owner =
            usize::try_from(partition.partition_id.get()).unwrap_or_default() % workers.len();
        Ok(workers[owner].0)
    }

    pub(crate) fn trace_query_owner_shard(
        &self,
        query: &TraceQuery,
    ) -> TelemetryResult<Option<ShardId>> {
        let partition = query.partition.or_else(|| {
            query
                .trace_id
                .map(|trace_id| self.router.trace(&query.tenant, trace_id))
        });
        partition
            .map(|partition| self.owner_shard_for_partition(partition))
            .transpose()
    }

    pub(crate) fn metric_query_owner_shard(
        &self,
        query: &MetricQuery,
    ) -> TelemetryResult<Option<ShardId>> {
        let partition = query.partition.or_else(|| {
            query
                .series
                .map(|series| self.router.metric(&query.tenant, series))
        });
        partition
            .map(|partition| self.owner_shard_for_partition(partition))
            .transpose()
    }
}
