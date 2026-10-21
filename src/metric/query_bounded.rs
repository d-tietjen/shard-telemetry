use super::*;

impl MetricStripe {
    pub(crate) fn set_query_work_limits(&mut self, limits: Option<crate::NativeQueryWorkLimits>) {
        self.query_work_limits = limits;
    }

    pub(super) fn query_bounded(
        &self,
        query: &MetricQuery,
        limits: crate::NativeQueryWorkLimits,
        timestamps: Option<&[u64]>,
    ) -> TelemetryResult<Vec<DurableMetricPoint>> {
        let mut winners = BTreeMap::<(SeriesFingerprint, u64), DurableMetricPoint>::new();
        let mut budget = crate::query_work::QueryWorkBudget::new(Some(limits));
        for (series, head) in &self.series {
            if query.series.is_some_and(|requested| requested != *series)
                || !metric_identity_matches(query, &head.identity)
            {
                continue;
            }
            let matches = |point: &DurableMetricPoint| {
                metric_exact_series_point_matches(query, point)
                    && timestamps.is_none_or(|times| {
                        times.binary_search(&point.timestamp_unix_nanos).is_ok()
                    })
            };
            for point in head.points.values().filter(|point| matches(point)) {
                let key = (*series, point.timestamp_unix_nanos);
                limits.winners(winners.len() + usize::from(!winners.contains_key(&key)))?;
                if winners
                    .get(&key)
                    .is_none_or(|old| old.record_ref.offset < point.record_ref.offset)
                {
                    budget.replace(
                        winners.get(&key),
                        point,
                        size_of::<(SeriesFingerprint, u64)>(),
                    )?;
                }
                retain_metric_winner(&mut winners, *series, point.clone());
            }
            for chunk in self.chunks.get(series).into_iter().flatten() {
                if query
                    .start_time_unix_nanos
                    .is_some_and(|start| chunk.max_timestamp_unix_nanos < start)
                    || query
                        .end_time_unix_nanos
                        .is_some_and(|end| chunk.min_timestamp_unix_nanos > end)
                {
                    continue;
                }
                for point in decode_metric_chunk_with_work_limits(&chunk.payload, Some(limits))?
                    .into_iter()
                    .filter(matches)
                {
                    let key = (*series, point.timestamp_unix_nanos);
                    limits.winners(winners.len() + usize::from(!winners.contains_key(&key)))?;
                    if winners
                        .get(&key)
                        .is_none_or(|old| old.record_ref.offset < point.record_ref.offset)
                    {
                        budget.replace(
                            winners.get(&key),
                            &point,
                            size_of::<(SeriesFingerprint, u64)>(),
                        )?;
                    }
                    retain_metric_winner(&mut winners, *series, point);
                }
            }
        }
        let mut points = winners
            .into_values()
            .filter(|point| metric_query_cursor_matches(query, point))
            .collect::<Vec<_>>();
        if query.partition.is_some() {
            points.sort_unstable_by_key(|point| point.record_ref.offset);
        } else {
            points.sort_unstable_by_key(|point| {
                (point.timestamp_unix_nanos, point.record_ref.offset)
            });
        }
        points.truncate(query.limit.max(1));
        Ok(points)
    }
}
