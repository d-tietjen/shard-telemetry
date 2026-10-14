use super::*;

impl TraceStripe {
    pub(crate) fn set_query_work_limits(&mut self, limits: Option<crate::NativeQueryWorkLimits>) {
        self.query_work_limits = limits;
    }

    pub(super) fn query_bounded(
        &self,
        query: &TraceQuery,
        limits: crate::NativeQueryWorkLimits,
    ) -> TelemetryResult<Vec<DurableSpan>> {
        let mut winners = HashMap::<(TraceId, SpanId), DurableSpan>::new();
        let mut budget = crate::query_work::QueryWorkBudget::new(Some(limits));
        for payload in self.sealed_blocks.values() {
            for span in decode_trace_block_with_work_limits(payload, Some(query), Some(limits))? {
                let key = (span.trace_id, span.span_id);
                limits.winners(winners.len() + usize::from(!winners.contains_key(&key)))?;
                if winners
                    .get(&key)
                    .is_none_or(|old| old.record_ref.offset < span.record_ref.offset)
                {
                    budget.replace(winners.get(&key), &span, size_of::<(TraceId, SpanId)>())?;
                }
                retain_newest_span(&mut winners, span);
            }
        }
        for span in self
            .traces
            .iter()
            .filter(|((tenant, _), _)| tenant.as_ref() == query.tenant.as_ref())
            .flat_map(|(_, trace)| trace.spans.values())
            .filter(|span| trace_query_matches(query, span))
        {
            let key = (span.trace_id, span.span_id);
            limits.winners(winners.len() + usize::from(!winners.contains_key(&key)))?;
            if winners
                .get(&key)
                .is_none_or(|old| old.record_ref.offset < span.record_ref.offset)
            {
                budget.replace(winners.get(&key), span, size_of::<(TraceId, SpanId)>())?;
            }
            retain_newest_span(&mut winners, span.clone());
        }
        let mut spans = winners
            .into_values()
            .filter(|span| trace_query_cursor_matches(query, span))
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
}
