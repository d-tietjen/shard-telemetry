use super::*;

impl DurableTelemetryStore {
    /// Executes a native trace query on the owner stripes.
    pub fn query_traces(
        &self,
        query: &crate::TraceQuery,
    ) -> Result<Vec<crate::DurableSpan>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end <= cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        if let Some(shard_id) = self.trace_query_owner_shard(&query) {
            self.service
                .query_traces_on_shard(shard_id, &query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        } else if let Some(shard_id) = self
            .service
            .trace_query_owner_shard(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))?
        {
            self.service
                .query_traces_on_shard(shard_id, &query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        } else {
            self.service
                .query_traces(&query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        }
    }

    pub(super) fn query_traces_unordered(
        &self,
        query: &crate::TraceQuery,
    ) -> Result<Vec<crate::DurableSpan>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end <= cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        if let Some(shard_id) = self
            .service
            .trace_query_owner_shard(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))?
        {
            self.service
                .query_traces_on_shard(shard_id, &query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        } else {
            self.service
                .query_traces_unordered(&query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        }
    }

    /// Executes a native exact raw-metric query on the owner stripes.
    pub fn query_metrics(
        &self,
        query: &crate::MetricQuery,
    ) -> Result<Vec<crate::DurableMetricPoint>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end < cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        if let Some(shard_id) = self.metric_query_owner_shard(&query) {
            self.service
                .query_metrics_on_shard(shard_id, &query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        } else if let Some(shard_id) = self
            .service
            .metric_query_owner_shard(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))?
        {
            self.service
                .query_metrics_on_shard(shard_id, &query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        } else {
            self.service
                .query_metrics(&query)
                .map_err(|error| LokiApiError::internal(error.to_string()))
        }
    }

    /// Returns bounded cross-signal record references for exact shared
    /// trace, resource, scope, and typed-label identities.
    pub fn query_correlations(
        &self,
        query: &crate::CorrelationQuery,
    ) -> Result<Vec<crate::TelemetryRecordRef>, LokiApiError> {
        let mut query = query.clone();
        if let Some(cutoff) = self.retention_cutoff() {
            if query.end_time_unix_nanos.is_some_and(|end| end < cutoff) {
                return Ok(Vec::new());
            }
            query.start_time_unix_nanos = Some(
                query
                    .start_time_unix_nanos
                    .map_or(cutoff, |start| start.max(cutoff)),
            );
        }
        self.service
            .query_correlations(&query)
            .map_err(|error| LokiApiError::internal(error.to_string()))
    }
}
