use super::*;

/// Deterministic signal-aware logical partition router.
#[derive(Debug, Clone, Copy)]
pub struct TelemetryRouter {
    logical_partitions: [u16; 3],
}

impl TelemetryRouter {
    /// Creates a router with a fixed logical partition count.
    #[must_use]
    pub const fn new(logical_partitions: NonZeroU16) -> Self {
        Self {
            logical_partitions: [logical_partitions.get(); 3],
        }
    }

    /// Creates a router using each signal's independently configured partition count.
    #[must_use]
    pub const fn from_config(config: &ShardTelemetryConfig) -> Self {
        Self {
            logical_partitions: [
                config.logs.logical_partitions.get(),
                config.traces.logical_partitions.get(),
                config.metrics.logical_partitions.get(),
            ],
        }
    }

    /// Returns the configured logical partition count for one telemetry signal.
    #[must_use]
    pub const fn logical_partition_count(&self, signal: TelemetrySignal) -> u16 {
        self.logical_partitions[match signal {
            TelemetrySignal::Logs => 0,
            TelemetrySignal::Traces => 1,
            TelemetrySignal::Metrics => 2,
        }]
    }

    /// Routes a trace by tenant and trace ID.
    #[must_use]
    pub fn trace(&self, tenant: &str, trace_id: TraceId) -> TopicPartition {
        self.route(TelemetrySignal::Traces, tenant, trace_id.as_bytes())
    }

    /// Routes a metric series by tenant and canonical series fingerprint.
    #[must_use]
    pub fn metric(&self, tenant: &str, series: SeriesFingerprint) -> TopicPartition {
        self.route(
            TelemetrySignal::Metrics,
            tenant,
            &series.get().to_le_bytes(),
        )
    }

    /// Routes a log by trace ID when present, otherwise by its stream/resource identity.
    #[must_use]
    pub fn log(
        &self,
        tenant: &str,
        trace_id: Option<TraceId>,
        stream_or_resource_fingerprint: &[u8],
    ) -> TopicPartition {
        match trace_id {
            Some(trace_id) => self.route(TelemetrySignal::Logs, tenant, trace_id.as_bytes()),
            None => self.route(
                TelemetrySignal::Logs,
                tenant,
                stream_or_resource_fingerprint,
            ),
        }
    }

    fn route(&self, signal: TelemetrySignal, tenant: &str, identity: &[u8]) -> TopicPartition {
        let signal_index = match signal {
            TelemetrySignal::Logs => 0,
            TelemetrySignal::Traces => 1,
            TelemetrySignal::Metrics => 2,
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"shard-telemetry-route-v1\0");
        hasher.update(&[signal as u8]);
        hasher.update(&(tenant.len() as u64).to_le_bytes());
        hasher.update(tenant.as_bytes());
        hasher.update(identity);
        let digest = hasher.finalize();
        let hash = u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("fixed digest"));
        TopicPartition::new(
            signal.topic_id(),
            LogicalPartitionId::new(
                (hash % u64::from(self.logical_partitions[signal_index])) as u32,
            ),
        )
    }
}
