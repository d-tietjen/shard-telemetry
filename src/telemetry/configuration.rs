use super::*;

/// Bounded configuration shared by a single telemetry signal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalConfig {
    /// Logical shard-stream partitions. The production default is 256.
    pub logical_partitions: NonZeroU16,
    /// Single-writer physical owner stripes. The production default is 16.
    pub physical_stripes: NonZeroU16,
    /// Optional retention. `None` retains exact data indefinitely.
    pub retention: Option<Duration>,
    /// Maximum mutable signal state per physical stripe.
    pub head_memory_bytes_per_stripe: usize,
    /// Target immutable block or chunk bytes.
    pub target_block_bytes: usize,
    /// Maximum records materialized by one query.
    pub max_query_records: usize,
}

impl SignalConfig {
    fn validate(&self, signal: TelemetrySignal) -> TelemetryResult<()> {
        if self.physical_stripes.get() > self.logical_partitions.get() {
            return Err(TelemetryError::InvalidConfiguration(format!(
                "{signal:?} physical stripes exceed logical partitions"
            )));
        }
        if self.head_memory_bytes_per_stripe == 0
            || self.target_block_bytes == 0
            || self.max_query_records == 0
        {
            return Err(TelemetryError::InvalidConfiguration(format!(
                "{signal:?} bounded limits must be nonzero"
            )));
        }
        Ok(())
    }
}

/// Complete single-node ShardTelemetry configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardTelemetryConfig {
    /// Log storage and query limits.
    pub logs: SignalConfig,
    /// Trace storage and query limits.
    pub traces: SignalConfig,
    /// Metric storage and query limits.
    pub metrics: SignalConfig,
    /// Maximum decompressed OTLP request body.
    pub max_otlp_request_bytes: usize,
    /// Maximum partition appends executing concurrently per request.
    pub max_parallel_partition_appends: NonZeroU16,
}

impl Default for ShardTelemetryConfig {
    fn default() -> Self {
        let common = |head_memory_bytes_per_stripe, target_block_bytes| SignalConfig {
            logical_partitions: NonZeroU16::new(256).expect("constant is nonzero"),
            physical_stripes: NonZeroU16::new(16).expect("constant is nonzero"),
            retention: None,
            head_memory_bytes_per_stripe,
            target_block_bytes,
            max_query_records: 1_000_000,
        };
        Self {
            logs: common(64 * 1024 * 1024, 8 * 1024 * 1024),
            traces: common(256 * 1024 * 1024, 8 * 1024 * 1024),
            metrics: common(512 * 1024 * 1024, 64 * 1024),
            max_otlp_request_bytes: 64 * 1024 * 1024,
            max_parallel_partition_appends: NonZeroU16::new(16).expect("constant is nonzero"),
        }
    }
}

impl ShardTelemetryConfig {
    /// Validates all bounded production limits.
    pub fn validate(&self) -> TelemetryResult<()> {
        self.logs.validate(TelemetrySignal::Logs)?;
        self.traces.validate(TelemetrySignal::Traces)?;
        self.metrics.validate(TelemetrySignal::Metrics)?;
        if self.max_otlp_request_bytes == 0 {
            return Err(TelemetryError::InvalidConfiguration(
                "OTLP request limit must be nonzero".into(),
            ));
        }
        Ok(())
    }
}
