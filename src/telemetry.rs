//! Telemetry ownership: models, stable fingerprints, signal configuration, and routing live in `telemetry/`.
use std::fmt;
use std::mem::size_of;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use shard_stream_core::{LogicalPartitionId, TopicId, TopicPartition};

use crate::{TelemetryError, TelemetryResult};

mod model;
pub use model::{
    LOGS_TOPIC_ID, METRICS_TOPIC_ID, ResourceContext, ScopeContext, TRACES_TOPIC_ID,
    TelemetryAttribute, TelemetryEntityRef, TelemetrySignal, TelemetryValue,
};
mod fingerprint;
use fingerprint::*;
pub use fingerprint::{
    AttributeFingerprint, ResourceContextId, ScopeContextId, SeriesFingerprint, SpanId, TraceId,
};
pub(crate) use fingerprint::{
    estimated_arc_str_bytes, estimated_arc_vec_storage, estimated_resource_context_bytes,
    estimated_scope_context_bytes, estimated_telemetry_attribute_bytes,
};
mod configuration;
pub use configuration::{ShardTelemetryConfig, SignalConfig};
mod routing;
pub use routing::TelemetryRouter;
#[cfg(test)]
mod tests;
