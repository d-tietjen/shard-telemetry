use std::collections::HashMap;
use std::sync::Arc;

use shard_stream_core::{LogicalOffset, ShardId, TopicPartition};

use crate::ingest_pack::{
    decode_ingest_pack, prepare_ingest_pack, prepare_single_cohort_ingest_pack,
};
use crate::{
    CompressionCohortId, LokiEntry, MetadataField, MetricIngestProtocol, OtlpLogEvent,
    OtlpMetricEvent, OtlpSpanEvent, ResourceContext, ScopeContext, StructuralLogMetadataRef,
    StructuralRecordView, TelemetryAttribute, TelemetryEnvelope, TelemetryError, TelemetryResult,
    TelemetrySignal, TelemetryValue, encode_metric_chunk, encode_trace_block,
};

const LABEL_PREFIX: &str = "resource.loki.label.";
const METADATA_PREFIX: &str = "attr.loki.metadata.";
const TENANT_FIELD: &str = "resource.loki.tenant";

/// Stream value from a Docker `json-file` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DockerLogStream {
    /// No stream metadata was present.
    Empty,
    /// The conventional Docker stdout stream.
    Stdout,
    /// The conventional Docker stderr stream.
    Stderr,
    /// An arbitrary stream value retained without normalization.
    Other(String),
}

/// Minimal owned Docker record used by the native ingestion fast path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerLogRecord {
    /// Event timestamp in Unix nanoseconds.
    pub timestamp_unix_nanos: u64,
    /// Exact decoded Docker log body.
    pub message: String,
    /// Exact Docker stream value.
    pub stream: DockerLogStream,
}

struct DockerStructuralRecord {
    ordinal: u32,
    timestamp_unix_nanos: u64,
    message: Arc<str>,
    body: TelemetryValue,
    fields: Arc<Vec<MetadataField>>,
    attributes: Arc<Vec<TelemetryAttribute>>,
    resource: Arc<ResourceContext>,
    scope: Arc<ScopeContext>,
    severity_text: Arc<str>,
    event_name: Arc<str>,
}

impl StructuralRecordView for DockerStructuralRecord {
    fn structural_offset(&self) -> shard_stream_core::LogicalOffset {
        shard_stream_core::LogicalOffset::new(u64::from(self.ordinal))
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        self.fields.len()
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        self.fields
            .get(index)
            .map(|field| (field.key.as_ref(), field.value.as_ref()))
    }

    fn structural_log_metadata(&self) -> Option<StructuralLogMetadataRef<'_>> {
        Some(StructuralLogMetadataRef {
            observed_timestamp_unix_nanos: 0,
            body: Some(&self.body),
            attributes: &self.attributes,
            resource: &self.resource,
            scope: &self.scope,
            severity_number: 0,
            severity_text: &self.severity_text,
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: None,
            span_id: None,
            event_name: &self.event_name,
        })
    }
}

/// Builds one durable STEL log envelope from already validated events.
pub fn prepare_log_envelope(
    tenant: &str,
    events: &[OtlpLogEvent],
) -> TelemetryResult<TelemetryEnvelope> {
    if tenant.is_empty() {
        return Err(TelemetryError::InvalidNativePayload(
            "log tenant must not be empty".into(),
        ));
    }
    let mut tenant_bound = Vec::with_capacity(events.len());
    for event in events {
        let mut event = event.clone();
        let mut fields = event.fields.as_ref().clone();
        match fields
            .iter()
            .find(|field| field.key.as_ref() == TENANT_FIELD)
        {
            Some(field) if field.value.as_ref() != tenant => {
                return Err(TelemetryError::InvalidNativePayload(
                    "log record tenant field conflicts with its envelope".into(),
                ));
            }
            Some(_) => {}
            None => fields.push(MetadataField::new(TENANT_FIELD, tenant)),
        }
        event.fields = Arc::new(fields);
        tenant_bound.push(event);
    }
    let prepared = prepare_ingest_pack(&tenant_bound)?;
    TelemetryEnvelope::new(
        TelemetrySignal::Logs,
        tenant,
        u32::try_from(events.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        Arc::<[u8]>::from([]),
        Arc::<[u8]>::from(prepared.payload),
    )
}

/// Decodes and verifies the exact log records in one durable v1 envelope.
///
/// This is primarily useful for offline verification, compaction, and codec
/// benchmarks. The live query path uses the embedded compressed-domain index
/// and selectively reconstructs only matching records.
pub fn decode_log_envelope(envelope: &TelemetryEnvelope) -> TelemetryResult<Vec<OtlpLogEvent>> {
    if envelope.signal != TelemetrySignal::Logs {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "log decoder received another telemetry signal",
        ));
    }
    let records = decode_ingest_pack(&envelope.payload)?;
    if records.len() != envelope.item_count as usize {
        return Err(TelemetryError::InvalidTelemetryEnvelope(
            "decoded log count disagrees with its envelope",
        ));
    }
    Ok(records)
}

/// Converts Loki entries into the single typed log representation and STEL envelope.
pub fn prepare_loki_log_envelope(
    tenant: &str,
    entries: Vec<LokiEntry>,
) -> TelemetryResult<TelemetryEnvelope> {
    if tenant.is_empty() {
        return Err(TelemetryError::InvalidNativePayload(
            "Loki tenant must not be empty".into(),
        ));
    }
    let mut events = Vec::with_capacity(entries.len());
    for entry in entries {
        let timestamp_unix_nanos = u64::try_from(entry.timestamp_unix_nanos).map_err(|_| {
            TelemetryError::InvalidNativePayload(
                "negative Loki timestamps are outside the storage epoch".into(),
            )
        })?;
        let mut fields = Vec::with_capacity(
            1 + entry
                .labels
                .len()
                .saturating_add(entry.structured_metadata.len()),
        );
        fields.push(MetadataField::new(TENANT_FIELD, tenant));
        let mut resource_attributes = Vec::with_capacity(entry.labels.len());
        let mut cohort = blake3::Hasher::new();
        for (key, value) in entry.labels {
            cohort.update(&(key.len() as u64).to_le_bytes());
            cohort.update(key.as_bytes());
            cohort.update(&(value.len() as u64).to_le_bytes());
            cohort.update(value.as_bytes());
            fields.push(MetadataField::new(
                format!("{LABEL_PREFIX}{key}"),
                value.clone(),
            ));
            resource_attributes.push(TelemetryAttribute::new(
                key,
                TelemetryValue::String(value.into()),
            ));
        }
        let mut attributes = Vec::with_capacity(entry.structured_metadata.len());
        for (key, value) in entry.structured_metadata {
            fields.push(MetadataField::new(
                format!("{METADATA_PREFIX}{key}"),
                value.clone(),
            ));
            attributes.push(TelemetryAttribute::new(
                key,
                TelemetryValue::String(value.into()),
            ));
        }
        let cohort_bytes = cohort.finalize();
        let compression_cohort = CompressionCohortId::new(u64::from_le_bytes(
            cohort_bytes.as_bytes()[..8]
                .try_into()
                .expect("BLAKE3 output contains eight bytes"),
        ));
        let resource = Arc::new(ResourceContext {
            attributes: Arc::new(resource_attributes),
            ..ResourceContext::default()
        });
        let scope = Arc::new(ScopeContext::default());
        fields.push(MetadataField::new(
            "otel.resource.id",
            resource.id().to_string(),
        ));
        fields.push(MetadataField::new("otel.scope.id", scope.id().to_string()));
        let message: Arc<str> = entry.line.into();
        events.push(OtlpLogEvent {
            timestamp_unix_nanos,
            observed_timestamp_unix_nanos: 0,
            body: Some(TelemetryValue::String(Arc::clone(&message))),
            message,
            fields: Arc::new(fields),
            attributes: Arc::new(attributes),
            resource,
            scope,
            compression_cohort,
            ..OtlpLogEvent::default()
        });
    }
    prepare_log_envelope(tenant, &events)
}

/// Builds a log envelope directly from Docker records without constructing
/// compatibility maps for every record.
pub fn prepare_docker_log_envelope(
    tenant: &str,
    records: Vec<DockerLogRecord>,
) -> TelemetryResult<TelemetryEnvelope> {
    prepare_docker_log_envelope_with_context(tenant, records).map(|(envelope, _)| envelope)
}

/// Builds a Docker log envelope and the process-local index context used by
/// the live owner stripe to avoid decompressing the frame during indexing.
pub fn prepare_docker_log_envelope_with_context(
    tenant: &str,
    records: Vec<DockerLogRecord>,
) -> TelemetryResult<(TelemetryEnvelope, Arc<[u8]>)> {
    if tenant.is_empty() {
        return Err(TelemetryError::InvalidNativePayload(
            "Docker tenant must not be empty".into(),
        ));
    }
    let tenant: Arc<str> = Arc::from(tenant);
    let source_key: Arc<str> = Arc::from("source");
    let source_value: Arc<str> = Arc::from("clickhouse-docker");
    let resource = Arc::new(ResourceContext {
        attributes: Arc::new(vec![TelemetryAttribute::new(
            Arc::clone(&source_key),
            TelemetryValue::String(Arc::clone(&source_value)),
        )]),
        ..ResourceContext::default()
    });
    let scope = Arc::new(ScopeContext::default());
    let resource_id: Arc<str> = Arc::from(resource.id().to_string());
    let scope_id: Arc<str> = Arc::from(scope.id().to_string());
    let cohort = docker_source_cohort();
    let stream_metadata = |stream: Option<&str>| {
        let stream = stream.map(Arc::<str>::from);
        let mut fields = Vec::with_capacity(if stream.is_some() { 5 } else { 4 });
        fields.push(MetadataField::new(
            Arc::<str>::from(TENANT_FIELD),
            Arc::clone(&tenant),
        ));
        fields.push(MetadataField::new(
            Arc::<str>::from(format!("{LABEL_PREFIX}{source_key}")),
            Arc::clone(&source_value),
        ));
        if let Some(stream) = &stream {
            fields.push(MetadataField::new(
                Arc::<str>::from(format!("{METADATA_PREFIX}docker_stream")),
                Arc::clone(stream),
            ));
        }
        fields.push(MetadataField::new(
            Arc::<str>::from("otel.resource.id"),
            Arc::clone(&resource_id),
        ));
        fields.push(MetadataField::new(
            Arc::<str>::from("otel.scope.id"),
            Arc::clone(&scope_id),
        ));
        let attributes = stream.map_or_else(Vec::new, |stream| {
            vec![TelemetryAttribute::new(
                Arc::<str>::from("docker_stream"),
                TelemetryValue::String(stream),
            )]
        });
        (Arc::new(fields), Arc::new(attributes))
    };
    let empty = stream_metadata(None);
    let stdout = stream_metadata(Some("stdout"));
    let stderr = stream_metadata(Some("stderr"));
    let mut other_streams =
        HashMap::<String, (Arc<Vec<MetadataField>>, Arc<Vec<TelemetryAttribute>>)>::new();
    let empty_text: Arc<str> = Arc::from("");
    let mut structural_records = Vec::with_capacity(records.len());
    for (ordinal, record) in records.into_iter().enumerate() {
        let metadata = match record.stream {
            DockerLogStream::Empty => &empty,
            DockerLogStream::Stdout => &stdout,
            DockerLogStream::Stderr => &stderr,
            DockerLogStream::Other(stream) => other_streams
                .entry(stream.clone())
                .or_insert_with(|| stream_metadata(Some(&stream))),
        };
        let (fields, attributes) = metadata;
        let message: Arc<str> = Arc::from(record.message);
        structural_records.push(DockerStructuralRecord {
            ordinal: u32::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?,
            timestamp_unix_nanos: record.timestamp_unix_nanos,
            body: TelemetryValue::String(Arc::clone(&message)),
            message,
            fields: Arc::clone(fields),
            attributes: Arc::clone(attributes),
            resource: Arc::clone(&resource),
            scope: Arc::clone(&scope),
            severity_text: Arc::clone(&empty_text),
            event_name: Arc::clone(&empty_text),
        });
    }
    let item_count =
        u32::try_from(structural_records.len()).map_err(|_| TelemetryError::RecordTooLarge)?;
    let prepared = prepare_single_cohort_ingest_pack(&structural_records, cohort)?;
    let envelope = TelemetryEnvelope::new(
        TelemetrySignal::Logs,
        tenant,
        item_count,
        Arc::<[u8]>::from([]),
        Arc::<[u8]>::from(prepared.payload),
    )?;
    Ok((envelope, Arc::<[u8]>::from(prepared.transient_context)))
}

fn docker_source_cohort() -> CompressionCohortId {
    let mut cohort = blake3::Hasher::new();
    cohort.update(&("source".len() as u64).to_le_bytes());
    cohort.update(b"source");
    cohort.update(&("clickhouse-docker".len() as u64).to_le_bytes());
    cohort.update(b"clickhouse-docker");
    let digest = cohort.finalize();
    CompressionCohortId::new(u64::from_le_bytes(
        digest.as_bytes()[..8]
            .try_into()
            .expect("BLAKE3 output contains eight bytes"),
    ))
}

/// Builds one durable STEL trace envelope for a single routed partition.
pub fn prepare_trace_envelope(
    topic_partition: TopicPartition,
    events: Vec<OtlpSpanEvent>,
) -> TelemetryResult<TelemetryEnvelope> {
    if topic_partition.topic_id != TelemetrySignal::Traces.topic_id() {
        return Err(TelemetryError::InvalidOtlpPayload(
            "trace envelope uses the wrong topic".into(),
        ));
    }
    let tenant = events
        .first()
        .map(|event| Arc::<str>::from(event.tenant()))
        .ok_or(TelemetryError::InvalidOtlpPayload(
            "trace envelope must contain at least one span".into(),
        ))?;
    if events.iter().any(|event| event.tenant() != tenant.as_ref()) {
        return Err(TelemetryError::InvalidOtlpPayload(
            "trace partition batch crosses tenants".into(),
        ));
    }
    let records = events
        .into_iter()
        .enumerate()
        .map(|(ordinal, event)| {
            Ok(event.into_durable(
                ShardId::new(0),
                topic_partition,
                LogicalOffset::new(
                    u64::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?,
                ),
            ))
        })
        .collect::<TelemetryResult<Vec<_>>>()?;
    let payload = encode_trace_block(&records)?;
    TelemetryEnvelope::new(
        TelemetrySignal::Traces,
        tenant,
        u32::try_from(records.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        topic_partition.partition_id.get().to_le_bytes().as_slice(),
        Arc::<[u8]>::from(payload),
    )
}

/// Builds one durable STEL metric envelope for a single routed partition.
pub fn prepare_metric_envelope(
    topic_partition: TopicPartition,
    events: Vec<OtlpMetricEvent>,
) -> TelemetryResult<TelemetryEnvelope> {
    prepare_metric_envelope_with_protocol(topic_partition, events, MetricIngestProtocol::Otlp)
}

/// Builds one durable STEL metric envelope with explicit conflict semantics.
pub fn prepare_metric_envelope_with_protocol(
    topic_partition: TopicPartition,
    events: Vec<OtlpMetricEvent>,
    protocol: MetricIngestProtocol,
) -> TelemetryResult<TelemetryEnvelope> {
    if topic_partition.topic_id != TelemetrySignal::Metrics.topic_id() {
        return Err(TelemetryError::InvalidOtlpPayload(
            "metric envelope uses the wrong topic".into(),
        ));
    }
    let tenant = events
        .first()
        .map(|event| Arc::<str>::from(event.tenant()))
        .ok_or(TelemetryError::InvalidOtlpPayload(
            "metric envelope must contain at least one point".into(),
        ))?;
    if events.iter().any(|event| event.tenant() != tenant.as_ref()) {
        return Err(TelemetryError::InvalidOtlpPayload(
            "metric partition batch crosses tenants".into(),
        ));
    }
    let records = events
        .into_iter()
        .enumerate()
        .map(|(ordinal, event)| {
            Ok(event.into_durable(
                ShardId::new(0),
                topic_partition,
                LogicalOffset::new(
                    u64::try_from(ordinal).map_err(|_| TelemetryError::RecordTooLarge)?,
                ),
            ))
        })
        .collect::<TelemetryResult<Vec<_>>>()?;
    let payload = encode_metric_chunk(&records)?;
    let mut routing_metadata = [0_u8; 5];
    routing_metadata[..4].copy_from_slice(&topic_partition.partition_id.get().to_le_bytes());
    routing_metadata[4] = protocol.to_wire();
    TelemetryEnvelope::new(
        TelemetrySignal::Metrics,
        tenant,
        u32::try_from(records.len()).map_err(|_| TelemetryError::RecordTooLarge)?,
        routing_metadata.as_slice(),
        Arc::<[u8]>::from(payload),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU16;

    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    use crate::{OtlpTelemetryDecoder, TelemetryRouter, decode_trace_block};

    use super::*;

    #[test]
    fn log_envelope_binds_every_record_to_its_authenticated_tenant() {
        let event = OtlpLogEvent {
            timestamp_unix_nanos: 1,
            message: Arc::from("hello"),
            ..OtlpLogEvent::default()
        };
        let envelope = prepare_log_envelope("tenant-a", std::slice::from_ref(&event)).unwrap();
        let decoded = decode_log_envelope(&envelope).unwrap();
        assert!(decoded[0].fields.iter().any(|field| {
            field.key.as_ref() == TENANT_FIELD && field.value.as_ref() == "tenant-a"
        }));

        let mut conflicting = event;
        conflicting.fields = Arc::new(vec![MetadataField::new(TENANT_FIELD, "tenant-b")]);
        assert!(prepare_log_envelope("tenant-a", &[conflicting]).is_err());
    }

    #[test]
    fn docker_fast_path_matches_loki_envelope_bytes_after_decode() {
        let optimized = prepare_docker_log_envelope(
            "tenant-a",
            vec![
                DockerLogRecord {
                    timestamp_unix_nanos: 10,
                    message: "hello".to_owned(),
                    stream: DockerLogStream::Stdout,
                },
                DockerLogRecord {
                    timestamp_unix_nanos: 20,
                    message: "world".to_owned(),
                    stream: DockerLogStream::Stderr,
                },
                DockerLogRecord {
                    timestamp_unix_nanos: 30,
                    message: "empty".to_owned(),
                    stream: DockerLogStream::Empty,
                },
            ],
        )
        .unwrap();
        let generic = prepare_loki_log_envelope(
            "tenant-a",
            vec![
                LokiEntry {
                    timestamp_unix_nanos: 10,
                    labels: BTreeMap::from([("source".to_owned(), "clickhouse-docker".to_owned())]),
                    line: "hello".to_owned(),
                    structured_metadata: BTreeMap::from([(
                        "docker_stream".to_owned(),
                        "stdout".to_owned(),
                    )]),
                },
                LokiEntry {
                    timestamp_unix_nanos: 20,
                    labels: BTreeMap::from([("source".to_owned(), "clickhouse-docker".to_owned())]),
                    line: "world".to_owned(),
                    structured_metadata: BTreeMap::from([(
                        "docker_stream".to_owned(),
                        "stderr".to_owned(),
                    )]),
                },
                LokiEntry {
                    timestamp_unix_nanos: 30,
                    labels: BTreeMap::from([("source".to_owned(), "clickhouse-docker".to_owned())]),
                    line: "empty".to_owned(),
                    structured_metadata: BTreeMap::new(),
                },
            ],
        )
        .unwrap();
        assert_eq!(
            decode_log_envelope(&optimized).unwrap(),
            decode_log_envelope(&generic).unwrap()
        );
    }

    #[test]
    fn trace_partition_envelope_is_self_verifying_and_counted() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        start_time_unix_nano: 10,
                        end_time_unix_nano: 20,
                        ..Span::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        };
        let decoder = OtlpTelemetryDecoder;
        let events = decoder
            .decode_traces("tenant-a", &request.encode_to_vec())
            .unwrap();
        let router = TelemetryRouter::new(NonZeroU16::new(256).unwrap());
        let mut partitioned = decoder.partition_traces(&router, events);
        let (partition, events) = partitioned.pop_first().unwrap();
        let envelope = prepare_trace_envelope(partition, events).unwrap();
        let encoded = envelope.encode().unwrap();
        let decoded = TelemetryEnvelope::decode(&encoded).unwrap();
        assert_eq!(decoded.item_count, 1);
        assert_eq!(decode_trace_block(&decoded.payload).unwrap().len(), 1);
    }
}
