use std::fs;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    trace::v1::{ResourceSpans, ScopeSpans, Span, span::Link},
};
use prost::Message;
use shard_stream_core::{
    BatchId, LeaderEpoch, LogicalOffset, LogicalPartitionId, Placement, PlacementSequence,
    RecordId, RingEpoch, TopicPartition, VirtualLaneId,
};
use shard_stream_engine::{
    DurableAppendDelivery, DurableSinkApply, DurableSinkCheckpoint, DurableSinkConfig,
    EngineConfig, StreamEngine, TopicConfig,
};
use shard_stream_protocol::{AppendRequest, Durability};

use crate::{
    LocalObjectStore, MetricExemplar, MetricIdentity, MetricKind, MetricValue, NumberValue,
    OtlpMetricEvent, OtlpTelemetryDecoder, ResourceContext, ScopeContext,
    SharedTelemetryObjectStore, TelemetryRecordRef,
};

use super::*;

struct TempDir(PathBuf);

#[test]
fn query_worker_registry_keeps_a_sorted_snapshot() {
    let (sender_three, _receiver_three) = sync_channel::<SinkCommand>(1);
    let (sender_one, _receiver_one) = sync_channel::<SinkCommand>(1);
    let (sender_two, _receiver_two) = sync_channel::<SinkCommand>(1);
    let mut registry = QueryWorkerRegistry::default();

    registry.insert(ShardId::new(3), sender_three);
    registry.insert(ShardId::new(1), sender_one);
    registry.insert(ShardId::new(2), sender_two);
    assert_eq!(
        registry
            .ordered
            .iter()
            .map(|(shard_id, _)| shard_id.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );

    registry.remove(ShardId::new(2));
    assert_eq!(
        registry
            .ordered
            .iter()
            .map(|(shard_id, _)| shard_id.get())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
}

#[test]
fn validated_signal_cache_shards_retain_and_consume_payloads() {
    let cache = ValidatedSignalCache::default();
    let mut trace_key = [0_u8; 32];
    trace_key[0] = 3;
    let mut metric_key = [0_u8; 32];
    metric_key[0] = 4;

    cache.insert(trace_key, 1, ValidatedSignalPayload::Traces(Vec::new()));
    cache.insert(metric_key, 1, ValidatedSignalPayload::Metrics(Vec::new()));

    assert!(matches!(
        cache.take(trace_key),
        Some(ValidatedSignalPayload::Traces(_))
    ));
    assert!(matches!(
        cache.take(metric_key),
        Some(ValidatedSignalPayload::Metrics(_))
    ));
    assert!(cache.take(trace_key).is_none());
}

#[test]
fn correlation_buffer_pool_reuses_only_bounded_buffers() {
    let mut pool = CorrelationBufferPool::default();
    pool.recycle(Vec::with_capacity(8));
    let reused = pool.take();
    assert!(reused.capacity() >= 8);
    assert!(reused.is_empty());

    pool.recycle(Vec::with_capacity(MAX_CORRELATION_BUFFER_CAPACITY + 1));
    assert!(pool.buffers.is_empty());
}

#[test]
fn bounded_fanout_merge_sorts_only_the_selected_prefix() {
    let mut values = vec![9, 1, 5, 1, 3, 8, 2];
    sort_and_limit(&mut values, Some(4), Ord::cmp);
    assert_eq!(values, vec![1, 1, 2, 3]);

    sort_and_limit(&mut values, None, Ord::cmp);
    assert_eq!(values, vec![1, 1, 2, 3]);
}

impl TempDir {
    fn new(name: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "shard-telemetry-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn engine_config(path: &Path) -> EngineConfig {
    EngineConfig {
        data_dir: path.to_path_buf(),
        object_store_dir: None,
        shard_count: 1,
        virtual_lane_count: 1,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: 64,
        queue_bytes_per_shard: 2 * 1024 * 1024,
        target_pack_bytes: 1024,
        max_pack_age: std::time::Duration::from_secs(1),
        max_batch_bytes: 64 * 1024,
        max_fetch_bytes: 1024 * 1024,
        append_linger: std::time::Duration::from_millis(1),
    }
}

fn payload() -> Vec<u8> {
    let protobuf = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 1,
                    observed_time_unix_nano: 0,
                    severity_number: 9,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("sink message".into())),
                    }),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: Vec::new(),
                    span_id: Vec::new(),
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
    .encode_to_vec();
    let events = crate::OtlpLogDecoder
        .decode(&protobuf)
        .expect("OTLP decodes");
    crate::prepare_log_envelope("tenant-a", &events)
        .expect("STEL envelope")
        .encode()
        .expect("STEL encodes")
}

fn durable_append(
    topic_partition: TopicPartition,
    payload: Vec<u8>,
    batch: u128,
) -> (DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint) {
    let expected = DurableSinkCheckpoint::initial(topic_partition);
    let next = DurableSinkCheckpoint {
        topic_partition,
        next_placement_sequence: PlacementSequence::new(2),
        next_offset: LogicalOffset::new(1),
    };
    (
        DurableAppend {
            event_id: RecordId::for_batch(
                topic_partition.topic_id,
                topic_partition.partition_id,
                BatchId::new(batch),
            ),
            physical_shard_id: ShardId::new(0),
            reservation: shard_stream_core::Reservation {
                topic_id: topic_partition.topic_id,
                partition_id: topic_partition.partition_id,
                batch_id: BatchId::new(batch),
                first_offset: LogicalOffset::new(0),
                last_offset: LogicalOffset::new(0),
                record_count: NonZeroU32::new(1).expect("one"),
                placement: Placement {
                    virtual_lane_id: VirtualLaneId::new(0),
                    ring_epoch: RingEpoch::new(1),
                    leader_epoch: LeaderEpoch::new(0),
                    sequence: PlacementSequence::new(1),
                },
            },
            producer_event_id: None,
            atomic_group: None,
            delivery: DurableAppendDelivery::Publish,
            payload: Bytes::from(payload),
            transient_context: None,
        },
        expected,
        next,
    )
}

#[test]
fn durable_otlp_sink_commits_its_checkpoint_with_the_index_update() {
    let factory = TelemetrySinkFactory::new([ShardId::new(0)], OtlpSinkConfig::default())
        .expect("factory opens");
    let service = factory.service();
    let payload = payload();
    factory
        .validate_append(&payload, NonZeroU32::new(1).expect("one"))
        .expect("payload validates");
    let sink = factory.open_shard(ShardId::new(0)).expect("sink opens");
    let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    let append = DurableAppend {
        event_id: RecordId::for_batch(
            crate::LOGS_TOPIC_ID,
            LogicalPartitionId::new(0),
            BatchId::new(1),
        ),
        physical_shard_id: ShardId::new(0),
        reservation: shard_stream_core::Reservation {
            topic_id: crate::LOGS_TOPIC_ID,
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(1),
            first_offset: LogicalOffset::new(0),
            last_offset: LogicalOffset::new(0),
            record_count: NonZeroU32::new(1).expect("one"),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(0),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: LeaderEpoch::new(0),
                sequence: PlacementSequence::new(1),
            },
        },
        producer_event_id: None,
        atomic_group: None,
        delivery: DurableAppendDelivery::Publish,
        payload: payload.into(),
        transient_context: None,
    };
    let expected = DurableSinkCheckpoint::initial(topic_partition);
    let next = DurableSinkCheckpoint {
        topic_partition,
        next_placement_sequence: PlacementSequence::new(2),
        next_offset: LogicalOffset::new(1),
    };
    assert_eq!(
        sink.apply(expected, &[append], next)
            .expect("durable append indexes"),
        DurableSinkApply::Applied
    );
    assert_eq!(
        factory
            .load_checkpoint(topic_partition)
            .expect("checkpoint loads"),
        Some(next)
    );
    let matches = service
        .query_all(&LogQuery::new(topic_partition).with_term("message"))
        .expect("owner stripe is queryable");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].record.message.as_ref(), "sink message");
}

#[test]
fn sink_journal_recovers_checkpoint_and_repairs_partial_tail() {
    let directory = TempDir::new("sink-journal-recovery");
    let config = OtlpSinkConfig {
        state_directory: Some(directory.0.join("sink")),
        ..OtlpSinkConfig::default()
    };
    let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    let expected = DurableSinkCheckpoint::initial(topic_partition);
    let next = DurableSinkCheckpoint {
        topic_partition,
        next_placement_sequence: PlacementSequence::new(2),
        next_offset: LogicalOffset::new(1),
    };
    let append = DurableAppend {
        event_id: RecordId::for_batch(
            crate::LOGS_TOPIC_ID,
            LogicalPartitionId::new(0),
            BatchId::new(1),
        ),
        physical_shard_id: ShardId::new(0),
        reservation: shard_stream_core::Reservation {
            topic_id: crate::LOGS_TOPIC_ID,
            partition_id: LogicalPartitionId::new(0),
            batch_id: BatchId::new(1),
            first_offset: LogicalOffset::new(0),
            last_offset: LogicalOffset::new(0),
            record_count: NonZeroU32::new(1).expect("one"),
            placement: Placement {
                virtual_lane_id: VirtualLaneId::new(0),
                ring_epoch: RingEpoch::new(1),
                leader_epoch: LeaderEpoch::new(0),
                sequence: PlacementSequence::new(1),
            },
        },
        producer_event_id: None,
        atomic_group: None,
        delivery: DurableAppendDelivery::Publish,
        payload: Bytes::from(payload()),
        transient_context: None,
    };

    let factory =
        TelemetrySinkFactory::new([ShardId::new(0)], config.clone()).expect("factory opens");
    let sink = factory.open_shard(ShardId::new(0)).expect("sink opens");
    assert_eq!(
        sink.apply(expected, &[append], next)
            .expect("transaction is journaled"),
        DurableSinkApply::Applied
    );
    drop(sink);
    drop(factory);

    let journal_path = config
        .state_directory
        .as_ref()
        .expect("state directory")
        .join("shard-0.journal");
    let committed_bytes = fs::metadata(&journal_path).expect("journal metadata").len();
    use std::io::Write as _;
    fs::OpenOptions::new()
        .append(true)
        .open(&journal_path)
        .expect("journal opens")
        .write_all(&[1, 2, 3])
        .expect("partial tail is written");

    let recovered = TelemetrySinkFactory::new([ShardId::new(0)], config).expect("factory recovers");
    assert_eq!(
        recovered
            .load_checkpoint(topic_partition)
            .expect("checkpoint loads"),
        Some(next)
    );
    assert_eq!(
        fs::metadata(journal_path).expect("journal metadata").len(),
        committed_bytes
    );
}

#[test]
fn stream_engine_acks_only_after_the_otlp_sink_indexes_the_append() {
    let directory = TempDir::new("engine-otlp-sink");
    let config = engine_config(&directory.0);
    let factory = Arc::new(
        TelemetrySinkFactory::new(config.shard_ids(), OtlpSinkConfig::default())
            .expect("sink factory opens"),
    );
    let engine = StreamEngine::open_with_durable_sink(config, DurableSinkConfig::new(factory))
        .expect("engine with sink opens");
    engine
        .create_topic(TopicConfig {
            topic_id: crate::LOGS_TOPIC_ID,
            partitions: 1,
            shards: None,
        })
        .expect("topic creates");

    let response = engine
        .append(AppendRequest {
            request_id: 1,
            topic_id: crate::LOGS_TOPIC_ID,
            partition_id: LogicalPartitionId::new(0),
            record_count: 1,
            payload: Bytes::from(payload()),
            durability: Durability::Leader,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        })
        .expect("durable OTLP append is indexed before acknowledgement");
    assert_eq!(response.first_offset, LogicalOffset::new(0));

    let error = engine
        .append(AppendRequest {
            request_id: 2,
            topic_id: crate::LOGS_TOPIC_ID,
            partition_id: LogicalPartitionId::new(0),
            record_count: 2,
            payload: Bytes::from(payload()),
            durability: Durability::Leader,
            producer: None,
            atomic_group: None,
            leader_epoch: None,
            extension_context: None,
        })
        .expect_err("mismatched OTLP record count rejects before it is durable");
    assert!(matches!(error, EngineError::InvalidConfig(_)));
}

#[test]
fn trace_and_metric_queries_survive_object_tier_restart() {
    let directory = TempDir::new("signal-object-tier-restart");
    let signals = ShardTelemetryConfig::default();
    let router = TelemetryRouter::from_config(&signals);
    let trace_id = crate::TraceId::from_bytes([1; 16]).expect("trace ID");
    let linked_trace_id = crate::TraceId::from_bytes([9; 16]).expect("linked trace ID");
    let trace_partition = router.trace("tenant-a", trace_id);

    let trace_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: trace_id.as_bytes().to_vec(),
                    span_id: vec![2; 8],
                    name: "cold trace".into(),
                    start_time_unix_nano: 10,
                    end_time_unix_nano: 20,
                    links: vec![Link {
                        trace_id: linked_trace_id.as_bytes().to_vec(),
                        span_id: vec![3; 8],
                        ..Link::default()
                    }],
                    ..Span::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };
    let trace_events = OtlpTelemetryDecoder
        .decode_traces("tenant-a", &trace_request.encode_to_vec())
        .expect("trace request decodes");
    let trace_payload = crate::prepare_trace_envelope(trace_partition, trace_events)
        .expect("trace envelope")
        .encode()
        .expect("trace STEL");

    let placeholder_metric_partition =
        TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0));
    let metric_point = DurableMetricPoint {
        stream_shard_id: ShardId::new(0),
        record_ref: TelemetryRecordRef::for_signal(
            TelemetrySignal::Metrics,
            placeholder_metric_partition,
            LogicalOffset::new(0),
        ),
        identity: Arc::new(MetricIdentity {
            tenant: Arc::from("tenant-a"),
            resource: Arc::new(ResourceContext::default()),
            scope: Arc::new(ScopeContext::default()),
            name: Arc::from("cold_metric"),
            unit: Arc::from("1"),
            kind: MetricKind::Gauge,
            point_attributes: Arc::new(Vec::new()),
        }),
        description: Arc::from("cold metric"),
        metadata: Arc::new(Vec::new()),
        start_time_unix_nanos: 0,
        timestamp_unix_nanos: 30,
        flags: 0,
        value: MetricValue::Gauge(NumberValue::Integer(7)),
        exemplars: Arc::new(vec![MetricExemplar {
            filtered_attributes: Arc::new(Vec::new()),
            timestamp_unix_nanos: 30,
            value: NumberValue::Integer(7),
            span_id: None,
            trace_id: Some(trace_id),
        }]),
    };
    let series = metric_point.series_fingerprint();
    let metric_partition = router.metric("tenant-a", series);
    let metric_payload = crate::prepare_metric_envelope(
        metric_partition,
        vec![OtlpMetricEvent::from_durable(metric_point).expect("metric event")],
    )
    .expect("metric envelope")
    .encode()
    .expect("metric STEL");

    let object_store =
        LocalObjectStore::open(directory.0.join("objects")).expect("object store opens");
    let mut partitions = vec![trace_partition, metric_partition];
    partitions.sort_unstable();
    let config = OtlpSinkConfig {
        signals,
        object_tier: Some(SinkObjectTierConfig {
            store: SharedTelemetryObjectStore::from(object_store),
            spool_directory: directory.0.join("spool"),
            control_cache_directory: directory.0.join("control-cache"),
            payload_cache_directory: directory.0.join("payload-cache"),
            partitions,
            tier: ObjectTierConfig {
                target_group_payload_bytes: 1,
                max_group_payload_bytes: 8 * 1024 * 1024,
                max_blocks_per_group: 64,
                groups_per_page: 8,
                max_control_object_bytes: 64 * 1024,
                max_retired_objects: 64,
                retirement_grace: std::time::Duration::from_secs(1),
                transaction_lease: std::time::Duration::from_secs(60),
            },
            control_cache: SsdCacheConfig {
                max_bytes: 16 * 1024 * 1024,
                chunk_bytes: 64 * 1024,
                max_read_bytes: 8 * 1024 * 1024,
                memory_bytes: 1024 * 1024,
                parsed_memory_bytes: 1024 * 1024,
            },
            payload_cache: SsdCacheConfig {
                max_bytes: 16 * 1024 * 1024,
                chunk_bytes: 64 * 1024,
                max_read_bytes: 8 * 1024 * 1024,
                memory_bytes: 4 * 1024 * 1024,
                parsed_memory_bytes: 0,
            },
            warm_local_cache_on_publish: false,
        }),
        ..OtlpSinkConfig::default()
    };

    let (trace_append, trace_expected, trace_next) =
        durable_append(trace_partition, trace_payload, 11);
    let (metric_append, metric_expected, metric_next) =
        durable_append(metric_partition, metric_payload, 12);
    {
        let factory =
            TelemetrySinkFactory::new([ShardId::new(0)], config.clone()).expect("factory opens");
        let service = factory.service();
        let sink = factory.open_shard(ShardId::new(0)).expect("sink opens");
        assert_eq!(
            sink.apply(trace_expected, &[trace_append], trace_next)
                .expect("trace applies"),
            DurableSinkApply::Applied
        );
        assert_eq!(
            sink.apply(metric_expected, &[metric_append], metric_next)
                .expect("metric applies"),
            DurableSinkApply::Applied
        );
        assert_eq!(service.flush_object_tier().expect("signals flush"), 2);
        assert_eq!(service.retained_payload_bytes().expect("resident bytes"), 0);
    }

    let recovered = TelemetrySinkFactory::new([ShardId::new(0)], config)
        .expect("factory reopens cold catalogs");
    assert_eq!(
        recovered
            .load_checkpoint(trace_partition)
            .expect("trace checkpoint"),
        Some(trace_next)
    );
    assert_eq!(
        recovered
            .load_checkpoint(metric_partition)
            .expect("metric checkpoint"),
        Some(metric_next)
    );
    let service = recovered.service();
    let _sink = recovered
        .open_shard(ShardId::new(0))
        .expect("recovered worker opens");
    let spans = service
        .query_traces(&TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(trace_id),
            limit: 10,
            ..TraceQuery::default()
        })
        .expect("cold trace query");
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name.as_ref(), "cold trace");
    let points = service
        .query_metrics(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            series: Some(series),
            limit: 10,
            ..MetricQuery::default()
        })
        .expect("cold metric query");
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value, MetricValue::Gauge(NumberValue::Integer(7)));
    let first_cache_stats = service
        .object_tier_cache_stats()
        .expect("object-tier cache diagnostics exist");
    assert!(first_cache_stats.control.misses > 0);
    assert!(first_cache_stats.payload.misses > 0);
    service
        .query_traces(&TraceQuery {
            tenant: Arc::from("tenant-a"),
            trace_id: Some(trace_id),
            limit: 10,
            ..TraceQuery::default()
        })
        .expect("warm trace query");
    service
        .query_metrics(&MetricQuery {
            tenant: Arc::from("tenant-a"),
            series: Some(series),
            limit: 10,
            ..MetricQuery::default()
        })
        .expect("warm metric query");
    let warm_cache_stats = service
        .object_tier_cache_stats()
        .expect("object-tier cache diagnostics exist");
    assert_eq!(
        warm_cache_stats.control.misses,
        first_cache_stats.control.misses
    );
    assert_eq!(
        warm_cache_stats.payload.misses,
        first_cache_stats.payload.misses
    );
    assert!(warm_cache_stats.control.hits > first_cache_stats.control.hits);
    assert!(warm_cache_stats.payload.hits > first_cache_stats.payload.hits);
    let correlated = service
        .query_correlations(
            &CorrelationQuery::new("tenant-a")
                .with_trace_id(trace_id)
                .with_limit(10),
        )
        .expect("cold correlation query");
    assert_eq!(correlated.len(), 2);
    assert!(
        [TelemetrySignal::Traces, TelemetrySignal::Metrics]
            .into_iter()
            .all(|signal| correlated.iter().any(|record| record.signal == signal))
    );
    let linked = service
        .query_correlations(
            &CorrelationQuery::new("tenant-a")
                .with_trace_id(linked_trace_id)
                .with_limit(10),
        )
        .expect("cold linked-trace correlation query");
    assert_eq!(linked.len(), 1);
    assert_eq!(linked[0].signal, TelemetrySignal::Traces);
    let before_absent = service
        .object_tier_cache_stats()
        .expect("object-tier cache diagnostics exist");
    let absent_trace_id = crate::TraceId::from_bytes([7; 16]).expect("absent trace ID");
    assert!(
        service
            .query_correlations(
                &CorrelationQuery::new("tenant-a")
                    .with_trace_id(absent_trace_id)
                    .with_limit(10),
            )
            .expect("absent cold correlation query")
            .is_empty()
    );
    let after_absent = service
        .object_tier_cache_stats()
        .expect("object-tier cache diagnostics exist");
    assert_eq!(after_absent.payload.hits, before_absent.payload.hits);
    assert_eq!(after_absent.payload.misses, before_absent.payload.misses);
}
