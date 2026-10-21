//! Real immutable-publication failures must not grow already indexed retries.

use super::tests::{TempDir, durable_append};
use super::*;
use std::num::{NonZeroU16, NonZeroU32};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use shard_stream_core::{LogicalOffset, LogicalPartitionId};

use crate::{LocalObjectStore, ObjectMetadata, OtlpTelemetryDecoder, TelemetryObjectStore};

#[test]
fn bounded_native_partitions_plateau_during_outage_and_recover_exactly() {
    let directory = TempDir::new("bounded-native-publication");
    let object_store = Arc::new(UnavailableImmutableStore {
        local: LocalObjectStore::open(directory.0.join("objects")).expect("real local tier"),
        unavailable: AtomicBool::new(false),
        failed_puts: AtomicUsize::new(0),
    });
    let mut bounded =
        crate::EmbeddedTelemetryConfig::bounded_local(&directory.0, Duration::from_secs(3_600));
    bounded.store.tenant_partitions = 2;
    bounded = bounded.with_max_ram_bytes(512 * 1024);
    let limits = bounded.local_limits;
    let resident = limits.native_resident.expect("finite policy");
    let mut signals = limits.signals;
    let two = NonZeroU16::new(2).expect("two partitions");
    let one = NonZeroU16::new(1).expect("one physical owner");
    for signal in [&mut signals.logs, &mut signals.traces, &mut signals.metrics] {
        signal.logical_partitions = two;
        signal.physical_stripes = one;
    }
    let partitions = [crate::TRACES_TOPIC_ID, crate::METRICS_TOPIC_ID]
        .into_iter()
        .flat_map(|topic| {
            (0..2).map(move |id| TopicPartition::new(topic, LogicalPartitionId::new(id)))
        })
        .collect::<Vec<_>>();
    let config = OtlpSinkConfig {
        stripe: bounded.store.stripe,
        signals,
        correlations: limits.correlations,
        native_query_work: limits.native_query_work,
        native_resident: Some(resident),
        object_tier: Some(SinkObjectTierConfig {
            store: SharedTelemetryObjectStore::new(object_store.clone()),
            spool_directory: directory.0.join("spool"),
            control_cache_directory: directory.0.join("control-cache"),
            payload_cache_directory: directory.0.join("payload-cache"),
            partitions,
            // Deliberately oversized overrides cannot enlarge the effective
            // resident publication threshold in the factory.
            tier: ObjectTierConfig::default(),
            control_cache: limits.control_cache,
            payload_cache: limits.payload_cache,
            warm_local_cache_on_publish: false,
        }),
        ..OtlpSinkConfig::default()
    };
    let active_partitions = [
        (crate::METRICS_TOPIC_ID, 0),
        (crate::METRICS_TOPIC_ID, 1),
        (crate::TRACES_TOPIC_ID, 0),
        (crate::TRACES_TOPIC_ID, 1),
    ]
    .map(|(topic, id)| TopicPartition::new(topic, LogicalPartitionId::new(id)));
    let mut counts = [0_u64; 4];
    let mut checkpoints = active_partitions.map(DurableSinkCheckpoint::initial);
    let mut blocked = Vec::new();
    {
        let factory = TelemetrySinkFactory::new([ShardId::new(0)], config.clone())
            .expect("bounded real factory");
        let service = factory.service();
        let sink = factory.open_shard(ShardId::new(0)).expect("real owner");
        for (index, partition) in active_partitions.into_iter().enumerate() {
            for _ in 0..4 {
                let (append, expected, next) =
                    bounded_signal_append(partition, checkpoints[index], counts[index]);
                factory
                    .validate_append(&append.payload, append.reservation.record_count)
                    .expect("preflight");
                sink.apply(expected, &[append], next)
                    .expect("healthy progression");
                checkpoints[index] = next;
                counts[index] += 1;
                assert!(
                    service.retained_payload_bytes().expect("sealed accounting")
                        <= resident.reserved_bytes(2, 2)
                );
            }
        }
        object_store.unavailable.store(true, Ordering::Release);
        for (index, partition) in active_partitions.into_iter().enumerate() {
            for attempt in 0..16 {
                let (append, expected, next) =
                    bounded_signal_append(partition, checkpoints[index], counts[index]);
                factory
                    .validate_append(&append.payload, append.reservation.record_count)
                    .expect("accepted append is bounded");
                counts[index] += 1;
                if sink
                    .apply(expected, std::slice::from_ref(&append), next)
                    .is_err()
                {
                    blocked.push((index, append, expected, next));
                    break;
                }
                checkpoints[index] = next;
                assert!(attempt < 15, "finite policy must trigger publication");
            }
        }
        assert_eq!(
            blocked.len(),
            4,
            "all four uniquely owned signal partitions encounter the outage"
        );
        let retained = service.retained_payload_bytes().expect("outage accounting");
        assert!(retained > 0 && retained <= resident.reserved_bytes(2, 2));
        for _ in 0..16 {
            for (_, append, expected, next) in &blocked {
                assert!(
                    sink.apply(*expected, std::slice::from_ref(append), *next)
                        .is_err()
                );
                assert_eq!(
                    factory
                        .load_checkpoint(expected.topic_partition)
                        .expect("checkpoint"),
                    Some(*expected)
                );
            }
            assert_eq!(
                service
                    .retained_payload_bytes()
                    .expect("same-boundary plateau"),
                retained
            );
        }
        assert!(object_store.failed_puts.load(Ordering::Relaxed) >= 4);
        object_store.unavailable.store(false, Ordering::Release);
        for (index, append, expected, next) in &blocked {
            sink.apply(*expected, std::slice::from_ref(append), *next)
                .expect("recovered publication");
            checkpoints[*index] = *next;
        }
        service
            .flush_object_tier()
            .expect("publish remaining healthy prefix");
        assert_eq!(
            service.retained_payload_bytes().expect("released payloads"),
            0
        );
        verify_bounded_signals(&service, active_partitions, counts);
    }
    let factory =
        TelemetrySinkFactory::new([ShardId::new(0)], config).expect("reopen published state");
    let service = factory.service();
    let _sink = factory.open_shard(ShardId::new(0)).expect("reopened owner");
    for (index, partition) in active_partitions.into_iter().enumerate() {
        assert_eq!(
            factory
                .load_checkpoint(partition)
                .expect("reopened checkpoint"),
            Some(checkpoints[index])
        );
    }
    verify_bounded_signals(&service, active_partitions, counts);
}

fn bounded_signal_append(
    partition: TopicPartition,
    expected: DurableSinkCheckpoint,
    ordinal: u64,
) -> (DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint) {
    if partition.topic_id == crate::TRACES_TOPIC_ID {
        let mut trace_id = [0_u8; 16];
        trace_id[..8].copy_from_slice(&u64::from(partition.partition_id.get()).to_be_bytes());
        trace_id[8..].copy_from_slice(&(ordinal + 1).to_be_bytes());
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.to_vec(),
                        span_id: (ordinal + 1).to_be_bytes().to_vec(),
                        name: format!("bounded-publication-{ordinal}"),
                        start_time_unix_nano: ordinal * 60_000_000_000 + 1,
                        end_time_unix_nano: ordinal * 60_000_000_000 + 2,
                        attributes: vec![KeyValue {
                            key: "fixture.payload".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue(noise(ordinal as u32 + 1))),
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let events = OtlpTelemetryDecoder
            .decode_traces("tenant-a", &request.encode_to_vec())
            .expect("real bounded trace decoding");
        let payload = crate::prepare_trace_envelope(partition, events)
            .expect("trace envelope")
            .encode()
            .expect("trace STEL");
        return bounded_append_boundary(partition, expected, ordinal, payload);
    }
    let point = crate::DurableMetricPoint {
        stream_shard_id: ShardId::new(0),
        record_ref: crate::TelemetryRecordRef::for_signal(
            TelemetrySignal::Metrics,
            partition,
            expected.next_offset,
        ),
        identity: Arc::new(crate::MetricIdentity {
            tenant: Arc::from("tenant-a"),
            resource: Arc::new(crate::ResourceContext::default()),
            scope: Arc::new(crate::ScopeContext::default()),
            name: Arc::from("observed_cpu_pct"),
            unit: Arc::from("%"),
            kind: crate::MetricKind::Gauge,
            point_attributes: Arc::new(vec![crate::TelemetryAttribute::new(
                "source_partition",
                crate::TelemetryValue::Integer(i64::from(partition.partition_id.get())),
            )]),
        }),
        description: Arc::from("synthetic measured CPU source"),
        metadata: Arc::new(vec![crate::TelemetryAttribute::new(
            "fixture.source",
            crate::TelemetryValue::String(Arc::from(noise(ordinal as u32 + 1))),
        )]),
        start_time_unix_nanos: 0,
        timestamp_unix_nanos: ordinal + 1,
        flags: 0,
        value: crate::MetricValue::Gauge(crate::NumberValue::Integer(ordinal as i64)),
        exemplars: Arc::new(Vec::new()),
    };
    let event = crate::OtlpMetricEvent::from_durable(point).expect("real normalized point");
    let payload = crate::prepare_metric_envelope(partition, vec![event])
        .expect("native envelope")
        .encode()
        .expect("STEL bytes");
    bounded_append_boundary(partition, expected, ordinal, payload)
}

fn bounded_append_boundary(
    partition: TopicPartition,
    expected: DurableSinkCheckpoint,
    ordinal: u64,
    payload: Vec<u8>,
) -> (DurableAppend, DurableSinkCheckpoint, DurableSinkCheckpoint) {
    let batch = u128::from(partition.partition_id.get()) * 1000 + u128::from(ordinal) + 1;
    let (mut append, _, _) = durable_append(partition, payload, batch);
    append.reservation.first_offset = expected.next_offset;
    append.reservation.last_offset = expected.next_offset;
    append.reservation.placement.sequence = expected.next_placement_sequence;
    let next = DurableSinkCheckpoint {
        topic_partition: partition,
        next_offset: LogicalOffset::new(expected.next_offset.get() + 1),
        next_placement_sequence: shard_stream_core::PlacementSequence::new(
            expected.next_placement_sequence.get() + 1,
        ),
    };
    (append, expected, next)
}

fn verify_bounded_signals(
    service: &TelemetryService,
    partitions: [TopicPartition; 4],
    counts: [u64; 4],
) {
    for (index, partition) in partitions.into_iter().enumerate() {
        if partition.topic_id == crate::TRACES_TOPIC_ID {
            let spans = service
                .query_traces(&TraceQuery {
                    tenant: Arc::from("tenant-a"),
                    partition: Some(partition),
                    limit: 32,
                    ..Default::default()
                })
                .expect("complete real bounded trace result");
            assert_eq!(spans.len(), counts[index] as usize);
            for (ordinal, span) in spans.iter().enumerate() {
                assert_eq!(span.name.as_ref(), format!("bounded-publication-{ordinal}"));
                assert_eq!(
                    span.start_time_unix_nanos,
                    ordinal as u64 * 60_000_000_000 + 1
                );
                assert_eq!(span.record_ref.offset.get(), ordinal as u64);
                assert_eq!(
                    span.attributes.as_ref(),
                    &vec![crate::TelemetryAttribute::new(
                        "fixture.payload",
                        crate::TelemetryValue::String(Arc::from(noise(ordinal as u32 + 1)))
                    )]
                );
            }
            continue;
        }
        let points = service
            .query_metrics(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                partition: Some(partition),
                limit: 32,
                ..Default::default()
            })
            .expect("complete real resident/cold result");
        assert_eq!(points.len(), counts[index] as usize);
        for (ordinal, point) in points.iter().enumerate() {
            assert_eq!(point.timestamp_unix_nanos, ordinal as u64 + 1);
            assert_eq!(
                point.value,
                crate::MetricValue::Gauge(crate::NumberValue::Integer(ordinal as i64))
            );
            assert_eq!(point.record_ref.offset.get(), ordinal as u64);
        }
    }
}

struct UnavailableImmutableStore {
    local: LocalObjectStore,
    unavailable: AtomicBool,
    failed_puts: AtomicUsize,
}

impl UnavailableImmutableStore {
    fn check_put(&self) -> TelemetryResult<()> {
        if self.unavailable.load(Ordering::Acquire) {
            self.failed_puts.fetch_add(1, Ordering::Relaxed);
            Err(TelemetryError::ObjectStore(
                "injected immutable publication outage".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl TelemetryObjectStore for UnavailableImmutableStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        self.check_put()?;
        self.local.put_bytes_if_absent(key, bytes)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        self.check_put()?;
        self.local.put_file_if_absent(key, source)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        self.local.get(key, max_bytes)
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        self.local.get_range(key, range)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        self.local.head(key)
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        self.local.delete(key)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        self.local.compare_and_swap(key, expected, bytes)
    }
}

fn noise(mut state: u32) -> String {
    (0..2048)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            char::from(b'!' + (state % 90) as u8)
        })
        .collect()
}

#[test]
fn trace_publication_outage_retries_keep_resident_bytes_and_checkpoint_stable() {
    const SPANS: u32 = 128;
    let directory = TempDir::new("trace-publication-retry");
    let object_store = Arc::new(UnavailableImmutableStore {
        local: LocalObjectStore::open(directory.0.join("objects"))
            .expect("real local immutable store"),
        unavailable: AtomicBool::new(false),
        failed_puts: AtomicUsize::new(0),
    });
    let partition = TopicPartition::new(crate::TRACES_TOPIC_ID, LogicalPartitionId::new(0));
    let mut signals = ShardTelemetryConfig::default();
    signals.traces.logical_partitions = NonZeroU16::new(1).expect("one trace partition");
    signals.traces.physical_stripes = NonZeroU16::new(1).expect("one trace owner");
    signals.traces.head_memory_bytes_per_stripe = 64 * 1024;
    let config = OtlpSinkConfig {
        signals,
        correlations: CorrelationConfig::disabled(),
        object_tier: Some(SinkObjectTierConfig {
            store: SharedTelemetryObjectStore::new(object_store.clone()),
            spool_directory: directory.0.join("spool"),
            control_cache_directory: directory.0.join("control-cache"),
            payload_cache_directory: directory.0.join("payload-cache"),
            partitions: vec![partition],
            tier: ObjectTierConfig {
                target_group_payload_bytes: 1,
                max_group_payload_bytes: 8 * 1024 * 1024,
                max_blocks_per_group: 1024,
                max_control_object_bytes: 1024 * 1024,
                retirement_grace: Duration::ZERO,
                ..ObjectTierConfig::default()
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
                memory_bytes: 1024 * 1024,
                parsed_memory_bytes: 0,
            },
            warm_local_cache_on_publish: false,
        }),
        ..OtlpSinkConfig::default()
    };
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: (1..=SPANS)
                    .map(|index| {
                        let mut trace_id = [0u8; 16];
                        trace_id[..8].copy_from_slice(&u64::from(index).to_be_bytes());
                        let start = u64::from(index) * 60_000_000_000;
                        Span {
                            trace_id: trace_id.to_vec(),
                            span_id: u64::from(index).to_be_bytes().to_vec(),
                            name: format!("publication-fixture-{index}"),
                            start_time_unix_nano: start,
                            end_time_unix_nano: start + 1_000_000_000,
                            attributes: vec![KeyValue {
                                key: "fixture.payload".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue(noise(index))),
                                }),
                                ..KeyValue::default()
                            }],
                            ..Span::default()
                        }
                    })
                    .collect(),
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    };
    let events = OtlpTelemetryDecoder
        .decode_traces("tenant-a", &request.encode_to_vec())
        .expect("real OTLP trace decoding");
    let payload = crate::prepare_trace_envelope(partition, events)
        .expect("native trace envelope")
        .encode()
        .expect("actual STEL payload");
    let (mut append, expected, mut next) = durable_append(partition, payload, 91);
    append.reservation.record_count = NonZeroU32::new(SPANS).expect("trace count");
    append.reservation.last_offset = LogicalOffset::new(u64::from(SPANS) - 1);
    next.next_offset = LogicalOffset::new(u64::from(SPANS));
    let query = TraceQuery {
        tenant: Arc::from("tenant-a"),
        partition: Some(partition),
        limit: SPANS as usize + 1,
        ..Default::default()
    };
    let original;
    {
        let factory = TelemetrySinkFactory::new([ShardId::new(0)], config.clone())
            .expect("real tiered sink factory");
        factory
            .validate_append(&append.payload, append.reservation.record_count)
            .expect("actual append validation");
        let service = factory.service();
        let sink = factory
            .open_shard(ShardId::new(0))
            .expect("actual owner worker");
        object_store.unavailable.store(true, Ordering::Release);
        assert!(
            sink.apply(expected, std::slice::from_ref(&append), next)
                .is_err()
        );
        assert!(
            object_store.failed_puts.load(Ordering::Relaxed) > 0,
            "fixture must reach immutable publication"
        );
        let resident = service
            .retained_payload_bytes()
            .expect("real resident sealed payloads");
        assert!(
            resident > 64 * 1024,
            "fixture must seal more payload than the bounded recent retry state can retain"
        );
        original = service
            .query_traces(&query)
            .expect("original logical spans remain visible during outage");
        assert_eq!(original.len(), SPANS as usize);
        for (index, span) in original.iter().enumerate() {
            assert_eq!(
                span.name.as_ref(),
                format!("publication-fixture-{}", index + 1)
            );
        }
        for _ in 0..16 {
            assert!(
                sink.apply(expected, std::slice::from_ref(&append), next)
                    .is_err()
            );
            assert_eq!(
                service
                    .retained_payload_bytes()
                    .expect("resident retry accounting"),
                resident,
                "same-boundary retries must plateau"
            );
            assert_eq!(
                factory
                    .load_checkpoint(partition)
                    .expect("unchanged checkpoint")
                    .unwrap_or(expected),
                expected
            );
        }
        let mut changed = append.clone();
        changed.payload = Bytes::from_static(b"changed retry payload");
        let error = sink
            .apply(expected, &[changed], next)
            .expect_err("changed retry must fail before indexing");
        assert!(
            error
                .to_string()
                .contains("already indexed publication boundary")
        );
        assert_eq!(
            service
                .retained_payload_bytes()
                .expect("unchanged after conflicting retry"),
            resident
        );
        assert_eq!(
            service
                .query_traces(&query)
                .expect("conflicting retry cannot change spans"),
            original
        );

        object_store.unavailable.store(false, Ordering::Release);
        assert_eq!(
            sink.apply(expected, std::slice::from_ref(&append), next)
                .expect("original publication recovers"),
            DurableSinkApply::Applied
        );
        assert_eq!(
            factory
                .load_checkpoint(partition)
                .expect("committed checkpoint"),
            Some(next)
        );
        assert_eq!(
            service
                .retained_payload_bytes()
                .expect("published blocks released"),
            0
        );
        assert_eq!(
            service.query_traces(&query).expect("exact published spans"),
            original
        );
    }
    let recovered = TelemetrySinkFactory::new([ShardId::new(0)], config)
        .expect("reopen actual published catalog");
    let service = recovered.service();
    let _sink = recovered
        .open_shard(ShardId::new(0))
        .expect("recovered worker");
    assert_eq!(
        recovered
            .load_checkpoint(partition)
            .expect("recovered boundary"),
        Some(next)
    );
    assert_eq!(
        service
            .query_traces(&query)
            .expect("exact cold spans after restart"),
        original
    );
}
