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
