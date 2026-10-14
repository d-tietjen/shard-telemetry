//! Real store/checkpoint/recovery boundaries for local provider status.

use super::*;
use crate::{
    LocalPartitionPopulation, ProviderCoverage, ProviderStatusPartition, ProviderStatusRequest,
    TelemetrySignal,
};
use opentelemetry_proto::tonic::{
    collector::{metrics::v1::ExportMetricsServiceRequest, trace::v1::ExportTraceServiceRequest},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use std::time::{SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);

impl Directory {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "shard-telemetry-status-{name}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).expect("clock").as_nanos()
        )))
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).expect("fixture cleanup");
        }
    }
}

fn config(directory: &Directory) -> DurableTelemetryConfig {
    DurableTelemetryConfig {
        data_directory: directory.0.clone(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: true,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::ZERO,
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(10),
    }
}

pub(crate) fn all_signal_request() -> ProviderStatusRequest {
    ProviderStatusRequest {
        tenant: "tenant-a".into(),
        organization_uuid: Some([7; 16]),
        telemetry_plane: Some("private".into()),
        start_timestamp_unix_nanos: Some(1),
        end_timestamp_unix_nanos: Some(1_000),
        partitions: [
            TelemetrySignal::Logs,
            TelemetrySignal::Traces,
            TelemetrySignal::Metrics,
        ]
        .into_iter()
        .map(|signal| ProviderStatusPartition {
            signal,
            partition_id: 0,
        })
        .collect(),
    }
}

fn native_batch() -> crate::NativeTelemetryBatch {
    let decoder = crate::OtlpTelemetryDecoder;
    let trace_partition = TopicPartition::new(crate::TRACES_TOPIC_ID, LogicalPartitionId::new(0));
    let metric_partition = TopicPartition::new(crate::METRICS_TOPIC_ID, LogicalPartitionId::new(0));
    let traces = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![1; 16],
                    span_id: vec![2; 8],
                    name: "status-proof-span".into(),
                    start_time_unix_nano: 10,
                    end_time_unix_nano: 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let metrics = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "status-proof-gauge".into(),
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 30,
                            value: Some(number_data_point::Value::AsInt(7)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    crate::NativeTelemetryBatch {
        partitions: vec![
            crate::NativePartitionAppend {
                topic_partition: TopicPartition::new(
                    crate::LOGS_TOPIC_ID,
                    LogicalPartitionId::new(0),
                ),
                envelope: crate::prepare_loki_log_envelope(
                    "tenant-a",
                    vec![LokiEntry {
                        timestamp_unix_nanos: 25,
                        labels: BTreeMap::new(),
                        line: "status proof log".into(),
                        structured_metadata: BTreeMap::new(),
                    }],
                )
                .expect("log STEL"),
                transient_context: None,
            },
            crate::NativePartitionAppend {
                topic_partition: trace_partition,
                envelope: crate::prepare_trace_envelope(
                    trace_partition,
                    decoder
                        .decode_traces("tenant-a", &traces.encode_to_vec())
                        .expect("OTLP traces"),
                )
                .expect("trace STEL"),
                transient_context: None,
            },
            crate::NativePartitionAppend {
                topic_partition: metric_partition,
                envelope: crate::prepare_metric_envelope(
                    metric_partition,
                    decoder
                        .decode_metrics("tenant-a", &metrics.encode_to_vec())
                        .expect("OTLP metrics"),
                )
                .expect("metric STEL"),
                transient_context: None,
            },
        ],
    }
}

#[test]
fn provider_status_observes_all_signals_first_offset_zero_and_reopen() {
    let directory = Directory::new("all-signals");
    let config = config(&directory);
    let request = all_signal_request();
    let store = DurableTelemetryStore::open(config.clone()).expect("real store");
    let empty = store
        .local_provider_status(&request)
        .expect("empty checkpoint observation");
    assert_eq!(empty.request, request);
    for status in &empty.partitions {
        assert_eq!(status.population, LocalPartitionPopulation::VerifiedEmpty);
        assert_eq!(
            status
                .progress
                .expect("observed zero frontiers")
                .indexed_end,
            Some(0)
        );
    }
    assert_eq!(empty.source_interval_coverage, ProviderCoverage::Unknown);
    assert_eq!(empty.scoped_loss_coverage, ProviderCoverage::Unknown);
    let ack = store
        .append_telemetry_batch(&native_batch(), true)
        .expect("real native append and index ACK");
    assert_eq!(ack.partitions.len(), 3);
    assert!(
        ack.partitions
            .iter()
            .all(|partition| partition.first_offset == 0 && partition.last_offset == 0)
    );
    let observed = store
        .local_provider_status(&request)
        .expect("real applied checkpoints");
    for status in &observed.partitions {
        let progress = status.progress.expect("coherent applied frontiers");
        assert_eq!(status.population, LocalPartitionPopulation::Allocated);
        assert_eq!(
            progress.indexed_end,
            Some(1),
            "exclusive applied end follows actual offset zero"
        );
        assert_eq!(progress.last_stable_offset, 1);
        assert_eq!(progress.contiguous_log_end, 1);
    }
    let wire = crate::encode_native_provider_status_response(&observed)
        .expect("bounded real status encoding");
    assert_eq!(
        crate::decode_native_provider_status_response(&wire).expect("wire decode"),
        observed
    );
    drop(store);
    let recovered = DurableTelemetryStore::open(config).expect("real journal/WAL reopen");
    assert_eq!(
        recovered
            .local_provider_status(&request)
            .expect("recovered checkpoint proof"),
        observed
    );
    drop(recovered);
}

#[test]
fn provider_status_keeps_applied_prefix_distinct_from_expired_query_data() {
    let directory = Directory::new("retention");
    let mut config = config(&directory);
    config.object_store_directory = Some(directory.0.join("objects"));
    let store = DurableTelemetryStore::open(config).expect("tiered real store");
    let mut request = all_signal_request();
    request.partitions.truncate(1);
    let mut batch = native_batch();
    batch.partitions.truncate(1);
    store
        .append_telemetry_batch(&batch, true)
        .expect("offset zero");
    LokiStore::flush(&store, Duration::from_secs(10)).expect("publish real payload");
    store
        .compact_retention_before(1_000)
        .expect("expire payload and reclaim WAL");
    let query = NativeQuery {
        tenant: "tenant-a".into(),
        labels: BTreeMap::new(),
        terms: Vec::new(),
        start_timestamp_unix_nanos: None,
        end_timestamp_unix_nanos: None,
        limit: 10,
        direction: NativeQueryDirection::OldestFirst,
    };
    assert!(
        store
            .query_native(&query)
            .expect("retained query")
            .is_empty()
    );
    let status = store
        .local_provider_status(&request)
        .expect("applied metadata after retention");
    let progress = status.partitions[0]
        .progress
        .expect("real retained/applied metadata");
    assert_eq!(
        progress.indexed_end,
        Some(1),
        "applied work remains advanced after rows expire"
    );
    assert_eq!(progress.wal_retained_start, 1);
    assert_eq!(status.query_retention_coverage, ProviderCoverage::Unknown);
    assert_eq!(status.source_interval_coverage, ProviderCoverage::Unknown);
    assert_eq!(status.indexed_quorum, ProviderCoverage::Unknown);
    drop(store);
}

#[test]
fn provider_status_declared_selection_and_sampling_budget_fail_closed() {
    let directory = Directory::new("selection");
    let store = DurableTelemetryStore::open(config(&directory)).expect("real store");
    let request = all_signal_request();
    let exhausted = store
        .local_provider_status_with_budget(&request, Duration::ZERO)
        .expect("finite empty budget");
    assert!(
        exhausted
            .partitions
            .iter()
            .all(|partition| partition.progress.is_none()
                && partition.population == LocalPartitionPopulation::Unknown)
    );
    let mut invalid = request.clone();
    invalid.partitions[0].partition_id = 1;
    assert!(
        store
            .local_provider_status(&invalid)
            .expect_err("unowned selection")
            .is_bad_request()
    );
    invalid = request.clone();
    invalid.partitions.push(invalid.partitions[0]);
    assert!(
        store
            .local_provider_status(&invalid)
            .expect_err("duplicate selection")
            .is_bad_request()
    );
    assert_eq!(
        store
            .local_provider_status(&request)
            .expect("later valid observation")
            .partitions[0]
            .population,
        LocalPartitionPopulation::VerifiedEmpty
    );
    drop(store);
}

#[test]
fn provider_status_preserves_independent_partition_and_checkpoint_coordinates() {
    let directory = Directory::new("coordinates");
    let mut config = config(&directory);
    config.tenant_partitions = 2;
    let store = DurableTelemetryStore::open(config).expect("two real partitions per signal");
    let mut first = native_batch();
    first.partitions.truncate(1);
    let log = first.partitions[0].topic_partition;
    for _ in 0..3 {
        store
            .append_telemetry_batch(&first, true)
            .expect("advance only log partition zero");
    }
    let events = (0..3)
        .map(|ordinal| LokiEntry {
            timestamp_unix_nanos: 100 + ordinal,
            labels: BTreeMap::new(),
            line: format!("multi-record extent {ordinal}"),
            structured_metadata: BTreeMap::new(),
        })
        .collect();
    first.partitions[0].envelope =
        crate::prepare_loki_log_envelope("tenant-a", events).expect("three records in one extent");
    store
        .append_telemetry_batch(&first, true)
        .expect("different record and placement coordinates");
    let mut last = native_batch();
    last.partitions.truncate(1);
    last.partitions[0].topic_partition =
        TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(1));
    let ack = store
        .append_telemetry_batch(&last, true)
        .expect("first extent on independent partition");
    assert_eq!(ack.partitions[0].first_offset, 0);
    let all = native_batch();
    for partition in all.partitions.into_iter().skip(1) {
        store
            .append_telemetry_batch(
                &crate::NativeTelemetryBatch {
                    partitions: vec![partition],
                },
                true,
            )
            .expect("independent signal");
    }
    let mut request = all_signal_request();
    request.partitions.push(ProviderStatusPartition {
        signal: TelemetrySignal::Logs,
        partition_id: 1,
    });
    let status = store
        .local_provider_status(&request)
        .expect("actual independent checkpoints");
    let first = status.partitions[0].progress.expect("advanced checkpoint");
    assert_eq!(first.indexed_end, Some(6));
    assert_eq!(first.indexed_next_placement_sequence, Some(4));
    assert_eq!(status.partitions[0].partition.topic_partition(), log);
    for partition in &status.partitions[1..] {
        let progress = partition.progress.expect("independent real checkpoint");
        assert_eq!(progress.indexed_end, Some(1));
        assert_eq!(progress.indexed_next_placement_sequence, Some(1));
    }
    drop(store);
}

#[test]
fn provider_status_attached_empty_partition_keeps_ownership_unknown() {
    let directory = Directory::new("attached");
    let standalone = DurableTelemetryStore::open(config(&directory))
        .expect("real externally owned engine and sink");
    let attached = DurableTelemetryStore::attach(
        directory.0.join("host-metadata"),
        Arc::clone(&standalone.engine),
        standalone.service.clone(),
        1,
        1,
        Duration::from_secs(10),
        None,
    )
    .expect("actual host attachment");
    let request = all_signal_request();
    let owned = standalone
        .local_provider_status(&request)
        .expect("owned native state");
    let observed = attached
        .local_provider_status(&request)
        .expect("attached native state");
    for (owned, attached) in owned.partitions.iter().zip(&observed.partitions) {
        assert_eq!(owned.population, LocalPartitionPopulation::VerifiedEmpty);
        assert_eq!(
            attached.progress, owned.progress,
            "same authoritative checkpoint/frontiers"
        );
        assert_eq!(
            attached.population,
            LocalPartitionPopulation::Unknown,
            "external ownership is not standalone proof"
        );
    }
    assert_eq!(observed.ha_epoch, ProviderCoverage::Unknown);
    assert_eq!(observed.indexed_quorum, ProviderCoverage::Unknown);
    drop(attached);
    drop(standalone);
}

#[test]
fn provider_status_abort_only_activity_is_not_empty_or_a_record_existence_claim() {
    let directory = Directory::new("aborted");
    let store = DurableTelemetryStore::open(config(&directory)).expect("real store and sink");
    let mut request = all_signal_request();
    request.partitions.truncate(1);
    let partition = request.partitions[0].topic_partition();
    let mut batch = native_batch();
    batch.partitions.truncate(1);
    let envelope = batch.partitions[0]
        .envelope
        .encode()
        .expect("actual STEL payload");
    store
        .engine
        .append(AppendRequest {
            request_id: 8,
            topic_id: partition.topic_id,
            partition_id: partition.partition_id,
            record_count: 1,
            payload: Bytes::from(envelope),
            durability: Durability::Leader,
            producer: None,
            leader_epoch: None,
            atomic_group: Some(shard_stream_protocol::AtomicGroupIdentity {
                transaction_id: 77,
                chunk_sequence: 0,
            }),
            extension_context: None,
        })
        .expect("actual unresolved WAL group");
    store
        .engine
        .wait_for_durable_sink_checkpoint(DurableSinkCheckpoint {
            topic_partition: partition,
            next_placement_sequence: PlacementSequence::new(1),
            next_offset: LogicalOffset::new(1),
        })
        .expect("default all-durable sink has previously applied the extent");
    let unresolved = store
        .local_provider_status(&request)
        .expect("unstable applied-prefix observation");
    assert!(
        unresolved.partitions[0].progress.is_none(),
        "applied checkpoint beyond stable WAL must remain unknown"
    );
    store
        .engine
        .abort_atomic_group(partition, 77)
        .expect("actual durable abort marker");
    assert!(
        store
            .engine
            .fetch(FetchRequest {
                request_id: 9,
                topic_id: partition.topic_id,
                partition_id: partition.partition_id,
                start_offset: LogicalOffset::new(0),
                max_bytes: 64 * 1024,
                mode: FetchMode::Ordered,
            })
            .expect("ordinary authoritative WAL fetch excludes abort-only extent")
            .is_empty()
    );
    let resolved = store
        .local_provider_status(&request)
        .expect("resolved abort frontier");
    assert_eq!(
        resolved.partitions[0].population,
        LocalPartitionPopulation::Allocated,
        "nonzero allocation is not a positive durable record/query-row assertion"
    );
    assert_eq!(
        resolved.partitions[0]
            .progress
            .expect("real resolved native metadata")
            .allocated_end,
        1
    );
    assert_eq!(resolved.source_interval_coverage, ProviderCoverage::Unknown);
    assert_eq!(resolved.query_retention_coverage, ProviderCoverage::Unknown);
    drop(store);
}

#[test]
fn provider_status_does_not_promote_durable_wal_or_query_rows_during_publication_outage() {
    let directory = Directory::new("outage");
    let objects = directory.0.join("objects");
    let saved = directory.0.join("objects-saved");
    let mut embedded =
        crate::EmbeddedTelemetryConfig::bounded_local(&directory.0, Duration::from_secs(3_600));
    embedded.store.tenant_partitions = 1;
    embedded = embedded.with_max_ram_bytes(512 * 1024);
    let mut config = config(&directory);
    config.object_store_directory = Some(objects.clone());
    let mut limits = embedded.local_limits;
    for signal in [
        &mut limits.signals.logs,
        &mut limits.signals.traces,
        &mut limits.signals.metrics,
    ] {
        signal.retention = None;
    }
    let tier = ObjectTierConfig {
        target_group_payload_bytes: 1,
        max_blocks_per_group: 1,
        ..ObjectTierConfig::default()
    };
    let store = DurableTelemetryStore::open_with_object_tier_config_and_local_limits(
        config.clone(),
        tier,
        limits.clone(),
    )
    .expect("real bounded tier and WAL");
    let mut request = all_signal_request();
    request.partitions = vec![request.partitions[1]];
    assert_eq!(
        store
            .local_provider_status(&request)
            .expect("loaded initial applied checkpoint")
            .partitions[0]
            .progress
            .expect("initial metadata")
            .indexed_end,
        Some(0)
    );
    // The actual LocalObjectStore now encounters ENOTDIR on immutable publication.
    fs::rename(&objects, &saved).expect("preserve actual object backend");
    fs::write(&objects, b"synthetic publication outage").expect("block backend directory path");
    let mut batch = native_batch();
    batch.partitions = vec![batch.partitions.remove(1)];
    let ack = store
        .append_telemetry_batch(&batch, false)
        .expect("local durable WAL ACK without index ACK");
    assert_eq!(ack.partitions[0].first_offset, 0);
    let started = Instant::now();
    while store.engine.durable_sink_stats().failed_attempts == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "real publication failure must be observed"
        );
        std::thread::yield_now();
    }
    let blocked = store
        .local_provider_status(&request)
        .expect("outage observation");
    let progress = blocked.partitions[0]
        .progress
        .expect("unchanged actual frontiers");
    assert_eq!(progress.contiguous_log_end, 1);
    assert_eq!(progress.last_stable_offset, 1);
    assert_eq!(
        progress.indexed_end,
        Some(0),
        "durable WAL alone cannot advance the applied checkpoint"
    );
    assert_eq!(blocked.source_interval_coverage, ProviderCoverage::Unknown);
    let query = crate::TraceQuery {
        tenant: Arc::from("tenant-a"),
        partition: Some(request.partitions[0].topic_partition()),
        limit: 10,
        ..Default::default()
    };
    assert_eq!(
        store
            .query_traces(&query)
            .expect("resident row indexed before failed publication")
            .len(),
        1,
        "a visible row is still insufficient to advance the applied publication checkpoint"
    );
    fs::remove_file(&objects).expect("clear synthetic outage");
    fs::rename(&saved, &objects).expect("restore the exact preserved backend");
    store
        .engine
        .wait_for_durable_sink_checkpoint(DurableSinkCheckpoint {
            topic_partition: request.partitions[0].topic_partition(),
            next_placement_sequence: PlacementSequence::new(1),
            next_offset: LogicalOffset::new(1),
        })
        .expect("real successful publication advances checkpoint");
    assert_eq!(
        store
            .local_provider_status(&request)
            .expect("recovered observation")
            .partitions[0]
            .progress
            .expect("recovered prefix")
            .indexed_end,
        Some(1)
    );
    drop(store);
    let reopened =
        DurableTelemetryStore::open_with_object_tier_config_and_local_limits(config, tier, limits)
            .expect("real durable reopen");
    assert_eq!(
        reopened
            .local_provider_status(&request)
            .expect("reopened published checkpoint")
            .partitions[0]
            .progress
            .expect("reopened prefix")
            .indexed_end,
        Some(1)
    );
    assert_eq!(
        reopened
            .query_traces(&query)
            .expect("exact recovered row")
            .len(),
        1
    );
    drop(reopened);
}
