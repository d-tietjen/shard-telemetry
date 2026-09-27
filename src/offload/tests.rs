use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
};
use prost::Message;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};
use tokio::net::TcpListener;

use super::*;
use crate::{
    DurableTelemetryConfig, LokiEntry, LokiStore, METRICS_TOPIC_ID, MetricQuery,
    NativeClientConfig, NativeServerConfig, OtlpTelemetryDecoder, StripeConfig, serve_native,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_offload_persists_progress_only_after_native_acknowledgement() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shard-telemetry-upstream-offload-{}-{nonce}",
        std::process::id()
    ));
    let source_directory = root.join("source");
    let destination_directory = root.join("destination");
    let config = |data_directory| DurableTelemetryConfig {
        data_directory,
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: 1,
        tenant_partitions: 1,
        append_linger: Duration::from_micros(250),
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let source =
        Arc::new(DurableTelemetryStore::open(config(source_directory.clone())).expect("source"));
    let destination =
        Arc::new(DurableTelemetryStore::open(config(destination_directory)).expect("destination"));
    let expected = LokiEntry {
        timestamp_unix_nanos: 100,
        labels: BTreeMap::from([("node".to_owned(), "node-a".to_owned())]),
        line: "store and forward".to_owned(),
        structured_metadata: BTreeMap::new(),
    };
    LokiStore::push(source.as_ref(), "tenant-a", vec![expected.clone()]).expect("source append");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_destination = Arc::clone(&destination);
    let server = tokio::spawn(async move {
        serve_native(
            listener,
            server_destination,
            NativeServerConfig::default(),
            async {
                let _ = stopped.await;
            },
        )
        .await
    });
    let client =
        Arc::new(ShardTelemetryClient::new(NativeClientConfig::new(address)).expect("client"));
    let metrics_only = UpstreamOffloader::open(
        Arc::clone(&source),
        Arc::clone(&client),
        UpstreamOffloadConfig::new(
            source_directory.join("metrics-only-offload-v1.json"),
            "test-node-a",
        )
        .with_signals([TelemetrySignal::Metrics]),
    )
    .expect("metrics-only offloader");
    let skipped = metrics_only.offload_once().await.expect("filtered offload");
    assert_eq!(skipped.scanned_partitions, 1);
    assert_eq!(skipped.offloaded_batches, 0);
    assert!(
        metrics_only
            .checkpoints()
            .expect("filtered checkpoints")
            .is_empty()
    );
    assert!(
        LokiStore::entries(destination.as_ref(), "tenant-a")
            .expect("destination remains empty")
            .is_empty()
    );

    let metric_request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        name: "node_cpu_seconds_total".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 200,
                                value: Some(number_data_point::Value::AsInt(7)),
                                ..NumberDataPoint::default()
                            }],
                        })),
                        ..Metric::default()
                    },
                    Metric {
                        name: "eden_local_only".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 201,
                                value: Some(number_data_point::Value::AsInt(3)),
                                ..NumberDataPoint::default()
                            }],
                        })),
                        ..Metric::default()
                    },
                ],
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        }],
    };
    let metric_points = OtlpTelemetryDecoder
        .decode_metrics("tenant-a", &metric_request.encode_to_vec())
        .expect("metric decode")
        .into_iter()
        .map(|event| {
            event.into_durable(
                ShardId::new(0),
                TopicPartition::new(METRICS_TOPIC_ID, LogicalPartitionId::new(0)),
                LogicalOffset::new(0),
            )
        })
        .collect::<Vec<_>>();
    source
        .append_metric_points(metric_points, true)
        .expect("source metric append");

    let selected_metrics = UpstreamOffloader::open(
        Arc::clone(&source),
        Arc::clone(&client),
        UpstreamOffloadConfig::new(
            source_directory.join("selected-metrics-offload-v1.json"),
            "test-node-a",
        )
        .with_signals([TelemetrySignal::Metrics])
        .with_metric_names(["node_cpu_seconds_total"]),
    )
    .expect("selected-metrics offloader");
    let selected = selected_metrics
        .offload_once()
        .await
        .expect("selected metric offload");
    assert_eq!(selected.scanned_partitions, 1);
    assert_eq!(selected.offloaded_batches, 1);
    assert_eq!(selected.offloaded_records, 1);
    assert_eq!(selected.skipped_batches, 1);
    assert_eq!(selected.skipped_records, 1);
    assert_eq!(selected.advanced_offsets, 2);
    assert_eq!(selected.checkpoint_writes, 1);
    assert_eq!(
        selected_metrics
            .checkpoints()
            .expect("metric checkpoints")
            .len(),
        1
    );
    assert_eq!(
        destination
            .query_metrics(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                name: Some(Arc::from("node_cpu_seconds_total")),
                limit: 10,
                ..MetricQuery::default()
            })
            .expect("selected metric query")
            .len(),
        1
    );
    assert!(
        destination
            .query_metrics(&MetricQuery {
                tenant: Arc::from("tenant-a"),
                name: Some(Arc::from("eden_local_only")),
                limit: 10,
                ..MetricQuery::default()
            })
            .expect("unselected metric query")
            .is_empty()
    );
    drop(selected_metrics);
    let recovered_selected_metrics = UpstreamOffloader::open(
        Arc::clone(&source),
        Arc::clone(&client),
        UpstreamOffloadConfig::new(
            source_directory.join("selected-metrics-offload-v1.json"),
            "test-node-a",
        )
        .with_signals([TelemetrySignal::Metrics])
        .with_metric_names(["node_cpu_seconds_total"]),
    )
    .expect("recovered selected-metrics offloader");
    let recovered_selected = recovered_selected_metrics
        .offload_once()
        .await
        .expect("recovered selected metric offload");
    assert_eq!(recovered_selected.advanced_offsets, 0);
    assert_eq!(recovered_selected.offloaded_batches, 0);
    assert_eq!(recovered_selected.skipped_batches, 0);
    assert_eq!(recovered_selected.checkpoint_writes, 0);

    let offloader = Arc::new(
        UpstreamOffloader::open(
            Arc::clone(&source),
            Arc::clone(&client),
            UpstreamOffloadConfig::new(
                source_directory.join("upstream-offload-v1.json"),
                "test-node-a",
            ),
        )
        .expect("offloader"),
    );
    let (offload_stop, offload_stopped) = tokio::sync::oneshot::channel();
    let loop_offloader = Arc::clone(&offloader);
    let offload_loop = tokio::spawn(async move {
        loop_offloader
            .run_until(
                UpstreamOffloadLoopConfig::new()
                    .with_idle_interval(Duration::from_millis(5))
                    .with_retry_interval(Duration::from_millis(5)),
                async {
                    let _ = offload_stopped.await;
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if LokiStore::entries(destination.as_ref(), "tenant-a").expect("destination query")
                == vec![expected.clone()]
                && offloader.checkpoints().expect("checkpoints").len() == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("background offload completes");
    offload_stop.send(()).expect("stop offload loop");
    let report = offload_loop
        .await
        .expect("offload loop joins")
        .expect("offload loop succeeds");
    assert_eq!(report.failed_rounds, 0);
    assert_eq!(
        LokiStore::entries(destination.as_ref(), "tenant-a").expect("destination query"),
        vec![expected.clone()]
    );
    assert_eq!(offloader.checkpoints().expect("checkpoints").len(), 1);

    // Both sources start at the same local WAL offset and emit the exact
    // same envelope. Their distinct stable source IDs must prevent the
    // central native receipt catalog from treating the second node as a
    // retry of the first node's append.
    let second_source_directory = root.join("second-source");
    let second_source = Arc::new(
        DurableTelemetryStore::open(config(second_source_directory.clone()))
            .expect("second source"),
    );
    LokiStore::push(second_source.as_ref(), "tenant-a", vec![expected.clone()])
        .expect("second source append");
    let second_offloader = UpstreamOffloader::open(
        Arc::clone(&second_source),
        client,
        UpstreamOffloadConfig::new(
            second_source_directory.join("upstream-offload-v2.json"),
            "test-node-b",
        ),
    )
    .expect("second offloader");
    let second_report = second_offloader
        .offload_once()
        .await
        .expect("second offload");
    assert_eq!(second_report.offloaded_batches, 1);
    assert_eq!(second_report.offloaded_records, 1);
    assert_eq!(
        LokiStore::entries(destination.as_ref(), "tenant-a")
            .expect("both sources reach destination")
            .len(),
        2
    );

    stop.send(()).expect("stop");
    server
        .await
        .expect("server joins")
        .expect("server succeeds");
    drop(source);
    drop(second_source);
    drop(destination);
    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn metric_name_filter_is_explicit_and_requires_metrics() {
    assert!(
        UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "")
            .validate()
            .is_err()
    );
    assert!(
        UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
            .with_metric_names(std::iter::empty::<&str>())
            .validate()
            .is_err()
    );
    assert!(
        UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
            .with_signals([TelemetrySignal::Logs])
            .with_metric_names(["node_cpu_seconds_total"])
            .validate()
            .is_err()
    );
    assert!(
        UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
            .with_max_in_flight_partitions(0)
            .validate()
            .is_err()
    );
    assert!(
        UpstreamOffloadConfig::new(PathBuf::from("checkpoint"), "test-node")
            .with_signals([TelemetrySignal::Metrics])
            .with_metric_names(["node_cpu_seconds_total"])
            .validate()
            .is_ok()
    );
}

#[test]
fn checkpoint_journal_binds_the_stable_source_identity() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shard-telemetry-offload-source-id-{}-{nonce}",
        std::process::id()
    ));
    let path = root.join("offload-v2.json");
    let partition = TopicPartition::new(TopicId::new(1), LogicalPartitionId::new(0));
    let mut journal =
        CheckpointJournal::open(path.clone(), Arc::from("node-a")).expect("open journal");
    journal
        .advance(partition, LogicalOffset::new(1))
        .expect("persist checkpoint");
    assert!(CheckpointJournal::open(path.clone(), Arc::from("node-a")).is_err());
    drop(journal);
    let reopened = CheckpointJournal::open(path.clone(), Arc::from("node-a"))
        .expect("same source reopens after prior owner exits");
    drop(reopened);
    assert!(CheckpointJournal::open(path, Arc::from("node-b")).is_err());

    std::fs::remove_dir_all(root).expect("cleanup");
}

#[test]
fn v1_checkpoint_journal_migrates_with_its_legacy_retry_namespace() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shard-telemetry-offload-v1-migration-{}-{nonce}",
        std::process::id()
    ));
    let path = root.join("offload.json");
    std::fs::create_dir_all(&root).expect("directory");
    std::fs::write(
        &path,
        serde_json::to_vec(&PersistedCheckpoints {
            version: 1,
            source_id: None,
            retry_namespace: None,
            checkpoints: vec![PersistedCheckpoint {
                topic_id: 9,
                partition_id: 2,
                next_offset: 11,
            }],
        })
        .expect("legacy journal"),
    )
    .expect("write legacy journal");
    let partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(2));
    let mut journal =
        CheckpointJournal::open(path.clone(), Arc::from("node-a")).expect("migrate v1 journal");
    assert!(matches!(
        journal.retry_namespace(),
        RetryNamespace::LegacyV1
    ));
    assert_eq!(journal.next(partition), Some(LogicalOffset::new(11)));
    journal
        .advance(partition, LogicalOffset::new(12))
        .expect("persist migration");
    drop(journal);
    let persisted: PersistedCheckpoints =
        serde_json::from_slice(&std::fs::read(&path).expect("read migrated journal"))
            .expect("decode migrated journal");
    assert_eq!(persisted.version, OFFLOAD_CHECKPOINT_VERSION);
    assert_eq!(persisted.source_id.as_deref(), Some("node-a"));
    assert_eq!(persisted.retry_namespace.as_deref(), Some("legacy-v1"));
    std::fs::remove_dir_all(root).expect("cleanup");
}
