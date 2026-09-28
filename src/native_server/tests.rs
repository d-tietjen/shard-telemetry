use std::collections::BTreeMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use shard_stream_core::{LogicalPartitionId, TopicPartition};
use tokio::net::TcpStream;

use super::*;
use crate::{
    DurableTelemetryConfig, LokiEntry, NativeLogQueryResult, NativePartitionAppend, NativeQuery,
    NativeQueryDirection, NativeTelemetryAppendAck, NativeTelemetryBatch, ServiceLifecycle,
    SingleTenantConfig, StripeConfig, decode_native_log_query_result, encode_native_query,
    prepare_loki_log_envelope,
};

#[derive(Debug)]
struct DenyGate;

impl NativeRequestGate for DenyGate {
    fn check(&self) -> Result<(), String> {
        Err("not the current leader".into())
    }
}

#[test]
fn native_server_rejects_an_inbound_budget_smaller_than_one_frame() {
    let config = NativeServerConfig {
        max_frame_bytes: 1024,
        max_inbound_bytes_total: 1023,
        ..NativeServerConfig::default()
    };
    let error = config.validate().expect_err("invalid inbound budget");
    assert!(
        error
            .to_string()
            .contains("total inbound bytes must cover one frame")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_server_reserves_global_input_before_reading_a_frame_body() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-native-input-budget-{}-{nonce}",
        std::process::id()
    ));
    let store = Arc::new(
        DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: std::time::Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: std::time::Duration::from_secs(30),
        })
        .expect("store"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_native(
            listener,
            server_store,
            NativeServerConfig {
                max_frame_bytes: 16,
                max_inbound_bytes_total: 16,
                ..NativeServerConfig::default()
            },
            async {
                let _ = stopped.await;
            },
        )
        .await
    });

    let header = NativeFrameHeader::request(NativeOpcode::Ping, 1, &[0; 16])
        .expect("header")
        .encode();
    let mut first = TcpStream::connect(address).await.expect("first connect");
    first.write_all(&header).await.expect("first header");
    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    let mut second = TcpStream::connect(address).await.expect("second connect");
    second.write_all(&header).await.expect("second header");
    let mut closed = [0_u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(1), second.read(&mut closed))
        .await
        .expect("inbound budget closes excess connection")
        .expect("read");
    assert_eq!(read, 0);

    drop(first);
    stop.send(()).expect("stop");
    server
        .await
        .expect("server joins")
        .expect("server succeeds");
    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_protocol_pings_appends_and_queries_with_request_ids() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-native-server-{}-{nonce}",
        std::process::id()
    ));
    let store = Arc::new(
        DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 2,
            tenant_partitions: 8,
            append_linger: std::time::Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: std::time::Duration::from_secs(30),
        })
        .expect("store"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_native(
            listener,
            server_store,
            NativeServerConfig::default(),
            async {
                let _ = stopped.await;
            },
        )
        .await
    });
    let mut client = TcpStream::connect(address).await.expect("connect");

    let ping = NativeFrame::request(NativeOpcode::Ping, 7, b"hello".to_vec()).expect("ping");
    write_frame(&mut client, &ping).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 7);
    assert_eq!(response.header.status, NativeStatus::Ok);
    assert_eq!(response.payload, b"hello");

    let entry = LokiEntry {
        timestamp_unix_nanos: 100,
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        line: "native timeout".to_owned(),
        structured_metadata: BTreeMap::from([("trace".to_owned(), "abc".to_owned())]),
    };
    let topic_partition = TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(0));
    let batch = NativeTelemetryBatch {
        partitions: vec![NativePartitionAppend {
            topic_partition,
            envelope: prepare_loki_log_envelope("tenant-a", vec![entry.clone()])
                .expect("log envelope"),
            transient_context: None,
        }],
    }
    .encode()
    .expect("batch");
    let append = NativeFrame::request(NativeOpcode::Append, 8, batch).expect("append");
    write_frame(&mut client, &append).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 8);
    assert_eq!(response.header.status, NativeStatus::Ok);
    assert_eq!(
        NativeTelemetryAppendAck::decode(&response.payload)
            .expect("ack")
            .partitions[0]
            .first_offset,
        0
    );

    // A retry after a lost acknowledgement uses the same native request
    // ID. It must return the original receipt rather than append a second
    // copy of the log event.
    write_frame(&mut client, &append).await;
    let retry = read_frame(&mut client).await;
    assert_eq!(retry.header.status, NativeStatus::Ok);
    assert_eq!(retry.payload, response.payload);

    let query = NativeQuery {
        tenant: "tenant-a".to_owned(),
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        terms: vec!["timeout".to_owned()],
        start_timestamp_unix_nanos: None,
        end_timestamp_unix_nanos: None,
        limit: 10,
        direction: NativeQueryDirection::OldestFirst,
    };
    let query = NativeFrame::request(
        NativeOpcode::Query,
        9,
        encode_native_query(&query).expect("query"),
    )
    .expect("query frame");
    write_frame(&mut client, &query).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 9);
    assert_eq!(response.header.status, NativeStatus::Ok);
    let NativeLogQueryResult { tenant, entries } =
        decode_native_log_query_result(&response.payload).expect("results");
    assert_eq!(tenant, "tenant-a");
    assert_eq!(entries, vec![entry.clone()]);

    let multi_partition_batch = NativeTelemetryBatch {
        partitions: vec![
            NativePartitionAppend {
                topic_partition,
                envelope: prepare_loki_log_envelope("tenant-a", vec![entry.clone()])
                    .expect("first multi-partition envelope"),
                transient_context: None,
            },
            NativePartitionAppend {
                topic_partition: TopicPartition::new(
                    crate::LOGS_TOPIC_ID,
                    LogicalPartitionId::new(1),
                ),
                envelope: prepare_loki_log_envelope("tenant-a", vec![entry])
                    .expect("second multi-partition envelope"),
                transient_context: None,
            },
        ],
    }
    .encode()
    .expect("multi-partition batch");
    let append_untracked =
        NativeFrame::request(NativeOpcode::AppendUntracked, 10, multi_partition_batch)
            .expect("multi-partition append");
    write_frame(&mut client, &append_untracked).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 10);
    assert_eq!(response.header.status, NativeStatus::Ok);
    let acknowledgement =
        NativeTelemetryAppendAck::decode(&response.payload).expect("multi-partition ack");
    assert_eq!(acknowledgement.partitions.len(), 2);
    assert_eq!(
        acknowledgement.partitions[0].topic_partition,
        topic_partition
    );
    assert_eq!(
        acknowledgement.partitions[1].topic_partition,
        TopicPartition::new(crate::LOGS_TOPIC_ID, LogicalPartitionId::new(1))
    );

    drop(client);
    stop.send(()).expect("stop");
    server
        .await
        .expect("server joins")
        .expect("server succeeds");
    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_native_protocol_requires_authentication_before_operations() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-native-auth-{}-{nonce}",
        std::process::id()
    ));
    let store = Arc::new(
        DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: false,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: std::time::Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: std::time::Duration::from_secs(30),
        })
        .expect("store"),
    );
    let lifecycle = Arc::new(ServiceLifecycle::new());
    lifecycle.mark_ready();
    let runtime = Arc::new(
        ProductionRuntime::new(
            SingleTenantConfig {
                tenant: Arc::from("tenant-a"),
                bearer_token: Arc::from("0123456789abcdef"),
                max_http_in_flight: 4,
                max_ingest_in_flight: 2,
                max_query_in_flight: 2,
                ingest_bytes_per_second: 0,
                ingest_burst_bytes: 0,
                max_tail_subscribers: 1,
                max_native_connections: 4,
                query_timeout: std::time::Duration::from_secs(30),
                native_auth_timeout: std::time::Duration::from_millis(50),
            },
            lifecycle,
        )
        .expect("runtime"),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_store = Arc::clone(&store);
    let server = tokio::spawn(async move {
        serve_native(
            listener,
            server_store,
            NativeServerConfig {
                production: Some(runtime),
                request_gate: Some(Arc::new(DenyGate)),
                ..NativeServerConfig::default()
            },
            async {
                let _ = stopped.await;
            },
        )
        .await
    });

    let mut idle = TcpStream::connect(address).await.expect("idle connect");
    let mut closed = [0_u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(1), idle.read(&mut closed))
        .await
        .expect("authentication deadline closes idle connection")
        .expect("idle read");
    assert_eq!(read, 0);

    let mut unauthenticated = TcpStream::connect(address).await.expect("connect");
    let ping = NativeFrame::request(NativeOpcode::Ping, 1, b"hello".to_vec()).expect("ping");
    write_frame(&mut unauthenticated, &ping).await;
    let response = read_frame(&mut unauthenticated).await;
    assert_eq!(response.header.status, NativeStatus::Unauthorized);

    let mut client = TcpStream::connect(address).await.expect("connect");
    let authenticate =
        NativeFrame::request(NativeOpcode::Authenticate, 2, b"0123456789abcdef".to_vec())
            .expect("authenticate");
    write_frame(&mut client, &authenticate).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 2);
    assert_eq!(response.header.status, NativeStatus::Ok);

    let ping = NativeFrame::request(NativeOpcode::Ping, 3, b"ready".to_vec()).expect("ping");
    write_frame(&mut client, &ping).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 3);
    assert_eq!(response.header.status, NativeStatus::Ok);
    assert_eq!(response.payload, b"ready");

    let query = NativeFrame::request(NativeOpcode::Query, 4, Vec::new()).expect("query");
    write_frame(&mut client, &query).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.request_id, 4);
    assert_eq!(response.header.status, NativeStatus::Unavailable);
    assert_eq!(response.payload, b"not the current leader");

    let page_query =
        NativeFrame::request(NativeOpcode::QueryLogsPage, 5, Vec::new()).expect("page query");
    write_frame(&mut client, &page_query).await;
    let response = read_frame(&mut client).await;
    assert_eq!(response.header.status, NativeStatus::Unavailable);
    assert_eq!(response.payload, b"not the current leader");

    drop(unauthenticated);
    drop(client);
    stop.send(()).expect("stop");
    server
        .await
        .expect("server joins")
        .expect("server succeeds");
    drop(store);
    fs::remove_dir_all(directory).expect("cleanup");
}

async fn write_frame(stream: &mut TcpStream, frame: &NativeFrame) {
    stream
        .write_all(&frame.header.encode())
        .await
        .expect("header");
    stream.write_all(&frame.payload).await.expect("payload");
}

async fn read_frame(stream: &mut TcpStream) -> NativeFrame {
    let mut header = [0; NATIVE_FRAME_HEADER_BYTES];
    stream.read_exact(&mut header).await.expect("header");
    let header = NativeFrameHeader::decode(&header).expect("decode header");
    let mut payload = vec![0; header.payload_len as usize];
    stream.read_exact(&mut payload).await.expect("payload");
    header.verify_payload(&payload).expect("checksum");
    NativeFrame { header, payload }
}
