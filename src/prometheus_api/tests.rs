use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::Request;
use tower::ServiceExt;

use super::*;
use crate::{DurableTelemetryConfig, StripeConfig};

static TEST_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn test_service() -> (PrometheusService, std::path::PathBuf) {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-prometheus-api-{}-{nonce}-{}",
        std::process::id(),
        TEST_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed)
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
            append_linger: Duration::from_micros(250),
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .expect("store opens"),
    );
    let service = PrometheusService::new(
        store,
        PrometheusApiConfig {
            tenant: Arc::from("tenant-a"),
            logical_partitions: NonZeroU16::new(8).unwrap(),
            ..PrometheusApiConfig::default()
        },
    )
    .expect("service");
    (service, directory)
}

#[test]
fn version_header_must_match_negotiated_schema() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-prometheus-remote-write-version",
        HeaderValue::from_static("2.0.0"),
    );
    assert!(validate_version_header(&headers, RemoteWriteVersion::V2).is_ok());
    assert!(validate_version_header(&headers, RemoteWriteVersion::V1).is_err());
}

#[test]
fn success_reports_all_required_written_headers() {
    let response = write_success(RemoteWriteStats {
        samples: 3,
        histograms: 2,
        exemplars: 1,
    });
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response.headers()["x-prometheus-remote-write-samples-written"],
        "3"
    );
    assert_eq!(
        response.headers()["x-prometheus-remote-write-histograms-written"],
        "2"
    );
    assert_eq!(
        response.headers()["x-prometheus-remote-write-exemplars-written"],
        "1"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_prometheus_route_surface_has_no_missing_or_wrong_method_routes() {
    let (service, directory) = test_service();
    let app = prometheus_router(service.clone());
    let routes = [
        (axum::http::Method::POST, "/api/v1/write"),
        (axum::http::Method::POST, "/api/v1/read"),
        (axum::http::Method::GET, "/api/v1/query?query=1"),
        (axum::http::Method::POST, "/api/v1/query"),
        (
            axum::http::Method::GET,
            "/api/v1/query_range?query=1&start=0&end=1&step=1",
        ),
        (axum::http::Method::POST, "/api/v1/query_range"),
        (axum::http::Method::GET, "/api/v1/series?start=0&end=1"),
        (axum::http::Method::POST, "/api/v1/series"),
        (axum::http::Method::GET, "/api/v1/labels?start=0&end=1"),
        (axum::http::Method::POST, "/api/v1/labels"),
        (
            axum::http::Method::GET,
            "/api/v1/label/job/values?start=0&end=1",
        ),
        (axum::http::Method::POST, "/api/v1/label/job/values"),
        (axum::http::Method::GET, "/api/v1/metadata"),
        (
            axum::http::Method::GET,
            "/api/v1/query_exemplars?query=%7B__name__%3D%22x%22%7D&start=0&end=1",
        ),
        (axum::http::Method::POST, "/api/v1/query_exemplars"),
    ];
    for (method, path) in routes {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(path)
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("Prometheus response");
        assert_ne!(response.status(), StatusCode::NOT_FOUND, "{method} {path}");
        assert_ne!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {path}"
        );
    }

    drop(app);
    drop(service);
    fs::remove_dir_all(directory).expect("cleanup");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_discovery_and_remote_read_routes_use_prometheus_envelopes() {
    let (service, directory) = test_service();
    let app = prometheus_router(service.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=1%2B2&time=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("query response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1_024 * 1_024)
        .await
        .expect("query body");
    let body: Value = serde_json::from_slice(&body).expect("query JSON");
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "scalar");
    assert_eq!(body["data"]["result"][1], "3");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?start=0&end=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("series response");
    assert_eq!(response.status(), StatusCode::OK);

    let request = prometheus_v1::ReadRequest {
        queries: Vec::new(),
        accepted_response_types: vec![prometheus_v1::ReadRequestResponseType::Samples as i32],
    };
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("Snappy request");
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header(header::CONTENT_ENCODING, "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .expect("read response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1_024 * 1_024)
        .await
        .expect("read body");
    let protobuf = snap::raw::Decoder::new()
        .decompress_vec(&body)
        .expect("Snappy response");
    assert!(
        prometheus_v1::ReadResponse::decode(protobuf.as_slice())
            .expect("read response protobuf")
            .results
            .is_empty()
    );

    drop(service);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streamed_remote_read_returns_crc_framed_prometheus_xor_chunks() {
    let (service, directory) = test_service();
    let app = prometheus_router(service.clone());
    let write = prometheus_v1::WriteRequest {
        timeseries: vec![prometheus_v1::TimeSeries {
            labels: vec![
                prometheus_v1::Label {
                    name: "job".into(),
                    value: "api".into(),
                },
                prometheus_v1::Label {
                    name: "__name__".into(),
                    value: "requests_total".into(),
                },
            ],
            samples: vec![
                prometheus_v1::Sample {
                    value: 1.0,
                    timestamp: 1_000,
                },
                prometheus_v1::Sample {
                    value: 2.0,
                    timestamp: 2_000,
                },
            ],
            exemplars: Vec::new(),
            histograms: Vec::new(),
        }],
        metadata: Vec::new(),
    };
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&write.encode_to_vec())
        .expect("Snappy write request");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/write")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(header::CONTENT_ENCODING, "snappy")
                .header("x-prometheus-remote-write-version", "1.0.0")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .expect("write response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let request = prometheus_v1::ReadRequest {
        queries: vec![prometheus_v1::Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 3_000,
            matchers: vec![prometheus_v1::LabelMatcher {
                r#type: prometheus_v1::LabelMatcherType::Equal as i32,
                name: "__name__".into(),
                value: "requests_total".into(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![
            prometheus_v1::ReadRequestResponseType::StreamedXorChunks as i32,
            prometheus_v1::ReadRequestResponseType::Samples as i32,
        ],
    };
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("Snappy read request");
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header(header::CONTENT_ENCODING, "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .expect("streamed read response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    assert!(!response.headers().contains_key(header::CONTENT_ENCODING));
    let body = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .expect("stream body");
    let (frame_bytes, delimiter_bytes) = decode_test_uvarint(&body);
    let checksum_start = delimiter_bytes;
    let protobuf_start = checksum_start + 4;
    let protobuf_end = protobuf_start + usize::try_from(frame_bytes).unwrap();
    assert_eq!(protobuf_end, body.len());
    assert_eq!(
        u32::from_be_bytes(body[checksum_start..protobuf_start].try_into().unwrap()),
        crc32c::crc32c(&body[protobuf_start..protobuf_end])
    );
    let frame = prometheus_v1::ChunkedReadResponse::decode(&body[protobuf_start..protobuf_end])
        .expect("chunked response protobuf");
    assert_eq!(frame.query_index, 0);
    assert_eq!(frame.chunked_series.len(), 1);
    assert_eq!(
        frame.chunked_series[0]
            .labels
            .iter()
            .map(|label| label.name.as_str())
            .collect::<Vec<_>>(),
        vec!["__name__", "job"]
    );
    assert_eq!(frame.chunked_series[0].chunks.len(), 1);
    let chunk = &frame.chunked_series[0].chunks[0];
    assert_eq!(chunk.min_time_ms, 1_000);
    assert_eq!(chunk.max_time_ms, 2_000);
    assert_eq!(chunk.r#type, prometheus_v1::ChunkEncoding::Xor as i32);
    assert_eq!(u16::from_be_bytes(chunk.data[..2].try_into().unwrap()), 2);

    drop(service);
    fs::remove_dir_all(directory).expect("remove test store");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sample_remote_read_round_trips_native_histograms() {
    let (service, directory) = test_service();
    let app = prometheus_router(service.clone());
    let write = prometheus_v1::WriteRequest {
        timeseries: vec![prometheus_v1::TimeSeries {
            labels: vec![prometheus_v1::Label {
                name: "__name__".into(),
                value: "request_duration".into(),
            }],
            samples: Vec::new(),
            exemplars: Vec::new(),
            histograms: vec![prometheus_v1::Histogram {
                count: Some(prometheus_v1::histogram::Count::Int(3)),
                sum: 6.0,
                schema: 0,
                zero_threshold: 0.001,
                zero_count: Some(prometheus_v1::histogram::ZeroCount::Int(0)),
                negative_spans: Vec::new(),
                negative_deltas: Vec::new(),
                negative_counts: Vec::new(),
                positive_spans: vec![prometheus_v1::BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                positive_deltas: vec![1, 1],
                positive_counts: Vec::new(),
                reset_hint: prometheus_v1::histogram::ResetHint::No as i32,
                timestamp: 2_000,
                custom_values: Vec::new(),
                start_timestamp: 1_000,
            }],
        }],
        metadata: Vec::new(),
    };
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&write.encode_to_vec())
        .unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/write")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .header(header::CONTENT_ENCODING, "snappy")
                .header("x-prometheus-remote-write-version", "1.0.0")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let read = prometheus_v1::ReadRequest {
        queries: vec![prometheus_v1::Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 3_000,
            matchers: vec![prometheus_v1::LabelMatcher {
                r#type: prometheus_v1::LabelMatcherType::Equal as i32,
                name: "__name__".into(),
                value: "request_duration".into(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![prometheus_v1::ReadRequestResponseType::Samples as i32],
    };
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&read.encode_to_vec())
        .unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header(header::CONTENT_ENCODING, "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1_024 * 1_024).await.unwrap();
    let protobuf = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
    let response = prometheus_v1::ReadResponse::decode(protobuf.as_slice()).unwrap();
    let histogram = &response.results[0].timeseries[0].histograms[0];
    assert_eq!(histogram.schema, 0);
    assert_eq!(histogram.positive_deltas, vec![1, 1]);
    assert_eq!(histogram.sum.to_bits(), 6.0_f64.to_bits());
    assert_eq!(histogram.start_timestamp, 1_000);

    drop(service);
    fs::remove_dir_all(directory).expect("remove test store");
}

fn decode_test_uvarint(bytes: &[u8]) -> (u64, usize) {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().copied().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return (value, index + 1);
        }
    }
    panic!("invalid test frame delimiter")
}
