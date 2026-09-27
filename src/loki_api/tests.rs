use std::io::Cursor;

use arrow_array::{StringArray, TimestampNanosecondArray};
use arrow_ipc::reader::StreamReader;
use axum::body::{Body, to_bytes};
use axum::http::{Method, Request};
use tower::ServiceExt;

use super::*;
use crate::{ServiceLifecycle, SingleTenantConfig};

fn production_runtime() -> (Arc<ProductionRuntime>, Arc<ServiceLifecycle>) {
    let lifecycle = Arc::new(ServiceLifecycle::new());
    let runtime = Arc::new(
        ProductionRuntime::new(
            SingleTenantConfig {
                tenant: Arc::from("tenant-a"),
                bearer_token: Arc::from("0123456789abcdef"),
                max_http_in_flight: 8,
                max_ingest_in_flight: 4,
                max_query_in_flight: 4,
                ingest_bytes_per_second: 0,
                ingest_burst_bytes: 0,
                max_tail_subscribers: 2,
                max_native_connections: 4,
                query_timeout: Duration::from_secs(30),
                native_auth_timeout: Duration::from_secs(5),
            },
            Arc::clone(&lifecycle),
        )
        .expect("valid production runtime"),
    );
    (runtime, lifecycle)
}

#[test]
fn json_push_requires_string_timestamps_and_preserves_metadata() {
    let entries = decode_json_push(
        br#"{"streams":[{"stream":{"app":"api"},"values":[["100","hello",{"trace_id":"abc"}]]}]}"#,
    )
    .expect("valid push");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].timestamp_unix_nanos, 100);
    assert_eq!(entries[0].structured_metadata["trace_id"], "abc");
    assert!(
        decode_json_push(br#"{"streams":[{"stream":{"app":"api"},"values":[[100,"hello"]]}]}"#)
            .is_err()
    );
}

#[test]
fn exact_and_regex_line_filters_are_lossless() {
    let selector =
        parse_log_query(r#"{app="api"} |= "request" !~ "health|metrics""#).expect("query");
    let entry = LokiEntry {
        timestamp_unix_nanos: 1,
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        line: "request completed".to_owned(),
        structured_metadata: BTreeMap::new(),
    };
    assert!(selector.matches(&entry));
}

#[test]
fn parser_filter_and_format_pipeline_stages_match_logql_semantics() {
    let selector = parse_log_query(
        r#"{app="api"} | json | duration >= 40ms and status =~ "5.." | label_format code=status | drop status | line_format "{{.method}} {{.code}} {{ __line__ }}""#,
    )
    .expect("query");
    let entry = LokiEntry {
        timestamp_unix_nanos: 1,
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        line: r#"{"method":"GET","status":"500","duration":"42ms"}"#.to_owned(),
        structured_metadata: BTreeMap::new(),
    };
    let processed = selector.process(entry).expect("matching entry");
    assert_eq!(processed.labels["method"], "GET");
    assert_eq!(processed.labels["code"], "500");
    assert!(!processed.labels.contains_key("status"));
    assert_eq!(
        processed.line,
        r#"GET 500 {"method":"GET","status":"500","duration":"42ms"}"#
    );

    let rejected = LokiEntry {
        timestamp_unix_nanos: 2,
        labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
        line: r#"{"method":"GET","status":"200","duration":"2ms"}"#.to_owned(),
        structured_metadata: BTreeMap::new(),
    };
    assert!(selector.process(rejected).is_none());
}

#[test]
fn regexp_pattern_logfmt_unpack_and_decolorize_are_lossless() {
    let regexp = parse_log_query(r#"{} | regexp "user=(?P<user>[^ ]+)""#).expect("regexp");
    let logfmt = parse_log_query(r#"{} | logfmt | status = 500"#).expect("logfmt");
    let pattern = parse_log_query(r#"{} | pattern "request <method> <path>""#).expect("pattern");
    let unpack = parse_log_query(r#"{} | unpack | decolorize"#).expect("unpack");
    let base = |line: &str| LokiEntry {
        timestamp_unix_nanos: 1,
        labels: BTreeMap::new(),
        line: line.to_owned(),
        structured_metadata: BTreeMap::new(),
    };
    assert_eq!(
        regexp.process(base("user=alice ok")).unwrap().labels["user"],
        "alice"
    );
    assert!(logfmt.process(base("status=500 duration=42ms")).is_some());
    let patterned = pattern.process(base("request GET /health")).unwrap();
    assert_eq!(patterned.labels["method"], "GET");
    assert_eq!(patterned.labels["path"], "/health");
    let unpacked = unpack
        .process(base(r#"{"_entry":"\u001b[31mfailed\u001b[0m","pod":"a"}"#))
        .unwrap();
    assert_eq!(unpacked.line, "failed");
    assert_eq!(unpacked.labels["pod"], "a");
}

#[test]
fn parser_errors_are_labels_and_can_be_filtered_explicitly() {
    let selector = parse_log_query(r#"{} | json | __error__ != """#).expect("query");
    let entry = LokiEntry {
        timestamp_unix_nanos: 1,
        labels: BTreeMap::new(),
        line: "not json".to_owned(),
        structured_metadata: BTreeMap::new(),
    };
    let processed = selector.process(entry).expect("parser error is selected");
    assert_eq!(processed.labels["__error__"], "JSONParserErr");
}

#[test]
fn metric_logql_range_aggregation_unwrap_and_binary_matching_are_exact() {
    let entries = vec![
        LokiEntry {
            timestamp_unix_nanos: 1_000_000_000,
            labels: BTreeMap::from([
                ("app".to_owned(), "api".to_owned()),
                ("pod".to_owned(), "a".to_owned()),
            ]),
            line: "duration=100ms".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
        LokiEntry {
            timestamp_unix_nanos: 2_000_000_000,
            labels: BTreeMap::from([
                ("app".to_owned(), "api".to_owned()),
                ("pod".to_owned(), "b".to_owned()),
            ]),
            line: "duration=300ms".to_owned(),
            structured_metadata: BTreeMap::new(),
        },
    ];
    let count = parse_metric_expression(r#"sum by (app) (count_over_time({app="api"}[2s]))"#)
        .expect("count query");
    let MetricValue::Vector(count) =
        evaluate_metric_expression(&count, &entries, 2_000_000_000).expect("evaluation")
    else {
        panic!("expected vector");
    };
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].labels["app"], "api");
    assert_eq!(count[0].value, 2.0);

    let average =
        parse_metric_expression(r#"avg_over_time({app="api"} | logfmt | unwrap duration [2s])"#)
            .expect("unwrap query");
    let MetricValue::Vector(average) =
        evaluate_metric_expression(&average, &entries, 2_000_000_000).expect("evaluation")
    else {
        panic!("expected vector");
    };
    assert_eq!(average.len(), 2);
    assert!(average.iter().any(|sample| sample.value == 0.1));
    assert!(average.iter().any(|sample| sample.value == 0.3));

    let comparison = parse_metric_expression(r#"sum(count_over_time({app="api"}[2s])) > bool 1"#)
        .expect("comparison");
    let MetricValue::Vector(comparison) =
        evaluate_metric_expression(&comparison, &entries, 2_000_000_000).expect("evaluation")
    else {
        panic!("expected vector");
    };
    assert_eq!(comparison[0].value, 1.0);
}

#[test]
fn timestamps_and_detected_line_fields_follow_loki_types() {
    assert_eq!(
        parse_timestamp("1970-01-01T00:00:01.000000002Z").unwrap(),
        1_000_000_002
    );
    assert_eq!(
        parse_timestamp("1970-01-01T01:00:01+01:00").unwrap(),
        1_000_000_000
    );
    assert_eq!(parse_delete_timestamp("2").unwrap(), 2_000_000_000);

    let fields = detect_line_fields("status=500 duration=42ms enabled=true");
    assert_eq!(fields.len(), 3);
    let values = BTreeSet::from(["42ms".to_owned(), "84ms".to_owned()]);
    assert_eq!(inferred_field_type(&values), "duration");
    let json = detect_line_fields(r#"{"status":500,"cached":true}"#);
    assert!(json.contains(&("status".to_owned(), "500".to_owned(), "json")));
}

#[test]
fn native_snappy_protobuf_push_round_trips_loki_logproto_fields() {
    let protobuf = ProtoPushRequest {
        streams: vec![ProtoStream {
            labels: r#"{app="api"}"#.to_owned(),
            entries: vec![ProtoEntry {
                timestamp: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 23,
                }),
                line: "protobuf message".to_owned(),
                structured_metadata: vec![ProtoLabelPair {
                    name: "trace_id".to_owned(),
                    value: "abc".to_owned(),
                }],
                parsed: Vec::new(),
            }],
            hash: 0,
        }],
    }
    .encode_to_vec();
    let compressed = snap::raw::Encoder::new()
        .compress_vec(&protobuf)
        .expect("Snappy encoding");
    let entries = decode_protobuf_push(&compressed).expect("push decoding");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].timestamp_unix_nanos, 1_000_000_023);
    assert_eq!(entries[0].line, "protobuf message");
    assert_eq!(entries[0].labels["service_name"], "api");
    assert_eq!(entries[0].structured_metadata["trace_id"], "abc");
}

#[tokio::test]
async fn stable_loki_route_surface_has_no_missing_or_wrong_method_routes() {
    let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let cases = [
        (Method::GET, "/ready"),
        (Method::GET, "/metrics"),
        (Method::GET, "/config"),
        (Method::GET, "/services"),
        (Method::GET, "/log_level"),
        (Method::POST, "/log_level"),
        (Method::POST, "/flush"),
        (Method::POST, "/ingester/prepare_shutdown"),
        (Method::POST, "/ingester/shutdown"),
        (Method::GET, "/loki/api/v1/status/buildinfo"),
        (Method::POST, "/loki/api/v1/push"),
        (Method::POST, "/otlp/v1/logs"),
        (
            Method::GET,
            "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D",
        ),
        (
            Method::POST,
            "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D",
        ),
        (
            Method::GET,
            "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D",
        ),
        (
            Method::POST,
            "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D",
        ),
        (Method::GET, "/loki/api/v1/labels"),
        (Method::POST, "/loki/api/v1/labels"),
        (Method::GET, "/loki/api/v1/label/app/values"),
        (Method::POST, "/loki/api/v1/label/app/values"),
        (Method::GET, "/loki/api/v1/series"),
        (Method::POST, "/loki/api/v1/series"),
        (Method::GET, "/loki/api/v1/index/stats"),
        (Method::POST, "/loki/api/v1/index/stats"),
        (Method::GET, "/loki/api/v1/index/volume"),
        (Method::POST, "/loki/api/v1/index/volume"),
        (Method::GET, "/loki/api/v1/index/volume_range"),
        (Method::POST, "/loki/api/v1/index/volume_range"),
        (Method::GET, "/loki/api/v1/patterns"),
        (Method::POST, "/loki/api/v1/patterns"),
        (Method::GET, "/loki/api/v1/detected_fields"),
        (Method::POST, "/loki/api/v1/detected_fields"),
        (Method::GET, "/loki/api/v1/detected_field/level/values"),
        (Method::POST, "/loki/api/v1/detected_field/level/values"),
        (Method::GET, "/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D"),
        (Method::GET, "/loki/api/v1/delete"),
        (
            Method::POST,
            "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1",
        ),
        (
            Method::PUT,
            "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1",
        ),
        (Method::DELETE, "/loki/api/v1/delete?request_id=missing"),
        (
            Method::GET,
            "/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D",
        ),
        (
            Method::POST,
            "/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D",
        ),
        (Method::POST, "/api/prom/push"),
        (Method::GET, "/api/prom/query?query=%7Bapp%3D%22api%22%7D"),
        (Method::GET, "/api/prom/label"),
        (Method::GET, "/api/prom/label/app/values"),
        (Method::GET, "/api/prom/series"),
        (Method::GET, "/api/prom/tail?query=%7Bapp%3D%22api%22%7D"),
    ];
    for (method, uri) in cases {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"streams":[{"stream":{"app":"api"},"values":[]}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("route response");
        assert_ne!(
            response.status(),
            StatusCode::NOT_FOUND,
            "missing route for {method} {uri}"
        );
        assert_ne!(
            response.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "wrong method for {method} {uri}"
        );
    }
}

#[tokio::test]
async fn configured_request_body_cap_rejects_ingest_before_deserialization() {
    let app = loki_router(
        Arc::new(LokiApiStore::default()),
        LokiApiConfig {
            max_request_bytes: 4,
            ..LokiApiConfig::default()
        },
    );

    for uri in ["/loki/api/v1/push", "/api/prom/push", "/otlp/v1/logs"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("12345"))
                    .expect("oversized request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{uri}");
    }
}

#[tokio::test]
async fn push_query_labels_series_stats_and_detected_fields_round_trip() {
    let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let push = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            r#"{"streams":[{"stream":{"app":"api","env":"prod"},"values":[["100","request completed",{"trace_id":"abc"}],["200","health check"]]}]}"#,
        ))
        .expect("push request");
    assert_eq!(
        app.clone().oneshot(push).await.expect("push").status(),
        StatusCode::NO_CONTENT
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22request%22&start=1&end=300&direction=forward")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("query request"),
        )
        .await
        .expect("query");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("query body");
    let body: Value = serde_json::from_slice(&body).expect("query JSON");
    assert_eq!(body["data"]["resultType"], "streams");
    assert_eq!(body["data"]["result"][0]["values"][0][0], "100");
    assert_eq!(body["data"]["result"][0]["stream"]["trace_id"], "abc");
    assert_eq!(body["data"]["result"][0]["stream"]["service_name"], "api");

    for (uri, pointer, expected) in [
        ("/loki/api/v1/labels?start=1&end=300", "/data/0", "app"),
        (
            "/loki/api/v1/label/env/values?start=1&end=300",
            "/data/0",
            "prod",
        ),
        (
            "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D&start=1&end=300",
            "/data/0/app",
            "api",
        ),
        (
            "/loki/api/v1/detected_field/trace_id/values?start=1&end=300",
            "/values/0",
            "abc",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let body: Value = serde_json::from_slice(&body).expect("JSON");
        assert_eq!(
            body.pointer(pointer),
            Some(&Value::String(expected.to_owned()))
        );
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=1&end=300")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("stats request"),
        )
        .await
        .expect("stats");
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("stats body");
    let body: Value = serde_json::from_slice(&body).expect("stats JSON");
    assert_eq!(body["entries"], 2);
    assert_eq!(body["streams"], 1);
}

#[tokio::test]
async fn post_form_parameters_match_url_query_parameters() {
    let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let push = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            r#"{"streams":[{"stream":{"app":"api"},"values":[["100","request complete"]]}]}"#,
        ))
        .expect("push request");
    assert_eq!(
        app.clone().oneshot(push).await.expect("push").status(),
        StatusCode::NO_CONTENT
    );

    let query = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/query_range?limit=5")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            "query=%7Bapp%3D%22api%22%7D&start=1&end=200&direction=forward",
        ))
        .expect("form query");
    let response = app.clone().oneshot(query).await.expect("query response");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body"),
    )
    .expect("JSON");
    assert_eq!(
        body["data"]["result"][0]["values"][0][1],
        "request complete"
    );

    let series = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/series")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            "match%5B%5D=%7Bapp%3D%22api%22%7D&start=1&end=200",
        ))
        .expect("form series");
    let response = app.oneshot(series).await.expect("series response");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body"),
    )
    .expect("JSON");
    assert_eq!(body["data"][0]["app"], "api");
}

#[tokio::test]
async fn metric_query_range_returns_loki_matrix_samples() {
    let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let push = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            r#"{"streams":[{"stream":{"app":"api"},"values":[["1000000000","one"],["2000000000","two"]]}]}"#,
        ))
        .expect("push request");
    assert_eq!(
        app.clone().oneshot(push).await.expect("push").status(),
        StatusCode::NO_CONTENT
    );
    let expression = form_urlencoded::byte_serialize(
        r#"sum by (app) (count_over_time({app="api"}[2s]))"#.as_bytes(),
    )
    .collect::<String>();
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/loki/api/v1/query_range?query={expression}&start=2000000000&end=3000000000&step=1s"
                ))
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("query request"),
        )
        .await
        .expect("query response");
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body"),
    )
    .expect("JSON");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"]["app"], "api");
    assert_eq!(body["data"]["result"][0]["values"][0][1], "2");
    assert_eq!(body["data"]["result"][0]["values"][1][1], "1");
}

#[tokio::test]
async fn delete_requests_hide_matching_logs_and_cancel_restores_visibility() {
    let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let push = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            r#"{"streams":[{"stream":{"app":"api"},"values":[["100","remove this"],["200","retain this"]]}]}"#,
        ))
        .expect("push request");
    assert_eq!(
        app.clone().oneshot(push).await.expect("push").status(),
        StatusCode::NO_CONTENT
    );

    let create = Request::builder()
        .method(Method::POST)
        .uri(
            "/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22remove%22&start=0&end=1",
        )
        .header("x-scope-orgid", "tenant-a")
        .body(Body::empty())
        .expect("delete request");
    assert_eq!(
        app.clone().oneshot(create).await.expect("delete").status(),
        StatusCode::NO_CONTENT
    );

    let listed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("list request"),
        )
        .await
        .expect("list response");
    let body = to_bytes(listed.into_body(), usize::MAX)
        .await
        .expect("list body");
    let deletes: Vec<DeleteRequest> = serde_json::from_slice(&body).expect("delete JSON");
    assert_eq!(deletes.len(), 1);

    let query_uri =
        "/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=1&end=300&direction=forward";
    let query = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(query_uri)
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("query request"),
        )
        .await
        .expect("query response");
    let body = to_bytes(query.into_body(), usize::MAX)
        .await
        .expect("query body");
    let body: Value = serde_json::from_slice(&body).expect("query JSON");
    assert_eq!(
        body["data"]["result"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(body["data"]["result"][0]["values"][0][1], "retain this");

    let cancel = Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/loki/api/v1/delete?request_id={}",
            deletes[0].request_id
        ))
        .header("x-scope-orgid", "tenant-a")
        .body(Body::empty())
        .expect("cancel request");
    assert_eq!(
        app.clone().oneshot(cancel).await.expect("cancel").status(),
        StatusCode::NO_CONTENT
    );

    let query = app
        .oneshot(
            Request::builder()
                .uri(query_uri)
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("query request"),
        )
        .await
        .expect("query response");
    let body = to_bytes(query.into_body(), usize::MAX)
        .await
        .expect("query body");
    let body: Value = serde_json::from_slice(&body).expect("query JSON");
    assert_eq!(
        body["data"]["result"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn patterns_group_structurally_similar_messages_into_time_samples() {
    let app = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let push = Request::builder()
        .method(Method::POST)
        .uri("/loki/api/v1/push")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-scope-orgid", "tenant-a")
        .body(Body::from(
            r#"{"streams":[{"stream":{"app":"api"},"values":[["1000000000","request id=123456 duration=42ms complete"],["2000000000","request id=654321 duration=84ms complete"]]}]}"#,
        ))
        .expect("push request");
    assert_eq!(
        app.clone().oneshot(push).await.expect("push").status(),
        StatusCode::NO_CONTENT
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=3000000000&step=1s")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("patterns request"),
        )
        .await
        .expect("patterns response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("patterns body");
    let body: Value = serde_json::from_slice(&body).expect("patterns JSON");
    assert_eq!(
        body["data"][0]["pattern"],
        "request id=<_> duration=<_> complete"
    );
    assert_eq!(body["data"][0]["samples"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn clickhouse_scan_is_absent_by_default_and_requires_its_bearer_token() {
    let disabled = loki_router(Arc::new(LokiApiStore::default()), LokiApiConfig::default());
    let response = disabled
        .oneshot(
            Request::builder()
                .uri("/shardtelemetry/api/v1/clickhouse/scan")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let store = Arc::new(LokiApiStore::default());
    store
        .push(
            "tenant-a",
            vec![LokiEntry {
                timestamp_unix_nanos: 123,
                labels: BTreeMap::from([("app".to_owned(), "api".to_owned())]),
                line: "request failed".to_owned(),
                structured_metadata: BTreeMap::from([("code".to_owned(), "500".to_owned())]),
            }],
        )
        .expect("push");
    let app = loki_router_with_clickhouse(
        store,
        LokiApiConfig::default(),
        Arc::from("analytics-secret"),
    )
    .expect("router");
    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/shardtelemetry/api/v1/clickhouse/scan")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/shardtelemetry/api/v1/clickhouse/scan?term=failed&label.app=api&metadata.code=500")
                .header("authorization", "Bearer analytics-secret")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(authorized.status(), StatusCode::OK);
    assert_eq!(
        authorized.headers()[header::CONTENT_TYPE],
        "application/vnd.apache.arrow.stream"
    );
    assert_eq!(authorized.headers()["x-shardtelemetry-relation"], "logs");
    let body = to_bytes(authorized.into_body(), usize::MAX)
        .await
        .expect("Arrow body");
    let mut reader = StreamReader::try_new(Cursor::new(body.to_vec()), None).expect("Arrow stream");
    let batch = reader.next().expect("one batch").expect("valid batch");
    assert_eq!(batch.num_rows(), 1);
    let timestamp_index = batch
        .schema()
        .index_of("timestamp")
        .expect("timestamp column");
    let message_index = batch.schema().index_of("message").expect("message column");
    assert_eq!(
        batch
            .column(timestamp_index)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("timestamp")
            .value(0),
        123
    );
    assert_eq!(
        batch
            .column(message_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("message")
            .value(0),
        "request failed"
    );

    let rowbinary = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/shardtelemetry/api/v1/clickhouse/scan?term=failed&columns=timestamp%2Cmessage&wire=rowbinary")
                .header("authorization", "Bearer analytics-secret")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(rowbinary.status(), StatusCode::OK);
    assert_eq!(
        rowbinary.headers()[header::CONTENT_TYPE],
        "application/octet-stream"
    );
    let body = to_bytes(rowbinary.into_body(), usize::MAX)
        .await
        .expect("RowBinary body");
    let mut expected = 123_i64.to_le_bytes().to_vec();
    expected.extend_from_slice(&[0, 14]);
    expected.extend_from_slice(b"request failed");
    assert_eq!(body.as_ref(), expected);

    let cardinality = app
        .oneshot(
            Request::builder()
                .uri("/shardtelemetry/api/v1/clickhouse/scan?columns=partition&cardinality_only=1&wire=rowbinary")
                .header("authorization", "Bearer analytics-secret")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    assert_eq!(cardinality.status(), StatusCode::OK);
    let body = to_bytes(cardinality.into_body(), usize::MAX)
        .await
        .expect("RowBinary cardinality body");
    assert_eq!(body.as_ref(), 0_u32.to_le_bytes());
}

#[tokio::test]
async fn production_surface_is_authenticated_single_tenant_and_drains_fail_closed() {
    let (runtime, lifecycle) = production_runtime();
    let app = single_tenant_loki_router(
        Arc::new(LokiApiStore::default()),
        LokiApiConfig {
            default_tenant: Arc::from("tenant-a"),
            ..LokiApiConfig::default()
        },
        Arc::clone(&runtime),
        None,
        Duration::from_secs(1),
    )
    .expect("production router");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("readiness request"),
        )
        .await
        .expect("readiness response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    lifecycle.mark_ready();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("readiness request"),
        )
        .await
        .expect("readiness response");
    assert_eq!(response.status(), StatusCode::OK);

    let push_body = r#"{"streams":[{"stream":{"app":"api"},"values":[["100","ready"]]}]}"#;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(push_body))
                .expect("push request"),
        )
        .await
        .expect("push response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                .header("x-scope-orgid", "another-tenant")
                .body(Body::from(push_body))
                .expect("push request"),
        )
        .await
        .expect("push response");
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::from(push_body))
                .expect("push request"),
        )
        .await
        .expect("push response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/ingester/prepare_shutdown")
                .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                .body(Body::empty())
                .expect("drain request"),
        )
        .await
        .expect("drain response");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(lifecycle.state(), ServiceState::Draining);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/loki/api/v1/push")
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, "Bearer 0123456789abcdef")
                .body(Body::from(push_body))
                .expect("push request"),
        )
        .await
        .expect("push response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("readiness request"),
        )
        .await
        .expect("readiness response");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");
    assert_eq!(metrics.status(), StatusCode::OK);
    let body = to_bytes(metrics.into_body(), usize::MAX)
        .await
        .expect("metrics body");
    let body = std::str::from_utf8(&body).expect("UTF-8 metrics");
    assert!(body.contains("shard_telemetry_authentication_failures_total"));
    assert!(body.contains("shard_telemetry_ingest_records_total"));
    let counters = runtime.metrics();
    assert_eq!(counters.authentication_failures, 1);
    assert_eq!(counters.ingest_records, 1);
}
