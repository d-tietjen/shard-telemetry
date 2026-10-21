//! Actual provider tests use an owned unsigned loopback endpoint, never process credentials.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ClientConfigKey, ClientOptions, RetryConfig};
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicPartition};

use super::super::*;
use super::{ValidatedTransportLimits, apply_transport_limits};
use crate::{
    EvidenceAdmission, EvidenceBatchContext, EvidenceClock, EvidenceCompleteness,
    EvidenceEventIdentity, EvidenceExportLimits, EvidenceLossStatus, EvidenceOwner, OtlpLogEvent,
    PreparedEvidenceBatch, TelemetryRecordRef, TelemetrySignal, prepare_log_envelope,
};

fn config() -> S3ObjectStoreConfig {
    S3ObjectStoreConfig {
        bucket: "bucket".into(),
        prefix: "writer".into(),
        region: Some("us-east-1".into()),
        endpoint: None,
        allow_http: false,
        virtual_hosted_style: false,
    }
}

fn limits() -> S3ObjectStoreTransportLimits {
    S3ObjectStoreTransportLimits {
        request_timeout_ms: 2_000,
        connect_timeout_ms: 500,
        retry_timeout_ms: 5_000,
        max_retries: 0,
    }
}

#[test]
fn explicit_transport_rejects_invalid_limits_before_client_or_runtime_creation() {
    for invalid in [
        S3ObjectStoreTransportLimits {
            request_timeout_ms: 0,
            ..limits()
        },
        S3ObjectStoreTransportLimits {
            connect_timeout_ms: 0,
            ..limits()
        },
        S3ObjectStoreTransportLimits {
            retry_timeout_ms: 0,
            ..limits()
        },
        S3ObjectStoreTransportLimits {
            connect_timeout_ms: limits().request_timeout_ms + 1,
            ..limits()
        },
        S3ObjectStoreTransportLimits {
            max_retries: usize::MAX,
            ..limits()
        },
    ] {
        assert!(matches!(
            S3ObjectStore::open_with_limits(config(), Some(1), invalid),
            Err(TelemetryError::InvalidConfiguration(_))
        ));
    }
    assert!(S3ObjectStore::open_with_limits(config(), Some(0), limits()).is_err());
    assert!(S3ObjectStore::open_with_limits(config(), Some(65), limits()).is_err());
    let widest = S3ObjectStoreTransportLimits {
        request_timeout_ms: u64::MAX,
        connect_timeout_ms: u64::MAX,
        retry_timeout_ms: u64::MAX,
        max_retries: usize::MAX - 1,
    };
    // Instant ranges differ by platform; compare against the actual monotonic range.
    let representable = Instant::now()
        .checked_add(Duration::from_millis(u64::MAX))
        .is_some();
    assert_eq!(ValidatedTransportLimits::new(widest).is_ok(), representable);
}

#[test]
fn explicit_transport_defaults_match_actual_provider_defaults() {
    let policy = S3ObjectStoreTransportLimits::default();
    let options = ClientOptions::new();
    assert_eq!(policy.request_timeout_ms, 30_000);
    assert_eq!(policy.connect_timeout_ms, 5_000);
    assert_eq!(
        options
            .get_config_value(&ClientConfigKey::Timeout)
            .as_deref(),
        Some("30s")
    );
    assert_eq!(
        options
            .get_config_value(&ClientConfigKey::ConnectTimeout)
            .as_deref(),
        Some("5s")
    );
    assert_eq!(policy.max_retries, RetryConfig::default().max_retries);
    assert_eq!(
        Duration::from_millis(policy.retry_timeout_ms),
        RetryConfig::default().retry_timeout
    );
}

#[test]
fn explicit_transport_preserves_other_real_builder_options_and_legacy_none_path() {
    let options = [
        (ClientConfigKey::ProxyUrl, "http://proxy.invalid:9000"),
        (ClientConfigKey::ProxyExcludes, "localhost,127.0.0.1"),
        (ClientConfigKey::AllowInvalidCertificates, "true"),
        (ClientConfigKey::NoSystemCertificates, "true"),
        (ClientConfigKey::UserAgent, "transport-policy-fixture"),
        (ClientConfigKey::ReadTimeout, "7s"),
        (ClientConfigKey::Timeout, "47s"),
        (ClientConfigKey::ConnectTimeout, "13s"),
    ];
    let builder = options.iter().fold(AmazonS3Builder::new(), |b, (k, v)| {
        b.with_config(AmazonS3ConfigKey::Client(*k), *v)
    });
    let legacy = apply_transport_limits(builder.clone(), None);
    for (key, _) in &options {
        let key = AmazonS3ConfigKey::Client(*key);
        assert_eq!(
            legacy.get_config_value(&key),
            builder.get_config_value(&key)
        );
    }
    let configured = apply_transport_limits(
        builder.clone(),
        Some(ValidatedTransportLimits::new(limits()).expect("finite policy")),
    );
    for (key, _) in &options[..6] {
        let key = AmazonS3ConfigKey::Client(*key);
        assert_eq!(
            configured.get_config_value(&key),
            builder.get_config_value(&key)
        );
    }
    assert_eq!(
        configured
            .get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::Timeout))
            .as_deref(),
        Some("2000ms")
    );
    assert_eq!(
        configured
            .get_config_value(&AmazonS3ConfigKey::Client(ClientConfigKey::ConnectTimeout))
            .as_deref(),
        Some("500ms")
    );
}

#[derive(Clone, Copy)]
enum Behavior {
    Fail503,
    Immutable,
    Stall,
    LateManifest(Instant),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

struct Endpoint {
    url: String,
    stopped: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<Request>>>,
    received: Receiver<()>,
    worker: Option<JoinHandle<Result<(), String>>>,
}

impl Endpoint {
    fn new(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("owned loopback listener");
        let url = format!("http://{}", listener.local_addr().expect("owned address"));
        listener.set_nonblocking(true).expect("bounded accept loop");
        let stopped = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let (notice, received) = channel();
        let stop = stopped.clone();
        let captured = requests.clone();
        let worker = thread::spawn(move || {
            let mut objects = BTreeMap::<String, Request>::new();
            while !stop.load(Ordering::Acquire) {
                let mut stream = match listener.accept() {
                    Ok((stream, _)) => stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .map_err(|e| e.to_string())?;
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .map_err(|e| e.to_string())?;
                let request = read_request(&mut stream)?;
                {
                    let mut log = captured.lock().map_err(|e| e.to_string())?;
                    if log.len() >= 16 {
                        return Err("fixture request cap exceeded".into());
                    }
                    log.push(request.clone());
                }
                notice.send(()).map_err(|e| e.to_string())?;
                if matches!(behavior, Behavior::Stall) {
                    let cleanup_cap = Instant::now() + Duration::from_secs(5);
                    while !stop.load(Ordering::Acquire) {
                        if Instant::now() >= cleanup_cap {
                            return Err(
                                "request timeout did not finish before fixture cleanup cap".into(),
                            );
                        }
                        thread::sleep(Duration::from_millis(2));
                    }
                    continue;
                }
                if matches!(behavior, Behavior::Fail503) {
                    respond(
                        &mut stream,
                        "503 Service Unavailable",
                        &[],
                        b"<Error><Code>SlowDown</Code><Message>retry</Message></Error>",
                    )?;
                    continue;
                }
                if let Behavior::LateManifest(deadline) = behavior
                    && request.path.contains("/commits/")
                {
                    while Instant::now() <= deadline && !stop.load(Ordering::Acquire) {
                        thread::sleep(Duration::from_millis(2));
                    }
                    if stop.load(Ordering::Acquire) {
                        continue;
                    }
                }
                match request.method.as_str() {
                    "PUT" if objects.contains_key(&request.path) => respond(
                        &mut stream,
                        "412 Precondition Failed",
                        &[],
                        b"<Error><Code>PreconditionFailed</Code></Error>",
                    )?,
                    "PUT" => {
                        if request.headers.get("if-none-match").map(String::as_str) != Some("*") {
                            return Err(
                                "real immutable PUT omitted its create-only precondition".into()
                            );
                        }
                        let expected = blake3::hash(&request.body).to_hex().to_string();
                        if request.headers.get("x-amz-meta-shard-telemetry-blake3")
                            != Some(&expected)
                        {
                            return Err(
                                "real PUT digest attribute does not match exact body".into()
                            );
                        }
                        objects.insert(request.path.clone(), request);
                        respond(&mut stream, "200 OK", &[("ETag", "\"original\"")], b"")?;
                    }
                    "GET" => {
                        let original = objects.get(&request.path).ok_or("unknown object")?;
                        let digest = original
                            .headers
                            .get("x-amz-meta-shard-telemetry-blake3")
                            .ok_or("missing original digest")?;
                        respond(
                            &mut stream,
                            "200 OK",
                            &[
                                ("ETag", "\"original\""),
                                ("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT"),
                                ("x-amz-meta-shard-telemetry-blake3", digest),
                            ],
                            &original.body,
                        )?;
                    }
                    _ => return Err("unexpected fixture request method".into()),
                }
            }
            Ok(())
        });
        Self {
            url,
            stopped,
            requests,
            received,
            worker: Some(worker),
        }
    }

    fn store(&self, limits: S3ObjectStoreTransportLimits) -> S3ObjectStore {
        let mut config = config();
        config.endpoint = Some(self.url.clone());
        config.allow_http = true;
        let builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region("us-east-1")
            .with_endpoint(&self.url)
            .with_allow_http(true)
            .with_skip_signature(true)
            .with_virtual_hosted_style_request(false)
            .with_config(
                AmazonS3ConfigKey::Client(ClientConfigKey::Http1Only),
                "true",
            );
        let builder = apply_transport_limits(
            builder,
            Some(ValidatedTransportLimits::new(limits).expect("finite real transport policy")),
        );
        S3ObjectStore::start_with_builder(config, 1, builder).expect("real provider runtime")
    }

    fn snapshot(&self) -> Vec<Request> {
        self.requests.lock().expect("fixture requests").clone()
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let result = worker.join();
            if !thread::panicking() {
                result
                    .expect("owned HTTP thread join")
                    .expect("owned HTTP fixture");
            }
        }
    }
}

fn read_request(stream: &mut TcpStream) -> Result<Request, String> {
    let mut buffer = Vec::new();
    let mut chunk = [0; 4096];
    let end = loop {
        let n = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("request ended before headers".into());
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(i) = buffer.windows(4).position(|s| s == b"\r\n\r\n") {
            break i + 4;
        }
        if buffer.len() > 16 * 1024 {
            return Err("fixture header cap".into());
        }
    };
    let text = std::str::from_utf8(&buffer[..end]).map_err(|e| e.to_string())?;
    let mut lines = text.split("\r\n");
    let first = lines.next().ok_or("missing request line")?;
    let parts = first.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err("invalid request line".into());
    }
    let method = parts[0].to_owned();
    let path = parts[1].to_owned();
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or("invalid fixture header")?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    if headers.contains_key("transfer-encoding") {
        return Err("byte PUT fixture expects the provider's known content length".into());
    }
    let length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(0);
    if length > 1024 * 1024 {
        return Err("fixture body cap".into());
    }
    if headers.get("expect").map(String::as_str) == Some("100-continue") {
        stream
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .map_err(|e| e.to_string())?;
    }
    let mut body = buffer[end..].to_vec();
    while body.len() < length {
        let n = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("request ended before exact body".into());
        }
        body.extend_from_slice(&chunk[..n]);
    }
    if body.len() != length {
        return Err("unexpected extra request bytes".into());
    }
    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

fn respond(
    stream: &mut TcpStream,
    status: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<(), String> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    stream
        .write_all(response.as_bytes())
        .map_err(|e| e.to_string())?;
    stream.write_all(body).map_err(|e| e.to_string())
}

#[test]
fn explicit_transport_503_retry_count_keeps_exact_original_put_path_and_bytes() {
    for retries in [0, 2] {
        let endpoint = Endpoint::new(Behavior::Fail503);
        let store = endpoint.store(S3ObjectStoreTransportLimits {
            max_retries: retries,
            ..limits()
        });
        let body = b"original immutable bytes, including \0 and \xff";
        assert!(store.put_bytes_if_absent("same", body).is_err());
        let requests = endpoint.snapshot();
        assert_eq!(requests.len(), retries + 1);
        for request in requests {
            assert_eq!(request.method, "PUT");
            assert_eq!(request.path, "/bucket/writer/same");
            assert_eq!(request.body, body);
        }
    }
}

#[test]
fn explicit_transport_stalled_real_request_returns_only_after_actual_transport_outcome() {
    let endpoint = Endpoint::new(Behavior::Stall);
    let store = endpoint.store(S3ObjectStoreTransportLimits {
        request_timeout_ms: 500,
        ..limits()
    });
    let caller = thread::spawn(move || store.put_bytes_if_absent("stalled", b"original"));
    endpoint
        .received
        .recv_timeout(Duration::from_secs(10))
        .expect("actual HTTP request entered");
    // The endpoint remains open and withholds its response. Only the configured
    // real request timeout can finish this controlled unsigned loopback request.
    let result = caller
        .join()
        .expect("own synchronous caller through actual return");
    assert!(matches!(result, Err(TelemetryError::ObjectStore(_))));
    assert_eq!(endpoint.snapshot().len(), 1);
    assert!(!endpoint.stopped.load(Ordering::Acquire));
}

#[test]
fn explicit_transport_real_immutable_retry_and_conflict_keep_original_remote_bytes() {
    let endpoint = Endpoint::new(Behavior::Immutable);
    let store = endpoint.store(limits());
    let first = store
        .put_bytes_if_absent("immutable", b"original")
        .expect("actual first PUT");
    let retry = store
        .put_bytes_if_absent("immutable", b"original")
        .expect("actual verified GET on412");
    assert_eq!(first, retry);
    assert!(
        store
            .put_bytes_if_absent("immutable", b"different")
            .is_err()
    );
    assert_eq!(
        store.get("immutable", 100).expect("actual GET"),
        b"original"
    );
    let requests = endpoint.snapshot();
    assert_eq!(requests.iter().filter(|r| r.method == "PUT").count(), 3);
    assert_eq!(requests.iter().filter(|r| r.method == "GET").count(), 3);
}

fn prepared_batch() -> PreparedEvidenceBatch {
    let signal = TelemetrySignal::Logs;
    let partition = TopicPartition::new(signal.topic_id(), LogicalPartitionId::new(7));
    let admission = EvidenceAdmission {
        dataset_version: 1,
        owner: EvidenceOwner {
            organization_id: [1; 16],
            plane: "test".into(),
            source_id: [2; 16],
            source_incarnation: [3; 16],
        },
        consent_revision: "fixture-grant".into(),
        allowed_signals: vec![signal],
    };
    let context = EvidenceBatchContext {
        source_sequence: 1,
        source_shard_id: ShardId::new(1),
        source_partition: partition,
        observed_source_next_offset: LogicalOffset::new(2),
        retained_source_start_offset: Some(LogicalOffset::new(0)),
        clock: EvidenceClock {
            domain: None,
            captured_unix_nanos: None,
            uncertainty_nanos: None,
        },
        completeness: EvidenceCompleteness::SelectedRecords,
        loss: EvidenceLossStatus::Unknown,
    };
    let event = EvidenceEventIdentity {
        original: TelemetryRecordRef::for_signal(signal, partition, LogicalOffset::new(1)),
        producer_event_id: Some("original-event".into()),
        process_incarnation: [4; 16],
    };
    let envelope = prepare_log_envelope(
        "test",
        &[OtlpLogEvent {
            timestamp_unix_nanos: 1,
            message: Arc::from("original"),
            ..OtlpLogEvent::default()
        }],
    )
    .expect("real log envelope");
    PreparedEvidenceBatch::prepare(
        &admission,
        &context,
        &[event],
        &envelope.encode().expect("actual STEL bytes"),
        EvidenceExportLimits::default(),
    )
    .expect("real publication primitive; caller admission is fixture input, not a service grant")
}

#[test]
fn explicit_transport_real_verified_final_manifest_success_remains_success_after_caller_deadline() {
    let batch = prepared_batch();
    let deadline = Instant::now() + Duration::from_secs(10);
    let endpoint = Endpoint::new(Behavior::LateManifest(deadline));
    let store = endpoint.store(S3ObjectStoreTransportLimits {
        request_timeout_ms: 20_000,
        retry_timeout_ms: 20_000,
        ..limits()
    });
    let receipt = batch
        .publish_before(&store, deadline)
        .expect("actual late final200 remains committed");
    assert_eq!(receipt.committed, *batch.reference());
    assert!(receipt.completed_after_deadline);
    let requests = endpoint.snapshot();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].path,
        format!("/bucket/writer/{}", batch.reference().object_key)
    );
    assert_eq!(
        requests[1].path,
        format!("/bucket/writer/{}", batch.reference().manifest_key)
    );
    assert_eq!(requests[1].body, batch.manifest_bytes());
}
