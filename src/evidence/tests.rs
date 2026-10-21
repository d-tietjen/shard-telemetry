use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use serde::Deserialize;
use shard_stream_core::{LogicalPartitionId, ShardId};

use super::*;
use crate::{
    DurableMetricPoint, DurableSpan, LocalObjectStore, MetricIdentity, MetricKind, MetricValue,
    NumberValue, ObjectMetadata, OtlpLogEvent, ResourceContext, ScopeContext, SpanId,
    TelemetryEnvelope, TelemetryResult, TraceId, decode_log_envelope, decode_metric_chunk,
    decode_trace_block, encode_metric_chunk, encode_trace_block, prepare_log_envelope,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

// Decode original addresses directly: TopicId is u128, while a generic JSON
// Value cannot preserve these identities through its ordinary numeric model.
#[derive(Deserialize)]
struct PublishedManifestIdentity {
    source: PublishedSourceIdentity,
    events: Vec<PublishedEventIdentity>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedSourceIdentity {
    source_sequence: u64,
    source_shard_id: ShardId,
    source_partition: TopicPartition,
    observed_source_next_offset: LogicalOffset,
    retained_source_start_offset: Option<LogicalOffset>,
    clock: serde_json::Value,
    completeness: serde_json::Value,
    loss: serde_json::Value,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishedEventIdentity {
    original: TelemetryRecordRef,
    producer_event_id: Option<String>,
    process_incarnation: [u8; 16],
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let n = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "shard-telemetry-evidence-{}-{n}",
            std::process::id()
        )))
    }

    fn store(&self) -> LocalObjectStore {
        LocalObjectStore::open(&self.0).unwrap()
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn admission() -> EvidenceAdmission {
    EvidenceAdmission {
        dataset_version: 1,
        owner: EvidenceOwner {
            organization_id: [1; 16],
            plane: "private".into(),
            source_id: [2; 16],
            source_incarnation: [3; 16],
        },
        consent_revision: "service-grant-7".into(),
        allowed_signals: vec![TelemetrySignal::Logs, TelemetrySignal::Metrics],
    }
}

fn fixture(
    signal: TelemetrySignal,
    marker: &str,
) -> (EvidenceBatchContext, Vec<EvidenceEventIdentity>, Vec<u8>) {
    let partition = TopicPartition::new(signal.topic_id(), LogicalPartitionId::new(7));
    let events = [10, 20]
        .into_iter()
        .map(|offset| EvidenceEventIdentity {
            original: TelemetryRecordRef::for_signal(signal, partition, LogicalOffset::new(offset)),
            producer_event_id: Some(format!("original-event-{offset}")),
            process_incarnation: [4; 16],
        })
        .collect::<Vec<_>>();
    let context = EvidenceBatchContext {
        source_sequence: 5,
        source_shard_id: ShardId::new(1),
        source_partition: partition,
        observed_source_next_offset: LogicalOffset::new(30),
        retained_source_start_offset: Some(LogicalOffset::new(0)),
        clock: EvidenceClock {
            domain: Some("source-clock-1".into()),
            captured_unix_nanos: Some(123_456),
            uncertainty_nanos: Some(50),
        },
        completeness: EvidenceCompleteness::SelectedRecords,
        loss: EvidenceLossStatus::Unknown,
    };
    let envelope = match signal {
        TelemetrySignal::Logs => {
            let logs = events
                .iter()
                .map(|event| OtlpLogEvent {
                    timestamp_unix_nanos: event.original.offset.get(),
                    message: Arc::from(marker),
                    ..OtlpLogEvent::default()
                })
                .collect::<Vec<_>>();
            prepare_log_envelope("private", &logs).unwrap()
        }
        TelemetrySignal::Traces => {
            let spans = events
                .iter()
                .enumerate()
                .map(|(i, event)| DurableSpan {
                    stream_shard_id: ShardId::new(1),
                    record_ref: event.original,
                    tenant: Arc::from("private"),
                    resource: Arc::new(ResourceContext::default()),
                    scope: Arc::new(ScopeContext::default()),
                    trace_id: TraceId::from_bytes([1; 16]).unwrap(),
                    span_id: SpanId::from_bytes([i as u8 + 1; 8]).unwrap(),
                    parent_span_id: None,
                    trace_state: Arc::from(""),
                    flags: 1,
                    name: Arc::from(marker),
                    kind: 2,
                    start_time_unix_nanos: event.original.offset.get(),
                    duration_nanos: 7,
                    attributes: Arc::new(vec![]),
                    dropped_attributes_count: 0,
                    events: Arc::new(vec![]),
                    dropped_events_count: 0,
                    links: Arc::new(vec![]),
                    dropped_links_count: 0,
                    status: None,
                })
                .collect::<Vec<_>>();
            TelemetryEnvelope::new(
                signal,
                "private",
                2,
                &b"original-routing"[..],
                encode_trace_block(&spans).unwrap(),
            )
            .unwrap()
        }
        TelemetrySignal::Metrics => {
            let points = events
                .iter()
                .map(|event| DurableMetricPoint {
                    stream_shard_id: ShardId::new(1),
                    record_ref: event.original,
                    identity: Arc::new(MetricIdentity {
                        tenant: Arc::from("private"),
                        resource: Arc::new(ResourceContext::default()),
                        scope: Arc::new(ScopeContext::default()),
                        name: Arc::from(marker),
                        unit: Arc::from("1"),
                        kind: MetricKind::Gauge,
                        point_attributes: Arc::new(vec![]),
                    }),
                    description: Arc::from("original point"),
                    metadata: Arc::new(vec![]),
                    start_time_unix_nanos: 0,
                    timestamp_unix_nanos: event.original.offset.get(),
                    flags: 0,
                    value: MetricValue::Gauge(NumberValue::DoubleBits(0x7ff8_0000_0000_0042)),
                    exemplars: Arc::new(vec![]),
                })
                .collect::<Vec<_>>();
            TelemetryEnvelope::new(
                signal,
                "private",
                2,
                &b"original-routing"[..],
                encode_metric_chunk(&points).unwrap(),
            )
            .unwrap()
        }
    };
    (context, events, envelope.encode().unwrap())
}

fn prepare(
    a: &EvidenceAdmission,
    c: &EvidenceBatchContext,
    e: &[EvidenceEventIdentity],
    bytes: &[u8],
) -> PreparedEvidenceBatch {
    PreparedEvidenceBatch::prepare(a, c, e, bytes, EvidenceExportLimits::default()).unwrap()
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

#[derive(Clone, Copy)]
enum Fault {
    Before,
    LostResponse,
    WrongMetadata,
    WrongDigest,
    OpaqueVersion,
}

struct FaultStore {
    inner: LocalObjectStore,
    calls: Mutex<Vec<String>>,
    fault: Mutex<Option<(usize, Fault)>>,
}

impl FaultStore {
    fn new(inner: LocalObjectStore, fault: Option<(usize, Fault)>) -> Self {
        Self {
            inner,
            calls: Mutex::new(vec![]),
            fault: Mutex::new(fault),
        }
    }
}

impl TelemetryObjectStore for FaultStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        let mut calls = self.calls.lock().unwrap();
        calls.push(key.into());
        let n = calls.len();
        drop(calls);
        let mut configured = self.fault.lock().unwrap();
        let fault = if configured.as_ref().is_some_and(|(at, _)| *at == n) {
            configured.take().map(|(_, f)| f)
        } else {
            None
        };
        drop(configured);
        if matches!(fault, Some(Fault::Before)) {
            return Err(TelemetryError::ObjectStore("injected before PUT".into()));
        }
        let mut metadata = self.inner.put_bytes_if_absent(key, bytes)?;
        match fault {
            Some(Fault::LostResponse) => {
                Err(TelemetryError::ObjectStore("injected lost response".into()))
            }
            Some(Fault::WrongMetadata) => {
                metadata.bytes += 1;
                Ok(metadata)
            }
            Some(Fault::WrongDigest) => {
                metadata.content_digest = "opaque-etag-is-not-a-content-digest".into();
                Ok(metadata)
            }
            Some(Fault::OpaqueVersion) => {
                metadata.version_token = "opaque-object-version".into();
                Ok(metadata)
            }
            _ => Ok(metadata),
        }
    }

    fn put_file_if_absent(&self, _: &str, _: &Path) -> TelemetryResult<ObjectMetadata> {
        panic!("no streamed file publication")
    }
    fn get(&self, _: &str, _: u64) -> TelemetryResult<Vec<u8>> {
        panic!("no readback or writer recovery")
    }
    fn get_range(&self, _: &str, _: Range<u64>) -> TelemetryResult<Vec<u8>> {
        panic!("no range reads")
    }
    fn head(&self, _: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        panic!("no check-then-PUT")
    }
    fn delete(&self, _: &str) -> TelemetryResult<()> {
        panic!("publisher never deletes")
    }
    fn compare_and_swap(
        &self,
        _: &str,
        _: Option<&str>,
        _: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        panic!("no shared mutable catalog")
    }
}

#[test]
fn typed_signal_payloads_and_original_identities_survive_complete_manifest_last_publication() {
    let directory = Directory::new();
    let store = directory.store();
    for signal in [
        TelemetrySignal::Logs,
        TelemetrySignal::Traces,
        TelemetrySignal::Metrics,
    ] {
        let mut a = admission();
        a.allowed_signals = vec![
            TelemetrySignal::Logs,
            TelemetrySignal::Traces,
            TelemetrySignal::Metrics,
        ];
        let (mut c, e, bytes) = fixture(signal, "golden-original");
        c.source_sequence = signal as u64;
        let p = prepare(&a, &c, &e, &bytes);
        assert!(store.head(&p.reference().manifest_key).unwrap().is_none());
        let receipt = p.publish_before(&store, deadline()).unwrap();
        assert_eq!(receipt.committed, *p.reference());
        assert!(!receipt.completed_after_deadline);
        assert_eq!(
            store
                .get(&p.reference().object_key, MAX_EVIDENCE_OBJECT_BYTES as u64)
                .unwrap(),
            bytes
        );
        let manifest = store
            .get(
                &p.reference().manifest_key,
                MAX_EVIDENCE_MANIFEST_BYTES as u64,
            )
            .unwrap();
        assert_eq!(manifest, p.manifest_bytes());
        let parsed: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(parsed["format_version"], 1);
        assert_eq!(parsed["payload_schema"], "STELv1");
        assert!(signal.topic_id().get() > u128::from(u64::MAX));
        let identities: PublishedManifestIdentity = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(
            identities.source,
            PublishedSourceIdentity {
                source_sequence: c.source_sequence,
                source_shard_id: c.source_shard_id,
                source_partition: c.source_partition,
                observed_source_next_offset: c.observed_source_next_offset,
                retained_source_start_offset: c.retained_source_start_offset,
                clock: serde_json::to_value(&c.clock).unwrap(),
                completeness: serde_json::to_value(c.completeness).unwrap(),
                loss: serde_json::to_value(&c.loss).unwrap(),
            }
        );
        assert_eq!(
            identities.events,
            e.iter()
                .map(|event| PublishedEventIdentity {
                    original: event.original,
                    producer_event_id: event.producer_event_id.clone(),
                    process_incarnation: event.process_incarnation,
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(parsed["admission"], serde_json::to_value(&a).unwrap());
        assert_eq!(
            parsed["object_digest"],
            blake3::hash(&bytes).to_hex().to_string()
        );
        let envelope = TelemetryEnvelope::decode(&bytes).unwrap();
        match signal {
            TelemetrySignal::Logs => assert!(
                decode_log_envelope(&envelope)
                    .unwrap()
                    .iter()
                    .all(|r| r.message.as_ref() == "golden-original")
            ),
            TelemetrySignal::Traces => assert!(
                decode_trace_block(&envelope.payload)
                    .unwrap()
                    .iter()
                    .all(|r| r.name.as_ref() == "golden-original")
            ),
            TelemetrySignal::Metrics => assert!(
                decode_metric_chunk(&envelope.payload)
                    .unwrap()
                    .iter()
                    .all(|r| r.value
                        == MetricValue::Gauge(NumberValue::DoubleBits(0x7ff8_0000_0000_0042)))
            ),
        }
    }
}

#[test]
fn partial_data_has_no_commit_and_restart_retry_preserves_exact_identity() {
    let directory = Directory::new();
    let store = FaultStore::new(directory.store(), Some((2, Fault::Before)));
    let a = admission();
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    let p = prepare(&a, &c, &e, &bytes);
    assert_eq!(
        p.reference().manifest_key,
        concat!(
            "datasets/v1/organizations/01010101010101010101010101010101/planes/private/",
            "sources/02020202020202020202020202020202/incarnations/03030303030303030303030303030303/",
            "commits/00000000000000000005.json"
        )
    );
    assert!(p.publish_before(&store, deadline()).is_err());
    assert!(
        store
            .inner
            .head(&p.reference().object_key)
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .inner
            .head(&p.reference().manifest_key)
            .unwrap()
            .is_none()
    );
    let saved_reference = p.reference().clone();
    drop(p);
    let restarted_store = directory.store();
    let restarted = prepare(&a, &c, &e, &bytes);
    assert_eq!(*restarted.reference(), saved_reference);
    assert_eq!(
        restarted
            .publish_before(&restarted_store, deadline())
            .unwrap()
            .committed,
        saved_reference
    );
    assert_eq!(
        store.calls.lock().unwrap().as_slice(),
        [saved_reference.object_key, saved_reference.manifest_key]
    );
}

#[test]
fn lost_commit_response_is_not_reported_success_and_exact_retry_reconciles() {
    let directory = Directory::new();
    let store = FaultStore::new(directory.store(), Some((2, Fault::LostResponse)));
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    let p = prepare(&admission(), &c, &e, &bytes);
    assert!(
        matches!(p.publish_before(&store,deadline()),Err(EvidenceExportError::Telemetry(TelemetryError::ObjectStore(ref s))) if s=="injected lost response")
    );
    assert_eq!(
        store
            .inner
            .get(
                &p.reference().manifest_key,
                MAX_EVIDENCE_MANIFEST_BYTES as u64
            )
            .unwrap(),
        p.manifest_bytes()
    );
    assert_eq!(
        p.publish_before(&store, deadline()).unwrap().committed,
        *p.reference()
    );
    assert_eq!(store.calls.lock().unwrap().len(), 4);
}

#[test]
fn conflicting_source_sequence_never_replaces_existing_commit_or_original_payload() {
    let directory = Directory::new();
    let store = directory.store();
    let a = admission();
    let (c, e, first) = fixture(TelemetrySignal::Logs, "first");
    let (_, _, second) = fixture(TelemetrySignal::Logs, "second");
    let p = prepare(&a, &c, &e, &first);
    let other = prepare(&a, &c, &e, &second);
    assert_eq!(p.reference().manifest_key, other.reference().manifest_key);
    assert_ne!(p.reference().bundle_id, other.reference().bundle_id);
    p.publish_before(&store, deadline()).unwrap();
    assert!(other.publish_before(&store, deadline()).is_err());
    assert_eq!(
        store
            .get(&p.reference().object_key, MAX_EVIDENCE_OBJECT_BYTES as u64)
            .unwrap(),
        first
    );
    assert_eq!(
        store
            .get(
                &p.reference().manifest_key,
                MAX_EVIDENCE_MANIFEST_BYTES as u64
            )
            .unwrap(),
        p.manifest_bytes()
    );
    let mut changed_policy = a;
    changed_policy.consent_revision = "changed".into();
    assert!(
        prepare(&changed_policy, &c, &e, &first)
            .publish_before(&store, deadline())
            .is_err()
    );
}

#[test]
fn concurrent_equal_retries_succeed_and_conflicting_writers_have_one_immutable_winner() {
    let directory = Directory::new();
    let store = directory.store();
    let a = admission();
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "first");
    let p = Arc::new(prepare(&a, &c, &e, &bytes));
    let barrier = Arc::new(Barrier::new(3));
    let workers = (0..2)
        .map(|_| {
            let store = store.clone();
            let p = p.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                p.publish_before(&store, deadline()).unwrap().committed
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), *p.reference());
    }
    let mut c = c;
    c.source_sequence += 1;
    let (_, _, different) = fixture(TelemetrySignal::Logs, "different");
    let p1 = prepare(&a, &c, &e, &bytes);
    let p2 = prepare(&a, &c, &e, &different);
    let expected = [p1.manifest_bytes().to_vec(), p2.manifest_bytes().to_vec()];
    let key = p1.reference().manifest_key.clone();
    let barrier = Arc::new(Barrier::new(3));
    let workers = [p1, p2]
        .into_iter()
        .map(|p| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                p.publish_before(&store, deadline()).is_ok()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    let successes = workers
        .into_iter()
        .map(|w| usize::from(w.join().unwrap()))
        .sum::<usize>();
    assert_eq!(successes, 1);
    assert!(expected.contains(&store.get(&key, MAX_EVIDENCE_MANIFEST_BYTES as u64).unwrap()));
}

#[test]
fn admitted_organizations_sources_planes_and_incarnations_have_disjoint_namespaces() {
    let directory = Directory::new();
    let store = directory.store();
    let mut keys = std::collections::BTreeSet::new();
    for org in [1, 2] {
        for source in [1, 2, 3] {
            for incarnation in [1, 2] {
                for plane in ["private", "public_marketplace"] {
                    let mut a = admission();
                    a.owner.organization_id = [org; 16];
                    a.owner.source_id = [source; 16];
                    a.owner.source_incarnation = [incarnation; 16];
                    a.owner.plane = plane.into();
                    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "same-original");
                    let mut envelope = TelemetryEnvelope::decode(&bytes).unwrap();
                    envelope.tenant = Arc::from(plane);
                    let bytes = envelope.encode().unwrap();
                    let p = prepare(&a, &c, &e, &bytes);
                    assert!(keys.insert(p.reference().manifest_key.clone()));
                    p.publish_before(&store, deadline()).unwrap();
                    assert_eq!(
                        store
                            .get(&p.reference().object_key, MAX_EVIDENCE_OBJECT_BYTES as u64)
                            .unwrap(),
                        bytes
                    );
                }
            }
        }
    }
    assert_eq!(keys.len(), 24);
}

#[test]
fn acknowledgements_require_exact_content_digest_and_length_before_success() {
    for (at, fault) in [
        (1, Fault::WrongMetadata),
        (2, Fault::WrongMetadata),
        (1, Fault::WrongDigest),
        (2, Fault::WrongDigest),
    ] {
        let directory = Directory::new();
        let store = FaultStore::new(directory.store(), Some((at, fault)));
        let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
        let p = prepare(&admission(), &c, &e, &bytes);
        assert!(matches!(
            p.publish_before(&store, deadline()),
            Err(EvidenceExportError::Telemetry(TelemetryError::CorruptTier(
                _
            )))
        ));
        assert_eq!(store.calls.lock().unwrap().len(), at);
        assert_eq!(
            store
                .inner
                .head(&p.reference().manifest_key)
                .unwrap()
                .is_some(),
            at == 2
        );
        assert_eq!(
            p.publish_before(&store, deadline()).unwrap().committed,
            *p.reference()
        );
    }
}

#[test]
fn opaque_store_versions_are_not_confused_with_complete_content_digests() {
    for at in [1, 2] {
        let directory = Directory::new();
        let store = FaultStore::new(directory.store(), Some((at, Fault::OpaqueVersion)));
        let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
        let p = prepare(&admission(), &c, &e, &bytes);
        assert_eq!(
            p.publish_before(&store, deadline()).unwrap().committed,
            *p.reference()
        );
        assert_eq!(store.calls.lock().unwrap().len(), 2);
    }
}

#[test]
fn conflicting_preexisting_content_addressed_object_cannot_be_overwritten_or_committed() {
    let directory = Directory::new();
    let store = directory.store();
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    let p = prepare(&admission(), &c, &e, &bytes);
    store
        .put_bytes_if_absent(&p.reference().object_key, b"conflicting bytes")
        .unwrap();
    assert!(p.publish_before(&store, deadline()).is_err());
    assert_eq!(
        store.get(&p.reference().object_key, 32).unwrap(),
        b"conflicting bytes"
    );
    assert!(store.head(&p.reference().manifest_key).unwrap().is_none());
}

#[test]
fn original_deadline_controls_next_call_and_retains_real_complete_commit_or_error() {
    let a = admission();
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    let p = prepare(&a, &c, &e, &bytes);
    let now = Instant::now();
    let expires = now + Duration::from_secs(1);
    let late = expires + Duration::from_secs(1);
    let directory = Directory::new();
    let store = FaultStore::new(directory.store(), None);
    assert!(matches!(
        publish::with_clock(&p, &store, expires, || late),
        Err(EvidenceExportError::Expired)
    ));
    assert!(store.calls.lock().unwrap().is_empty());
    let mut times = [now, late].into_iter();
    assert!(matches!(
        publish::with_clock(&p, &store, expires, || times.next().unwrap()),
        Err(EvidenceExportError::Expired)
    ));
    assert!(
        store
            .inner
            .head(&p.reference().manifest_key)
            .unwrap()
            .is_none()
    );
    let mut times = [now, now, late].into_iter();
    let completed = publish::with_clock(&p, &store, expires, || times.next().unwrap()).unwrap();
    assert!(completed.completed_after_deadline);
    assert_eq!(completed.committed, *p.reference());
    let fault = FaultStore::new(directory.store(), Some((2, Fault::LostResponse)));
    let mut times = [now, now, late].into_iter();
    assert!(
        matches!(publish::with_clock(&p,&fault,expires,||times.next().unwrap()),Err(EvidenceExportError::Telemetry(TelemetryError::ObjectStore(ref s))) if s=="injected lost response")
    );
}

#[test]
fn malformed_unadmitted_and_unbounded_inputs_fail_without_object_store_work() {
    let a = admission();
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    let check = |a: &EvidenceAdmission,
                 c: &EvidenceBatchContext,
                 e: &[EvidenceEventIdentity],
                 bytes: &[u8]| {
        assert!(
            PreparedEvidenceBatch::prepare(a, c, e, bytes, EvidenceExportLimits::default())
                .is_err()
        )
    };
    let mut invalid = a.clone();
    invalid.dataset_version = 2;
    check(&invalid, &c, &e, &bytes);
    for plane in ["", "../private", "..", "Private", "private/other"] {
        let mut invalid = a.clone();
        invalid.owner.plane = plane.into();
        check(&invalid, &c, &e, &bytes);
    }
    let mut invalid = a.clone();
    invalid.owner.organization_id = [0; 16];
    check(&invalid, &c, &e, &bytes);
    let mut invalid = a.clone();
    invalid.owner.source_incarnation = [0; 16];
    check(&invalid, &c, &e, &bytes);
    let mut invalid = a.clone();
    invalid.owner.source_id = [0; 16];
    check(&invalid, &c, &e, &bytes);
    let mut invalid = a.clone();
    invalid.allowed_signals.clear();
    check(&invalid, &c, &e, &bytes);
    let mut invalid = a.clone();
    invalid.allowed_signals = vec![TelemetrySignal::Logs, TelemetrySignal::Logs];
    check(&invalid, &c, &e, &bytes);
    let mut invalid = a.clone();
    invalid.consent_revision = String::new();
    check(&invalid, &c, &e, &bytes);
    let mut wrong_count = e.clone();
    wrong_count.pop();
    check(&a, &c, &wrong_count, &bytes);
    let mut duplicate = e.clone();
    duplicate[1].original = duplicate[0].original;
    check(&a, &c, &duplicate, &bytes);
    let mut reversed = e.clone();
    reversed.reverse();
    check(&a, &c, &reversed, &bytes);
    let mut invalid_event = e.clone();
    invalid_event[0].process_incarnation = [0; 16];
    check(&a, &c, &invalid_event, &bytes);
    let mut invalid_event = e.clone();
    invalid_event[0].original.signal = TelemetrySignal::Metrics;
    check(&a, &c, &invalid_event, &bytes);
    let mut invalid_event = e.clone();
    invalid_event[0].original.topic_partition.partition_id = LogicalPartitionId::new(8);
    check(&a, &c, &invalid_event, &bytes);
    let mut invalid_context = c.clone();
    invalid_context.observed_source_next_offset = LogicalOffset::new(20);
    check(&a, &invalid_context, &e, &bytes);
    let mut invalid_context = c.clone();
    invalid_context.clock.domain = None;
    check(&a, &invalid_context, &e, &bytes);
    let mut corrupt = bytes.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    check(&a, &c, &e, &corrupt);
    for n in [0, 1, 63] {
        check(&a, &c, &e, &bytes[..n]);
    }
    let too_large = vec![0; MAX_EVIDENCE_OBJECT_BYTES + 1];
    check(&a, &c, &e, &too_large);
    let (traces, trace_events, trace_bytes) = fixture(TelemetrySignal::Traces, "trace");
    check(&a, &traces, &trace_events, &trace_bytes);
    let mut wrong_plane = TelemetryEnvelope::decode(&bytes).unwrap();
    wrong_plane.tenant = Arc::from("public_marketplace");
    check(&a, &c, &e, &wrong_plane.encode().unwrap());
}

#[test]
fn limits_are_checked_before_copy_and_manifest_serialization_stays_bounded_with_escaping() {
    let mut a = admission();
    let (c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    for limits in [
        EvidenceExportLimits {
            max_object_bytes: bytes.len() - 1,
            ..EvidenceExportLimits::default()
        },
        EvidenceExportLimits {
            max_events: 1,
            ..EvidenceExportLimits::default()
        },
        EvidenceExportLimits {
            max_events: 0,
            ..EvidenceExportLimits::default()
        },
        EvidenceExportLimits {
            max_object_bytes: MAX_EVIDENCE_OBJECT_BYTES + 1,
            ..EvidenceExportLimits::default()
        },
    ] {
        assert!(matches!(
            PreparedEvidenceBatch::prepare(&a, &c, &e, &bytes, limits),
            Err(EvidenceExportError::Limits(_))
        ));
    }
    a.consent_revision = "\u{0001}".repeat(128);
    assert!(matches!(
        PreparedEvidenceBatch::prepare(
            &a,
            &c,
            &e,
            &bytes,
            EvidenceExportLimits {
                max_manifest_bytes: 256,
                ..EvidenceExportLimits::default()
            }
        ),
        Err(EvidenceExportError::Limits("manifest bytes"))
    ));
    let valid = prepare(&a, &c, &e, &bytes);
    assert!(valid.manifest_bytes().len() <= MAX_EVIDENCE_MANIFEST_BYTES);
    assert!(valid.object.capacity() <= MAX_EVIDENCE_OBJECT_BYTES);
    assert!(valid.manifest.capacity() <= MAX_EVIDENCE_MANIFEST_BYTES);
}

#[test]
fn noncontiguous_original_addresses_and_unknown_loss_are_preserved_without_completeness_promotion()
{
    let a = admission();
    let (mut c, e, bytes) = fixture(TelemetrySignal::Logs, "original");
    let first = prepare(&a, &c, &e, &bytes);
    c.source_sequence += 1;
    let repacked = prepare(&a, &c, &e, &bytes);
    assert_ne!(first.reference().bundle_id, repacked.reference().bundle_id);
    let first_manifest: serde_json::Value = serde_json::from_slice(first.manifest_bytes()).unwrap();
    let repacked_manifest: serde_json::Value =
        serde_json::from_slice(repacked.manifest_bytes()).unwrap();
    assert_eq!(first_manifest["events"], repacked_manifest["events"]);
    assert_eq!(first_manifest["source"]["loss"], "unknown");
    assert_eq!(first_manifest["source"]["completeness"], "selected_records");
    c.loss = EvidenceLossStatus::Reported(vec![]);
    let empty = prepare(&a, &c, &e, &bytes);
    assert_ne!(empty.reference().bundle_id, repacked.reference().bundle_id);
    c.loss = EvidenceLossStatus::Reported(vec![EvidenceLossInterval {
        first_offset: 11,
        next_offset: 20,
        reason: EvidenceLossReason::CapacityPolicy,
    }]);
    prepare(&a, &c, &e, &bytes);
    c.loss = EvidenceLossStatus::Reported(vec![EvidenceLossInterval {
        first_offset: 10,
        next_offset: 11,
        reason: EvidenceLossReason::SourceRetention,
    }]);
    assert!(
        PreparedEvidenceBatch::prepare(&a, &c, &e, &bytes, EvidenceExportLimits::default())
            .is_err()
    );
}
