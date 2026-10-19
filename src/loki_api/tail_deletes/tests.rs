//! Production session/codec tests with a controlled Sink/Stream transport.
//! These do not establish a physical HTTP upgrade or network WebSocket result.

use super::*;
use std::cell::RefCell;
use std::io;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Waker};
use tokio::sync::{Notify, mpsc};

type EncodingHook = Box<dyn FnMut(&Message) -> bool>;
thread_local! {
    static ENCODED: RefCell<Option<EncodingHook>> = const { RefCell::new(None) };
}

pub(super) fn encoded(message: &Message) {
    ENCODED.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_mut().is_some_and(|hook| hook(message)) {
            let _ = slot.take();
        }
    });
}

struct EncodingObserver;
impl EncodingObserver {
    fn install(hook: impl FnMut(&Message) -> bool + 'static) -> Self {
        ENCODED.with(|slot| {
            assert!(slot.borrow_mut().replace(Box::new(hook)).is_none());
        });
        Self
    }
}
impl Drop for EncodingObserver {
    fn drop(&mut self) {
        let _ = ENCODED.with(|slot| slot.borrow_mut().take());
    }
}

#[derive(Debug)]
struct Gate {
    query: AtomicBool,
    checks: AtomicUsize,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            query: AtomicBool::new(true),
            checks: AtomicUsize::new(0),
        })
    }
}
impl crate::NativeRequestGate for Gate {
    fn check(&self) -> Result<(), String> {
        Ok(())
    }
    fn check_query(&self) -> Result<(), String> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        if self.query.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("fixture query source invalidated".into())
        }
    }
}

struct TransportState {
    ready: AtomicBool,
    waiting: AtomicBool,
    waker: Mutex<Option<Waker>>,
    messages: Mutex<Vec<Message>>,
    changed: Notify,
}
struct Transport {
    state: Arc<TransportState>,
    incoming: mpsc::Receiver<Result<Message, io::Error>>,
}
struct Peer {
    state: Arc<TransportState>,
    incoming: mpsc::Sender<Result<Message, io::Error>>,
}
impl Peer {
    fn pair(ready: bool) -> (Self, Transport) {
        let state = Arc::new(TransportState {
            ready: AtomicBool::new(ready),
            waiting: AtomicBool::new(false),
            waker: Mutex::new(None),
            messages: Mutex::new(Vec::new()),
            changed: Notify::new(),
        });
        let (incoming, receiver) = mpsc::channel(1);
        (
            Self {
                state: Arc::clone(&state),
                incoming,
            },
            Transport {
                state,
                incoming: receiver,
            },
        )
    }
    fn ready(&self) {
        self.state.ready.store(true, Ordering::SeqCst);
        if let Some(waker) = self.state.waker.lock().expect("transport waker").take() {
            waker.wake();
        }
    }
    async fn wait(&self, condition: impl Fn(&TransportState) -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.state.changed.notified();
                if condition(&self.state) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("bounded session progress");
    }
    async fn messages(&self, count: usize) -> Vec<Value> {
        self.wait(|state| state.messages.lock().expect("encoded messages").len() >= count)
            .await;
        self.state
            .messages
            .lock()
            .expect("encoded messages")
            .iter()
            .map(json_message)
            .collect()
    }
}
impl Sink<Message> for Transport {
    type Error = io::Error;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.state.ready.load(Ordering::SeqCst) {
            return Poll::Ready(Ok(()));
        }
        *self.state.waker.lock().expect("transport waker") = Some(cx.waker().clone());
        self.state.waiting.store(true, Ordering::SeqCst);
        self.state.changed.notify_one();
        if self.state.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        let mut messages = self.state.messages.lock().expect("encoded messages");
        assert!(messages.len() < 16, "finite test transport");
        messages.push(message);
        self.state.changed.notify_one();
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
impl Stream for Transport {
    type Item = Result<Message, io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.incoming.poll_recv(cx)
    }
}

fn json_message(message: &Message) -> Value {
    let Message::Text(text) = message else {
        panic!("real tail text message")
    };
    serde_json::from_str(text.as_str()).expect("actual Loki JSON serialization")
}

struct Fixture {
    state: ApiState,
    runtime: Arc<ProductionRuntime>,
    lifecycle: Arc<crate::ServiceLifecycle>,
}
impl Fixture {
    fn new(capacity: usize) -> Self {
        let lifecycle = Arc::new(crate::ServiceLifecycle::new());
        lifecycle.mark_ready();
        let runtime = Arc::new(
            ProductionRuntime::new(
                crate::SingleTenantConfig {
                    tenant: Arc::from("tenant-a"),
                    bearer_token: Arc::from("synthetic-tail-token"),
                    max_http_in_flight: 8,
                    max_ingest_in_flight: 4,
                    max_query_in_flight: 4,
                    ingest_bytes_per_second: 0,
                    ingest_burst_bytes: 0,
                    max_tail_subscribers: 1,
                    max_native_connections: 4,
                    query_timeout: Duration::from_secs(30),
                    native_auth_timeout: Duration::from_secs(5),
                },
                Arc::clone(&lifecycle),
            )
            .expect("unchanged bounded runtime"),
        );
        Self {
            state: ApiState {
                store: Arc::new(LokiApiStore::default()),
                config: LokiApiConfig {
                    default_tenant: Arc::from("tenant-a"),
                    ..LokiApiConfig::default()
                },
                live: broadcast::channel(capacity).0,
                analytics_bearer_token: None,
                production: Some(Arc::clone(&runtime)),
                flush_timeout: Duration::from_secs(30),
            },
            runtime,
            lifecycle,
        }
    }
    fn body(line: &str) -> Bytes {
        Bytes::from(
            json!({"streams": [{"stream": {"app": "tail"}, "values": [[
                now_nanos().to_string(), line, {"trace_id": "real-metadata"}
            ]]}]})
            .to_string(),
        )
    }
    async fn push(&self, line: &str) {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/json".parse().expect("JSON type"),
        );
        assert_eq!(
            push_logs(State(self.state.clone()), headers, Self::body(line))
                .await
                .expect("genuine ingest/store/broadcast path"),
            StatusCode::NO_CONTENT
        );
    }
    fn burst(&self, count: usize) {
        // Synchronous real codec/store operations make receiver lag deterministic;
        // no fabricated encoded tail response or replacement query predicate.
        for index in 0..count {
            let entries = decode_json_push(&Self::body(&format!("lag entry {index}")))
                .expect("real JSON push codec");
            self.state
                .store
                .push("tenant-a", entries.clone())
                .expect("actual SDK store push");
            self.state
                .live
                .send(LivePush {
                    tenant: "tenant-a".into(),
                    entries,
                })
                .expect("actual subscription");
        }
    }
    async fn session(&self, gate: Option<Arc<dyn crate::NativeRequestGate>>) -> TailSession {
        let live = self.state.live.subscribe();
        let initial = execute_stream_query(
            self.state.clone(),
            HeaderMap::new(),
            QueryParams {
                query: Some(r#"{app="tail"}"#.into()),
                start: Some("0".into()),
                end: Some(now_nanos().to_string()),
                ..QueryParams::default()
            },
        )
        .await
        .expect("genuine initial Loki store query")
        .0;
        TailSession {
            initial,
            tenant: "tenant-a".into(),
            selector: parse_log_query(r#"{app="tail"}"#).expect("real selector"),
            store: Arc::clone(&self.state.store),
            live,
            shutdown: Some(self.lifecycle.subscribe_shutdown()),
            _permit: Some(self.runtime.try_tail().expect("original subscriber permit")),
            gate,
        }
    }
    fn permit_released(&self) {
        assert!(
            self.runtime.try_tail().is_some(),
            "actual original subscriber permit released"
        );
    }
}

async fn joined(task: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("finite session shutdown")
        .expect("actual session physically joined");
}

#[tokio::test]
async fn tail_session_preserves_real_query_push_codec_and_optional_gate_behavior() {
    for gated in [false, true] {
        let fixture = Fixture::new(2);
        fixture.push("initial actual entry").await;
        let gate = Gate::new();
        let session = fixture
            .session(gated.then(|| gate.clone() as Arc<dyn crate::NativeRequestGate>))
            .await;
        assert!(fixture.runtime.try_tail().is_none());
        let (peer, socket) = Peer::pair(true);
        let task = tokio::spawn(session.run(socket));
        let initial = peer.messages(1).await;
        assert_eq!(
            initial[0]["streams"][0]["values"][0][1],
            "initial actual entry"
        );
        assert_eq!(
            initial[0]["streams"][0]["stream"]["trace_id"],
            "real-metadata"
        );
        fixture.push("live actual entry").await;
        let messages = peer.messages(2).await;
        assert_eq!(
            messages[1]["streams"][0]["values"][0][1],
            "live actual entry"
        );
        assert_eq!(messages[1]["dropped_entries"], json!([]));
        assert_eq!(
            gate.checks.load(Ordering::SeqCst),
            if gated { 2 } else { 0 }
        );
        fixture.burst(5);
        let lag = peer.messages(3).await;
        assert_eq!(lag[2]["streams"], json!([]));
        assert_eq!(lag[2]["dropped_entries"][0]["dropped"], 3);
        fixture.lifecycle.request_shutdown();
        joined(task).await;
        fixture.permit_released();
    }
}

#[tokio::test]
async fn tail_session_fences_encoded_initial_live_and_lag_with_the_same_gate() {
    for kind in ["initial", "live", "lag"] {
        let fixture = Fixture::new(2);
        fixture.push("initial actual entry").await;
        let gate = Gate::new();
        let weak = Arc::downgrade(&gate);
        let session = fixture.session(Some(gate.clone())).await;
        let (peer, socket) = Peer::pair(true);
        let seen = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&seen);
        let held = Arc::clone(&gate);
        let hook = move |message: &Message| {
            let json = json_message(message);
            let is_target = match kind {
                "initial" => json["streams"][0]["values"][0][1] == "initial actual entry",
                "live" => json["streams"][0]["values"][0][1] == "live actual entry",
                "lag" => json["dropped_entries"][0]["dropped"]
                    .as_u64()
                    .is_some_and(|count| count > 0),
                _ => unreachable!(),
            };
            if is_target {
                observed.store(true, Ordering::SeqCst);
                held.query.store(false, Ordering::SeqCst);
            }
            is_target
        };
        let observer = EncodingObserver::install(hook);
        drop(gate);
        let task = tokio::spawn(session.run(socket));
        if kind != "initial" {
            peer.messages(1).await;
            assert!(
                weak.upgrade().is_some(),
                "same gate remains in actual session"
            );
            if kind == "live" {
                fixture.push("live actual entry").await;
            } else {
                fixture.burst(5);
            }
        }
        joined(task).await;
        assert!(
            seen.load(Ordering::SeqCst),
            "actual target serialization reached"
        );
        assert_eq!(
            peer.state.messages.lock().expect("actual sends").len(),
            usize::from(kind != "initial")
        );
        drop(observer);
        assert!(
            weak.upgrade().is_none(),
            "session and hook release the same gate"
        );
        fixture.permit_released();
        // Query refusal does not alter the actual independent ingest path.
        fixture.push("append after query refusal").await;
        assert!(
            fixture
                .state
                .store
                .entries("tenant-a")
                .expect("real stored rows")
                .iter()
                .any(|row| row.line == "append after query refusal")
        );
    }
}

#[tokio::test]
async fn tail_session_checks_the_retained_encoded_message_after_backpressure() {
    let fixture = Fixture::new(1_024);
    fixture.push("retained initial entry").await;
    let gate = Gate::new();
    let weak = Arc::downgrade(&gate);
    let session = fixture.session(Some(gate.clone())).await;
    let (peer, socket) = Peer::pair(false);
    let task = tokio::spawn(session.run(socket));
    peer.wait(|state| state.waiting.load(Ordering::SeqCst))
        .await;
    assert_eq!(
        gate.checks.load(Ordering::SeqCst),
        0,
        "not admitted before readiness"
    );
    assert!(peer.state.messages.lock().expect("actual sends").is_empty());
    gate.query.store(false, Ordering::SeqCst);
    drop(gate);
    assert!(
        weak.upgrade().is_some(),
        "pending send retains original gate"
    );
    peer.ready();
    joined(task).await;
    assert!(peer.state.messages.lock().expect("actual sends").is_empty());
    assert!(weak.upgrade().is_none());
    fixture.permit_released();
}

#[tokio::test]
async fn tail_session_releases_owners_on_peer_close_and_pending_shutdown() {
    for pending in [false, true] {
        let fixture = Fixture::new(1_024);
        let gate = Gate::new();
        let weak = Arc::downgrade(&gate);
        let session = fixture.session(Some(gate.clone())).await;
        drop(gate);
        let (peer, socket) = Peer::pair(!pending);
        let task = tokio::spawn(session.run(socket));
        if pending {
            peer.wait(|state| state.waiting.load(Ordering::SeqCst))
                .await;
            assert!(peer.state.messages.lock().expect("actual sends").is_empty());
            fixture.lifecycle.request_shutdown();
        } else {
            peer.messages(1).await;
            peer.incoming
                .send(Ok(Message::Close(None)))
                .await
                .expect("actual peer close event");
        }
        joined(task).await;
        assert!(weak.upgrade().is_none(), "no session owner cycle");
        fixture.permit_released();
    }
}

struct PendingPeerState {
    events: std::collections::VecDeque<Result<Message, io::Error>>,
    ended: bool,
    waker: Option<Waker>,
}
struct PendingPeer {
    state: Mutex<PendingPeerState>,
}
impl PendingPeer {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PendingPeerState {
                events: std::collections::VecDeque::with_capacity(2),
                ended: false,
                waker: None,
            }),
        })
    }
    fn finish(&self, terminal: &str) {
        let waker = {
            let mut state = self.state.lock().expect("finite controlled peer state");
            assert!(state.events.is_empty());
            // Both events arrive with ONE wake. After consuming this buffered
            // nonterminal item, the actual sender must schedule its next poll
            // even though the sink never wakes and no later peer event arrives.
            state.events.push_back(Ok(Message::Ping(Bytes::new())));
            match terminal {
                "close" => state.events.push_back(Ok(Message::Close(None))),
                "error" => state
                    .events
                    .push_back(Err(io::Error::other("actual fixture peer error"))),
                "eof" => state.ended = true,
                _ => unreachable!(),
            }
            state
                .waker
                .take()
                .expect("actual pending stream registered its owner")
        };
        waker.wake();
    }
}

struct PendingTransport {
    inner: Transport,
    peer: Arc<PendingPeer>,
    flush_pending: bool,
    flush_entered: Arc<AtomicBool>,
}
impl Sink<Message> for PendingTransport {
    type Error = io::Error;
    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }
    fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        Pin::new(&mut self.inner).start_send(message)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.flush_pending {
            self.flush_entered.store(true, Ordering::SeqCst);
            self.inner.state.changed.notify_one();
            Poll::Pending
        } else {
            Pin::new(&mut self.inner).poll_flush(cx)
        }
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}
impl Stream for PendingTransport {
    type Item = Result<Message, io::Error>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self
            .peer
            .state
            .lock()
            .expect("finite controlled peer state");
        if let Some(event) = state.events.pop_front() {
            Poll::Ready(Some(event))
        } else if state.ended {
            Poll::Ready(None)
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[tokio::test]
async fn tail_session_releases_owners_on_peer_end_during_pending_ready_and_flush() {
    for stage in ["ready", "flush"] {
        for terminal in ["close", "error", "eof"] {
            let fixture = Fixture::new(1_024);
            fixture
                .push("actual entry retained during pending send")
                .await;
            let gate = Gate::new();
            let weak = Arc::downgrade(&gate);
            let session = fixture.session(Some(gate.clone())).await;
            drop(gate);
            let (sink, transport) = Peer::pair(stage != "ready");
            let peer = PendingPeer::new();
            let flush_entered = Arc::new(AtomicBool::new(false));
            let socket = PendingTransport {
                inner: transport,
                peer: Arc::clone(&peer),
                flush_pending: stage == "flush",
                flush_entered: Arc::clone(&flush_entered),
            };
            let task = tokio::spawn(session.run(socket));
            sink.wait(|state| {
                if stage == "ready" {
                    state.waiting.load(Ordering::SeqCst)
                } else {
                    flush_entered.load(Ordering::SeqCst)
                }
            })
            .await;
            assert!(
                fixture.runtime.try_tail().is_none(),
                "original permit still held while pending"
            );
            assert_eq!(
                weak.upgrade()
                    .expect("same gate retained while pending")
                    .checks
                    .load(Ordering::SeqCst),
                usize::from(stage == "flush")
            );
            let enqueued = sink.state.messages.lock().expect("actual enqueues").len();
            assert_eq!(enqueued, usize::from(stage == "flush"));
            if stage == "flush" {
                assert_eq!(
                    json_message(&sink.state.messages.lock().expect("actual encoded message")[0])["streams"]
                        [0]["values"][0][1],
                    "actual entry retained during pending send"
                );
            }
            peer.finish(terminal);
            joined(task).await;
            assert!(
                weak.upgrade().is_none(),
                "actual terminal peer event releases gate custody"
            );
            fixture.permit_released();
            assert_eq!(
                fixture.lifecycle.state(),
                crate::ServiceState::Ready,
                "no service shutdown was needed"
            );
            assert_eq!(
                sink.state.messages.lock().expect("actual enqueues").len(),
                enqueued
            );
            if stage == "ready" {
                assert!(
                    !sink.state.ready.load(Ordering::SeqCst),
                    "sink never became ready"
                );
            } else {
                assert!(
                    flush_entered.load(Ordering::SeqCst),
                    "actual flush stayed pending"
                );
            }
            assert!(
                peer.state
                    .lock()
                    .expect("finite peer state")
                    .events
                    .is_empty(),
                "the buffered terminal event was consumed without a second peer or sink wake"
            );
        }
    }
}
