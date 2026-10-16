//! Actual recovered-engine custody through telemetry attachment and native work.

use super::*;
use shard_stream_core::{DirectoryOwnership, DirectoryOwnershipError};
use shard_stream_engine::{NativeRecoveryCensus, NativeRecoveryLimits};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("fixture clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "telemetry-owned-engine-{}-{now}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).expect("unique native fixture directory");
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).expect("remove fixture after physical joins");
    }
}

// An escaping assertion still releases and joins the actual thread before its
// outer fixture can remove files. Explicit finish also checks the job result.
struct HeldJob {
    release: Option<SyncSender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl HeldJob {
    fn finish(mut self) {
        self.release
            .take()
            .expect("release sender")
            .send(())
            .expect("release actual job");
        self.thread
            .take()
            .expect("actual job handle")
            .join()
            .expect("native store job");
    }
}

impl Drop for HeldJob {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn recovered_attachment_keeps_original_engine_root_until_real_background_work_joins() {
    let directory = Directory::new();
    let config = EngineConfig {
        data_dir: directory.0.join("stream"),
        object_store_dir: None,
        shard_count: 1,
        virtual_lane_count: 1,
        replication_factor: 1,
        min_in_sync_replicas: 1,
        queue_slots_per_shard: 64,
        queue_bytes_per_shard: 2 * 1024 * 1024,
        target_pack_bytes: 64 * 1024,
        max_pack_age: Duration::from_secs(1),
        max_batch_bytes: 1024 * 1024,
        max_fetch_bytes: 1024 * 1024,
        append_linger: Duration::ZERO,
    };
    // Create an actual WAL before the ownership-aware pre-open census. Its
    // opaque native topic is separate from the three telemetry signal topics.
    let seed_topic = TopicId::new(u128::MAX - 1);
    {
        let producer = StreamEngine::open(config.clone()).expect("real native producer");
        producer
            .create_topic(TopicConfig {
                topic_id: seed_topic,
                partitions: 1,
                shards: None,
            })
            .expect("actual native topic");
        producer
            .append(AppendRequest {
                request_id: 1,
                topic_id: seed_topic,
                partition_id: LogicalPartitionId::new(0),
                record_count: 1,
                payload: Bytes::from_static(b"original native body"),
                durability: Durability::Leader,
                producer: None,
                atomic_group: None,
                leader_epoch: None,
                extension_context: None,
            })
            .expect("actual native WAL commit");
    }
    let owner = DirectoryOwnership::acquire(&config.data_dir).expect("original root owner");
    let census = NativeRecoveryCensus::read_for_engine(
        config.clone(),
        owner,
        None,
        NativeRecoveryLimits {
            max_files: 256,
            max_entries: 1_000_000,
            max_read_bytes: 128 << 20,
            max_declared_bytes: 128 << 20,
            max_work_bytes: 1 << 30,
            max_frame_bytes: 1 << 20,
            max_manifest_bytes: 1 << 20,
            max_partial_tail_bytes: 1 << 20,
            max_path_bytes: 8192,
        },
        Instant::now() + Duration::from_secs(30),
    )
    .expect("actual native census");
    let storage = census
        .restore_storage()
        .expect("actual retained WAL restore");
    let factory = Arc::new(
        TelemetrySinkFactory::new(
            config.shard_ids(),
            OtlpSinkConfig {
                state_directory: Some(directory.0.join("index-journal")),
                ..OtlpSinkConfig::default()
            },
        )
        .expect("real telemetry sink factory"),
    );
    let service = factory.service();
    let recovered = Arc::new(
        StreamEngine::open_recovered_storage(
            storage,
            None,
            None,
            None,
            Some(DurableSinkConfig::new(factory)),
        )
        .expect("actual owned engine and sink"),
    );
    let native_weak = Arc::downgrade(&recovered);
    let root_weak = Arc::downgrade(recovered.directory_ownership());
    let handle = EngineHandle::from_recovered(Arc::clone(&recovered));
    let store = Arc::new(
        DurableTelemetryStore::attach_with_engine_handle(TelemetryEngineAttachment {
            data_directory: directory.0.join("product"),
            engine: handle.clone(),
            service,
            tenant_partitions: 1,
            ingest_stripes_per_tenant: 1,
            indexed_ack_timeout: Duration::from_secs(10),
            retention: None,
            append_durability: TelemetryAppendDurability::Leader,
            append_gate: None,
        })
        .expect("real recovered host attachment"),
    );
    assert!(
        store.engine.ptr_eq(&handle),
        "exact supplied recovered wrapper"
    );
    store
        .engine
        .resume_durable_sink(&store.telemetry_partitions())
        .expect("resume actual telemetry partitions");
    let batch = super::provider_status_qualification::native_batch();
    let acknowledgement = store
        .append_telemetry_batch(&batch, true)
        .expect("real indexed append");
    assert_eq!(acknowledgement.partitions.len(), 3);
    drop(handle);
    drop(recovered);
    assert!(native_weak.upgrade().is_some());
    assert!(root_weak.upgrade().is_some());

    let (entered, observed) = sync_channel(1);
    let (release, wait) = sync_channel(1);
    let held = Arc::clone(&store);
    let partition = batch.partitions[0].topic_partition;
    let original = batch.partitions[0]
        .envelope
        .encode()
        .expect("original telemetry envelope");
    let job = HeldJob {
        release: Some(release),
        thread: Some(thread::spawn(move || {
            entered.send(()).expect("actual job entered");
            wait.recv_timeout(Duration::from_secs(10))
                .expect("physical job release");
            let rows = held
                .fetch_telemetry_batches(partition, LogicalOffset::new(0), 1024 * 1024)
                .expect("real telemetry WAL fetch");
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].envelope.encode().expect("stored envelope"),
                original
            );
        })),
    };
    observed
        .recv_timeout(Duration::from_secs(10))
        .expect("actual job holds store");
    drop(store);
    assert!(
        native_weak.upgrade().is_some(),
        "physical job retains recovered engine"
    );
    assert!(
        root_weak.upgrade().is_some(),
        "physical job retains original root"
    );
    assert!(matches!(
        DirectoryOwnership::acquire(&config.data_dir),
        Err(DirectoryOwnershipError::Contended)
    ));
    job.finish();
    assert!(native_weak.upgrade().is_none());
    assert!(root_weak.upgrade().is_none());
    drop(DirectoryOwnership::acquire(&config.data_dir).expect("root released after real joins"));
}
