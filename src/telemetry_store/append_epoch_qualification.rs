//! Real engine/store regression boundaries for trusted append-token handoff.
//! The installed test fence proves SDK forwarding, not private HA authority or quorum.

use super::*;
use shard_stream_core::{WriteFence, WriteFenceError};
use std::sync::atomic::AtomicBool;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
struct EpochAuthority {
    required: AtomicU64,
    supplied: AtomicU64,
    supplier_available: AtomicBool,
    leader: AtomicBool,
}

impl EpochAuthority {
    fn new() -> Self {
        Self {
            required: AtomicU64::new(7),
            supplied: AtomicU64::new(7),
            supplier_available: AtomicBool::new(true),
            leader: AtomicBool::new(true),
        }
    }
}

impl WriteFence for EpochAuthority {
    fn check_write(&self, _partition: TopicPartition, epoch: u64) -> Result<(), WriteFenceError> {
        if !self.leader.load(Ordering::SeqCst) || epoch != self.required.load(Ordering::SeqCst) {
            return Err(WriteFenceError::new(
                "writer epoch is not currently admitted",
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct EpochGate(Arc<EpochAuthority>);

impl TelemetryAppendGate for EpochGate {
    fn check_append_partitions(
        &self,
        _partitions: &[crate::NativePartitionAppend],
    ) -> Result<(), String> {
        if !self.0.leader.load(Ordering::SeqCst) {
            return Err("local writer is not a leader".into());
        }
        Ok(())
    }

    fn append_leader_epoch(&self, _partition: TopicPartition) -> Result<Option<u64>, String> {
        if !self.0.leader.load(Ordering::SeqCst)
            || !self.0.supplier_available.load(Ordering::SeqCst)
        {
            return Err("trusted writer epoch is unavailable".into());
        }
        Ok(Some(self.0.supplied.load(Ordering::SeqCst)))
    }
}

#[derive(Debug)]
struct LegacyGate;

impl TelemetryAppendGate for LegacyGate {
    fn check_append_partitions(
        &self,
        _partitions: &[crate::NativePartitionAppend],
    ) -> Result<(), String> {
        Ok(())
    }
}

struct Directory(PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            fs::remove_dir_all(&self.0).expect("remove epoch fixture after store shutdown");
        }
    }
}

struct Fixture {
    store: DurableTelemetryStore,
    authority: Arc<EpochAuthority>,
    _directory: Directory,
}

impl Fixture {
    fn new(fenced: bool, legacy_gate: bool) -> Self {
        let directory = Directory(std::env::temp_dir().join(format!(
            "shard-telemetry-append-epoch-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).expect("fixture clock").as_nanos()
        )));
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
        let factory = Arc::new(
            TelemetrySinkFactory::new(
                config.shard_ids(),
                OtlpSinkConfig {
                    state_directory: Some(directory.0.join("index-journal")),
                    ..OtlpSinkConfig::default()
                },
            )
            .expect("actual telemetry factory"),
        );
        let service = factory.service();
        let authority = Arc::new(EpochAuthority::new());
        let fence = fenced.then(|| Arc::clone(&authority) as Arc<dyn WriteFence>);
        let engine = Arc::new(
            StreamEngine::open_with_all_components_and_durable_sink(
                config,
                fence,
                None,
                None,
                Some(DurableSinkConfig::new(factory)),
            )
            .expect("actual fenced engine and sink"),
        );
        let gate: Arc<dyn TelemetryAppendGate> = if legacy_gate {
            Arc::new(LegacyGate)
        } else {
            Arc::new(EpochGate(Arc::clone(&authority)))
        };
        let store = DurableTelemetryStore::attach_with_durability(TelemetryHostAttachment {
            data_directory: directory.0.join("store"),
            engine: Arc::clone(&engine),
            service,
            tenant_partitions: 1,
            ingest_stripes_per_tenant: 1,
            indexed_ack_timeout: Duration::from_secs(10),
            retention: None,
            append_durability: TelemetryAppendDurability::Leader,
            append_gate: Some(gate),
        })
        .expect("actual host attachment");
        for partition in store.telemetry_partitions() {
            engine
                .resume_durable_sink(&[partition])
                .expect("resume actual leader sink");
        }
        Self {
            store,
            authority,
            _directory: directory,
        }
    }
}

fn single_batch() -> crate::NativeTelemetryBatch {
    let mut batch = super::provider_status_qualification::native_batch();
    batch.partitions.truncate(1);
    batch
}

fn wal(store: &DurableTelemetryStore, partition: TopicPartition) -> Vec<(u64, u64, Vec<u8>)> {
    store
        .fetch_telemetry_batches(partition, LogicalOffset::new(0), 1024 * 1024)
        .expect("actual authoritative WAL")
        .into_iter()
        .map(|batch| {
            (
                batch.first_offset.get(),
                batch.last_offset.get(),
                batch.envelope.encode().expect("actual WAL STEL"),
            )
        })
        .collect()
}

#[test]
fn append_epoch_handoff_indexes_all_signals_without_a_client_epoch_field() {
    let fixture = Fixture::new(true, false);
    let batch = super::provider_status_qualification::native_batch();
    let ack = fixture
        .store
        .append_telemetry_batch(&batch, true)
        .expect("trusted epochs reach real engine fence");
    assert_eq!(ack.partitions.len(), 3);
    for (partition, ack) in batch.partitions.iter().zip(&ack.partitions) {
        assert_eq!(ack.topic_partition, partition.topic_partition);
        assert_eq!((ack.first_offset, ack.last_offset), (0, 0));
        assert_eq!(
            wal(&fixture.store, partition.topic_partition),
            vec![(0, 0, partition.envelope.encode().expect("original STEL"))]
        );
        assert_eq!(
            fixture
                .store
                .engine
                .durable_sink_checkpoint(partition.topic_partition)
                .expect("real applied checkpoint")
                .expect("loaded checkpoint")
                .next_offset
                .get(),
            1
        );
    }
}

#[test]
fn append_epoch_missing_default_keeps_fenced_wal_empty_and_unfenced_compatibility() {
    let batch = single_batch();
    let partition = batch.partitions[0].topic_partition;
    let fenced = Fixture::new(true, true);
    assert!(
        fenced
            .store
            .append_telemetry_batch_with_retry_id(&batch, true, 17)
            .is_err()
    );
    assert!(wal(&fenced.store, partition).is_empty());
    assert_eq!(
        fenced
            .store
            .engine
            .watermarks(partition)
            .expect("fenced WAL state")
            .allocated_end
            .get(),
        0
    );
    let unfenced = Fixture::new(false, true);
    let ack = unfenced
        .store
        .append_telemetry_batch_with_retry_id(&batch, true, 17)
        .expect("legacy gate remains compatible without a write fence");
    assert_eq!(ack.partitions[0].first_offset, 0);
    assert_eq!(wal(&unfenced.store, partition).len(), 1);
}

#[test]
fn append_epoch_stale_or_missing_authority_preserves_wal_retry_and_later_progress() {
    let fixture = Fixture::new(true, false);
    let batch = single_batch();
    let partition = batch.partitions[0].topic_partition;
    fixture.authority.supplied.store(6, Ordering::SeqCst);
    assert!(
        fixture
            .store
            .append_telemetry_batch_with_retry_id(&batch, true, 17)
            .is_err()
    );
    assert!(wal(&fixture.store, partition).is_empty());
    assert_eq!(
        fixture
            .store
            .engine
            .watermarks(partition)
            .expect("stale refusal state")
            .allocated_end
            .get(),
        0
    );
    fixture.authority.supplied.store(7, Ordering::SeqCst);
    let first = fixture
        .store
        .append_telemetry_batch_with_retry_id(&batch, true, 17)
        .expect("same abandoned retry ID progresses with the current epoch");
    let before = wal(&fixture.store, partition);
    fixture.authority.required.store(8, Ordering::SeqCst);
    assert!(
        fixture
            .store
            .append_telemetry_batch_with_retry_id(&batch, true, 18)
            .is_err()
    );
    assert_eq!(wal(&fixture.store, partition), before);
    fixture.authority.supplied.store(8, Ordering::SeqCst);
    fixture
        .authority
        .supplier_available
        .store(false, Ordering::SeqCst);
    assert!(
        fixture
            .store
            .append_telemetry_batch_with_retry_id(&batch, true, 18)
            .is_err()
    );
    assert_eq!(wal(&fixture.store, partition), before);
    fixture
        .authority
        .supplier_available
        .store(true, Ordering::SeqCst);
    let second = fixture
        .store
        .append_telemetry_batch_with_retry_id(&batch, true, 18)
        .expect("current trusted authority restores append progress");
    assert_eq!(second.partitions[0].first_offset, 1);
    let after = wal(&fixture.store, partition);
    assert_eq!(after.len(), 2);
    assert_eq!(
        fixture
            .store
            .append_telemetry_batch_with_retry_id(&batch, true, 17)
            .expect("matching cached retry remains the original durable ACK"),
        first
    );
    assert_eq!(wal(&fixture.store, partition), after);
    fixture.authority.leader.store(false, Ordering::SeqCst);
    assert!(
        fixture
            .store
            .append_telemetry_batch_with_retry_id(&batch, true, 17)
            .is_err()
    );
    assert_eq!(
        wal(&fixture.store, partition),
        after,
        "cached identity cannot bypass host admission"
    );
}
