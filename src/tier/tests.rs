use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId};

use super::*;
use crate::{
    CompressionCohortId, CompressionPlacementId, CompressionTemperature, DictionaryId, TraceId,
};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "shard-telemetry-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("test directory is created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(91), LogicalPartitionId::new(3))
}

fn tier_config(groups_per_page: usize) -> ObjectTierConfig {
    ObjectTierConfig {
        groups_per_page,
        ..ObjectTierConfig::default()
    }
}

fn write_test_file(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("test artifact is written");
}

#[test]
fn catalog_correlation_summary_keeps_primary_and_linked_trace_matches() {
    let primary = TraceId::from_bytes([1; 16]).expect("primary trace ID is valid");
    let linked = TraceId::from_bytes([2; 16]).expect("linked trace ID is valid");
    let absent = TraceId::from_bytes([3; 16]).expect("absent trace ID is valid");
    let primary_value = u128::from_be_bytes(*primary.as_bytes());
    let linked_query = CorrelationQuery::new("tenant-a").with_trace_id(linked);
    let linked_filter = CorrelationBlockFilter::from_query(&linked_query);

    assert!(catalog_correlation_may_match(
        Some(&CorrelationBlockFilter::default()),
        Some(primary_value),
        Some(primary_value),
        &CorrelationQuery::new("tenant-a").with_trace_id(primary),
        TelemetrySignal::Traces,
    ));
    assert!(catalog_correlation_may_match(
        Some(&linked_filter),
        Some(primary_value),
        Some(primary_value),
        &linked_query,
        TelemetrySignal::Traces,
    ));
    assert!(!catalog_correlation_may_match(
        Some(&linked_filter),
        Some(primary_value),
        Some(primary_value),
        &CorrelationQuery::new("tenant-a").with_trace_id(absent),
        TelemetrySignal::Traces,
    ));
    assert!(catalog_correlation_may_match(
        Some(&linked_filter),
        Some(99),
        Some(99),
        &linked_query,
        TelemetrySignal::Metrics,
    ));
}

fn group_source(
    directory: &Path,
    sequence: u64,
    first_offset: u64,
    timestamp: u64,
) -> TierGroupSource {
    let payload = vec![u8::try_from(sequence).unwrap_or(u8::MAX); 32];
    let payload_path = directory.join(format!("group-{sequence}.payload"));
    let index_path = directory.join(format!("group-{sequence}.query-index"));
    write_test_file(&payload_path, &payload);
    write_test_file(&index_path, format!("query-index-{sequence}").as_bytes());
    TierGroupSource {
        group_sequence: sequence,
        checkpoint: TierCheckpoint {
            next_placement_sequence: sequence + 1,
            next_offset: first_offset + 10,
        },
        blocks: vec![TierBlockEntry {
            block_id: sequence,
            source_compression_cohort: 7,
            placement_id: 11,
            dictionary_id: None,
            compression_codec: "zstd".into(),
            compression_level: 1,
            first_offset,
            last_offset: first_offset + 9,
            record_count: 10,
            source_bytes: 320,
            structural_bytes: 128,
            stored_bytes: 32,
            min_timestamp_unix_nanos: timestamp,
            max_timestamp_unix_nanos: timestamp + 99,
            compression_temperature: 17,
            compression_shape_hash: 19,
            compression_temperature_variance_q8: 2,
            max_compression_temperature_deviation: 1,
            payload_offset: 0,
            payload_bytes: 32,
            payload_checksum: checksum_bytes(&payload),
            min_signal_identity: None,
            max_signal_identity: None,
            correlation_filter: None,
        }],
        artifacts: vec![
            TierArtifactSource {
                kind: TierArtifactKind::PayloadPack,
                name: "blocks.pack".into(),
                path: payload_path,
            },
            TierArtifactSource {
                kind: TierArtifactKind::QueryIndex,
                name: "query.slogqix".into(),
                path: index_path,
            },
        ],
    }
}

#[test]
fn local_object_store_is_immutable_conditional_and_key_safe() {
    let directory = TestDirectory::new("local-object-store");
    let store = LocalObjectStore::open(&directory.path).expect("store opens");
    let first = store
        .put_bytes_if_absent("objects/one", b"first")
        .expect("immutable object is created");
    assert_eq!(
        store
            .put_bytes_if_absent("objects/one", b"first")
            .expect("identical retry succeeds"),
        first
    );
    assert!(matches!(
        store.put_bytes_if_absent("objects/one", b"different"),
        Err(TelemetryError::ObjectStore(_))
    ));
    assert!(matches!(
        store.put_bytes_if_absent("../escape", b"bad"),
        Err(TelemetryError::ObjectStore(_))
    ));

    let current = store
        .compare_and_swap("catalog/CURRENT", None, b"one")
        .expect("missing pointer is created");
    assert!(matches!(
        store.compare_and_swap("catalog/CURRENT", None, b"two"),
        Err(TelemetryError::StaleCatalog { .. })
    ));
    let next = store
        .compare_and_swap("catalog/CURRENT", Some(&current.version_token), b"two")
        .expect("matching pointer is replaced");
    assert_ne!(current.version_token, next.version_token);
    assert_eq!(current.content_digest, checksum_bytes(b"one"));
    assert_eq!(next.content_digest, checksum_bytes(b"two"));
}

#[test]
fn publication_pages_prune_restart_and_reject_stale_writers() {
    let directory = TestDirectory::new("tier-publication");
    let artifact_directory = directory.path.join("sources");
    fs::create_dir_all(&artifact_directory).expect("artifact directory is created");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let mut tier =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("empty tier opens");
    let source0 = group_source(&artifact_directory, 0, 0, 1_000);
    let manifest0 = tier
        .publish_group(source0.clone())
        .expect("first group publishes");
    assert_eq!(tier.root().generation, 1);
    assert_eq!(tier.root().pages.len(), 1);
    assert_eq!(
        tier.publish_group(source0)
            .expect("identical last-group retry succeeds"),
        manifest0
    );
    assert_eq!(tier.root().generation, 1);

    tier.publish_group(group_source(&artifact_directory, 1, 10, 2_000))
        .expect("second group publishes");
    assert_eq!(tier.root().pages.len(), 1);
    tier.publish_group(group_source(&artifact_directory, 2, 20, 3_000))
        .expect("page rollover publishes");
    assert_eq!(tier.root().pages.len(), 2);
    assert_eq!(tier.root().generation, 3);

    let candidates = tier
        .candidate_groups(TierQueryRange {
            first_offset: Some(25),
            last_offset: Some(25),
            ..TierQueryRange::default()
        })
        .expect("offset pruning succeeds");
    assert_eq!(
        candidates
            .iter()
            .map(|entry| entry.group_sequence)
            .collect::<Vec<_>>(),
        vec![2]
    );
    assert!(
        !serde_json::to_string(&candidates[0])
            .expect("log catalog entry serializes")
            .contains("correlation_filter")
    );
    let loaded = tier
        .load_group(&candidates[0])
        .expect("candidate group loads");
    assert_eq!(loaded.group_sequence, 2);
    assert_eq!(
        tier.read_artifact(
            loaded
                .artifact(TierArtifactKind::QueryIndex)
                .expect("query index exists"),
            1024,
        )
        .expect("query index verifies"),
        b"query-index-2"
    );

    let reopened =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("published tier recovers");
    assert_eq!(reopened.root(), tier.root());

    let mut first_writer =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("first writer opens");
    let mut stale_writer =
        TelemetryObjectTier::open(store, ShardId::new(4), partition(), tier_config(2))
            .expect("stale writer opens");
    first_writer
        .publish_group(group_source(&artifact_directory, 3, 30, 4_000))
        .expect("first writer advances catalog");
    assert!(matches!(
        stale_writer.publish_group(group_source(&artifact_directory, 4, 40, 5_000)),
        Err(TelemetryError::StaleCatalog { .. })
    ));
}

#[test]
fn retired_generation_waits_for_its_last_rust_lease() {
    let directory = TestDirectory::new("tier-ownership-lease");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let config = ObjectTierConfig {
        retirement_grace: std::time::Duration::ZERO,
        ..tier_config(2)
    };
    let mut tier = TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), config)
        .expect("tier opens");
    tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
        .expect("first group publishes");
    let lease = tier.catalog_lease();
    let retired_page = lease.root().pages[0].page_key.clone();
    let current = store
        .get(&format!("{}/CURRENT", tier.namespace), POINTER_READ_LIMIT)
        .expect("CURRENT reads");
    let retired_root = decode_json::<CatalogPointer>(&current, "CURRENT")
        .expect("CURRENT decodes")
        .root_key;

    tier.publish_group(group_source(&artifacts, 1, 10, 2_000))
        .expect("second group publishes");
    assert_eq!(tier.pending_retired_objects(), 2);
    assert!(store.head(&retired_root).expect("root head").is_some());
    assert!(store.head(&retired_page).expect("page head").is_some());

    drop(lease);
    tier.reclaim_retired_objects()
        .expect("lease release reclaims");
    assert_eq!(tier.pending_retired_objects(), 0);
    assert!(store.head(&retired_root).expect("root head").is_none());
    assert!(store.head(&retired_page).expect("page head").is_none());
}

#[derive(Debug, Clone)]
struct FailDeleteOnceStore {
    inner: LocalObjectStore,
    fail_delete: Arc<AtomicBool>,
}

impl TelemetryObjectStore for FailDeleteOnceStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        self.inner.put_bytes_if_absent(key, bytes)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        self.inner.put_file_if_absent(key, source)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        self.inner.get(key, max_bytes)
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        self.inner.get_range(key, range)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        self.inner.head(key)
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        if !key.ends_with("/PENDING") && self.fail_delete.swap(false, Ordering::Relaxed) {
            return Err(TelemetryError::ObjectStore(
                "injected exact-key delete failure".into(),
            ));
        }
        self.inner.delete(key)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        self.inner.compare_and_swap(key, expected_version, bytes)
    }
}

#[test]
fn selected_root_replays_exact_retirements_after_delete_failure() {
    let directory = TestDirectory::new("tier-retirement-replay");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let store = FailDeleteOnceStore {
        inner: LocalObjectStore::open(directory.path.join("objects")).expect("object store opens"),
        fail_delete: Arc::new(AtomicBool::new(false)),
    };
    let config = ObjectTierConfig {
        retirement_grace: std::time::Duration::ZERO,
        ..tier_config(2)
    };
    let mut tier = TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), config)
        .expect("tier opens");
    tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
        .expect("first group publishes");
    store.fail_delete.store(true, Ordering::Relaxed);
    assert!(matches!(
        tier.publish_group(group_source(&artifacts, 1, 10, 2_000)),
        Err(TelemetryError::ObjectStore(_))
    ));
    drop(tier);

    let recovered = TelemetryObjectTier::open(store, ShardId::new(4), partition(), config)
        .expect("selected root replays its exact retirement set");
    assert_eq!(recovered.root().generation, 2);
    assert_eq!(recovered.pending_retired_objects(), 0);
}

#[test]
fn startup_relinquishes_every_uncommitted_transaction_key_without_listing() {
    let directory = TestDirectory::new("tier-pending-abort-replay");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let tier =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("tier opens");
    let transaction_id = "0123456789abcdef0123456789abcdef".to_owned();
    let first = format!(
        "{}/transactions/{transaction_id}/groups/00000000000000000000/payload-test",
        tier.namespace
    );
    let root = format!(
        "{}/transactions/{transaction_id}/roots/root-00000000000000000001-test.json",
        tier.namespace
    );
    let transaction = CatalogTransaction {
        format_version: TIER_FORMAT_VERSION,
        transaction_id,
        target_generation: 1,
        target_root_key: root.clone(),
        reclaim_after_unix_millis: 1,
        owned_objects: vec![first.clone(), root.clone()],
    };
    let pending_key = tier.pending_key();
    store
        .compare_and_swap(
            &pending_key,
            None,
            &encode_json(&transaction, "test PENDING").expect("transaction encodes"),
        )
        .expect("PENDING is selected");
    store
        .put_bytes_if_absent(&first, b"uncommitted payload")
        .expect("first transaction object exists");
    store
        .put_bytes_if_absent(&root, b"uncommitted root")
        .expect("root transaction object exists");
    drop(tier);

    TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
        .expect("startup replays PENDING cleanup");
    assert!(store.head(&first).expect("first head").is_none());
    assert!(store.head(&root).expect("root head").is_none());
    assert!(store.head(&pending_key).expect("PENDING head").is_none());
}

#[test]
fn startup_preserves_every_key_owned_by_an_active_writer_lease() {
    let directory = TestDirectory::new("tier-pending-active-writer");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let tier =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("tier opens");
    let transaction_id = "fedcba9876543210fedcba9876543210".to_owned();
    let payload = format!(
        "{}/transactions/{transaction_id}/groups/00000000000000000000/payload-test",
        tier.namespace
    );
    let root = format!(
        "{}/transactions/{transaction_id}/roots/root-00000000000000000001-test.json",
        tier.namespace
    );
    let transaction = CatalogTransaction {
        format_version: TIER_FORMAT_VERSION,
        transaction_id,
        target_generation: 1,
        target_root_key: root.clone(),
        reclaim_after_unix_millis: u64::MAX,
        owned_objects: vec![payload.clone(), root.clone()],
    };
    let pending_key = tier.pending_key();
    store
        .compare_and_swap(
            &pending_key,
            None,
            &encode_json(&transaction, "test PENDING").expect("transaction encodes"),
        )
        .expect("PENDING is selected");
    store
        .put_bytes_if_absent(&payload, b"active payload")
        .expect("active payload exists");
    store
        .put_bytes_if_absent(&root, b"active root")
        .expect("active root exists");
    drop(tier);

    assert!(matches!(
        TelemetryObjectTier::open(
            store.clone(),
            ShardId::new(4),
            partition(),
            tier_config(2)
        ),
        Err(TelemetryError::ObjectStore(message))
            if message.contains("active writer lease")
    ));
    assert!(store.head(&payload).expect("payload head").is_some());
    assert!(store.head(&root).expect("root head").is_some());
    assert!(store.head(&pending_key).expect("PENDING head").is_some());
}

#[test]
fn startup_preserves_a_committed_transaction_when_pending_cleanup_was_interrupted() {
    let directory = TestDirectory::new("tier-pending-commit-replay");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let mut tier =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("tier opens");
    tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
        .expect("group publishes");
    let root = tier
        .current_root_key
        .clone()
        .expect("published catalog has a root");
    let transaction_id = root
        .strip_prefix(&format!("{}/transactions/", tier.namespace))
        .and_then(|path| path.split('/').next())
        .expect("root carries transaction identity")
        .to_owned();
    let transaction = CatalogTransaction {
        format_version: TIER_FORMAT_VERSION,
        transaction_id,
        target_generation: tier.root().generation,
        target_root_key: root.clone(),
        reclaim_after_unix_millis: u64::MAX,
        owned_objects: vec![root.clone()],
    };
    let pending_key = tier.pending_key();
    store
        .compare_and_swap(
            &pending_key,
            None,
            &encode_json(&transaction, "test PENDING").expect("transaction encodes"),
        )
        .expect("interrupted committed PENDING is restored");
    drop(tier);

    let recovered =
        TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), tier_config(2))
            .expect("committed PENDING is recognized");
    assert_eq!(recovered.root().generation, 1);
    assert!(store.head(&root).expect("root head").is_some());
    assert!(store.head(&pending_key).expect("PENDING head").is_none());
}

#[test]
fn retention_relinquishes_only_catalog_owned_exact_keys() {
    let directory = TestDirectory::new("tier-retention-ownership");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let config = ObjectTierConfig {
        retirement_grace: std::time::Duration::ZERO,
        ..tier_config(2)
    };
    let mut tier = TelemetryObjectTier::open(store.clone(), ShardId::new(4), partition(), config)
        .expect("tier opens");
    for sequence in 0..4 {
        tier.publish_group(group_source(
            &artifacts,
            sequence,
            sequence * 10,
            (sequence + 1) * 1_000,
        ))
        .expect("group publishes");
    }
    let lease = tier.catalog_lease();
    let removed_manifest = tier
        .candidate_groups(TierQueryRange::default())
        .expect("groups load")[0]
        .manifest_key
        .clone();
    let report = tier
        .retain_since_timestamp(3_500)
        .expect("retention publishes");
    assert_eq!(report.retired_groups, 3);
    assert!(report.retired_objects >= 9);
    assert_eq!(
        tier.pending_retired_objects(),
        report.retired_objects as usize
    );
    assert!(
        store
            .head(&removed_manifest)
            .expect("manifest head")
            .is_some()
    );
    assert_eq!(
        tier.candidate_groups(TierQueryRange::default())
            .expect("retained groups")
            .into_iter()
            .map(|group| group.group_sequence)
            .collect::<Vec<_>>(),
        vec![3]
    );

    drop(lease);
    tier.reclaim_retired_objects().expect("retired keys delete");
    assert!(
        store
            .head(&removed_manifest)
            .expect("manifest head")
            .is_none()
    );
    let recovered = TelemetryObjectTier::open(store, ShardId::new(4), partition(), config)
        .expect("retained catalog reopens");
    assert_eq!(
        recovered.root().latest_checkpoint,
        tier.root().latest_checkpoint
    );
    assert_eq!(recovered.root().next_block_id, tier.root().next_block_id);
}

#[test]
fn payload_budget_retires_oldest_complete_groups_first() {
    let directory = TestDirectory::new("tier-capacity-retention");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let config = ObjectTierConfig {
        retirement_grace: std::time::Duration::ZERO,
        ..tier_config(8)
    };
    let mut tier =
        TelemetryObjectTier::open(store, ShardId::new(4), partition(), config).expect("tier opens");
    for sequence in 0..4 {
        tier.publish_group(group_source(
            &artifacts,
            sequence,
            sequence * 10,
            (sequence + 1) * 1_000,
        ))
        .expect("group publishes");
    }

    let report = tier
        .retain_to_payload_bytes(64)
        .expect("capacity retention");
    assert_eq!(report.retired_groups, 2);
    assert_eq!(report.retired_payload_bytes, 64);
    assert_eq!(
        tier.candidate_groups(TierQueryRange::default())
            .expect("retained groups")
            .into_iter()
            .map(|group| group.group_sequence)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[test]
fn startup_is_shallow_and_touched_pages_are_verified() {
    let directory = TestDirectory::new("lazy-verification");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let mut tier =
        TelemetryObjectTier::open(store.clone(), ShardId::new(2), partition(), tier_config(2))
            .expect("tier opens");
    tier.publish_group(group_source(&artifacts, 0, 0, 100))
        .expect("group publishes");
    let page_key = tier.root().pages[0].page_key.clone();
    write_test_file(&store.root().join(page_key), b"corrupt");

    let reopened = TelemetryObjectTier::open(store, ShardId::new(2), partition(), tier_config(2))
        .expect("startup does not scan every page or payload");
    assert!(matches!(
        reopened.candidate_groups(TierQueryRange::default()),
        Err(TelemetryError::CorruptTier(_))
    ));
}

fn block_descriptor(offset: u64) -> BlockDescriptor {
    BlockDescriptor {
        block_id: BlockId::new(0),
        stream_shard_id: ShardId::new(8),
        topic_partition: partition(),
        source_compression_cohort: CompressionCohortId::new(4),
        placement_id: CompressionPlacementId::new(5),
        dictionary_id: Some(DictionaryId::new(6)),
        compression_codec: CompressionCodec::Zstd,
        compression_level: 1,
        first_offset: LogicalOffset::new(offset),
        last_offset: LogicalOffset::new(offset),
        record_count: 1,
        source_bytes: 20,
        structural_bytes: 12,
        stored_bytes: 4,
        min_timestamp_unix_nanos: offset,
        max_timestamp_unix_nanos: offset,
        compression_temperature: CompressionTemperature::new(7).get(),
        compression_shape_hash: 8,
        compression_temperature_variance_q8: 0,
        max_compression_temperature_deviation: 0,
        object_key: None,
        object_offset: None,
    }
}

#[test]
fn staged_blocks_become_exact_object_ranges() {
    let directory = TestDirectory::new("payload-pack");
    let mut catalog = BlockCatalog::default();
    let first = catalog.seal(block_descriptor(0), Arc::from(&b"abcd"[..]));
    let second = catalog.seal(block_descriptor(1), Arc::from(&b"efgh"[..]));
    let pack_path = directory.path.join("blocks.pack");
    let entries =
        write_staged_payload_pack(&catalog, &[first.block_id, second.block_id], &pack_path)
            .expect("payload pack is written");
    assert_eq!(fs::read(&pack_path).expect("pack reads"), b"abcdefgh");
    assert_eq!(entries[0].payload_offset, 0);
    assert_eq!(entries[1].payload_offset, 4);
    assert_eq!(entries[0].payload_checksum, checksum_bytes(b"abcd"));
    assert_eq!(entries[1].payload_checksum, checksum_bytes(b"efgh"));

    let query_path = directory.path.join("query.index");
    write_test_file(&query_path, b"index");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let mut tier = TelemetryObjectTier::open(store, ShardId::new(8), partition(), tier_config(2))
        .expect("tier opens");
    let manifest = tier
        .publish_group(TierGroupSource {
            group_sequence: 0,
            checkpoint: TierCheckpoint {
                next_placement_sequence: 1,
                next_offset: 2,
            },
            blocks: entries,
            artifacts: vec![
                TierArtifactSource {
                    kind: TierArtifactKind::PayloadPack,
                    name: "blocks.pack".into(),
                    path: pack_path,
                },
                TierArtifactSource {
                    kind: TierArtifactKind::QueryIndex,
                    name: "query.index".into(),
                    path: query_path,
                },
            ],
        })
        .expect("group publishes");
    mark_group_offloaded(&mut catalog, &manifest).expect("catalog is advanced to object ranges");
    assert!(catalog.staged_payload(first.block_id).is_none());
    assert!(catalog.staged_payload(second.block_id).is_none());
    assert_eq!(
        catalog
            .get(first.block_id)
            .expect("first block exists")
            .object_offset,
        Some(0)
    );
    assert_eq!(
        catalog
            .get(second.block_id)
            .expect("second block exists")
            .object_offset,
        Some(4)
    );
}

#[test]
fn invalid_group_offload_is_atomic() {
    let directory = TestDirectory::new("atomic-offload");
    let mut catalog = BlockCatalog::default();
    let first = catalog.seal(block_descriptor(0), Arc::from(&b"abcd"[..]));
    let second = catalog.seal(block_descriptor(1), Arc::from(&b"efgh"[..]));
    let pack_path = directory.path.join("blocks.pack");
    let entries =
        write_staged_payload_pack(&catalog, &[first.block_id, second.block_id], &pack_path)
            .expect("payload pack is written");
    let query_path = directory.path.join("query.index");
    write_test_file(&query_path, b"index");
    let store = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let mut tier = TelemetryObjectTier::open(store, ShardId::new(8), partition(), tier_config(2))
        .expect("tier opens");
    let mut manifest = tier
        .publish_group(TierGroupSource {
            group_sequence: 0,
            checkpoint: TierCheckpoint {
                next_placement_sequence: 1,
                next_offset: 2,
            },
            blocks: entries,
            artifacts: vec![
                TierArtifactSource {
                    kind: TierArtifactKind::PayloadPack,
                    name: "blocks.pack".into(),
                    path: pack_path,
                },
                TierArtifactSource {
                    kind: TierArtifactKind::QueryIndex,
                    name: "query.index".into(),
                    path: query_path,
                },
            ],
        })
        .expect("group publishes");
    manifest.blocks[1].block_id = 999;

    assert!(matches!(
        mark_group_offloaded(&mut catalog, &manifest),
        Err(TelemetryError::UnknownBlock(999))
    ));
    for block_id in [first.block_id, second.block_id] {
        assert!(catalog.staged_payload(block_id).is_some());
        assert!(
            catalog
                .get(block_id)
                .expect("block remains")
                .object_key
                .is_none()
        );
    }
}

#[derive(Debug)]
struct CountingStore {
    inner: LocalObjectStore,
    range_reads: AtomicUsize,
}

impl CountingStore {
    fn new(inner: LocalObjectStore) -> Self {
        Self {
            inner,
            range_reads: AtomicUsize::new(0),
        }
    }
}

impl TelemetryObjectStore for CountingStore {
    fn put_bytes_if_absent(&self, key: &str, bytes: &[u8]) -> TelemetryResult<ObjectMetadata> {
        self.inner.put_bytes_if_absent(key, bytes)
    }

    fn put_file_if_absent(&self, key: &str, source: &Path) -> TelemetryResult<ObjectMetadata> {
        self.inner.put_file_if_absent(key, source)
    }

    fn get(&self, key: &str, max_bytes: u64) -> TelemetryResult<Vec<u8>> {
        self.inner.get(key, max_bytes)
    }

    fn get_range(&self, key: &str, range: Range<u64>) -> TelemetryResult<Vec<u8>> {
        self.range_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(key, range)
    }

    fn head(&self, key: &str) -> TelemetryResult<Option<ObjectMetadata>> {
        self.inner.head(key)
    }

    fn delete(&self, key: &str) -> TelemetryResult<()> {
        self.inner.delete(key)
    }

    fn compare_and_swap(
        &self,
        key: &str,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> TelemetryResult<ObjectMetadata> {
        self.inner.compare_and_swap(key, expected_version, bytes)
    }
}

#[test]
fn ssd_cache_reuses_ranges_and_stays_byte_bounded() {
    let directory = TestDirectory::new("ssd-cache");
    let local = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let store = CountingStore::new(local);
    store
        .put_bytes_if_absent("payload/object", b"abcdefghijklmnop")
        .expect("payload object is written");
    let cache = SsdObjectCache::open(
        directory.path.join("cache"),
        SsdCacheConfig {
            max_bytes: 2 * (CACHE_HEADER_BYTES as u64 + 4),
            chunk_bytes: 4,
            max_read_bytes: 16,
            memory_bytes: 8,
            parsed_memory_bytes: 0,
        },
    )
    .expect("cache opens");

    assert_eq!(
        cache
            .read_range(&store, "payload/object", 4..12)
            .expect("first range reads"),
        b"efghijkl"
    );
    assert_eq!(store.range_reads.load(Ordering::Relaxed), 2);
    assert_eq!(
        cache
            .read_range(&store, "payload/object", 4..12)
            .expect("second range reads from SSD"),
        b"efghijkl"
    );
    assert_eq!(store.range_reads.load(Ordering::Relaxed), 2);

    cache
        .read_range(&store, "payload/object", 12..16)
        .expect("third chunk is admitted and evicts the oldest");
    assert!(cache.used_bytes() <= 2 * (CACHE_HEADER_BYTES as u64 + 4));
    cache
        .read_range(&store, "payload/object", 4..8)
        .expect("evicted chunk can be fetched again");
    assert_eq!(store.range_reads.load(Ordering::Relaxed), 4);
}

#[test]
fn ssd_cache_admits_a_published_staging_file_without_a_remote_read() {
    let directory = TestDirectory::new("ssd-cache-admit-file");
    let source = directory.path.join("payload.pack");
    let bytes = b"abcdefghijklmnop";
    write_test_file(&source, bytes);
    let checksum = checksum_bytes(bytes);
    let artifact = TierArtifact {
        kind: TierArtifactKind::PayloadPack,
        name: "payload.pack".into(),
        object_key: "remote/payload".into(),
        bytes: u64::try_from(bytes.len()).expect("length"),
        checksum_algorithm: CHECKSUM_ALGORITHM.into(),
        checksum: checksum.clone(),
    };
    let cache = SsdObjectCache::open(
        directory.path.join("cache"),
        SsdCacheConfig {
            max_bytes: 4 * (CACHE_HEADER_BYTES as u64 + 4),
            chunk_bytes: 4,
            max_read_bytes: 16,
            memory_bytes: 0,
            parsed_memory_bytes: 0,
        },
    )
    .expect("cache opens");
    cache.admit_file(&artifact, &source).expect("file admits");

    let empty_store =
        LocalObjectStore::open(directory.path.join("empty-remote")).expect("store opens");
    let cached = cache
        .read_range_with_metadata(
            &empty_store,
            &artifact.object_key,
            &ObjectMetadata {
                bytes: artifact.bytes,
                version_token: checksum.clone(),
                content_digest: checksum,
            },
            0..artifact.bytes,
        )
        .expect("cache serves without the remote object");
    assert_eq!(cached, bytes);
    assert!(cache.used_bytes() <= cache.config.max_bytes);
}

#[test]
fn batched_ssd_ranges_load_a_shared_chunk_once() {
    let directory = TestDirectory::new("ssd-cache-batch");
    let local = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let store = CountingStore::new(local);
    let metadata = store
        .put_bytes_if_absent("payload/object", b"abcdefghijklmnop")
        .expect("payload object is written");
    let cache = SsdObjectCache::open(
        directory.path.join("cache"),
        SsdCacheConfig {
            max_bytes: 4 * (CACHE_HEADER_BYTES as u64 + 4),
            chunk_bytes: 4,
            max_read_bytes: 16,
            memory_bytes: 16,
            parsed_memory_bytes: 0,
        },
    )
    .expect("cache opens");
    let ranges = [4..6, 6..8];

    assert_eq!(
        cache
            .read_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
            .expect("batched ranges read"),
        [b"ef".to_vec(), b"gh".to_vec()]
    );
    assert_eq!(store.range_reads.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().misses, 1);
    assert_eq!(cache.stats().source_bytes, 4);
    cache
        .read_shared_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
        .expect("batched ranges are served from verified RAM");
    assert_eq!(store.range_reads.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().hits, 1);
    assert_eq!(cache.stats().memory_hits, 1);
    assert!(cache.stats().memory_used_bytes <= 16);

    let shared = cache
        .read_shared_ranges_with_metadata(&store, "payload/object", &metadata, &ranges)
        .expect("shared ranges remain readable");
    assert_eq!(shared[0].as_ref(), b"ef");
    assert_eq!(shared[1].as_ref(), b"gh");
    assert!(Arc::ptr_eq(&shared[0].bytes, &shared[1].bytes));
}

#[test]
fn cached_catalog_reads_do_not_revisit_object_storage() {
    let directory = TestDirectory::new("catalog-cache");
    let artifacts = directory.path.join("sources");
    fs::create_dir_all(&artifacts).expect("artifact directory is created");
    let local = LocalObjectStore::open(directory.path.join("objects")).expect("object store opens");
    let mut tier = TelemetryObjectTier::open(
        CountingStore::new(local),
        ShardId::new(5),
        partition(),
        tier_config(16),
    )
    .expect("tier opens");
    tier.publish_group(group_source(&artifacts, 0, 0, 1_000))
        .expect("group publishes");
    let cache = SsdObjectCache::open(
        directory.path.join("cache"),
        SsdCacheConfig {
            max_bytes: 32 * (CACHE_HEADER_BYTES as u64 + 1_024),
            chunk_bytes: 1_024,
            max_read_bytes: 64 * 1_024,
            memory_bytes: 32 * 1_024,
            parsed_memory_bytes: 32 * 1_024,
        },
    )
    .expect("cache opens");

    let groups = tier
        .candidate_groups_cached(TierQueryRange::default(), &cache)
        .expect("catalog page loads through cache");
    let manifest = tier
        .load_group_cached(&groups[0], &cache)
        .expect("manifest loads through cache");
    let artifact = manifest
        .artifact(TierArtifactKind::QueryIndex)
        .expect("query index exists");
    assert_eq!(
        tier.read_artifact_cached(artifact, 1_024, &cache)
            .expect("artifact loads through cache"),
        b"query-index-0"
    );
    let reads_after_first_query = tier.object_store().range_reads.load(Ordering::Relaxed);
    assert!(reads_after_first_query >= 3);

    let groups = tier
        .candidate_groups_cached(TierQueryRange::default(), &cache)
        .expect("catalog page is cached");
    let manifest = tier
        .load_group_cached(&groups[0], &cache)
        .expect("manifest is cached");
    tier.read_artifact_cached(
        manifest
            .artifact(TierArtifactKind::QueryIndex)
            .expect("query index exists"),
        1_024,
        &cache,
    )
    .expect("artifact is cached");
    assert_eq!(
        tier.object_store().range_reads.load(Ordering::Relaxed),
        reads_after_first_query
    );
    let stats = cache.stats();
    assert_eq!(stats.parsed_hits, 2);
    assert_eq!(stats.parsed_entries, 2);
    assert!(stats.memory_used_bytes <= 32 * 1_024);
}
