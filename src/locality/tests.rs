use super::*;

fn test_config() -> CompressionLocalityConfig {
    CompressionLocalityConfig {
        enabled: true,
        min_split_records: 2,
        min_split_bytes: 1,
        split_variance_q8: 64,
        max_shard_variance_q8: u16::MAX,
        min_admission_bytes: 1,
        ..CompressionLocalityConfig::default()
    }
}

fn locality_record(signature: u16, source_bytes: u64) -> CompressionLocalityRecord {
    CompressionLocalityRecord {
        fingerprint: MessageFingerprint {
            shape_hash: u64::from(signature),
            locality_signature: signature,
        },
        source_bytes,
    }
}

#[test]
fn dynamic_values_preserve_template_shape() {
    let first = fingerprint_message("2026-07-29T10:22:31Z request 12345 took 981 ms", &[]);
    let second = fingerprint_message("2027-01-03T09:00:02Z request 987654321 took 12 ms", &[]);
    assert_eq!(first.shape_hash, second.shape_hash);
}

#[test]
fn static_changes_affect_the_fingerprint() {
    let first = fingerprint_message("request 123 failed", &[]);
    let second = fingerprint_message("request 123 succeeded", &[]);
    assert_ne!(first, second);
}

#[test]
fn exact_shape_mismatch_adds_a_grouping_penalty() {
    let first = MessageFingerprint {
        shape_hash: 1,
        locality_signature: 0x1234,
    };
    let second = MessageFingerprint {
        shape_hash: 2,
        locality_signature: 0x1234,
    };
    assert_eq!(fingerprint_distance(first, second), SHAPE_MISMATCH_DISTANCE);
}

#[test]
fn scanner_reports_unicode_terms_without_allocating_a_collection() {
    let mut terms = Vec::new();
    let _ = analyze_message("Échec: request-42 東京", &[], |term| {
        terms.push(term.to_owned());
    });
    assert_eq!(terms, ["Échec", "request", "42", "東京"]);
}

#[test]
fn term_only_scanner_matches_full_analysis_for_ascii_and_unicode() {
    for message in [
        "ERROR request-42 took 981ms",
        "Échec: request-42 東京",
        "",
        "---",
    ] {
        let mut full = Vec::new();
        let _ = analyze_message(message, &[], |term| full.push(term.to_owned()));
        let mut terms_only = Vec::new();
        scan_message_terms(message, |term| terms_only.push(term.to_owned()));
        assert_eq!(terms_only, full, "{message:?}");
    }
}

#[test]
fn homogeneous_block_admits_a_compression_shard() {
    let mut router =
        CompressionBlockCollator::new(test_config(), 1_024).expect("collator validates");
    let source = CompressionCohortId::new(9);
    let records = vec![locality_record(0x1234, 128); 16];
    let home = CompressionPlacementId::from_source_cohort(source);
    let assignments = router.collate(source, home, &records);
    assert_eq!(assignments.len(), 1);
    assert_eq!(
        assignments[0].placement.granularity,
        LocalityGranularity::Collated
    );
    assert_eq!(assignments[0].score.internal_variance_q8, 0);
    assert_eq!(router.stats().active_compression_shards, 1);
    assert_eq!(
        router
            .tentative_placement(source, records[0].fingerprint)
            .placement_id,
        assignments[0].placement.placement_id
    );
}

#[test]
fn sparse_and_restarted_collators_fail_open_to_base() {
    let config = CompressionLocalityConfig {
        enabled: true,
        min_admission_bytes: 1_024,
        ..CompressionLocalityConfig::default()
    };
    let source = CompressionCohortId::new(4);
    let mut first = CompressionBlockCollator::new(config.clone(), 8 * 1024 * 1024)
        .expect("collator config validates");
    let records = [locality_record(0x1234, 128)];
    assert_eq!(
        first.collate(
            source,
            CompressionPlacementId::from_source_cohort(source),
            &records
        )[0]
        .placement
        .granularity,
        LocalityGranularity::Base
    );
    let mut restarted =
        CompressionBlockCollator::new(config, 8 * 1024 * 1024).expect("collator config validates");
    assert_eq!(
        restarted.collate(
            source,
            CompressionPlacementId::from_source_cohort(source),
            &records
        )[0]
        .placement
        .granularity,
        LocalityGranularity::Base
    );
}

#[test]
fn high_variance_blocks_split_with_farthest_seeds() {
    let mut router =
        CompressionBlockCollator::new(test_config(), 1_024).expect("collator validates");
    let source = CompressionCohortId::new(7);
    let records = (0..8)
        .map(|_| locality_record(0x0000, 128))
        .chain((0..8).map(|_| locality_record(0xffff, 128)))
        .collect::<Vec<_>>();
    let assignments = router.collate(
        source,
        CompressionPlacementId::from_source_cohort(source),
        &records,
    );
    assert_eq!(assignments.len(), 2);
    assert!(
        assignments
            .iter()
            .all(|assignment| assignment.score.internal_variance_q8 == 0)
    );
    assert_eq!(router.stats().blocks_split, 1);
    assert_eq!(router.stats().subblocks_created, 2);
    assert_eq!(router.stats().active_compression_shards, 2);
}

#[test]
fn blocks_closer_to_another_shard_are_reassigned_after_splitting() {
    let mut router =
        CompressionBlockCollator::new(test_config(), 1_024).expect("collator validates");
    let source = CompressionCohortId::new(7);
    let first_records = vec![locality_record(0x0000, 128); 8];
    let first = router.collate(
        source,
        CompressionPlacementId::from_source_cohort(source),
        &first_records,
    );
    let first_home = first[0].placement.placement_id;
    let second_records = vec![locality_record(0xffff, 128); 8];
    let second = router.collate(source, first_home, &second_records);
    assert_ne!(second[0].placement.placement_id, first_home);
    assert_eq!(router.stats().records_reassigned, 16);
}

#[test]
fn collation_is_deterministic_and_bounded() {
    let config = CompressionLocalityConfig {
        max_compression_shards: 4,
        ..test_config()
    };
    let mut first =
        CompressionBlockCollator::new(config.clone(), 512).expect("collator config validates");
    let mut second = CompressionBlockCollator::new(config, 512).expect("collator config validates");
    let source = CompressionCohortId::new(7);
    let records = (0..64)
        .map(|index| locality_record((index % 4 * 0x1111) as u16, 128))
        .collect::<Vec<_>>();
    let home = CompressionPlacementId::from_source_cohort(source);
    let first_assignments = first.collate(source, home, &records);
    let second_assignments = second.collate(source, home, &records);
    assert_eq!(
        first_assignments
            .iter()
            .map(|assignment| (
                assignment.placement,
                assignment.record_indices().collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>(),
        second_assignments
            .iter()
            .map(|assignment| (
                assignment.placement,
                assignment.record_indices().collect::<Vec<_>>()
            ))
            .collect::<Vec<_>>()
    );
    assert!(first_assignments.len() <= 1usize << MAX_SPLIT_DEPTH);
    assert!(first.stats().active_compression_shards <= 4);
    assert!(first.stats().allocated_state_bytes <= MAX_ROUTER_STATE_BYTES);
}

#[test]
fn excess_compression_shards_fall_back_instead_of_exceeding_the_cap() {
    let config = CompressionLocalityConfig {
        max_compression_shards: 1,
        max_assignment_distance: 0,
        ..test_config()
    };
    let mut router = CompressionBlockCollator::new(config, 512).expect("collator config validates");
    let source = CompressionCohortId::new(12);
    let home = CompressionPlacementId::from_source_cohort(source);
    let first = router.collate(source, home, &[locality_record(0, 128); 4]);
    let second = router.collate(source, home, &[locality_record(u16::MAX, 128); 4]);
    assert_eq!(router.stats().active_compression_shards, 1);
    assert_eq!(
        usize::from(first[0].placement.granularity != LocalityGranularity::Base)
            + usize::from(second[0].placement.granularity != LocalityGranularity::Base),
        1
    );
}

#[test]
fn membership_handoff_preserves_every_record_exactly_once() {
    let records = (0..32)
        .map(|index| locality_record(if index % 2 == 0 { 0 } else { u16::MAX }, 64))
        .collect::<Vec<_>>();
    let membership = BlockMembership::Contiguous(0..records.len());
    let scored = score_membership(&records, &membership);
    let (left, right) =
        split_membership(&records, &membership, &test_config(), scored).expect("block splits");
    let mut observed = left.indices().chain(right.indices()).collect::<Vec<_>>();
    observed.sort_unstable();
    assert_eq!(observed, (0..records.len()).collect::<Vec<_>>());
}

#[test]
fn repeated_unproductive_splits_enter_bounded_backoff() {
    let config = CompressionLocalityConfig {
        min_split_bytes: 1024,
        max_shard_variance_q8: 64,
        ..test_config()
    };
    let mut collator =
        CompressionBlockCollator::new(config, 512).expect("collator config validates");
    let source = CompressionCohortId::new(19);
    let home = CompressionPlacementId::from_source_cohort(source);
    let records = [
        locality_record(0, 64),
        locality_record(u16::MAX, 64),
        locality_record(0, 64),
        locality_record(u16::MAX, 64),
    ];
    for _ in 0..4 {
        let assignments = collator.collate(source, home, &records);
        assert!(
            assignments
                .iter()
                .all(|assignment| assignment.placement.granularity == LocalityGranularity::Base)
        );
    }
    assert!(collator.stats().split_explorations_suppressed >= 1);
    assert_eq!(collator.stats().active_compression_shards, 0);
}

#[test]
fn production_defaults_fit_the_collator_memory_budget() {
    let config = CompressionLocalityConfig::default();
    config.validate().expect("default collator is bounded");
    assert!(estimated_state_bytes(&config) <= MAX_ROUTER_STATE_BYTES);
}

#[test]
fn invalid_collator_limits_are_rejected_without_allocating() {
    let error = CompressionBlockCollator::new(
        CompressionLocalityConfig {
            max_compression_shards: 0,
            ..CompressionLocalityConfig::default()
        },
        8 * 1024 * 1024,
    )
    .expect_err("zero compression shards is invalid");
    assert_eq!(
        error,
        TelemetryError::InvalidConfig("locality max_compression_shards must be between 1 and 16")
    );
}
