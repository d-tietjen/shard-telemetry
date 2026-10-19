use super::*;
use crate::TelemetryRecoveryLimits;
use shard_stream_core::{LogicalPartitionId, TopicId};
use std::cell::Cell;
use std::time::Duration;

thread_local! {
    static TARGET: Cell<Option<(&'static str, bool)>> = const { Cell::new(None) };
    static REACHED: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn after_phase(expiry: Instant, actual: &'static str) {
    if TARGET.get().is_some_and(|(phase, _)| phase == actual) {
        let (_, panic) = TARGET.take().expect("actual matched phase");
        REACHED.set(true);
        if panic {
            panic!("after real stripe setup state/result capture");
        }
        while Instant::now() < expiry {
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        TARGET.set(None);
        REACHED.set(false);
    }
}
fn policy() -> LogStripeSetupLimits {
    LogStripeSetupLimits {
        max_dictionary_cache_bytes: 32 << 20,
        max_locality_slots: 32,
        max_cache_slots: 4096,
        max_native_context_bytes: 256 << 10,
        max_retained_bytes: 4 << 20,
        max_work_bytes: 16 << 20,
    }
}
fn budget_limits() -> TelemetryRecoveryLimits {
    TelemetryRecoveryLimits {
        max_path_bytes: 8192,
        max_file_bytes: 1 << 20,
        max_read_bytes: 1 << 20,
        max_directory_entries: 1024,
        max_records: 1024,
        max_json_values: 4096,
        max_string_bytes: 1 << 20,
        max_retained_bytes: 16 << 20,
        max_work_bytes: 64 << 20,
    }
}
fn budget() -> TelemetryRecoveryBudget {
    TelemetryRecoveryBudget::new(budget_limits(), Instant::now() + Duration::from_secs(30))
        .expect("actual finite original ledger")
}
fn shard() -> ShardId {
    ShardId::new(7)
}

#[test]
fn actual_admitted_fields_locality_and_native_compression_match_ordinary_constructor() {
    for enabled in [false, true] {
        for level in [-3, 0, 5] {
            let mut config = StripeConfig {
                compression_level: level,
                ..StripeConfig::default()
            };
            config.compression_locality.enabled = enabled;
            let mut ordinary = LogStripe::new(shard(), config.clone()).expect("ordinary reference");
            let mut original = budget();
            let prepared =
                LogStripe::prepare_until(shard(), config.clone(), policy(), &mut original)
                    .expect("actual admitted constructor");
            assert_eq!(prepared.stream_shard_id(), shard());
            assert_eq!(prepared.original_expiry(), original.original_expiry());
            assert_eq!(prepared.setup_usage(), original.usage());
            let observed = prepared.observation();
            assert!(observed.assembled && observed.native_creation_entered);
            assert!(observed.native_context_bytes.iter().all(Option::is_some));
            assert!(observed.level_result.expect("actual setter").is_ok());
            assert!(observed.dictionary_result.expect("actual load").is_ok());
            let mut actual = prepared.into_stripe();
            assert_eq!(actual.config, config);
            assert_eq!(
                actual.message_term_cache.len(),
                ordinary.message_term_cache.len()
            );
            assert_eq!(actual.field_cache.len(), ordinary.field_cache.len());
            assert!(actual.message_term_cache.iter().all(Option::is_none));
            assert!(actual.field_cache.iter().all(Option::is_none));
            assert_eq!(
                actual.dictionary_cache.capacity_bytes(),
                ordinary.dictionary_cache.capacity_bytes()
            );
            assert!(actual.dictionary_cache.is_empty() && actual.catalog.is_empty());
            assert_eq!(
                actual.block_collator.stats().allocated_state_bytes,
                ordinary.block_collator.stats().allocated_state_bytes
            );
            assert_eq!(actual.block_collator.is_enabled(), enabled);
            assert_eq!(actual.block_collator.compression_shards().len(), 0);
            let partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(0));
            assert_eq!(actual.indexed_through(partition), None);
            assert!(
                actual
                    .query_checked(&LogQuery::new(partition))
                    .expect("ordinary query")
                    .is_empty()
            );
            assert!(
                actual.tier.is_none()
                    && actual.dictionary_catalog.is_none()
                    && actual.dictionary_snapshot.is_none()
                    && actual.realtime_dictionary.is_none()
            );
            let source = b"actual repeated log message alpha beta alpha beta and value 42";
            let dictionary = b"actual dictionary alpha beta repeated log message";
            for use_dictionary in [false, true, true, false] {
                let id = use_dictionary.then(|| DictionaryId::new(11));
                let payload = use_dictionary.then_some(dictionary.as_slice());
                assert_eq!(
                    actual
                        .compressor
                        .compress(source, id, payload)
                        .expect("actual native compression"),
                    ordinary
                        .compressor
                        .compress(source, id, payload)
                        .expect("unchanged ordinary reference")
                );
            }
        }
    }
}

#[test]
fn composing_producers_spend_original_ledger_without_consuming_local_limits_twice() {
    let config = StripeConfig::default();
    let mut limits = policy();
    let exact = limits.setup_cost(&config).expect("plain source sizing");
    limits.max_retained_bytes = exact.retained_bytes;
    limits.max_work_bytes = exact.work_bytes;
    let mut original = budget();
    let earlier = TelemetryRecoveryUsage {
        retained_bytes: 65_536,
        work_bytes: 131_072,
        ..Default::default()
    };
    original
        .charge(earlier)
        .expect("actual prior composing producer");
    let prepared = LogStripe::prepare_until(shard(), config, limits, &mut original)
        .expect("local producer ceiling excludes earlier actual cumulative fees");
    assert_eq!(
        original.usage().retained_bytes,
        earlier.retained_bytes + exact.retained_bytes
    );
    assert_eq!(
        original.usage().work_bytes,
        earlier.work_bytes + exact.work_bytes
    );
    assert_eq!(prepared.setup_usage(), original.usage());
}

#[test]
fn exact_and_one_less_shared_byte_work_allowances_use_real_original_setup_cost() {
    let config = StripeConfig::default();
    let mut first = budget();
    let success = LogStripe::prepare_until(shard(), config.clone(), policy(), &mut first)
        .expect("real measured setup");
    let measured = success.setup_usage();
    drop(success);
    for axis in [false, true] {
        for one_less in [false, true] {
            let mut limits = budget_limits();
            if axis {
                limits.max_work_bytes = measured.work_bytes - usize::from(one_less);
            } else {
                limits.max_retained_bytes = measured.retained_bytes - usize::from(one_less);
            }
            let mut original =
                TelemetryRecoveryBudget::new(limits, Instant::now() + Duration::from_secs(30))
                    .expect("actual finite boundary ledger");
            let result = LogStripe::prepare_until(shard(), config.clone(), policy(), &mut original);
            if one_less {
                let error =
                    result.expect_err("original one-less ceiling refuses before allocation");
                assert!(matches!(
                    error.cause,
                    Failure::Recovery(TelemetryRecoveryError::Limits)
                ));
                assert_eq!(error.config(), Some(&config));
                assert!(!error.observation().native_creation_entered);
                assert_eq!(error.observation().message_cache_capacity, 0);
                assert!(
                    original.check().is_err(),
                    "original ledger remains terminal"
                );
            } else {
                assert!(
                    result
                        .expect("exact cumulative allowance")
                        .observation()
                        .assembled
                );
            }
        }
    }
}

#[test]
fn invalid_config_policy_and_expired_original_ledger_preserve_input_without_native_entry() {
    for mode in 0..6 {
        let mut config = StripeConfig::default();
        let mut limits = policy();
        match mode {
            0 => config.target_block_bytes = 0,
            1 => config.dictionary_cache_bytes = 0,
            2 => config.compression_level = i32::MAX,
            3 => config.compression_locality.max_compression_shards = 0,
            4 => limits.max_cache_slots = 1,
            _ => (),
        }
        let expiry = if mode == 5 {
            Instant::now()
        } else {
            Instant::now() + Duration::from_secs(30)
        };
        let original = TelemetryRecoveryBudget::new(budget_limits(), expiry);
        if mode == 5 {
            assert!(matches!(original, Err(TelemetryRecoveryError::Expired)));
            continue;
        }
        let mut original = original.expect("genuine finite ledger");
        let error = LogStripe::prepare_until(shard(), config.clone(), limits, &mut original)
            .expect_err("actual configuration/policy refusal");
        assert_eq!(error.config(), Some(&config));
        assert!(!error.observation().native_creation_entered);
        assert_eq!(error.observation().message_cache_capacity, 0);
        assert!(original.check().is_err());
    }
    let mut original =
        TelemetryRecoveryBudget::new(budget_limits(), Instant::now() + Duration::from_millis(1))
            .expect("genuine soon-expiring ledger");
    std::thread::sleep(Duration::from_millis(2));
    let error = LogStripe::prepare_until(shard(), StripeConfig::default(), policy(), &mut original)
        .expect_err("same original ledger expired before producer entry");
    assert!(matches!(
        error.cause,
        Failure::Recovery(TelemetryRecoveryError::Expired)
    ));
    assert_eq!(error.setup_usage(), TelemetryRecoveryUsage::default());
    assert!(!error.observation().native_creation_entered);
}

#[test]
fn actual_partial_vectors_native_results_and_complete_owner_survive_late_checks_and_panics() {
    let phases = [
        "configuration",
        "locality-reserved",
        "message-reserved",
        "message-initialized",
        "field-reserved",
        "field-initialized",
        "native-created",
        "native-level",
        "native-dictionary",
        "assembled",
    ];
    for (index, phase) in phases.into_iter().enumerate() {
        for panic in [false, true] {
            let _hooks = Hooks;
            let config = StripeConfig::default();
            let mut original = TelemetryRecoveryBudget::new(
                budget_limits(),
                Instant::now() + Duration::from_secs(1),
            )
            .expect("actual saved deadline");
            TARGET.set(Some((phase, panic)));
            let error = LogStripe::prepare_until(shard(), config.clone(), policy(), &mut original)
                .expect_err("real captured phase cannot escape original expiry or unwind");
            assert!(REACHED.get(), "actual requested phase was entered");
            assert!(matches!(
                error.cause,
                Failure::Panic | Failure::Recovery(TelemetryRecoveryError::Expired)
            ));
            assert_eq!(error.retained.panic.is_some(), panic);
            assert_eq!(error.config(), Some(&config));
            assert_eq!(error.setup_usage(), original.usage());
            let actual = error.observation();
            assert_eq!(actual.native_creation_entered, index >= 6);
            assert_eq!(actual.level_result.is_some(), index >= 7);
            assert_eq!(actual.dictionary_result.is_some(), index >= 8);
            assert_eq!(actual.assembled, index == 9);
            if index >= 6 {
                assert!(actual.native_context_bytes[0].is_some());
                assert!(error.retained.native.is_some() || error.retained.stripe.is_some());
            }
            if index >= 2 {
                assert!(actual.message_cache_capacity >= MESSAGE_TERM_CACHE_ENTRIES);
            }
            if index >= 4 {
                assert!(actual.field_cache_capacity >= FIELD_CACHE_ENTRIES);
            }
            if index == 9 {
                assert!(error.retained.stripe.is_some());
            }
            assert!(
                original.check().is_err(),
                "no renewal after retained setup failure"
            );
        }
    }
}

#[test]
fn native_size_refusal_retains_actual_context_and_measured_excess_on_same_ledger() {
    let mut reference = zstd::bulk::Compressor::default();
    let bytes = reference.context_mut().sizeof();
    assert!(bytes > 1);
    let mut limits = policy();
    limits.max_native_context_bytes = bytes - 1;
    let config = StripeConfig::default();
    let requested = limits
        .setup_cost(&config)
        .expect("real requested context reservation");
    let mut original = budget();
    let error = LogStripe::prepare_until(shard(), config, limits, &mut original)
        .expect_err("actual native allocation exceeds reserved policy");
    assert!(matches!(error.cause, Failure::Limits));
    let measured = error.observation().native_context_bytes[0].expect("real sizeof before refusal");
    assert!(measured >= bytes);
    assert!(error.retained.native.is_some());
    assert!(error.observation().level_result.is_none());
    assert_eq!(
        error.setup_usage().retained_bytes,
        requested.retained_bytes + measured - limits.max_native_context_bytes
    );
}
