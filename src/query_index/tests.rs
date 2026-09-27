use shard_stream_core::{ShardId, TopicPartition};

use super::*;
use crate::{
    CaseSensitivity, CompressionCohortId, DurableLog, LogPredicate, LogStripe, MetadataField,
    NumericComparison, StripeConfig, TextMatchKind, TextMatcher, decode_structural_block,
    encode_structural_records,
};

fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3))
}

fn records(start: u64, count: u64) -> Vec<DurableLog> {
    (start..start + count)
        .map(|offset| {
            let mut message = format!("common request_id={offset}");
            if offset % 10 == 0 {
                message.push_str(" medium");
            }
            if offset % 100 == 0 {
                message.push_str(" rare");
            }
            DurableLog::new(
                ShardId::new(1),
                partition(),
                LogicalOffset::new(offset),
                offset * 10,
                message,
                CompressionCohortId::new(1),
            )
            .with_field("service", if offset % 2 == 0 { "api" } else { "worker" })
        })
        .collect()
}

fn compatibility_records(count: u64) -> Vec<DurableLog> {
    (0..count)
        .map(|offset| {
            let message = match offset % 4 {
                0 => format!("INFO request {offset} completed"),
                1 => format!("ERROR request {offset} cannot access storage"),
                2 => format!("WARN request {offset} timed out after 250ms"),
                _ => format!("DEBUG heartbeat node-{offset}"),
            };
            DurableLog::new(
                ShardId::new(1),
                partition(),
                LogicalOffset::new(offset),
                (offset * 37 % count) * 100 + offset,
                message,
                CompressionCohortId::new(1),
            )
            .with_field(
                "service",
                match offset % 3 {
                    0 => "api",
                    1 => "worker",
                    _ => "storage",
                },
            )
            .with_field("env", if offset % 5 == 0 { "dev" } else { "prod" })
            .with_field(
                "status",
                if offset % 4 == 1 {
                    "503"
                } else if offset % 4 == 2 {
                    "429"
                } else {
                    "200"
                },
            )
        })
        .collect()
}

fn compatibility_index(records: &[DurableLog], block_records: usize) -> PersistentQueryIndex {
    PersistentQueryIndex::from_blocks(
        records
            .chunks(block_records)
            .enumerate()
            .map(|(ordinal, records)| {
                indexed_block(u32::try_from(ordinal).expect("ordinal fits"), records)
            })
            .collect(),
    )
    .expect("compatibility index builds")
}

fn cold_matches(
    index: &PersistentQueryIndex,
    records: &[DurableLog],
    block_records: usize,
    query: &LogQuery,
) -> Vec<DurableLog> {
    let candidates = index.candidate_hits(query).into_iter().map(|hit| {
        let index = usize::try_from(hit.block_ordinal).expect("block fits") * block_records
            + usize::try_from(hit.record_ordinal).expect("record fits");
        records[index].clone()
    });
    query.select(candidates)
}

fn indexed_block(
    block_ordinal: u32,
    records: &[DurableLog],
) -> (QueryBlockMetadata, BlockQueryIndex) {
    let (min_timestamp_unix_nanos, max_timestamp_unix_nanos) =
        records
            .iter()
            .fold((u64::MAX, 0), |(minimum, maximum), record| {
                (
                    minimum.min(record.timestamp_unix_nanos),
                    maximum.max(record.timestamp_unix_nanos),
                )
            });
    (
        QueryBlockMetadata {
            block_ordinal,
            topic_partition: partition(),
            first_offset: records.first().expect("records exist").record_ref.offset,
            last_offset: records.last().expect("records exist").record_ref.offset,
            min_timestamp_unix_nanos,
            max_timestamp_unix_nanos,
            record_count: u32::try_from(records.len()).expect("record count fits"),
        },
        BlockQueryIndex::build(records).expect("index builds"),
    )
}

#[test]
fn persistent_index_round_trips_and_preserves_exact_and_results() {
    let first = records(0, 1_000);
    let second = records(1_000, 1_000);
    let index = PersistentQueryIndex::from_blocks(vec![
        indexed_block(0, &first),
        indexed_block(1, &second),
    ])
    .expect("directory builds");
    let encoded = index.encode().expect("index encodes");
    assert_eq!(
        PersistentQueryIndex::decode(&encoded).expect("index decodes"),
        index
    );
    let compressed = index.encode_compressed(1).expect("index compresses");
    let decoded = PersistentQueryIndex::decode_compressed(&compressed).expect("index decompresses");
    let hits = decoded.candidate_hits(
        &LogQuery::new(partition())
            .with_term("COMMON")
            .with_term("rare")
            .with_field("service", "api"),
    );
    assert_eq!(
        hits,
        (0..20)
            .map(|index| QueryHit {
                block_ordinal: u32::from(index >= 10),
                record_ordinal: u32::try_from((index % 10) * 100).expect("ordinal fits"),
            })
            .collect::<Vec<_>>()
    );
}

#[test]
fn persistent_field_predicates_use_exact_value_postings() {
    let records = compatibility_records(60);
    let index = compatibility_index(&records, 10);
    let queries = [
        LogQuery::new(partition()).where_predicate(LogPredicate::field_exists("service")),
        LogQuery::new(partition()).where_predicate(LogPredicate::field_in("env", ["dev"])),
        LogQuery::new(partition()).where_predicate(LogPredicate::field(
            "service",
            TextMatcher::new("tor", TextMatchKind::Contains, CaseSensitivity::Sensitive),
        )),
        LogQuery::new(partition()).where_predicate(
            LogPredicate::field_regex("status", r"5\d+", CaseSensitivity::Sensitive)
                .expect("regex compiles"),
        ),
        LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
            "status",
            NumericComparison::GreaterThanOrEqual,
            500,
        )),
    ];
    for query in queries {
        let expected = query.select(records.iter().cloned());
        let actual = cold_matches(&index, &records, 10, &query);
        assert_eq!(
            actual
                .iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|record| record.record_ref.offset)
                .collect::<Vec<_>>(),
            "persistent candidate filtering changed results for {query:?}"
        );
    }
}

#[test]
fn newest_limit_and_block_ranges_prune_candidates() {
    let first = records(0, 100);
    let second = records(100, 100);
    let index = PersistentQueryIndex::from_blocks(vec![
        indexed_block(4, &first),
        indexed_block(9, &second),
    ])
    .expect("directory builds");
    assert_eq!(
        index.candidate_hits(
            &LogQuery::new(partition())
                .with_term("common")
                .newest_first()
                .with_limit(3)
        ),
        vec![
            QueryHit {
                block_ordinal: 9,
                record_ordinal: 99,
            },
            QueryHit {
                block_ordinal: 9,
                record_ordinal: 98,
            },
            QueryHit {
                block_ordinal: 9,
                record_ordinal: 97,
            },
        ]
    );
    let ranged = index.candidate_hits(
        &LogQuery::new(partition())
            .with_offset_range(LogicalOffset::new(100), LogicalOffset::new(200)),
    );
    assert_eq!(ranged.len(), 100);
    assert!(ranged.iter().all(|hit| hit.block_ordinal == 9));
}

#[test]
fn hybrid_posting_prefers_runs_for_dense_ordinals() {
    let posting = (0..10_000).collect::<Vec<_>>();
    let posting = PostingList::from_ordinals(posting.clone()).expect("posting builds");
    let mut encoded = Vec::new();
    encode_posting(&posting, &mut encoded).expect("posting encodes");
    assert_eq!(encoded[0], RUN_POSTING);
    let mut cursor = 0;
    assert_eq!(
        decode_posting(&encoded, &mut cursor, 10_000)
            .expect("posting decodes")
            .to_vec(),
        (0..10_000).collect::<Vec<_>>()
    );
    assert_eq!(cursor, encoded.len());
}

#[test]
fn delta_posting_checkpoints_preserve_membership() {
    let ordinals = (0..1_024)
        .map(|ordinal| ordinal * 2 + 1)
        .collect::<Vec<_>>();
    let posting = PostingList::from_ordinals(ordinals.clone()).expect("posting builds");
    let mut encoded = Vec::new();
    encode_posting(&posting, &mut encoded).expect("posting encodes");
    let mut cursor = 0;
    let (kind, cardinality, end, checkpoints) =
        scan_posting(&encoded, &mut cursor, 2_100, 0).expect("posting scans");
    assert_eq!(kind, DELTA_POSTING);
    assert_eq!(cardinality, ordinals.len());
    assert_eq!(end, encoded.len());
    assert!(!checkpoints.is_empty());
    for ordinal in 0..2_100 {
        assert_eq!(
            encoded_posting_contains(&encoded, kind, &checkpoints, ordinal),
            ordinal % 2 == 1 && ordinal < 2_049,
            "membership mismatch for ordinal {ordinal}"
        );
    }
}

#[test]
fn run_posting_checkpoints_preserve_membership() {
    let ordinals = (0..100)
        .flat_map(|run| (run * 4)..(run * 4 + 3))
        .collect::<Vec<_>>();
    let posting = PostingList::from_ordinals(ordinals.clone()).expect("posting builds");
    let mut encoded = Vec::new();
    encode_posting(&posting, &mut encoded).expect("posting encodes");
    let mut cursor = 0;
    let (kind, cardinality, end, checkpoints) =
        scan_posting(&encoded, &mut cursor, 400, 0).expect("posting scans");
    assert_eq!(kind, RUN_POSTING);
    assert_eq!(cardinality, ordinals.len());
    assert_eq!(end, encoded.len());
    assert!(!checkpoints.is_empty());
    for ordinal in 0..400 {
        assert_eq!(
            encoded_posting_contains(&encoded, kind, &checkpoints, ordinal),
            ordinal < 399 && ordinal % 4 != 3,
            "membership mismatch for ordinal {ordinal}"
        );
    }
}

#[test]
fn duplicate_metadata_fields_are_indexed_once() {
    let record = DurableLog {
        fields: vec![
            MetadataField::new("service", "api"),
            MetadataField::new("service", "api"),
        ]
        .into(),
        ..records(0, 1).pop().expect("record exists")
    };
    let index = BlockQueryIndex::build(&[record]).expect("index builds");
    assert_eq!(index.field_postings["service"]["api"].to_vec(), vec![0]);
}

#[test]
fn dense_postings_remain_run_encoded_in_resident_index() {
    let records = records(0, 10_000);
    let index = PersistentQueryIndex::from_blocks(vec![indexed_block(0, &records)])
        .expect("directory builds");
    let common = &index.term_postings[&partition()]["common"][0].record_ordinals;
    assert_eq!(common.cardinality(), records.len());
    assert!(common.storage_bytes() < common.cardinality().saturating_mul(size_of::<u32>()) / 2);
    assert_eq!(
        index
            .candidate_hits(
                &LogQuery::new(partition())
                    .with_term("common")
                    .newest_first()
                    .with_limit(100)
            )
            .len(),
        100
    );
    let limited_intersection = index.candidate_hits(
        &LogQuery::new(partition())
            .with_term("common")
            .with_field("service", "api")
            .newest_first()
            .with_limit(3),
    );
    assert_eq!(
        limited_intersection
            .iter()
            .map(|hit| hit.record_ordinal)
            .collect::<Vec<_>>(),
        vec![9_998, 9_996, 9_994]
    );
}

#[test]
fn hot_and_sealed_lookups_share_full_boolean_compatibility() {
    let records = compatibility_records(60);
    let index = compatibility_index(&records, 10);
    let decoded_blocks = records
        .chunks(10)
        .map(|records| {
            decode_structural_block(
                &encode_structural_records(records).expect("structural block encodes"),
            )
            .expect("structural block decodes")
        })
        .collect::<Vec<_>>();
    let mut stripe =
        LogStripe::new(ShardId::new(1), StripeConfig::default()).expect("stripe opens");
    for record in records.iter().cloned() {
        stripe.apply_durable(record).expect("record indexes");
    }

    let message_regex =
        LogPredicate::message_regex("cannot|timed out", CaseSensitivity::Insensitive)
            .expect("regex compiles");
    let service_regex =
        LogPredicate::field_regex("service", "^(api|storage)$", CaseSensitivity::Sensitive)
            .expect("regex compiles");
    let queries = vec![
        LogQuery::new(partition())
            .where_predicate(LogPredicate::and(vec![
                LogPredicate::or(vec![
                    LogPredicate::term("error"),
                    LogPredicate::field_numeric(
                        "status",
                        NumericComparison::GreaterThanOrEqual,
                        500,
                    ),
                ]),
                LogPredicate::field_exists("service"),
                LogPredicate::negate(LogPredicate::field_equals("env", "dev")),
            ]))
            .newest_first()
            .with_limit(11),
        LogQuery::new(partition())
            .where_predicate(LogPredicate::and(vec![
                message_regex,
                service_regex,
                LogPredicate::field_in("env", ["prod", "staging"]),
            ]))
            .with_offset_range(LogicalOffset::new(5), LogicalOffset::new(55)),
        LogQuery::new(partition())
            .where_predicate(LogPredicate::or(vec![
                LogPredicate::message_contains("heartbeat"),
                LogPredicate::field_numeric("status", NumericComparison::GreaterThan, 400),
            ]))
            .with_timestamp_range(500, 5_500)
            .sort_by_timestamp()
            .newest_first()
            .with_limit(17),
        LogQuery::new(partition()).where_predicate(LogPredicate::or(vec![
            LogPredicate::message(TextMatcher::new(
                "INFO request 12 completed",
                TextMatchKind::Exact,
                CaseSensitivity::Sensitive,
            )),
            LogPredicate::message(TextMatcher::new(
                "debug heartbeat",
                TextMatchKind::Prefix,
                CaseSensitivity::Insensitive,
            )),
            LogPredicate::message(TextMatcher::new(
                "250ms",
                TextMatchKind::Suffix,
                CaseSensitivity::Sensitive,
            )),
        ])),
        LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::field(
                "service",
                TextMatcher::new("TOR", TextMatchKind::Contains, CaseSensitivity::Insensitive),
            ),
            LogPredicate::field_numeric("status", NumericComparison::Equal, 429),
            LogPredicate::field_numeric("status", NumericComparison::NotEqual, 503),
            LogPredicate::field_numeric("status", NumericComparison::LessThanOrEqual, 429),
        ])),
        LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::field_numeric("status", NumericComparison::LessThan, 500),
            LogPredicate::field_numeric("status", NumericComparison::GreaterThan, 199),
        ])),
        LogQuery::new(partition()).where_predicate(LogPredicate::MatchNone),
    ];

    for query in queries {
        let hot = stripe
            .query(&query)
            .into_iter()
            .map(|matched| matched.record.record_ref.offset)
            .collect::<Vec<_>>();
        let cold = cold_matches(&index, &records, 10, &query)
            .into_iter()
            .map(|record| record.record_ref.offset)
            .collect::<Vec<_>>();
        assert_eq!(hot, cold);

        let decoded_candidates = index.candidate_hits(&query).into_iter().map(|hit| {
            decoded_blocks[usize::try_from(hit.block_ordinal).expect("block fits")]
                [usize::try_from(hit.record_ordinal).expect("record fits")]
            .clone()
        });
        let decoded = query
            .select(decoded_candidates)
            .into_iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>();
        assert_eq!(hot, decoded);
    }
}

#[test]
fn timestamp_cursor_pages_are_stable_across_hot_and_sealed_tiers() {
    let records = compatibility_records(48);
    let index = compatibility_index(&records, 8);
    let mut stripe =
        LogStripe::new(ShardId::new(1), StripeConfig::default()).expect("stripe opens");
    for record in records.iter().cloned() {
        stripe.apply_durable(record).expect("record indexes");
    }

    let first_query = LogQuery::new(partition())
        .where_predicate(LogPredicate::field_exists("service"))
        .sort_by_timestamp()
        .newest_first()
        .with_limit(7);
    let hot_first = stripe.query(&first_query);
    let cold_first = cold_matches(&index, &records, 8, &first_query);
    assert_eq!(
        hot_first
            .iter()
            .map(|matched| matched.record.record_ref.offset)
            .collect::<Vec<_>>(),
        cold_first
            .iter()
            .map(|record| record.record_ref.offset)
            .collect::<Vec<_>>()
    );

    let cursor = first_query.cursor_for(&hot_first.last().expect("first page exists").record);
    let second_query = first_query.clone().after(cursor);
    let hot_second = stripe.query(&second_query);
    let cold_second = cold_matches(&index, &records, 8, &second_query);
    assert_eq!(
        hot_second
            .iter()
            .map(|matched| matched.record.record_ref.offset)
            .collect::<Vec<_>>(),
        cold_second
            .iter()
            .map(|record| record.record_ref.offset)
            .collect::<Vec<_>>()
    );
    assert!(hot_first.iter().all(|first| {
        hot_second
            .iter()
            .all(|second| first.record.record_ref != second.record.record_ref)
    }));
}

#[test]
fn residual_queries_do_not_apply_the_limit_before_exact_filtering() {
    let records = compatibility_records(40);
    let index = compatibility_index(&records, 10);
    let query = LogQuery::new(partition())
        .where_predicate(LogPredicate::or(vec![
            LogPredicate::message_contains("heartbeat"),
            LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
        ]))
        .newest_first()
        .with_limit(2);
    let candidates = index.candidate_hits(&query);
    assert_eq!(candidates.len(), records.len());
    assert_eq!(cold_matches(&index, &records, 10, &query).len(), 2);
}

#[test]
fn residual_queries_stream_sealed_blocks_until_the_page_is_complete() {
    let records = compatibility_records(60);
    let index = compatibility_index(&records, 10);
    let query = LogQuery::new(partition())
        .where_predicate(LogPredicate::message_contains("heartbeat"))
        .newest_first()
        .with_limit(4);

    let mut streamed = Vec::new();
    let mut visited_blocks = 0usize;
    for block_ordinal in index.candidate_blocks(&query) {
        visited_blocks += 1;
        let candidates = index
            .candidate_hits_in_block(&query, block_ordinal)
            .into_iter()
            .map(|hit| {
                records[usize::try_from(hit.block_ordinal).expect("block fits") * 10
                    + usize::try_from(hit.record_ordinal).expect("record fits")]
                .clone()
            });
        streamed.extend(candidates.filter(|record| query.matches(record)));
        if streamed.len() >= query.limit.expect("query has a limit") {
            break;
        }
    }

    assert!(visited_blocks < index.blocks().len());
    assert_eq!(
        query
            .select(streamed)
            .into_iter()
            .map(|record| record.record_ref.offset)
            .collect::<Vec<_>>(),
        cold_matches(&index, &records, 10, &query)
            .into_iter()
            .map(|record| record.record_ref.offset)
            .collect::<Vec<_>>()
    );
}

#[test]
fn message_trigrams_reject_missing_literal_blocks_without_false_negatives() {
    let mut records = compatibility_records(20);
    records[7].message = "KELVIN alarm from 東京 worker".into();
    let index = compatibility_index(&records, 10);

    let unicode_query =
        LogQuery::new(partition()).where_predicate(LogPredicate::message_contains("kelvin alarm"));
    assert_eq!(index.candidate_blocks(&unicode_query), vec![0]);
    assert_eq!(
        cold_matches(&index, &records, 10, &unicode_query)
            .into_iter()
            .map(|record| record.record_ref.offset)
            .collect::<Vec<_>>(),
        vec![LogicalOffset::new(7)]
    );

    let missing_query = LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(
        "impossible-substring-9f82c4",
    ));
    assert!(index.candidate_blocks(&missing_query).is_empty());
    assert!(index.candidate_hits(&missing_query).is_empty());
    assert!(index.candidate_hits_in_block(&missing_query, 0).is_empty());
}

#[test]
fn case_sensitive_message_literals_reject_case_only_block_candidates() {
    let records = (0..2)
        .map(|offset| {
            DurableLog::new(
                ShardId::new(1),
                partition(),
                LogicalOffset::new(offset),
                offset,
                "ERROR request failed",
                CompressionCohortId::new(1),
            )
        })
        .collect::<Vec<_>>();
    let index = compatibility_index(&records, 2);
    let case_sensitive = LogQuery::new(partition()).where_predicate(
        LogPredicate::message_regex("error", CaseSensitivity::Sensitive).expect("regex compiles"),
    );
    assert!(index.candidate_blocks(&case_sensitive).is_empty());
    assert!(index.candidate_hits(&case_sensitive).is_empty());

    let case_insensitive = LogQuery::new(partition()).where_predicate(
        LogPredicate::message_regex("error", CaseSensitivity::Insensitive).expect("regex compiles"),
    );
    assert_eq!(index.candidate_blocks(&case_insensitive), vec![0]);
}

#[test]
fn every_literal_mode_and_case_policy_retains_unicode_matches() {
    let messages = [
        "İSTANBUL 東京 suffix",
        "Straße/Δelta END",
        "ASCII prefix and suffix",
    ];
    let records = messages
        .iter()
        .enumerate()
        .map(|(offset, message)| {
            DurableLog::new(
                ShardId::new(1),
                partition(),
                LogicalOffset::new(u64::try_from(offset).expect("offset fits")),
                u64::try_from(offset).expect("timestamp fits"),
                *message,
                CompressionCohortId::new(1),
            )
        })
        .collect::<Vec<_>>();
    let index = compatibility_index(&records, 1);
    let queries = [
        TextMatcher::new(
            "İSTANBUL 東京 suffix",
            TextMatchKind::Exact,
            CaseSensitivity::Sensitive,
        ),
        TextMatcher::new(
            "i\u{307}stanbul",
            TextMatchKind::Prefix,
            CaseSensitivity::Insensitive,
        ),
        TextMatcher::new("東京", TextMatchKind::Contains, CaseSensitivity::Sensitive),
        TextMatcher::new("end", TextMatchKind::Suffix, CaseSensitivity::Insensitive),
        TextMatcher::new(
            "ASCII prefix",
            TextMatchKind::Prefix,
            CaseSensitivity::Sensitive,
        ),
        TextMatcher::new("Δ", TextMatchKind::Contains, CaseSensitivity::Sensitive),
    ]
    .map(|matcher| LogQuery::new(partition()).where_predicate(LogPredicate::message(matcher)));

    for query in queries {
        let candidate_blocks = index.candidate_blocks(&query);
        for (block_ordinal, record) in records.iter().enumerate() {
            if query.matches(record) {
                assert!(
                    candidate_blocks.contains(&u32::try_from(block_ordinal).expect("block fits")),
                    "true literal match was pruned: {query:?}"
                );
            }
        }
    }
}

#[test]
fn message_trigram_pruning_only_uses_literals_required_by_boolean_logic() {
    let records = compatibility_records(20);
    let index = compatibility_index(&records, 10);
    let missing = LogPredicate::message_contains("impossible-substring-9f82c4");

    let conjunction = LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
        LogPredicate::term("request"),
        missing.clone(),
    ]));
    assert!(index.candidate_blocks(&conjunction).is_empty());

    let disjunction = LogQuery::new(partition()).where_predicate(LogPredicate::or(vec![
        LogPredicate::term("request"),
        missing.clone(),
    ]));
    assert_eq!(index.candidate_blocks(&disjunction), vec![0, 1]);

    let negation = LogQuery::new(partition()).where_predicate(LogPredicate::negate(missing));
    assert_eq!(index.candidate_blocks(&negation), vec![0, 1]);

    let missing_regex =
        LogPredicate::message_regex("impossible-regex-9f82c4\\d+", CaseSensitivity::Insensitive)
            .expect("regex compiles");
    let regex_query = LogQuery::new(partition()).where_predicate(missing_regex.clone());
    assert!(index.candidate_blocks(&regex_query).is_empty());
    assert!(cold_matches(&index, &records, 10, &regex_query).is_empty());

    let regex_or = LogQuery::new(partition()).where_predicate(LogPredicate::or(vec![
        LogPredicate::term("request"),
        missing_regex,
    ]));
    assert_eq!(index.candidate_blocks(&regex_or), vec![0, 1]);
}

#[test]
fn trigram_hash_collisions_can_only_create_extra_candidates() {
    let mut by_slot = vec![None::<[u8; 3]>; MESSAGE_TRIGRAM_FILTER_BITS];
    let mut collision = None;
    let lowercase_stable = (b'!'..=b'~')
        .filter(|byte| !byte.is_ascii_uppercase())
        .collect::<Vec<_>>();
    'search: for &first in &lowercase_stable {
        for &second in &lowercase_stable {
            for &third in &lowercase_stable {
                let trigram = [first, second, third];
                let slot = message_trigram_slot_for_bits(trigram, MESSAGE_TRIGRAM_FILTER_BITS);
                if let Some(previous) = by_slot[slot]
                    && previous != trigram
                {
                    collision = Some((previous, trigram));
                    break 'search;
                }
                by_slot[slot] = Some(trigram);
            }
        }
    }
    let (stored, queried) = collision.expect("the bounded hash has a printable collision");
    let stored = String::from_utf8(stored.to_vec()).expect("printable bytes are UTF-8");
    let queried = String::from_utf8(queried.to_vec()).expect("printable bytes are UTF-8");
    let record = DurableLog::new(
        ShardId::new(1),
        partition(),
        LogicalOffset::new(0),
        0,
        stored,
        CompressionCohortId::new(1),
    );
    let index = compatibility_index(std::slice::from_ref(&record), 1);
    let query = LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(queried));

    assert_eq!(index.candidate_blocks(&query), vec![0]);
    assert!(cold_matches(&index, &[record], 1, &query).is_empty());
}

#[test]
fn message_trigram_memory_is_fixed_per_block() {
    let records = compatibility_records(30);
    let index = compatibility_index(&records, 10);
    assert_eq!(
        index.message_trigram_filter_bytes(),
        3 * (MESSAGE_TRIGRAM_FILTER_BYTES + CASE_SENSITIVE_MESSAGE_TRIGRAM_FILTER_BYTES)
    );
}
