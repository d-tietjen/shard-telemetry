use std::sync::Arc;

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use prost::Message;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, ShardId, TopicId, TopicPartition};

use super::*;
use crate::{
    CaseSensitivity, LocalityGranularity, LogPredicate, MetadataField, TelemetryValue,
    TextMatchKind, TextMatcher, ingest_pack::prepare_ingest_pack,
};

fn partition() -> TopicPartition {
    TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(3))
}

#[test]
fn structural_candidate_ordinals_are_sorted_and_deduplicated() {
    let mut candidates = vec![210, 42, 210, 7, 42];

    normalize_structural_candidate_ordinals(&mut candidates);

    assert_eq!(candidates, vec![7, 42, 210]);

    let mut sorted = vec![7, 42, 210];
    normalize_structural_candidate_ordinals(&mut sorted);
    assert_eq!(sorted, vec![7, 42, 210]);
}

fn record(offset: u64, message: &str) -> DurableLog {
    record_on(ShardId::new(7), offset, message)
}

fn record_on(stream_shard_id: ShardId, offset: u64, message: &str) -> DurableLog {
    DurableLog::new(
        stream_shard_id,
        partition(),
        LogicalOffset::new(offset),
        offset * 10,
        message,
        CompressionCohortId::new(4),
    )
}

#[test]
fn batched_message_scores_match_indexed_scores_for_any_candidate_order() {
    let mut request = crate::AnalyticsScanRequest::for_relation(
        Arc::from("tenant"),
        crate::AnalyticsRelation::Logs,
    );
    request.case_insensitive_message_tokens = vec![Arc::from("error"), Arc::from("failed")];
    let scorer = crate::analytics::RelevanceScorer::from_request(&request);
    let posting = |ordinals: &[u32], frequencies: &[u32]| {
        Arc::new(MessageTokenPosting {
            ordinals: Arc::from(ordinals.to_vec()),
            frequencies: Arc::from(frequencies.to_vec()),
        })
    };
    let mut postings = HashMap::new();
    postings.insert(Arc::from("error"), posting(&[0, 2], &[1, 2]));
    postings.insert(Arc::from("failed"), posting(&[1, 2], &[3, 1]));
    let stats = CachedMessageTokenStats {
        postings,
        document_lengths: Arc::from(vec![4, 8, 6]),
        messages: Arc::from(vec![Arc::from(""), Arc::from(""), Arc::from("")]),
        token_ids_by_term: HashMap::new(),
        token_sequence: Arc::from(Vec::<u32>::new()),
        token_offsets: Arc::from(vec![0, 0, 0, 0]),
    };
    for ordinals in [[0, 1, 2], [2, 0, 1], [2, 1, 0]] {
        let mut batched = Vec::new();
        stats
            .score_batch(&scorer, ordinals.into_iter(), |score| {
                batched.push(score);
                Ok::<_, ()>(())
            })
            .expect("batched score emission succeeds");
        let expected = ordinals
            .iter()
            .map(|ordinal| {
                scorer.score_indexed_by_index(stats.document_lengths[*ordinal as usize], |index| {
                    let term = scorer.terms()[index].as_ref();
                    let Some(posting) = stats.postings.get(term) else {
                        return 0;
                    };
                    posting
                        .ordinals
                        .binary_search(ordinal)
                        .ok()
                        .and_then(|position| posting.frequencies.get(position).copied())
                        .unwrap_or_default()
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(batched, expected, "candidate ordinals: {ordinals:?}");
    }
}

#[test]
fn compressed_frame_preserves_multi_token_and_bounded_phrase_matches() {
    let events = (0..1_024)
        .map(|index| {
            let message = if index % 2 == 0 {
                format!(
                    "failed to send order confirmation to user{index}@example.com: failed POST to email service: expected 200, got 500"
                )
            } else {
                "Failed to place order".to_owned()
            };
            OtlpLogEvent {
                timestamp_unix_nanos: 1_000 + index,
                body: Some(TelemetryValue::String(Arc::from(message.as_str()))),
                message: Arc::from(message),
                ..OtlpLogEvent::default()
            }
        })
        .collect::<Vec<_>>();
    let prepared = prepare_ingest_pack(&events).expect("pack prepares");
    let mut stripe = LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe");
    stripe
        .apply_indexed_ingest_pack(
            partition(),
            LogicalOffset::new(0),
            events.len() as u32,
            Bytes::from(prepared.payload),
        )
        .expect("frame append indexes");

    let terms = [
        "failed",
        "send",
        "order",
        "confirmation",
        "email",
        "service",
        "expected",
        "post",
    ];
    let conjunction = LogQuery::new(partition()).where_predicate(LogPredicate::and(
        terms
            .into_iter()
            .map(|term| LogPredicate::message_token(term, CaseSensitivity::Insensitive))
            .collect::<Vec<_>>(),
    ));
    assert_eq!(
        stripe.count_query_checked(&conjunction).expect("count"),
        512
    );
    assert_eq!(
        stripe.query_checked(&conjunction).expect("query").len(),
        512
    );

    let phrase = LogQuery::new(partition())
        .where_predicate(LogPredicate::message_phrase(
            ["failed", "to", "place", "order"],
            0,
            CaseSensitivity::Insensitive,
        ))
        .sort_by_timestamp()
        .newest_first()
        .with_timestamp_range(1_000, 2_024)
        .with_limit(10);
    assert_eq!(
        stripe
            .query_checked(&phrase)
            .expect("bounded phrase query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        (0..10).map(|index| 2_023 - index * 2).collect::<Vec<_>>()
    );
    assert_eq!(
        stripe
            .query_checked_with_typed_metadata(&phrase, false, false)
            .expect("projected bounded phrase query")
            .len(),
        10
    );
}

fn string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        key_strindex: 0,
    }
}

#[test]
fn hot_predicate_indexes_preserve_text_numeric_regex_and_cursor_results() {
    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: u64::MAX,
            ..StripeConfig::default()
        },
    )
    .expect("stripe opens");
    for (offset, message, service, status) in [
        (0, "request rare", "api", "503"),
        (1, "request common", "api", "200"),
        (2, "worker common", "worker", "404"),
        (3, "request rare", "worker", "503"),
    ] {
        stripe
            .apply_durable(
                record(offset, message)
                    .with_field("service", service)
                    .with_field("status", status),
            )
            .expect("record indexes");
    }
    let offsets = |query: LogQuery| {
        stripe
            .query_checked(&query)
            .expect("hot query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(LogPredicate::field(
                "service",
                TextMatcher::new("PI", TextMatchKind::Contains, CaseSensitivity::Insensitive),
            ))
        ),
        vec![0, 1]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(
                LogPredicate::field_regex("status", r"5\d+", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            )
        ),
        vec![0, 3]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
                "status",
                NumericComparison::GreaterThanOrEqual,
                500,
            ))
        ),
        vec![0, 3]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(" rare"))
        ),
        vec![0, 3]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(
                LogPredicate::message_regex(r"request.*rare$", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            )
        ),
        vec![0, 3]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
                LogPredicate::message_contains(" rare"),
                LogPredicate::field(
                    "service",
                    TextMatcher::new("api", TextMatchKind::Exact, CaseSensitivity::Sensitive,),
                ),
            ])),
        ),
        vec![0]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition())
                .newest_first()
                .after(crate::QueryCursor::new(20, LogicalOffset::new(2)))
                .with_limit(2),
        ),
        vec![1, 0]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition())
                .sort_by_timestamp()
                .newest_first()
                .with_limit(2),
        ),
        vec![3, 2]
    );
}

#[test]
fn bounded_boolean_queries_stream_union_candidates_until_residual_matches() {
    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: u64::MAX,
            ..StripeConfig::default()
        },
    )
    .expect("stripe opens");
    for offset in 0..256 {
        stripe
            .apply_durable(
                record(
                    offset,
                    if offset % 2 == 0 {
                        "common event"
                    } else {
                        "ordinary event"
                    },
                )
                .with_field("service", if offset >= 56 { "late" } else { "early" }),
            )
            .expect("record indexes");
    }

    let query = LogQuery::new(partition())
        .where_predicate(LogPredicate::and(vec![
            LogPredicate::or(vec![
                LogPredicate::term("common"),
                LogPredicate::term("rare"),
            ]),
            LogPredicate::field_equals("service", "late"),
        ]))
        .with_limit(100);
    let offsets = stripe
        .query_checked(&query)
        .expect("bounded boolean query succeeds")
        .into_iter()
        .map(|matched| matched.record.record_ref.offset.get())
        .collect::<Vec<_>>();

    assert_eq!(offsets.len(), 100);
    assert_eq!(offsets.first(), Some(&56));
    assert_eq!(offsets.last(), Some(&254));
}

#[test]
fn message_posting_candidates_keep_contains_and_regex_exact() {
    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: u64::MAX,
            ..StripeConfig::default()
        },
    )
    .expect("stripe opens");
    for (offset, message) in [
        (0, "rare"),
        (1, "request rare"),
        (2, "rarely"),
        (3, "connection refused"),
        (4, "request_id=123 slow rare"),
        (5, "ÄBC"),
    ] {
        stripe
            .apply_durable(record(offset, message))
            .expect("record indexes");
    }

    let offsets = |query: LogQuery| {
        stripe
            .query_checked(&query)
            .expect("hot query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>()
    };
    assert!(crate::query::text_matches(
        "ÄBC",
        &TextMatcher::new("äbc", TextMatchKind::Contains, CaseSensitivity::Insensitive)
    ));
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(" rare")),
        ),
        vec![1, 4]
    );
    assert_eq!(
        offsets(LogQuery::new(partition()).where_predicate(LogPredicate::message_contains("rare")),),
        vec![0, 1, 2, 4]
    );
    assert_eq!(
        offsets(LogQuery::new(partition()).where_predicate(LogPredicate::message_contains("äbc")),),
        vec![5]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(
                LogPredicate::message_regex(r"request.*rare$", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            ),
        ),
        vec![1, 4]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(LogPredicate::message(TextMatcher::new(
                "conn",
                TextMatchKind::Prefix,
                CaseSensitivity::Sensitive
            ),))
        ),
        vec![3]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(
                LogPredicate::message_regex(r"conn.*", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            ),
        ),
        vec![3]
    );
    assert_eq!(
        offsets(
            LogQuery::new(partition()).where_predicate(
                LogPredicate::message_regex(r"\brare$", CaseSensitivity::Sensitive)
                    .expect("regex compiles"),
            ),
        ),
        vec![0, 1, 4]
    );
    assert_eq!(
            offsets(
                LogQuery::new(partition()).where_predicate(
                    LogPredicate::message_regex(
                        r"request_id=\d+.*\brare$",
                        CaseSensitivity::Sensitive,
                    )
                    .expect("regex compiles"),
                ),
            ),
            vec![4]
        );
}

#[test]
fn min_match_posting_candidates_preserve_all_matches() {
    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: u64::MAX,
            ..StripeConfig::default()
        },
    )
    .expect("stripe opens");
    let records = [
        (0, "error failed"),
        (1, "error"),
        (2, "failed charge"),
        (3, "charge cache"),
        (4, "error failed charge cache"),
        (5, "error cache"),
        (6, "healthy"),
    ];
    for (offset, message) in records {
        stripe
            .apply_durable(record(offset, message))
            .expect("record indexes");
    }
    let tokens = ["error", "failed", "charge", "cache"];
    let mut combinations = Vec::new();
    for left in 0..tokens.len() {
        for right in (left + 1)..tokens.len() {
            combinations.push(LogPredicate::and(vec![
                LogPredicate::message_token(tokens[left], CaseSensitivity::Insensitive),
                LogPredicate::message_token(tokens[right], CaseSensitivity::Insensitive),
            ]));
        }
    }
    let query = LogQuery::new(partition()).where_predicate(LogPredicate::or(combinations));
    let offsets = stripe
        .query_checked(&query)
        .expect("min-match query succeeds")
        .into_iter()
        .map(|matched| matched.record.record_ref.offset.get())
        .collect::<Vec<_>>();
    assert_eq!(offsets, vec![0, 2, 3, 4, 5]);

    let structural_records = records
        .into_iter()
        .map(|(offset, message)| record(offset, message))
        .collect::<Vec<_>>();
    let indexed = crate::encode_indexed_structural_records(&structural_records)
        .expect("indexed structural block encodes");
    let candidates = embedded_message_predicate_candidates(&query.predicate, &indexed.index)
        .expect("embedded min-match candidates are indexable");
    assert!(
        [0, 2, 3, 4, 5]
            .into_iter()
            .all(|ordinal| candidates.binary_search(&ordinal).is_ok())
    );
}

#[test]
fn timestamp_top_k_falls_back_for_out_of_order_ingest() {
    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: u64::MAX,
            ..StripeConfig::default()
        },
    )
    .expect("stripe opens");
    for offset in 0..1_024 {
        let mut record = record(offset, &format!("request {offset}"));
        record.timestamp_unix_nanos = 1_024 - offset;
        stripe.apply_durable(record).expect("record indexes");
    }

    let matches = stripe.query(
        &LogQuery::new(partition())
            .sort_by_timestamp()
            .newest_first()
            .with_limit(2),
    );
    assert_eq!(
        matches
            .iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn rebalanced_sub_blocks_merge_in_logical_offset_order() {
    let pending = |offset| {
        let record = record(offset, &format!("request id={offset} completed"));
        PendingRecord {
            source_bytes: row_source_bytes(&record).expect("record size fits"),
            fingerprint: fingerprint_message(&record.message, &record.fields),
            record,
        }
    };
    let mut even = ActiveBlock::from_records(vec![pending(4), pending(0), pending(2)], None, 1);
    let odd = ActiveBlock::from_records(vec![pending(5), pending(1), pending(3)], None, 1);
    even.append_block(odd);

    assert_eq!(
        even.records
            .iter()
            .map(|pending| pending.record.record_ref.offset)
            .collect::<Vec<_>>(),
        (0..6).map(LogicalOffset::new).collect::<Vec<_>>()
    );
    let borrowed = encode_structural_records(&even.records).expect("borrowed encoding");
    let owned = crate::encode_structural_block(
        &even
            .records
            .iter()
            .map(|pending| pending.record.clone())
            .collect::<Vec<_>>(),
    )
    .expect("owned encoding");
    assert_eq!(borrowed, owned);
}

#[test]
fn durable_records_become_visible_to_term_and_metadata_queries() {
    let mut database =
        ShardTelemetry::new([ShardId::new(7)], StripeConfig::default()).expect("database opens");
    database
        .apply_durable(record(0, "ERROR cannot connect").with_field("service", "api"))
        .expect("first append indexes");
    database
        .apply_durable(record(1, "error timeout").with_field("service", "worker"))
        .expect("second append indexes");

    let matches = database
        .query(
            ShardId::new(7),
            &LogQuery::new(partition())
                .with_term("error")
                .with_field("service", "api"),
        )
        .expect("query succeeds");
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].record.record_ref.offset, LogicalOffset::new(0));
    assert_eq!(
        database
            .stripe(ShardId::new(7))
            .expect("stripe exists")
            .indexed_through(partition()),
        Some(LogicalOffset::new(1))
    );
}

#[test]
fn hot_count_queries_match_materialized_results_without_index_vector_storage() {
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    for offset in 0..1_024 {
        stripe
            .apply_durable(
                record(
                    offset,
                    if offset % 2 == 0 {
                        "error api"
                    } else {
                        "info worker"
                    },
                )
                .with_field("service", if offset % 2 == 0 { "api" } else { "worker" })
                .with_field("status", if offset % 10 == 0 { "500" } else { "200" }),
            )
            .expect("record indexes");
    }
    let queries = [
        LogQuery::new(partition()).where_predicate(LogPredicate::field_exists("service")),
        LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::term("error"),
            LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
        ])),
        LogQuery::new(partition())
            .where_predicate(LogPredicate::field_in("service", ["api", "worker"])),
        LogQuery::new(partition())
            .with_field("service", "api")
            .where_predicate(LogPredicate::message_contains("error"))
            .with_timestamp_range(2_000, 8_000),
        LogQuery::new(partition()).newest_first().with_limit(17),
    ];
    for query in queries {
        let expected = stripe.query_checked(&query).expect("query succeeds").len() as u64;
        assert_eq!(
            stripe.count_query_checked(&query).expect("count succeeds"),
            expected,
            "count and materialized query diverged for {query:?}"
        );
    }
}

#[test]
fn active_tenant_partition_cache_invalidates_after_indexed_append() {
    let events = vec![OtlpLogEvent {
        timestamp_unix_nanos: 1_000,
        message: Arc::from("cache invalidation"),
        ..OtlpLogEvent::default()
    }];
    let payload = Bytes::from(
        prepare_ingest_pack(&events)
            .expect("indexed ingest pack prepares")
            .payload,
    );
    let second_partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(4));
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");

    stripe
        .apply_indexed_ingest_pack(partition(), LogicalOffset::new(0), 1, payload.clone())
        .expect("first indexed append installs");
    assert_eq!(
        stripe
            .tenant_partitions("test-tenant")
            .expect("first tenant partition lookup"),
        vec![partition()]
    );

    stripe
        .apply_indexed_ingest_pack(second_partition, LogicalOffset::new(0), 1, payload)
        .expect("second indexed append installs");
    assert_eq!(
        stripe
            .tenant_partitions("test-tenant")
            .expect("cached tenant partition lookup refreshes"),
        vec![partition(), second_partition]
    );
}

#[test]
fn compressed_frame_queries_preserve_interleaved_offsets_and_full_exactness() {
    let events = (0..12)
        .map(|ordinal| OtlpLogEvent {
            timestamp_unix_nanos: 1_000 + ordinal,
            message: Arc::from(if ordinal % 2 == 0 {
                format!("ERROR request id={ordinal} failed")
            } else {
                format!("INFO request id={ordinal} completed")
            }),
            fields: Arc::new(vec![
                MetadataField::new("service", if ordinal % 2 == 0 { "api" } else { "worker" }),
                MetadataField::new("trace", format!("trace-{ordinal}")),
                MetadataField::new("status", if ordinal % 2 == 0 { "503" } else { "200" }),
            ]),
            compression_cohort: CompressionCohortId::new(ordinal % 3),
            ..OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
    let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
    let payload = Bytes::from(prepared.payload);
    let first_offset = LogicalOffset::new(50);
    let mut live =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("live stripe opens");
    live.apply_indexed_ingest_pack(
        partition(),
        first_offset,
        events.len() as u32,
        payload.clone(),
    )
    .expect("live frame indexes install");
    assert_eq!(
        live.count_tenant_records("test-tenant", &[partition()])
            .expect("resident count"),
        events.len() as u64
    );
    assert_eq!(
        live.count_tenant_records("another-tenant", &[partition()])
            .expect("other tenant count"),
        0
    );
    let mut recovered =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("recovered stripe opens");
    recovered
        .apply_indexed_ingest_pack(partition(), first_offset, events.len() as u32, payload)
        .expect("durable frame indexes recover");

    let queries = [
        LogQuery::new(partition())
            .with_term("error")
            .with_field("service", "api"),
        LogQuery::new(partition())
            .with_term("7")
            .with_field("trace", "trace-7"),
        LogQuery::new(partition())
            .with_offset_range(LogicalOffset::new(53), LogicalOffset::new(58))
            .sort_by_timestamp()
            .newest_first()
            .with_limit(3),
        LogQuery::new(partition()).with_field("service", "missing"),
        LogQuery::new(partition()).where_predicate(LogPredicate::field_exists("service")),
        LogQuery::new(partition()).where_predicate(LogPredicate::field_in("service", ["api"])),
        LogQuery::new(partition()).where_predicate(LogPredicate::field(
            "service",
            TextMatcher::new("ork", TextMatchKind::Contains, CaseSensitivity::Sensitive),
        )),
        LogQuery::new(partition()).where_predicate(
            LogPredicate::field_regex("service", "^a", CaseSensitivity::Sensitive)
                .expect("regex compiles"),
        ),
        LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
            "status",
            NumericComparison::GreaterThanOrEqual,
            500,
        )),
    ];
    let expected = [
        vec![50, 52, 54, 56, 58, 60],
        vec![57],
        vec![57, 56, 55],
        vec![],
        (50..62).collect::<Vec<_>>(),
        vec![50, 52, 54, 56, 58, 60],
        vec![51, 53, 55, 57, 59, 61],
        vec![50, 52, 54, 56, 58, 60],
        vec![50, 52, 54, 56, 58, 60],
    ];
    for (query, expected) in queries.iter().zip(expected) {
        let live_offsets = live
            .query_checked(query)
            .expect("live query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>();
        let recovered_offsets = recovered
            .query_checked(query)
            .expect("recovered query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>();
        assert_eq!(live_offsets, expected);
        assert_eq!(recovered_offsets, expected);
    }
    assert_eq!(
        live.query_refs(
            &LogQuery::new(partition())
                .with_term("error")
                .with_field("service", "api"),
        )
        .into_iter()
        .map(|record_ref| record_ref.offset.get())
        .collect::<Vec<_>>(),
        vec![50, 52, 54, 56, 58, 60]
    );
    assert_eq!(
        live.query_refs(
            &LogQuery::new(partition())
                .where_predicate(LogPredicate::field_exists("service"))
                .newest_first()
                .with_limit(3),
        )
        .into_iter()
        .map(|record_ref| record_ref.offset.get())
        .collect::<Vec<_>>(),
        vec![61, 60, 59]
    );
    let case_insensitive_count = LogQuery::new(partition()).where_predicate(
        LogPredicate::message_token("error", CaseSensitivity::Insensitive),
    );
    assert_eq!(
        live.count_query_checked(&case_insensitive_count)
            .expect("indexed exact-token count"),
        6
    );
    let parity_queries = [
        LogQuery::new(partition()).where_predicate(LogPredicate::message_token(
            "ERROR",
            CaseSensitivity::Sensitive,
        )),
        LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::message_token("error", CaseSensitivity::Insensitive),
            LogPredicate::message_token("failed", CaseSensitivity::Sensitive),
        ])),
        LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
            LogPredicate::message_token("error", CaseSensitivity::Insensitive),
            LogPredicate::field_numeric("status", NumericComparison::GreaterThanOrEqual, 500),
        ])),
        LogQuery::new(partition())
            .with_field("service", "api")
            .where_predicate(LogPredicate::message_token(
                "error",
                CaseSensitivity::Insensitive,
            ))
            .with_timestamp_range(1_004, 1_010)
            .sort_by_timestamp()
            .newest_first(),
        LogQuery::new(partition())
            .with_field("service", "api")
            .with_timestamp_range(1_004, 1_010)
            .with_limit(2),
        LogQuery::new(partition()).where_predicate(
            LogPredicate::field_regex("service", "^a", CaseSensitivity::Sensitive)
                .expect("regex compiles"),
        ),
        LogQuery::new(partition()).where_predicate(LogPredicate::field_numeric(
            "status",
            NumericComparison::GreaterThanOrEqual,
            500,
        )),
    ];
    for query in parity_queries {
        let expected = live
            .query_checked(&query)
            .expect("materialized exact query succeeds");
        assert_eq!(
            live.count_query_checked(&query)
                .expect("cardinality exact query succeeds"),
            expected.len() as u64,
            "count and materialized query diverged for {query:?}"
        );
        assert_eq!(
            recovered
                .query_checked(&query)
                .expect("recovered exact query succeeds")
                .len(),
            expected.len(),
            "recovered and live query diverged for {query:?}"
        );
    }
    assert_eq!(
        live.indexed_through(partition()),
        Some(LogicalOffset::new(61))
    );
    assert!(!live.partitions.contains_key(&partition()));
}

#[test]
fn indexed_group_queries_preserve_single_and_two_key_counts() {
    let events = [
        ("ERROR", "frontend", "error request"),
        ("ERROR", "frontend", "error retry"),
        ("INFO", "frontend", "error completed"),
        ("ERROR", "payments", "error declined"),
        ("INFO", "payments", "healthy"),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (severity, scope, message))| OtlpLogEvent {
        timestamp_unix_nanos: ordinal as u64,
        message: Arc::from(message),
        fields: Arc::new(vec![
            MetadataField::new("attr.loki.metadata.severity_text", severity),
            MetadataField::new("attr.loki.metadata.scope_name", scope),
        ]),
        compression_cohort: CompressionCohortId::new(1),
        ..OtlpLogEvent::default()
    })
    .collect::<Vec<_>>();
    let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    stripe
        .apply_indexed_ingest_pack(
            partition(),
            LogicalOffset::new(0),
            events.len() as u32,
            Bytes::from(prepared.payload),
        )
        .expect("frame append indexes");
    let query = LogQuery::new(partition()).where_predicate(LogPredicate::message_token(
        "error",
        CaseSensitivity::Insensitive,
    ));

    let by_severity = stripe
        .group_query_partitions_checked(
            std::slice::from_ref(&query),
            &[AnalyticsGroupKey::SeverityText],
        )
        .expect("single-key grouping succeeds");
    assert_eq!(by_severity.get(&vec![Some(Arc::from("ERROR"))]), Some(&3));
    assert_eq!(by_severity.get(&vec![Some(Arc::from("INFO"))]), Some(&1));

    let by_pair = stripe
        .group_query_partitions_checked(
            &[query],
            &[
                AnalyticsGroupKey::SeverityText,
                AnalyticsGroupKey::ScopeName,
            ],
        )
        .expect("two-key grouping succeeds");
    assert_eq!(
        by_pair.get(&vec![Some(Arc::from("ERROR")), Some(Arc::from("frontend"))]),
        Some(&2)
    );
    assert_eq!(
        by_pair.get(&vec![Some(Arc::from("INFO")), Some(Arc::from("frontend"))]),
        Some(&1)
    );
    assert_eq!(
        by_pair.get(&vec![Some(Arc::from("ERROR")), Some(Arc::from("payments"))]),
        Some(&1)
    );
}

#[test]
fn projected_severity_text_matches_typed_metadata() {
    let events = vec![OtlpLogEvent {
        timestamp_unix_nanos: 1,
        message: Arc::from("projected severity"),
        fields: Arc::new(vec![MetadataField::new("otel.severity_text", "WARN")]),
        severity_text: Arc::from("WARN"),
        compression_cohort: CompressionCohortId::new(1),
        ..OtlpLogEvent::default()
    }];
    let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    stripe
        .apply_indexed_ingest_pack(
            partition(),
            LogicalOffset::new(0),
            1,
            Bytes::from(prepared.payload),
        )
        .expect("frame append indexes");
    let query = LogQuery::new(partition()).with_term("projected");
    let typed = stripe
        .query_partitions_checked_projected(std::slice::from_ref(&query), true)
        .expect("typed query succeeds");
    let projected = stripe
        .query_partitions_checked_projected(std::slice::from_ref(&query), false)
        .expect("projected query succeeds");
    assert_eq!(typed.len(), 1);
    assert_eq!(projected.len(), 1);
    assert_eq!(
        typed[0].record.severity_text,
        projected[0].record.severity_text
    );
}

#[test]
fn compressed_frame_partition_fanout_applies_one_global_timestamp_limit() {
    let other_partition = TopicPartition::new(TopicId::new(9), LogicalPartitionId::new(4));
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    for (topic_partition, timestamp_groups) in [
        (partition(), [[100, 101], [300, 301]]),
        (other_partition, [[200, 201], [400, 401]]),
    ] {
        for (batch, timestamps) in timestamp_groups.into_iter().enumerate() {
            let events = timestamps.map(|timestamp| OtlpLogEvent {
                timestamp_unix_nanos: timestamp,
                message: Arc::from(format!("ERROR request {timestamp} failed")),
                fields: Arc::new(vec![MetadataField::new("service", "api")]),
                compression_cohort: CompressionCohortId::new(1),
                ..OtlpLogEvent::default()
            });
            let prepared = prepare_ingest_pack(&events).expect("pack prepares");
            stripe
                .apply_indexed_ingest_pack(
                    topic_partition,
                    LogicalOffset::new((batch * 2) as u64),
                    events.len() as u32,
                    Bytes::from(prepared.payload),
                )
                .expect("frame append indexes");
        }
    }
    let queries = [partition(), other_partition].map(|topic_partition| {
        LogQuery::new(topic_partition)
            .with_term("error")
            .sort_by_timestamp()
            .newest_first()
            .with_limit(3)
    });
    assert_eq!(
        stripe
            .query_partitions_checked(&queries)
            .expect("newest fanout query succeeds")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![401, 400, 301]
    );
    let queries = [partition(), other_partition].map(|topic_partition| {
        LogQuery::new(topic_partition)
            .with_term("error")
            .sort_by_timestamp()
            .with_limit(3)
    });
    assert_eq!(
        stripe
            .query_partitions_checked(&queries)
            .expect("oldest fanout query succeeds")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![100, 101, 200]
    );
}

#[test]
fn high_cardinality_indexed_fields_use_selective_fallback() {
    let events = (0..4_100u64)
        .map(|ordinal| OtlpLogEvent {
            timestamp_unix_nanos: ordinal,
            message: Arc::from("field fallback"),
            fields: Arc::new(vec![MetadataField::new(
                "trace",
                format!("trace-{ordinal}"),
            )]),
            compression_cohort: CompressionCohortId::new(1),
            ..OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
    let prepared = prepare_ingest_pack(&events).expect("indexed ingest pack prepares");
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    stripe
        .apply_indexed_ingest_pack(
            partition(),
            LogicalOffset::new(0),
            events.len() as u32,
            Bytes::from(prepared.payload),
        )
        .expect("frame append indexes");

    let query = LogQuery::new(partition()).where_predicate(
        LogPredicate::field_regex("trace", "^trace-4096$", CaseSensitivity::Sensitive)
            .expect("regex compiles"),
    );
    let matches = stripe.query_checked(&query).expect("query succeeds");
    assert_eq!(
        matches
            .iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![4096]
    );
    assert_eq!(
        stripe.count_query_checked(&query).expect("count succeeds"),
        1
    );
}

#[test]
fn compressed_frame_timestamp_top_k_selects_before_full_record_decode() {
    let fields = Arc::new(vec![crate::MetadataField::new("docker_stream", "stderr")]);
    let events = (0..1_024u64)
        .map(|ordinal| OtlpLogEvent {
            timestamp_unix_nanos: ordinal,
            message: Arc::from(if matches!(ordinal, 10 | 20 | 30) {
                "prefix target suffix".to_owned()
            } else {
                "prefix_target suffix".to_owned()
            }),
            fields: Arc::clone(&fields),
            compression_cohort: CompressionCohortId::new(1),
            ..OtlpLogEvent::default()
        })
        .collect::<Vec<_>>();
    let prepared = prepare_ingest_pack(&events).expect("pack prepares");
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    stripe
        .apply_indexed_ingest_pack(
            partition(),
            LogicalOffset::new(0),
            events.len() as u32,
            Bytes::from(prepared.payload),
        )
        .expect("frame append indexes");

    let latest = LogQuery::new(partition())
        .sort_by_timestamp()
        .newest_first()
        .with_limit(3);
    assert_eq!(
        stripe
            .query_checked(&latest)
            .expect("latest query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![1_023, 1_022, 1_021]
    );

    let latest_stream = LogQuery::new(partition())
        .with_field("docker_stream", "stderr")
        .sort_by_timestamp()
        .newest_first()
        .with_limit(3);
    assert_eq!(
        stripe
            .query_checked(&latest_stream)
            .expect("latest exact-stream query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![1_023, 1_022, 1_021]
    );
    let latest_stream_window = latest_stream.clone().with_timestamp_range(1_000, 1_024);
    assert_eq!(
        stripe
            .query_checked(&latest_stream_window)
            .expect("latest exact-stream timestamp window query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![1_023, 1_022, 1_021]
    );

    let sparse_residual = LogQuery::new(partition())
        .where_predicate(LogPredicate::message_token(
            "target",
            CaseSensitivity::Sensitive,
        ))
        .sort_by_timestamp()
        .newest_first()
        .with_limit(2);
    assert_eq!(
        stripe
            .query_checked(&sparse_residual)
            .expect("sparse residual query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![30, 20]
    );

    let phrase = LogQuery::new(partition()).where_predicate(LogPredicate::message_phrase(
        ["prefix", "target"],
        0,
        CaseSensitivity::Sensitive,
    ));
    assert_eq!(
        stripe
            .query_checked(&phrase)
            .expect("phrase query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert_eq!(
        stripe.count_query_checked(&phrase).expect("phrase count"),
        3
    );
    let proximity = LogQuery::new(partition()).where_predicate(LogPredicate::message_phrase(
        ["prefix", "suffix"],
        1,
        CaseSensitivity::Insensitive,
    ));
    assert_eq!(
        stripe
            .count_query_checked(&proximity)
            .expect("indexed proximity count"),
        3
    );
    let regex_prefix = LogQuery::new(partition()).where_predicate(
        LogPredicate::message_token_regex("^prefix.*$", CaseSensitivity::Insensitive)
            .expect("token regex compiles"),
    );
    assert_eq!(
        stripe
            .count_query_checked(&regex_prefix)
            .expect("indexed token regex count"),
        1_024
    );
    let token_prefix = LogQuery::new(partition()).where_predicate(
        LogPredicate::message_token_prefix("prefix", CaseSensitivity::Insensitive),
    );
    assert_eq!(
        stripe
            .count_query_checked(&token_prefix)
            .expect("indexed token prefix count"),
        1_024
    );
    let fuzzy =
        LogQuery::new(partition()).where_predicate(LogPredicate::message_fuzzy("targit", 1));
    assert_eq!(
        stripe
            .count_query_checked(&fuzzy)
            .expect("indexed fuzzy count"),
        3
    );
    let fuzzy_and_prefix = LogQuery::new(partition()).where_predicate(LogPredicate::and(vec![
        LogPredicate::message_fuzzy("targit", 1),
        LogPredicate::message_token_prefix("prefix", CaseSensitivity::Insensitive),
    ]));
    assert_eq!(
        stripe
            .count_query_checked(&fuzzy_and_prefix)
            .expect("indexed fuzzy and prefix count"),
        3
    );

    let static_phrase = LogQuery::new(partition()).where_predicate(LogPredicate::message_phrase(
        ["prefix", "target"],
        0,
        CaseSensitivity::Insensitive,
    ));
    assert_eq!(
        stripe
            .query_checked(&static_phrase)
            .expect("static phrase query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert_eq!(
        stripe
            .count_query_checked(&static_phrase)
            .expect("static phrase count"),
        3
    );

    let literal =
        LogQuery::new(partition()).where_predicate(LogPredicate::message_contains(" target "));
    assert_eq!(
        stripe
            .query_checked(&literal)
            .expect("message literal query")
            .into_iter()
            .map(|matched| matched.record.timestamp_unix_nanos)
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert_eq!(
        stripe
            .count_query_checked(&literal)
            .expect("message literal count"),
        3
    );
}

#[test]
fn homogeneous_event_batches_publish_one_posting_range_per_value() {
    let message: Arc<str> = Arc::from("repeated request completed");
    let fields = Arc::new(vec![crate::MetadataField::new("service", "api")]);
    let event = OtlpLogEvent {
        timestamp_unix_nanos: 42,
        message,
        fields,
        compression_cohort: CompressionCohortId::new(4),
        ..OtlpLogEvent::default()
    };
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    let receipts = stripe
        .apply_otlp_events(partition(), LogicalOffset::new(0), vec![event; 1_024])
        .expect("homogeneous event range indexes");

    assert_eq!(receipts.len(), 1_024);
    assert_eq!(
        stripe.indexed_through(partition()),
        Some(LogicalOffset::new(1_023))
    );
    let indexed = stripe
        .partitions
        .get(&partition())
        .expect("partition was indexed");
    let repeated_id = indexed.term_ids["repeated"];
    assert_eq!(
        indexed.term_postings[repeated_id].runs,
        vec![OrdinalRun {
            first: 0,
            last: 1_023
        }]
    );
    let service_id = indexed.field_ids["service"]["api"];
    assert_eq!(
        indexed.field_postings[service_id].runs,
        vec![OrdinalRun {
            first: 0,
            last: 1_023
        }]
    );
    assert_eq!(
        stripe
            .query_refs(
                &LogQuery::new(partition())
                    .with_term("repeated")
                    .with_field("service", "api")
            )
            .len(),
        1_024
    );
}

#[test]
fn heterogeneous_event_batches_retain_exact_sparse_postings() {
    let fields = Arc::new(vec![crate::MetadataField::new("service", "api")]);
    let event = |message: &'static str| OtlpLogEvent {
        timestamp_unix_nanos: 42,
        message: Arc::from(message),
        fields: Arc::clone(&fields),
        compression_cohort: CompressionCohortId::new(4),
        ..OtlpLogEvent::default()
    };
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    stripe
        .apply_otlp_events(
            partition(),
            LogicalOffset::new(0),
            [
                event("repeated request completed"),
                event("different request failed"),
                event("repeated request completed"),
            ],
        )
        .expect("heterogeneous events index");

    let indexed = stripe
        .partitions
        .get(&partition())
        .expect("partition was indexed");
    let repeated_id = indexed.term_ids["repeated"];
    assert_eq!(
        indexed.term_postings[repeated_id].runs,
        vec![
            OrdinalRun { first: 0, last: 0 },
            OrdinalRun { first: 2, last: 2 }
        ]
    );
    assert_eq!(
        stripe
            .query_refs(&LogQuery::new(partition()).with_term("repeated"))
            .into_iter()
            .map(|record_ref| record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
}

#[test]
fn database_fanout_merges_selected_stripes_without_duplicate_results() {
    let mut database =
        ShardTelemetry::new([ShardId::new(7), ShardId::new(8)], StripeConfig::default())
            .expect("database opens");
    for offset in 0..10 {
        let shard = if offset < 5 {
            ShardId::new(7)
        } else {
            ShardId::new(8)
        };
        database
            .apply_durable(
                record_on(shard, offset, &format!("request {offset} completed"))
                    .with_field("service", "api"),
            )
            .expect("record indexes");
    }
    let query = LogQuery::new(partition())
        .with_predicate(crate::LogPredicate::field_exists("service"))
        .newest_first()
        .with_limit(3);
    assert_eq!(
        database
            .query_all(&query)
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![9, 8, 7]
    );
    assert_eq!(
        database
            .query_stripes([ShardId::new(8), ShardId::new(8), ShardId::new(7)], &query,)
            .expect("selected query succeeds")
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![9, 8, 7]
    );
    assert!(matches!(
        database.query_stripes([ShardId::new(99)], &query),
        Err(TelemetryError::UnknownStripe(shard)) if shard == ShardId::new(99)
    ));
}

#[test]
fn query_intersection_is_order_independent_and_offset_sorted() {
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    for offset in 0..1_000u64 {
        let mut message = format!("common request_id={offset}");
        if offset % 10 == 0 {
            message.push_str(" medium");
        }
        if offset % 100 == 0 {
            message.push_str(" rare");
        }
        stripe
            .apply_durable(
                record(offset, &message)
                    .with_field("service", if offset % 20 == 0 { "api" } else { "worker" }),
            )
            .expect("record indexes");
    }

    let common_first = stripe.query_refs(
        &LogQuery::new(partition())
            .with_term("common")
            .with_term("medium")
            .with_term("rare")
            .with_field("service", "api"),
    );
    let rare_first = stripe.query_refs(
        &LogQuery::new(partition())
            .with_field("service", "api")
            .with_term("rare")
            .with_term("medium")
            .with_term("common"),
    );

    assert_eq!(common_first, rare_first);
    assert_eq!(
        common_first
            .iter()
            .map(|reference| reference.offset.get())
            .collect::<Vec<_>>(),
        (0..1_000).step_by(100).collect::<Vec<_>>()
    );
}

#[test]
fn query_ranges_order_and_limit_bound_materialized_results() {
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    for offset in 0..100u64 {
        stripe
            .apply_durable(record(offset, "common event"))
            .expect("record indexes");
    }

    let matches = stripe.query(
        &LogQuery::new(partition())
            .with_term("common")
            .with_offset_range(LogicalOffset::new(20), LogicalOffset::new(80))
            .with_timestamp_range(300, 700)
            .newest_first()
            .with_limit(3),
    );
    assert_eq!(
        matches
            .iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![69, 68, 67]
    );

    assert!(
        stripe
            .query(
                &LogQuery::new(partition())
                    .with_offset_range(LogicalOffset::new(5), LogicalOffset::new(5))
            )
            .is_empty()
    );
    assert!(
        stripe
            .query(&LogQuery::new(partition()).with_limit(0))
            .is_empty()
    );
}

#[test]
fn bounded_exact_boolean_queries_preserve_oldest_order() {
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    for offset in 0..100_u64 {
        stripe
            .apply_durable(
                record(offset, "common event")
                    .with_field("severity", if offset % 4 == 0 { "ERROR" } else { "INFO" }),
            )
            .expect("record indexes");
    }

    let query = LogQuery::new(partition())
        .where_predicate(LogPredicate::and(vec![
            LogPredicate::term("common"),
            LogPredicate::or(vec![
                LogPredicate::field_equals("severity", "ERROR"),
                LogPredicate::term("rare"),
            ]),
        ]))
        .with_limit(3);
    assert_eq!(
        stripe
            .query(&query)
            .into_iter()
            .map(|matched| matched.record.record_ref.offset.get())
            .collect::<Vec<_>>(),
        vec![0, 4, 8]
    );
}

#[test]
fn sorted_posting_intersection_handles_disjoint_and_overlapping_ranges() {
    let mut candidates = vec![1, 3, 4, 8, 10];
    let runs = [
        OrdinalRun { first: 0, last: 0 },
        OrdinalRun { first: 3, last: 5 },
        OrdinalRun {
            first: 10,
            last: 10,
        },
        OrdinalRun {
            first: 12,
            last: 12,
        },
    ];
    intersect_ordinal_runs(&mut candidates, &runs, 0, u32::MAX);
    assert_eq!(candidates, [3, 4, 10]);

    intersect_ordinal_runs(
        &mut candidates,
        &[OrdinalRun {
            first: 20,
            last: 20,
        }],
        0,
        u32::MAX,
    );
    assert!(candidates.is_empty());
}

#[test]
fn skewed_frame_candidate_intersection_uses_the_sparse_side() {
    let sparse = vec![0, 1_000, 50_000, 99_999];
    let dense = (0..100_000).collect::<Vec<_>>();
    let mut current = Some(sparse.clone());
    intersect_frame_candidate_slice(&mut current, &dense);
    assert_eq!(current, Some(sparse.clone()));

    let mut current = Some(dense);
    intersect_frame_candidate_slice(&mut current, &sparse);
    assert_eq!(current, Some(sparse));
}

#[test]
fn skewed_posting_intersection_preserves_sparse_matches() {
    let mut candidates = vec![0, 1_000, 50_000, 99_999];
    let runs = [OrdinalRun {
        first: 0,
        last: 99_999,
    }];
    intersect_ordinal_runs(&mut candidates, &runs, 0, 100_000);
    assert_eq!(candidates, [0, 1_000, 50_000, 99_999]);

    let mut candidates = vec![0, 1_001, 50_000, 99_998];
    let runs = (0..100_000)
        .step_by(1_000)
        .map(|ordinal| OrdinalRun {
            first: ordinal,
            last: ordinal,
        })
        .collect::<Vec<_>>();
    intersect_ordinal_runs(&mut candidates, &runs, 0, 100_000);
    assert_eq!(candidates, [0, 50_000]);
}

#[test]
fn bounded_hot_posting_intersection_stops_in_requested_order() {
    let mut dense = HotPostingList::default();
    dense.push_range(0, 999_999);
    let mut every_thousand = HotPostingList::default();
    for ordinal in (0..1_000_000).step_by(1_000) {
        every_thousand.push(ordinal);
    }
    let postings = [&dense, &every_thousand];
    assert_eq!(
        collect_hot_posting_intersection(&postings, 0, 1_000_000, QueryOrder::OldestFirst, Some(3),),
        [0, 1_000, 2_000]
    );
    assert_eq!(
        collect_hot_posting_intersection(&postings, 0, 1_000_000, QueryOrder::NewestFirst, Some(3),),
        [999_000, 998_000, 997_000]
    );
}

#[test]
fn hot_posting_union_merges_runs_without_duplicates() {
    let mut left = HotPostingList::default();
    left.push_range(0, 2);
    left.push_range(8, 9);
    let mut right = HotPostingList::default();
    right.push(2);
    right.push_range(4, 8);
    let postings = [&left, &right];
    assert_eq!(
        collect_hot_posting_union(&postings, 1, 9, None),
        [1, 2, 4, 5, 6, 7, 8]
    );
    assert_eq!(
        collect_hot_posting_union(&postings, 1, 9, Some(3)),
        [1, 2, 4]
    );

    let mut third = HotPostingList::default();
    third.push_range(1, 7);
    let postings = [&left, &right, &third];
    assert_eq!(
        collect_hot_posting_union(&postings, 0, 10, None),
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    );
}

#[test]
fn lane_global_offset_gaps_are_accepted_but_regressions_are_rejected() {
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    stripe
        .apply_durable(record(5, "first"))
        .expect("first append");
    stripe
        .apply_durable(record(7, "lane gap"))
        .expect("offsets occupied by sibling lane partitions may be skipped");
    let error = stripe
        .apply_durable(record(6, "regressed"))
        .expect_err("offset regression is rejected");
    assert_eq!(
        error,
        TelemetryError::OffsetOutOfOrder {
            partition: partition(),
            expected: LogicalOffset::new(8),
            observed: LogicalOffset::new(6),
        }
    );
    assert_eq!(
        stripe.indexed_through(partition()),
        Some(LogicalOffset::new(7))
    );
}

#[test]
fn disabled_locality_seals_without_collator_work() {
    let config = StripeConfig {
        target_block_bytes: 1,
        ..StripeConfig::default()
    };
    let mut stripe = LogStripe::new(ShardId::new(7), config).expect("stripe opens");
    let receipt = stripe
        .apply_durable(record(0, "repeated message"))
        .expect("record indexes");
    assert_eq!(receipt.sealed_blocks.len(), 1);
    assert_eq!(
        receipt.sealed_blocks[0].placement_id,
        CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4))
    );
    assert_eq!(receipt.sealed_blocks[0].compression_temperature, 0);
    assert_eq!(
        receipt.sealed_blocks[0].compression_temperature_variance_q8,
        0
    );
    let stats = stripe.compression_collation_stats();
    assert_eq!(stats.observations, 0);
    assert_eq!(stats.blocks_scored, 0);
}

#[test]
fn sealing_records_dictionary_identity_and_object_location() {
    let config = StripeConfig {
        target_block_bytes: 1,
        dictionary_cache_bytes: 8,
        compression_level: 1,
        compression_locality: CompressionLocalityConfig {
            enabled: false,
            ..CompressionLocalityConfig::default()
        },
    };
    let mut stripe = LogStripe::new(ShardId::new(7), config).expect("stripe opens");
    stripe
        .install_dictionary(
            CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4)),
            DictionaryId::new(11),
            Arc::from(&b"dict"[..]),
        )
        .expect("dictionary installs");
    let receipt = stripe
        .apply_durable(record(0, "message"))
        .expect("record indexes");
    let block = receipt
        .sealed_blocks
        .into_iter()
        .next()
        .expect("small target seals block");
    assert_eq!(block.dictionary_id, Some(DictionaryId::new(11)));
    assert_eq!(block.compression_codec, CompressionCodec::Zstd);
    let compressed = stripe
        .catalog()
        .staged_payload(block.block_id)
        .expect("sealed payload remains staged until offload");
    assert_eq!(
        u64::try_from(compressed.len()).expect("payload length fits"),
        block.stored_bytes
    );
    let decoded = zstd::bulk::Decompressor::with_dictionary(&b"dict"[..])
        .expect("decoder opens")
        .decompress(
            &compressed,
            usize::try_from(block.structural_bytes).expect("structural size fits"),
        )
        .expect("payload decompresses");
    assert_eq!(
        u64::try_from(decoded.len()).expect("structural size fits"),
        block.structural_bytes
    );
    assert_eq!(
        block.source_bytes,
        row_source_bytes(&record(0, "message")).expect("source accounting succeeds")
    );
    let records =
        crate::structural::decode_structural_block(&decoded).expect("structural payload decodes");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, LogicalOffset::new(0));
    assert_eq!(records[0].timestamp_unix_nanos, 0);
    assert_eq!(records[0].message.as_ref(), "message");
    assert!(records[0].fields.is_empty());
    stripe
        .mark_block_offloaded(block.block_id, "objects/7/00000000.log")
        .expect("block is known");
    assert!(stripe.catalog().staged_payload(block.block_id).is_none());
    assert_eq!(
        stripe
            .catalog()
            .get(block.block_id)
            .expect("block exists")
            .object_key
            .as_deref(),
        Some("objects/7/00000000.log")
    );
}

#[test]
fn locality_placement_preserves_utf8_records_queries_and_block_diagnostics() {
    let locality = CompressionLocalityConfig {
        enabled: true,
        min_split_records: 2,
        min_split_bytes: 1,
        min_admission_bytes: 1,
        ..CompressionLocalityConfig::default()
    };
    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: 150,
            dictionary_cache_bytes: 128,
            compression_level: 1,
            compression_locality: locality,
        },
    )
    .expect("stripe opens");
    let source = CompressionCohortId::new(4);
    let records = [
        DurableLog::new(
            ShardId::new(7),
            partition(),
            LogicalOffset::new(0),
            10,
            "Échec request 123 at 東京",
            source,
        )
        .with_field("service", "paiements"),
        DurableLog::new(
            ShardId::new(7),
            partition(),
            LogicalOffset::new(1),
            20,
            "Échec request 456 at 東京",
            source,
        )
        .with_field("service", "paiements"),
        DurableLog::new(
            ShardId::new(7),
            partition(),
            LogicalOffset::new(2),
            30,
            "Échec request 890 at 東京",
            source,
        )
        .with_field("service", "paiements"),
        DurableLog::new(
            ShardId::new(7),
            partition(),
            LogicalOffset::new(3),
            40,
            "Échec request 042 at 東京",
            source,
        )
        .with_field("service", "paiements"),
    ];

    let first = stripe
        .apply_durable(records[0].clone())
        .expect("first record indexes");
    let second = stripe
        .apply_durable(records[1].clone())
        .expect("second record indexes");
    assert_eq!(
        first.tentative_compression_placement.granularity,
        LocalityGranularity::Base
    );
    assert_eq!(
        second.tentative_compression_placement.granularity,
        LocalityGranularity::Base
    );
    let third = stripe
        .apply_durable(records[2].clone())
        .expect("third record indexes");
    let fourth = stripe
        .apply_durable(records[3].clone())
        .expect("fourth record indexes");
    assert_eq!(
        third.tentative_compression_placement.granularity,
        LocalityGranularity::Collated
    );
    assert_eq!(
        fourth.tentative_compression_placement.granularity,
        LocalityGranularity::Collated
    );

    let matches = stripe.query(
        &LogQuery::new(partition())
            .with_term("東京")
            .with_term("échec")
            .with_field("service", "paiements"),
    );
    assert_eq!(
        matches
            .iter()
            .map(|matched| matched.record.record_ref.offset)
            .collect::<Vec<_>>(),
        (0..4).map(LogicalOffset::new).collect::<Vec<_>>()
    );

    stripe
        .seal_active_blocks()
        .expect("remaining active blocks seal");
    let mut reconstructed = Vec::new();
    for block in stripe.catalog().iter() {
        assert_eq!(block.source_compression_cohort, source);
        assert!(block.record_count > 0);
        assert!(block.max_compression_temperature_deviation <= 20);
        let compressed = stripe
            .catalog()
            .staged_payload(block.block_id)
            .expect("payload is staged");
        let structural = zstd::bulk::decompress(
            &compressed,
            usize::try_from(block.structural_bytes).expect("structural bytes fit"),
        )
        .expect("payload decompresses");
        reconstructed.extend(
            crate::structural::decode_structural_block(&structural)
                .expect("structural records decode"),
        );
    }
    reconstructed.sort_unstable_by_key(|record| record.offset);
    assert_eq!(reconstructed.len(), records.len());
    for (decoded, original) in reconstructed.iter().zip(records) {
        assert!(
            stripe
                .final_compression_placement(original.record_ref)
                .is_some()
        );
        assert_eq!(decoded.offset, original.record_ref.offset);
        assert_eq!(decoded.timestamp_unix_nanos, original.timestamp_unix_nanos);
        assert_eq!(decoded.message, original.message);
        assert_eq!(decoded.fields.as_ref(), original.fields.as_ref());
    }
}

#[test]
fn mixed_blocks_filter_deviations_and_refill_compression_shards() {
    let candidates = [
        "alpha scheduler accepted static work",
        "database replica checkpoint completed",
        "network listener rejected malformed frame",
        "payment gateway authorized transaction",
        "kernel allocator reclaimed cold pages",
        "telemetry exporter flushed pending spans",
    ];
    let mut selected = (candidates[0], candidates[1], 0u8);
    for left in candidates {
        for right in candidates {
            let distance =
                CompressionTemperature::new(fingerprint_message(left, &[]).locality_signature)
                    .distance(CompressionTemperature::new(
                        fingerprint_message(right, &[]).locality_signature,
                    ));
            if distance > selected.2 {
                selected = (left, right, distance);
            }
        }
    }
    assert!(selected.2 >= 2, "test messages need separated temperatures");

    let mut stripe = LogStripe::new(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: 400,
            dictionary_cache_bytes: 1024,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig {
                enabled: true,
                min_split_records: 2,
                min_split_bytes: 1,
                split_variance_q8: 1,
                max_shard_variance_q8: u16::MAX,
                max_assignment_distance: selected.2.saturating_sub(1),
                min_admission_bytes: 1,
                ..CompressionLocalityConfig::default()
            },
        },
    )
    .expect("stripe opens");
    let source = CompressionCohortId::new(44);
    let messages = (0..8)
        .map(|index| {
            if index % 2 == 0 {
                selected.0
            } else {
                selected.1
            }
        })
        .chain((0..8).map(|_| selected.0))
        .chain((0..8).map(|_| selected.1))
        .collect::<Vec<_>>();
    for (index, message) in messages.iter().enumerate() {
        stripe
            .apply_durable(DurableLog::new(
                ShardId::new(7),
                partition(),
                LogicalOffset::new(u64::try_from(index).expect("offset fits")),
                u64::try_from(index).expect("timestamp fits"),
                *message,
                source,
            ))
            .expect("record indexes");
    }
    stripe
        .seal_active_blocks()
        .expect("remaining compression shards seal");

    let placements = stripe
        .catalog()
        .iter()
        .map(|block| block.placement_id)
        .collect::<HashSet<_>>();
    assert!(placements.len() >= 2);
    assert!(stripe.compression_collation_stats().blocks_split > 0);
    assert!(stripe.compression_collation_stats().records_reassigned > 0);

    let mut reconstructed = Vec::new();
    for block in stripe.catalog().iter() {
        let compressed = stripe
            .catalog()
            .staged_payload(block.block_id)
            .expect("payload staged");
        let structural = zstd::bulk::decompress(
            &compressed,
            usize::try_from(block.structural_bytes).expect("size fits"),
        )
        .expect("block decompresses");
        reconstructed.extend(
            crate::structural::decode_structural_block(&structural).expect("block reconstructs"),
        );
    }
    reconstructed.sort_unstable_by_key(|record| record.offset);
    assert_eq!(reconstructed.len(), messages.len());
    assert_eq!(
        reconstructed
            .iter()
            .map(|record| record.message.as_ref())
            .collect::<Vec<_>>(),
        messages
    );
}

#[test]
fn dictionary_cache_refreshes_lru_before_eviction() {
    let mut cache = DictionaryCache::new(4).expect("cache opens");
    cache
        .insert(DictionaryId::new(1), Arc::from(&b"aa"[..]))
        .expect("first dictionary");
    cache
        .insert(DictionaryId::new(2), Arc::from(&b"bb"[..]))
        .expect("second dictionary");
    let _ = cache
        .get(DictionaryId::new(1))
        .expect("first dictionary cached");
    let insert = cache
        .insert(DictionaryId::new(3), Arc::from(&b"cc"[..]))
        .expect("third dictionary");
    assert_eq!(insert.evicted, vec![DictionaryId::new(2)]);
    assert!(cache.contains(DictionaryId::new(1)));
    assert!(cache.contains(DictionaryId::new(3)));
}

#[test]
fn catalog_shares_immutable_bytes_but_each_stripe_owns_its_lru_and_compressor() {
    let catalog = Arc::new(DictionaryCatalog::new());
    let dictionary_id = DictionaryId::new(42);
    catalog
        .publish(
            CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4)),
            dictionary_id,
            Arc::from(&b"repeated clickhouse exception service context"[..]),
        )
        .expect("dictionary publishes");
    let config = StripeConfig {
        target_block_bytes: 1,
        dictionary_cache_bytes: 128,
        compression_level: 1,
        compression_locality: CompressionLocalityConfig::default(),
    };
    let mut first =
        LogStripe::with_dictionary_catalog(ShardId::new(7), config.clone(), Arc::clone(&catalog))
            .expect("first stripe opens");
    let mut second =
        LogStripe::with_dictionary_catalog(ShardId::new(8), config, Arc::clone(&catalog))
            .expect("second stripe opens");

    first
        .apply_durable(record_on(ShardId::new(7), 0, "repeated exception"))
        .expect("first stripe indexes");
    second
        .apply_durable(record_on(ShardId::new(8), 0, "repeated exception"))
        .expect("second stripe indexes");

    let first_payload = first
        .dictionary_cache_mut()
        .get(dictionary_id)
        .expect("first stripe caches dictionary");
    let second_payload = second
        .dictionary_cache_mut()
        .get(dictionary_id)
        .expect("second stripe caches dictionary");
    assert!(Arc::ptr_eq(&first_payload, &second_payload));
    assert_eq!(first.dictionary_generation(), 1);
    assert_eq!(second.dictionary_generation(), 1);
    assert_eq!(first.catalog().len(), 1);
    assert_eq!(second.catalog().len(), 1);
}

#[test]
fn dictionary_rotation_only_affects_new_active_blocks_after_refresh() {
    let catalog = Arc::new(DictionaryCatalog::new());
    let cohort = CompressionCohortId::new(4);
    let placement_id = CompressionPlacementId::from_source_cohort(cohort);
    let first_dictionary = DictionaryId::new(101);
    let second_dictionary = DictionaryId::new(102);
    catalog
        .publish(
            placement_id,
            first_dictionary,
            Arc::from(&b"first dictionary"[..]),
        )
        .expect("first dictionary publishes");
    let mut stripe = LogStripe::with_dictionary_catalog(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: u64::MAX,
            dictionary_cache_bytes: 128,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig::default(),
        },
        Arc::clone(&catalog),
    )
    .expect("stripe opens");
    stripe
        .apply_durable(record(0, "first message"))
        .expect("first record indexes");

    catalog
        .publish(
            placement_id,
            second_dictionary,
            Arc::from(&b"second dictionary"[..]),
        )
        .expect("second dictionary publishes");
    assert!(
        stripe
            .refresh_dictionary_catalog()
            .expect("stripe refreshes catalog")
    );
    stripe
        .apply_durable(record(1, "second message"))
        .expect("second record indexes");

    let mut dictionary_ids = stripe
        .seal_active_blocks()
        .expect("active blocks seal")
        .into_iter()
        .map(|block| block.dictionary_id.expect("dictionary selected"))
        .collect::<Vec<_>>();
    dictionary_ids.sort_unstable();
    assert_eq!(dictionary_ids, vec![first_dictionary, second_dictionary]);
    assert_eq!(stripe.dictionary_generation(), 2);
}

#[test]
fn realtime_dictionary_publications_are_adopted_by_future_blocks() {
    let catalog = Arc::new(DictionaryCatalog::new());
    let trainer = RealtimeDictionaryTrainer::start(
        crate::RealtimeDictionaryConfig {
            max_block_sample_bytes: 1024,
            training_sample_bytes: 8 * 1024,
            dictionary_bytes: 1024,
            holdout_blocks: 8,
            queue_blocks: 64,
            max_placements: 4,
            min_net_savings_bytes: 1,
            min_net_savings_bps: 1,
            retrain_after_bytes: u64::MAX,
        },
        1,
        Arc::clone(&catalog),
    )
    .expect("trainer starts");
    let placement_id = CompressionPlacementId::from_source_cohort(CompressionCohortId::new(4));
    let observer = trainer.observer();
    for index in 0..16u64 {
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        let mut sample = Vec::with_capacity(1024);
        for _ in 0..512 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            sample.push(state as u8);
        }
        sample.extend_from_slice(format!(" unique suffix {index:020}").as_bytes());
        while sample.len() < 1024 {
            sample.push(index.wrapping_mul(31).wrapping_add(sample.len() as u64) as u8);
        }
        assert!(observer.observe_structural_block(placement_id, sample));
    }
    trainer.flush().expect("trainer flushes");
    assert_eq!(trainer.stats().dictionaries_published, 1);

    let mut stripe = LogStripe::with_realtime_dictionary(
        ShardId::new(7),
        StripeConfig {
            target_block_bytes: 1,
            dictionary_cache_bytes: 4096,
            compression_level: 1,
            compression_locality: CompressionLocalityConfig::default(),
        },
        &trainer,
    )
    .expect("stripe opens");
    let block = stripe
        .apply_durable(record(0, "future block uses the learned dictionary"))
        .expect("record indexes")
        .sealed_blocks
        .into_iter()
        .next()
        .expect("block seals");
    assert!(block.dictionary_id.is_some());
    let payload = stripe
        .catalog()
        .staged_payload(block.block_id)
        .expect("payload staged");
    let dictionary = catalog
        .snapshot()
        .expect("catalog snapshot")
        .dictionary(block.dictionary_id.expect("dictionary id"))
        .expect("dictionary payload");
    let structural = zstd::bulk::Decompressor::with_dictionary(&dictionary)
        .expect("decompressor opens")
        .decompress(
            &payload,
            usize::try_from(block.structural_bytes).expect("size fits"),
        )
        .expect("block decompresses");
    let decoded =
        crate::structural::decode_structural_block(&structural).expect("block reconstructs");
    assert_eq!(
        decoded[0].message.as_ref(),
        "future block uses the learned dictionary"
    );
}

#[test]
fn otlp_export_is_decoded_and_published_on_the_owning_stripe() {
    let export = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_attribute("service.name", "billing")],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: 9,
                    observed_time_unix_nano: 0,
                    severity_number: 17,
                    severity_text: "ERROR".into(),
                    body: Some(AnyValue {
                        value: Some(Value::StringValue("card declined".into())),
                    }),
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: Vec::new(),
                    span_id: Vec::new(),
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    let mut stripe =
        LogStripe::new(ShardId::new(7), StripeConfig::default()).expect("stripe opens");
    let events = OtlpLogDecoder
        .decode(&export.encode_to_vec())
        .expect("OTLP export decodes before append");
    let receipts = stripe
        .apply_otlp_events(partition(), LogicalOffset::new(0), events)
        .expect("OTLP events index after append");
    assert_eq!(receipts.len(), 1);
    assert_eq!(
        stripe.query(
            &LogQuery::new(partition())
                .with_term("declined")
                .with_field("service.name", "billing")
        ),
        vec![LogMatch {
            record: stripe
                .partitions
                .get(&partition())
                .and_then(|partition| partition.record(LogicalOffset::new(0)))
                .expect("record retained")
                .record
                .clone(),
        }]
    );
}
