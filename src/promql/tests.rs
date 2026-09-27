use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use crate::{DurableTelemetryConfig, StripeConfig};

fn sample(labels: &[(&str, &str)], value: f64) -> PromqlSample {
    PromqlSample {
        labels: labels
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        timestamp_ms: 1,
        value,
    }
}

fn binary(expression: &str) -> BinaryExpr {
    match parse(expression).unwrap() {
        Expr::Binary(binary) => binary,
        _ => panic!("expected binary expression"),
    }
}

#[test]
fn comparisons_filter_unless_bool_is_requested() {
    assert_eq!(binary_float(">", 2.0, 1.0, false).unwrap(), Some(2.0));
    assert_eq!(binary_float(">", 0.0, 1.0, false).unwrap(), None);
    assert_eq!(binary_float(">", 0.0, 1.0, true).unwrap(), Some(0.0));
}

#[test]
fn counter_rate_accounts_for_resets() {
    let samples = vec![(0, 8.0), (1_000, 10.0), (2_000, 2.0), (3_000, 4.0)];
    assert_eq!(counter_rate(&samples, false), 2.0);
    assert_eq!(counter_rate(&samples, true), 2.0);
}

#[test]
fn subqueries_execute_through_the_bounded_evaluator() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-promql-subquery-{}-{nonce}",
        std::process::id()
    ));
    let store = Arc::new(
        DurableTelemetryStore::open(DurableTelemetryConfig {
            data_directory: directory.clone(),
            object_store_directory: None,
            s3_object_store: None,
            recovery_journal: true,
            retention: None,
            shard_count: 1,
            tenant_partitions: 1,
            append_linger: Duration::ZERO,
            stripe: StripeConfig::default(),
            indexed_ack_timeout: Duration::from_secs(30),
        })
        .unwrap(),
    );
    let engine = PromqlEngine::new(store, Arc::from("tenant"), PromqlLimits::default());
    assert_eq!(
        engine.query("up[5m:1m]", 600_000).unwrap(),
        PromqlValue::Matrix(Vec::new())
    );
    drop(engine);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn vector_matching_honors_group_left_and_included_labels() {
    let expression = binary("left * on(job) group_left(zone) right");
    let left = vec![
        sample(
            &[("__name__", "left"), ("job", "api"), ("instance", "a")],
            2.0,
        ),
        sample(
            &[("__name__", "left"), ("job", "api"), ("instance", "b")],
            3.0,
        ),
    ];
    let right = vec![sample(
        &[("__name__", "right"), ("job", "api"), ("zone", "west")],
        4.0,
    )];
    let output = binary_vectors(left, right, &expression, "*").unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].value, 8.0);
    assert_eq!(output[1].value, 12.0);
    assert_eq!(
        output[0].labels.get("zone").map(String::as_str),
        Some("west")
    );
    assert!(!output[0].labels.contains_key("__name__"));
}

#[test]
fn vector_set_operators_keep_prometheus_side_semantics() {
    let left = vec![
        sample(&[("job", "api")], 1.0),
        sample(&[("job", "worker")], 2.0),
    ];
    let right = vec![
        sample(&[("job", "api")], 10.0),
        sample(&[("job", "db")], 30.0),
    ];
    let and = binary("left and on(job) right");
    assert_eq!(
        binary_vectors(left.clone(), right.clone(), &and, "and")
            .unwrap()
            .into_iter()
            .map(|sample| sample.value)
            .collect::<Vec<_>>(),
        vec![1.0]
    );
    let or = binary("left or on(job) right");
    assert_eq!(
        binary_vectors(left, right, &or, "or")
            .unwrap()
            .into_iter()
            .map(|sample| sample.value)
            .collect::<Vec<_>>(),
        vec![1.0, 2.0, 30.0]
    );
}
