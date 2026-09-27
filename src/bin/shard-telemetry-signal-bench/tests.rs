use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

#[test]
fn generated_metric_corpus_has_128_distinct_series() {
    let corpus = Corpus::generate(256).unwrap();
    let mut counts = BTreeMap::<SeriesFingerprint, usize>::new();
    for point in &corpus.points {
        *counts.entry(point.series_fingerprint()).or_default() += 1;
    }
    assert_eq!(counts.len(), 128);
    assert!(counts.values().all(|count| *count == 2));
}

#[test]
fn clickhouse_export_records_exact_lookup_inputs() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let output = std::env::temp_dir().join(format!(
        "shard-telemetry-clickhouse-export-{}-{unique}",
        std::process::id()
    ));
    let corpus = Corpus::generate(128).unwrap();
    export_clickhouse_corpus(&corpus, &output).unwrap();

    let manifest = fs::read_to_string(output.join("manifest.env")).unwrap();
    assert!(manifest.contains("records_per_signal=128\n"));
    assert!(manifest.contains("trace_lookup_rows=8\n"));
    assert!(manifest.contains("metric_lookup_rows=1\n"));
    let series_id = manifest
        .lines()
        .find_map(|line| line.strip_prefix("series_id="))
        .unwrap()
        .parse::<u128>()
        .unwrap();
    let series_id_hex = manifest
        .lines()
        .find_map(|line| line.strip_prefix("series_id_hex="))
        .unwrap();
    assert_eq!(series_id_hex, format!("{series_id:032x}"));
    assert!(fs::metadata(output.join("traces.rowbinary")).unwrap().len() > 0);
    assert!(
        fs::metadata(output.join("metrics.rowbinary"))
            .unwrap()
            .len()
            > 0
    );
    assert!(
        fs::metadata(output.join("trace-lookup-expected.rowbinary"))
            .unwrap()
            .len()
            > 0
    );
    assert!(
        fs::metadata(output.join("metric-lookup-expected.rowbinary"))
            .unwrap()
            .len()
            > 0
    );
    fs::remove_dir_all(output).unwrap();
}
