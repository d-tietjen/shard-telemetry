use super::*;

pub(super) fn render_report(settings: &Settings, benchmark: &Benchmark) -> String {
    let mut report = String::from("shard-telemetry Docker JSON structural benchmark\n");
    report.push_str(&format!("input: {}\n", settings.input.display()));
    if let Some(output_dir) = &settings.output_dir {
        report.push_str(&format!("output directory: {}\n", output_dir.display()));
    }
    report.push_str(&format!(
        "complete-line input bytes: {}\n",
        benchmark.input_bytes
    ));
    report.push_str(&format!(
        "leading partial bytes discarded: {}\n",
        benchmark.leading_discarded_bytes
    ));
    report.push_str(&format!("source bytes: {}\n", benchmark.source_bytes));
    report.push_str(&format!(
        "rejected complete records: {}\n",
        benchmark.rejected_records
    ));
    report.push_str(&format!(
        "rejected complete-record bytes: {}\n",
        benchmark.rejected_bytes
    ));
    report.push_str(&format!("block target: {}\n", settings.block_bytes));
    report.push_str(&format!("workers: {}\n", benchmark.workers));
    report.push_str(&format!(
        "locality routing: {}\n",
        if settings.locality_routing {
            "enabled"
        } else {
            "disabled"
        }
    ));
    report.push_str(&format!(
        "real-time dictionary: {}\n",
        if settings.realtime_dictionary {
            "enabled"
        } else {
            "disabled"
        }
    ));
    report.push_str(&format!(
        "persistent query index: {}\n",
        if settings.persistent_query_index {
            "enabled"
        } else {
            "disabled"
        }
    ));
    report.push_str(&format!("blocks: {}\n", benchmark.blocks));
    report.push_str(&format!("records: {}\n", benchmark.records));
    report.push_str(&format!(
        "structural payload before zstd: {}\n",
        storage_line(benchmark.structural_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!(
        "embedded compression-derived index before zstd: {}\n",
        storage_line(benchmark.embedded_index_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!(
        "structural payload zstd-1: {}\n",
        storage_line(benchmark.structural_stored_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!("manifest bytes: {}\n", benchmark.manifest_bytes));
    report.push_str(&format!(
        "persistent term/field index bytes: {}\n",
        benchmark.query_index_bytes
    ));
    report.push_str(&format!(
        "compression dictionary bytes: {}\n",
        benchmark.dictionary_bytes
    ));
    let durable_bytes = benchmark
        .structural_stored_bytes
        .saturating_add(benchmark.manifest_bytes)
        .saturating_add(benchmark.query_index_bytes)
        .saturating_add(benchmark.dictionary_bytes);
    report.push_str(&format!(
        "durable pack plus manifest: {}\n",
        storage_line(durable_bytes, benchmark.source_bytes)
    ));
    report.push_str(&format!(
        "structural zstd-1 CPU-time throughput: {:.2} MiB/s\n",
        throughput_mib(
            benchmark.structural_bytes,
            benchmark.structural_compression_time
        )
    ));
    report.push_str(&format!(
        "durable end-to-end ingest throughput: {:.2} MiB/s\n",
        throughput_mib(benchmark.source_bytes, benchmark.elapsed)
    ));
    report.push_str(&format!(
        "ingest elapsed seconds: {:.6}\n",
        benchmark.elapsed.as_secs_f64()
    ));
    report.push_str(&format!(
        "temperature placement distribution collated/base: {}/{}\n",
        benchmark.locality.collated_placements, benchmark.locality.base_placements
    ));
    report.push_str(&format!(
        "locality fallback rate: {:.6}\n",
        benchmark.locality.base_placements as f64 / benchmark.locality.observations.max(1) as f64
    ));
    report.push_str(&format!(
        "blocks scored/split/sub-blocks: {}/{}/{}\n",
        benchmark.locality.blocks_scored,
        benchmark.locality.blocks_split,
        benchmark.locality.subblocks_created
    ));
    report.push_str(&format!(
        "split explorations suppressed: {}\n",
        benchmark.locality.split_explorations_suppressed
    ));
    report.push_str(&format!(
        "reassigned records/bytes: {}/{}\n",
        benchmark.locality.records_reassigned, benchmark.locality.bytes_reassigned
    ));
    report.push_str(&format!(
        "active compression shards: {}\n",
        benchmark.locality.active_compression_shards
    ));
    report.push_str(&format!(
        "maximum internal variance Q8: {}\n",
        benchmark.locality.max_internal_variance_q8
    ));
    report.push_str(&format!(
        "bytes-handoff membership bytes: {}\n",
        benchmark.locality.handoff_membership_bytes
    ));
    report.push_str(&format!(
        "collator state bytes across workers: {}\n",
        benchmark.locality.allocated_state_bytes
    ));
    if settings.realtime_dictionary {
        let stats = benchmark.dictionary_stats;
        report.push_str(&format!(
            "dictionary observed/sampled/dropped blocks: {}/{}/{}\n",
            stats.observed_blocks,
            stats.observed_blocks.saturating_sub(stats.dropped_blocks),
            stats.dropped_blocks
        ));
        report.push_str(&format!(
            "dictionary placement budget rejections/max tracked: {}/{}\n",
            stats.placement_budget_rejections, stats.max_tracked_placements
        ));
        report.push_str(&format!(
            "dictionary observed/sampled bytes: {}/{}\n",
            stats.observed_bytes, stats.sampled_bytes
        ));
        report.push_str(&format!(
            "dictionary training runs/failures/rejections/publications: {}/{}/{}/{}\n",
            stats.training_runs,
            stats.training_failures,
            stats.candidates_rejected,
            stats.dictionaries_published
        ));
        report.push_str(&format!(
            "dictionary holdout baseline/candidate bytes: {}/{}\n",
            stats.holdout_baseline_bytes, stats.holdout_candidate_bytes
        ));
        report.push_str(&format!(
            "dictionary training/evaluation seconds: {:.6}/{:.6}\n",
            stats.training_nanos as f64 / 1_000_000_000.0,
            stats.evaluation_nanos as f64 / 1_000_000_000.0
        ));
    }
    if benchmark.verified_blocks > 0 {
        report.push_str(&format!(
            "post-ingest verification: {} block checksums plus first/middle/last decode in {:.3} seconds\n",
            benchmark.verified_blocks,
            benchmark.verification_elapsed.as_secs_f64()
        ));
    }
    report.push_str(
        "note: elapsed time includes deterministic line-boundary discovery, Docker JSON parsing, structural encoding, zstd-1 compression, pack writes, sync_all, manifest creation, and manifest sync.\n",
    );
    report.push_str(
        "note: post-ingest checksum and sampled decode verification is intentionally excluded from ingest throughput.\n",
    );
    report.push_str(
        "note: Docker JSON is normalized into body, RFC3339 timestamp, and docker.stream before structural encoding; the durable representation retains those typed semantics but not JSON wrapper bytes.\n",
    );
    report
}

pub(super) fn storage_line(stored_bytes: u64, source_bytes: u64) -> String {
    let ratio = source_bytes as f64 / stored_bytes.max(1) as f64;
    let retained = stored_bytes as f64 * 100.0 / source_bytes.max(1) as f64;
    format!("{stored_bytes} bytes ({ratio:.2}x, {retained:.2}% of source)")
}

pub(super) fn throughput_mib(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64().max(f64::MIN_POSITIVE)
}
