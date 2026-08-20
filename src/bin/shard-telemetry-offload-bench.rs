use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use shard_stream_core::{LogicalPartitionId, TopicPartition};
use shard_telemetry::{
    DurableTelemetryConfig, DurableTelemetryStore, LOGS_TOPIC_ID, LokiEntry, NativeClientConfig,
    NativePartitionAppend, NativeServerConfig, NativeTelemetryBatch, ShardTelemetryClient,
    StripeConfig, UpstreamOffloadConfig, UpstreamOffloader, prepare_loki_log_envelope,
    serve_native,
};
use tokio::net::TcpListener;

const TENANT: &str = "offload-benchmark";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut partitions = 4_usize;
    let mut batches_per_partition = 64_usize;
    let mut connections = 4_usize;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--partitions" => partitions = parse_usize(args.next(), "--partitions")?,
            "--batches-per-partition" => {
                batches_per_partition = parse_usize(args.next(), "--batches-per-partition")?;
            }
            "--connections" => connections = parse_usize(args.next(), "--connections")?,
            _ => return Err(format!("unknown argument {argument}").into()),
        }
    }
    if partitions == 0 || partitions > 256 {
        return Err("--partitions must be in 1..=256".into());
    }
    if batches_per_partition == 0 || connections == 0 {
        return Err("--batches-per-partition and --connections must be nonzero".into());
    }

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "shard-telemetry-offload-bench-{}-{nonce}",
        std::process::id()
    ));
    let source_directory = root.join("source");
    let destination_directory = root.join("destination");
    let tenant_partitions = u32::try_from(partitions)?;
    let store_config = |data_directory| DurableTelemetryConfig {
        data_directory,
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal: false,
        retention: None,
        shard_count: tenant_partitions,
        tenant_partitions,
        append_linger: Duration::from_micros(250),
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(30),
    };
    let source = Arc::new(DurableTelemetryStore::open(store_config(
        source_directory.clone(),
    ))?);
    let destination = Arc::new(DurableTelemetryStore::open(store_config(
        destination_directory,
    ))?);
    populate_source(source.as_ref(), partitions, batches_per_partition)?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server_destination = Arc::clone(&destination);
    let server = tokio::spawn(async move {
        serve_native(
            listener,
            server_destination,
            NativeServerConfig {
                wait_for_index: false,
                ..NativeServerConfig::default()
            },
            async {
                let _ = stopped.await;
            },
        )
        .await
    });
    let client = Arc::new(ShardTelemetryClient::new(
        NativeClientConfig::new(address).with_max_connections(connections),
    )?);
    let offloader = UpstreamOffloader::open(
        source,
        client,
        UpstreamOffloadConfig::new(source_directory.join("offload-v1.json"), "benchmark-node")
            .with_max_in_flight_partitions(connections),
    )?;

    let started = Instant::now();
    let report = offloader.offload_once().await?;
    let elapsed = started.elapsed();
    let expected = partitions.saturating_mul(batches_per_partition);
    let expected_records = u64::try_from(expected)?;
    if report.offloaded_batches != expected || report.offloaded_records != expected_records {
        return Err(format!(
            "offload result mismatch: expected {expected} records and batches, got {} records and {} batches",
            report.offloaded_records, report.offloaded_batches
        )
        .into());
    }
    let seconds = elapsed.as_secs_f64();
    let batches_per_second = expected as f64 / seconds;
    println!("ShardTelemetry upstream offload benchmark (v1)");
    println!(
        "partitions={partitions} batches_per_partition={batches_per_partition} connections={connections}"
    );
    println!(
        "offloaded_records={} offloaded_batches={} checkpoint_writes={} elapsed_seconds={seconds:.6} batches_per_second={batches_per_second:.2}",
        report.offloaded_records, report.offloaded_batches, report.checkpoint_writes,
    );

    stop.send(())
        .map_err(|_| "native benchmark server stopped early")?;
    server.await??;
    drop(destination);
    std::fs::remove_dir_all(root)?;
    Ok(())
}

fn populate_source(
    store: &DurableTelemetryStore,
    partitions: usize,
    batches_per_partition: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    for partition in 0..partitions {
        let topic_partition = TopicPartition::new(
            LOGS_TOPIC_ID,
            LogicalPartitionId::new(u32::try_from(partition)?),
        );
        for sequence in 0..batches_per_partition {
            let entry = LokiEntry {
                timestamp_unix_nanos: i64::try_from(sequence)?,
                labels: BTreeMap::from([("benchmark".to_owned(), "offload".to_owned())]),
                line: format!("partition={partition} sequence={sequence}"),
                structured_metadata: BTreeMap::new(),
            };
            store.append_telemetry_batch(
                &NativeTelemetryBatch {
                    partitions: vec![NativePartitionAppend {
                        topic_partition,
                        envelope: prepare_loki_log_envelope(TENANT, vec![entry])?,
                        transient_context: None,
                    }],
                },
                true,
            )?;
        }
    }
    Ok(())
}

fn parse_usize(value: Option<String>, argument: &str) -> Result<usize, Box<dyn std::error::Error>> {
    value
        .ok_or_else(|| format!("missing value for {argument}"))?
        .parse()
        .map_err(|error| format!("invalid {argument}: {error}").into())
}
