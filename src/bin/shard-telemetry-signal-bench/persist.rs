use super::*;

pub(super) fn persist_server_store(
    corpus: &Corpus,
    data_directory: &Path,
    shard_count: usize,
    partition_count: usize,
    append_linger_micros: u64,
    recovery_journal: bool,
) -> Result<(DurableTelemetryStore, EmbeddedPhases), Box<dyn std::error::Error>> {
    if data_directory.exists() {
        return Err(format!(
            "server data directory already exists: {}",
            data_directory.display()
        )
        .into());
    }
    let open_started = Instant::now();
    let store = DurableTelemetryStore::open(server_store_config(
        data_directory,
        shard_count,
        partition_count,
        append_linger_micros,
        recovery_journal,
    )?)?;
    let open = open_started.elapsed();
    let open_resident_kib = resident_set_kib().unwrap_or_default();

    let router = TelemetryRouter::new(
        NonZeroU16::new(u16::try_from(partition_count)?).expect("validated nonzero partitions"),
    );
    let logs_started = Instant::now();
    let mut log_partitions = BTreeMap::<TopicPartition, Vec<OtlpLogEvent>>::new();
    for record in &corpus.durable_logs {
        let resource_id = record.resource_id().get().to_le_bytes();
        log_partitions
            .entry(router.log(TENANT, record.trace_id, &resource_id))
            .or_default()
            .push(OtlpLogEvent {
                timestamp_unix_nanos: record.timestamp_unix_nanos,
                observed_timestamp_unix_nanos: record.observed_timestamp_unix_nanos,
                body: record.body.clone(),
                message: Arc::clone(&record.message),
                fields: Arc::clone(&record.fields),
                attributes: Arc::clone(&record.attributes),
                resource: Arc::clone(&record.resource),
                scope: Arc::clone(&record.scope),
                severity_number: record.severity_number,
                severity_text: Arc::clone(&record.severity_text),
                dropped_attributes_count: record.dropped_attributes_count,
                flags: record.flags,
                trace_id: record.trace_id,
                span_id: record.span_id,
                event_name: Arc::clone(&record.event_name),
                compression_cohort: record.compression_cohort,
            });
    }
    let mut log_appends = log_partitions
        .into_par_iter()
        .map(|(topic_partition, events)| {
            let (envelope, transient_context) =
                shard_telemetry::prepare_log_envelope_with_context(TENANT, &events)?;
            Ok(NativePartitionAppend {
                topic_partition,
                envelope,
                transient_context: Some(transient_context),
            })
        })
        .collect::<shard_telemetry::TelemetryResult<Vec<_>>>()?;
    log_appends.sort_unstable_by_key(|append| append.topic_partition);
    store.append_telemetry_batch(
        &NativeTelemetryBatch {
            partitions: log_appends,
        },
        true,
    )?;
    let logs = logs_started.elapsed();
    let logs_resident_kib = resident_set_kib().unwrap_or_default();

    let traces_started = Instant::now();
    let mut trace_partitions = BTreeMap::<TopicPartition, Vec<DurableSpan>>::new();
    for span in &corpus.spans {
        trace_partitions
            .entry(router.trace(TENANT, span.trace_id))
            .or_default()
            .push(span.clone());
    }
    let trace_append_groups = trace_partitions
        .into_par_iter()
        .map(|(trace_partition, partition_records)| {
            partition_records
                .chunks(32_768)
                .map(|chunk| {
                    let mut records = chunk.to_vec();
                    for (ordinal, record) in records.iter_mut().enumerate() {
                        record.stream_shard_id = ShardId::new(0);
                        record.record_ref = TelemetryRecordRef::for_signal(
                            TelemetrySignal::Traces,
                            trace_partition,
                            LogicalOffset::new(
                                u64::try_from(ordinal)
                                    .map_err(|_| shard_telemetry::TelemetryError::RecordTooLarge)?,
                            ),
                        );
                    }
                    let payload = encode_trace_block(&records)?;
                    let envelope = TelemetryEnvelope::new(
                        TelemetrySignal::Traces,
                        TENANT,
                        u32::try_from(records.len())
                            .map_err(|_| shard_telemetry::TelemetryError::RecordTooLarge)?,
                        trace_partition.partition_id.get().to_le_bytes().as_slice(),
                        Arc::<[u8]>::from(payload),
                    )?;
                    Ok(NativePartitionAppend {
                        topic_partition: trace_partition,
                        envelope,
                        transient_context: None,
                    })
                })
                .collect::<shard_telemetry::TelemetryResult<Vec<_>>>()
        })
        .collect::<shard_telemetry::TelemetryResult<Vec<_>>>()?;
    let mut trace_appends = trace_append_groups
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    trace_appends.sort_unstable_by_key(|append| append.topic_partition);
    store.append_telemetry_batch(
        &NativeTelemetryBatch {
            partitions: trace_appends,
        },
        true,
    )?;
    let traces = traces_started.elapsed();
    let traces_resident_kib = resident_set_kib().unwrap_or_default();

    let metrics_started = Instant::now();
    let mut series =
        BTreeMap::<(TopicPartition, SeriesFingerprint), Vec<DurableMetricPoint>>::new();
    for point in &corpus.points {
        let fingerprint = point.series_fingerprint();
        series
            .entry((router.metric(TENANT, fingerprint), fingerprint))
            .or_default()
            .push(point.clone());
    }
    let mut metric_appends = series
        .into_par_iter()
        .map(|((metric_partition, fingerprint), mut records)| {
            for (ordinal, record) in records.iter_mut().enumerate() {
                record.stream_shard_id = ShardId::new(0);
                record.record_ref = TelemetryRecordRef::for_signal(
                    TelemetrySignal::Metrics,
                    metric_partition,
                    LogicalOffset::new(
                        u64::try_from(ordinal)
                            .map_err(|_| shard_telemetry::TelemetryError::RecordTooLarge)?,
                    ),
                );
            }
            let payload = encode_metric_chunk(&records)?;
            let mut routing_metadata = [0_u8; 5];
            routing_metadata[..4]
                .copy_from_slice(&metric_partition.partition_id.get().to_le_bytes());
            routing_metadata[4] = 1;
            let envelope = TelemetryEnvelope::new(
                TelemetrySignal::Metrics,
                TENANT,
                u32::try_from(records.len())
                    .map_err(|_| shard_telemetry::TelemetryError::RecordTooLarge)?,
                routing_metadata.as_slice(),
                Arc::<[u8]>::from(payload),
            )?;
            Ok((
                metric_partition,
                fingerprint,
                NativePartitionAppend {
                    topic_partition: metric_partition,
                    envelope,
                    transient_context: None,
                },
            ))
        })
        .collect::<shard_telemetry::TelemetryResult<Vec<_>>>()?;
    metric_appends.sort_unstable_by_key(|(partition, fingerprint, _)| (*partition, *fingerprint));
    let mut metric_rounds = Vec::<(BTreeSet<TopicPartition>, Vec<NativePartitionAppend>)>::new();
    for (metric_partition, _, append) in metric_appends {
        if let Some((partitions, appends)) = metric_rounds
            .iter_mut()
            .find(|(partitions, _)| !partitions.contains(&metric_partition))
        {
            partitions.insert(metric_partition);
            appends.push(append);
        } else {
            metric_rounds.push((BTreeSet::from([metric_partition]), vec![append]));
        }
    }
    let metric_batches = metric_rounds.len();
    for (_, partitions) in metric_rounds {
        store.append_telemetry_batch(&NativeTelemetryBatch { partitions }, true)?;
    }
    let metrics = metrics_started.elapsed();
    let metrics_resident_kib = resident_set_kib().unwrap_or_default();
    Ok((
        store,
        EmbeddedPhases {
            open,
            logs,
            traces,
            metrics,
            metric_batches,
            open_resident_kib,
            logs_resident_kib,
            traces_resident_kib,
            metrics_resident_kib,
        },
    ))
}

pub(super) fn server_store_config(
    data_directory: &Path,
    shard_count: usize,
    partition_count: usize,
    append_linger_micros: u64,
    recovery_journal: bool,
) -> Result<DurableTelemetryConfig, Box<dyn std::error::Error>> {
    Ok(DurableTelemetryConfig {
        data_directory: data_directory.to_path_buf(),
        object_store_directory: None,
        s3_object_store: None,
        recovery_journal,
        retention: None,
        shard_count: u32::try_from(shard_count)?,
        tenant_partitions: u32::try_from(partition_count)?,
        append_linger: Duration::from_micros(append_linger_micros),
        stripe: StripeConfig::default(),
        indexed_ack_timeout: Duration::from_secs(300),
    })
}
