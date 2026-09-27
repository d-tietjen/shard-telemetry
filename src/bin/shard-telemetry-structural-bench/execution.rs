use super::*;

pub(super) fn run_benchmark(settings: &Settings) -> Result<Benchmark, Box<dyn Error>> {
    let started = Instant::now();
    let (source_start, spans) = build_block_spans(settings)?;
    let span_build_time = started.elapsed();
    if let Some(output_dir) = &settings.output_dir {
        std::fs::create_dir(output_dir)?;
    }
    let spans = Arc::<[BlockSpan]>::from(spans);
    let completed_input = AtomicU64::new(0);
    let mut manifest_entries = Vec::with_capacity(spans.len());
    let dictionary_catalog = settings
        .realtime_dictionary
        .then(|| Arc::new(DictionaryCatalog::new()));
    let dictionary_trainer = dictionary_catalog
        .as_ref()
        .map(|catalog| {
            RealtimeDictionaryTrainer::start(
                RealtimeDictionaryConfig::default(),
                ZSTD_LEVEL,
                Arc::clone(catalog),
            )
        })
        .transpose()?;
    let dictionary_observer = dictionary_trainer
        .as_ref()
        .map(RealtimeDictionaryTrainer::observer);
    let mut benchmark = Benchmark {
        input_bytes: spans
            .iter()
            .map(|span| u64::try_from(span.length).unwrap_or(u64::MAX))
            .sum(),
        leading_discarded_bytes: source_start,
        blocks: u64::try_from(spans.len())?,
        workers: settings.workers,
        ..Benchmark::default()
    };
    let input_file = File::open(&settings.input)?;
    let mapped_input = map_read_only(&input_file)?;
    thread::scope(|scope| -> Result<(), Box<dyn Error>> {
        let mut handles = Vec::with_capacity(settings.workers);
        for worker_id in 0..settings.workers {
            let spans = Arc::clone(&spans);
            let mapped_input = &mapped_input;
            let output_dir = settings.output_dir.clone();
            let completed_input = &completed_input;
            let total_blocks = spans.len();
            let locality_routing = settings.locality_routing;
            let worker_count = settings.workers;
            let block_bytes = settings.block_bytes;
            let persistent_query_index = settings.persistent_query_index;
            let dictionary_catalog = dictionary_catalog.clone();
            let dictionary_observer = dictionary_observer.clone();
            handles.push(scope.spawn(move || -> Result<WorkerResult, String> {
                let mut compressor =
                    BenchmarkCompressor::new(dictionary_catalog, dictionary_observer)?;
                let mut locality = CompressionBlockCollator::new(
                    CompressionLocalityConfig {
                        enabled: locality_routing,
                        ..CompressionLocalityConfig::default()
                    },
                    u64::try_from(block_bytes).map_err(|error| error.to_string())?,
                )
                .map_err(|error| error.to_string())?;
                let mut pack = output_dir
                    .map(|directory| {
                        OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(directory.join(format!("worker-{worker_id:02}.pack")))
                    })
                    .transpose()
                    .map_err(|error| error.to_string())?;
                let mut results = Vec::new();
                let mut index = worker_id;
                while let Some(span) = spans.get(index).copied() {
                    let start = usize::try_from(span.start).map_err(|error| error.to_string())?;
                    let end = start
                        .checked_add(span.length)
                        .ok_or_else(|| "mapped block range overflows".to_owned())?;
                    let raw = mapped_input
                        .get(start..end)
                        .ok_or_else(|| "mapped block range exceeds input".to_owned())?;
                    let (mut result, compressed) = process_block(
                        RawBlock {
                            ordinal: span.ordinal,
                            source_offset: span.start.saturating_sub(source_start),
                            raw,
                            verify: span.ordinal == 0,
                        },
                        &mut compressor,
                        &mut locality,
                        persistent_query_index,
                    )?;
                    result.pack_worker = worker_id;
                    if let Some(pack) = pack.as_mut() {
                        result.pack_offset =
                            pack.stream_position().map_err(|error| error.to_string())?;
                        pack.write_all(&compressed)
                            .map_err(|error| error.to_string())?;
                    }
                    results.push(result);
                    let span_bytes =
                        u64::try_from(span.length).map_err(|error| error.to_string())?;
                    let previous = completed_input.fetch_add(span_bytes, Ordering::Relaxed);
                    let completed = previous.saturating_add(span_bytes);
                    let mut boundary = previous / PROGRESS_BYTES + 1;
                    while boundary.saturating_mul(PROGRESS_BYTES) <= completed {
                        eprintln!(
                            "progress: {} GiB complete, {total_blocks} blocks total",
                            boundary
                        );
                        boundary = boundary.saturating_add(1);
                    }
                    index = index.saturating_add(worker_count);
                }
                if let Some(pack) = pack.as_mut() {
                    pack.sync_all().map_err(|error| error.to_string())?;
                }
                Ok(WorkerResult {
                    blocks: results,
                    locality: locality.stats(),
                })
            }));
        }

        for handle in handles {
            let worker = handle.join().map_err(|_| "benchmark worker panicked")??;
            merge_locality_stats(&mut benchmark.locality, worker.locality);
            for result in worker.blocks {
                benchmark.source_bytes = benchmark.source_bytes.saturating_add(result.source_bytes);
                benchmark.rejected_records = benchmark
                    .rejected_records
                    .saturating_add(result.rejected_records);
                benchmark.rejected_bytes = benchmark
                    .rejected_bytes
                    .saturating_add(result.rejected_bytes);
                benchmark.structural_bytes = benchmark
                    .structural_bytes
                    .saturating_add(result.structural_bytes);
                benchmark.embedded_index_bytes = benchmark
                    .embedded_index_bytes
                    .saturating_add(result.embedded_index_bytes);
                benchmark.records = benchmark.records.saturating_add(result.record_count);
                benchmark.structural_stored_bytes = benchmark
                    .structural_stored_bytes
                    .saturating_add(result.structural_stored_bytes);
                benchmark.structural_compression_time += result.structural_compression_time;
                manifest_entries.push(result);
            }
        }
        Ok(())
    })?;
    if let Some(trainer) = &dictionary_trainer {
        trainer.flush()?;
        benchmark.dictionary_stats = trainer.stats();
    }
    if let Some(output_dir) = &settings.output_dir {
        if settings.persistent_query_index {
            benchmark.query_index_bytes = write_query_index(output_dir, &mut manifest_entries)?;
        }
        if let Some(catalog) = &dictionary_catalog {
            let snapshot = catalog.snapshot()?;
            benchmark.dictionary_bytes = write_dictionaries(output_dir, &snapshot)?;
        }
        benchmark.manifest_bytes = write_manifest(output_dir, &mut manifest_entries)?;
        if settings.realtime_dictionary {
            benchmark.manifest_bytes =
                benchmark
                    .manifest_bytes
                    .saturating_add(write_dictionary_assignments(
                        output_dir,
                        &mut manifest_entries,
                    )?);
        }
    } else {
        if let Some(catalog) = &dictionary_catalog {
            benchmark.dictionary_bytes = catalog
                .snapshot()?
                .dictionaries()
                .map(|(_, payload)| u64::try_from(payload.len()).unwrap_or(u64::MAX))
                .sum();
        }
        if settings.persistent_query_index {
            benchmark.query_index_bytes =
                u64::try_from(encode_query_index(&mut manifest_entries)?.len())?;
        }
    }
    benchmark.elapsed = started.elapsed();
    if let Some(output_dir) = &settings.output_dir {
        let verification_started = Instant::now();
        benchmark.verified_blocks = verify_durable_output(output_dir)?;
        benchmark.verification_elapsed = verification_started.elapsed();
    }
    eprintln!(
        "span directory: {} blocks in {:.3} seconds",
        benchmark.blocks,
        span_build_time.as_secs_f64()
    );
    Ok(benchmark)
}

#[allow(unsafe_code)]
pub(super) fn map_read_only(file: &File) -> std::io::Result<Mmap> {
    // SAFETY: the benchmark corpus is immutable for the complete run. The map
    // is read-only, remains owned until every scoped worker exits, and no code
    // in this process can resize or mutate the underlying file.
    unsafe { MmapOptions::new().map(file) }
}

pub(super) fn build_block_spans(
    settings: &Settings,
) -> Result<(u64, Vec<BlockSpan>), Box<dyn Error>> {
    let file = File::open(&settings.input)?;
    let file_bytes = file.metadata()?.len();
    let first_line_end =
        find_newline_forward(&file, 0, file_bytes)?.ok_or("input contains no complete line")?;
    let mut first_line = vec![0; usize::try_from(first_line_end)?];
    file.read_exact_at(&mut first_line, 0)?;
    let source_start = if serde_json::from_slice::<DockerJsonLine<'_>>(&first_line).is_ok() {
        0
    } else {
        first_line_end
    };
    let nominal_end = source_start
        .saturating_add(settings.limit_bytes)
        .min(file_bytes);
    let source_end = find_newline_backward(&file, nominal_end, source_start)?
        .ok_or("input contains no complete Docker JSON records")?;
    if source_end <= source_start {
        return Err("input contained no complete Docker JSON log records".into());
    }
    let mut spans = Vec::new();
    let mut start = source_start;
    let mut nominal = source_start.saturating_add(u64::try_from(settings.block_bytes)?);
    while nominal < source_end {
        let boundary = find_newline_forward(&file, nominal, source_end)?.unwrap_or(source_end);
        if boundary > start {
            spans.push(BlockSpan {
                ordinal: spans.len(),
                start,
                length: usize::try_from(boundary - start)?,
            });
            start = boundary;
        }
        nominal = nominal.saturating_add(u64::try_from(settings.block_bytes)?);
    }
    if start < source_end {
        spans.push(BlockSpan {
            ordinal: spans.len(),
            start,
            length: usize::try_from(source_end - start)?,
        });
    }
    Ok((source_start, spans))
}

pub(super) fn find_newline_forward(
    file: &File,
    mut offset: u64,
    limit: u64,
) -> Result<Option<u64>, Box<dyn Error>> {
    let mut buffer = [0u8; 4096];
    while offset < limit {
        let length = usize::try_from((limit - offset).min(buffer.len() as u64))?;
        let read = file.read_at(&mut buffer[..length], offset)?;
        if read == 0 {
            break;
        }
        if let Some(position) = buffer[..read].iter().position(|byte| *byte == b'\n') {
            return Ok(Some(offset + u64::try_from(position)? + 1));
        }
        offset = offset.saturating_add(u64::try_from(read)?);
    }
    Ok(None)
}

pub(super) fn find_newline_backward(
    file: &File,
    mut end: u64,
    lower_bound: u64,
) -> Result<Option<u64>, Box<dyn Error>> {
    let mut buffer = [0u8; 4096];
    while end > lower_bound {
        let start = end.saturating_sub(buffer.len() as u64).max(lower_bound);
        let length = usize::try_from(end - start)?;
        file.read_exact_at(&mut buffer[..length], start)?;
        if let Some(position) = buffer[..length].iter().rposition(|byte| *byte == b'\n') {
            return Ok(Some(start + u64::try_from(position)? + 1));
        }
        end = start;
    }
    Ok(None)
}
