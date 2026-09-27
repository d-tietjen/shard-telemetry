use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn parses_docker_rfc3339_nanos() {
    assert_eq!(
        parse_docker_timestamp("1970-01-01T00:00:00.123Z").expect("timestamp parses"),
        123_000_000
    );
    assert_eq!(
        parse_docker_timestamp("2024-02-29T01:02:03.123456789Z")
            .expect("leap-day timestamp parses"),
        1_709_168_523_123_456_789
    );
}

#[test]
fn canonical_docker_parser_reuses_exact_decoded_messages() {
    let first = br#"{"log":"quoted \"value\" and newline\n","stream":"stderr","time":"2024-02-29T01:02:03.123456789Z"}"#;
    let second = br#"{"log":"quoted \"value\" and newline\n","stream":"stderr","time":"2024-02-29T01:02:04.123456789Z"}"#;
    let mut cache = DockerMessageCache::new();
    let mut timestamp_cache = DockerTimestampPrefixCache::default();
    let (first_message, first_stream, first_timestamp) =
        parse_canonical_docker_json(first, &mut cache, &mut timestamp_cache)
            .expect("first line uses fast path");
    let (second_message, second_stream, second_timestamp) =
        parse_canonical_docker_json(second, &mut cache, &mut timestamp_cache)
            .expect("second line uses fast path");
    assert_eq!(first_message.as_ref(), "quoted \"value\" and newline\n");
    assert_eq!(first_stream, "stderr");
    assert_eq!(second_stream, "stderr");
    assert_eq!(second_timestamp - first_timestamp, 1_000_000_000);
    assert!(Rc::ptr_eq(&first_message, &second_message));
}

#[test]
fn unicode_json_escapes_use_the_cached_fast_path() {
    let line = br#"{"log":"snowman \u2603 rocket \uD83D\uDE80\n","stream":"stdout","time":"2024-02-29T01:02:03.123456789Z"}"#;
    let mut cache = DockerMessageCache::new();
    let mut timestamp_cache = DockerTimestampPrefixCache::default();
    let (message, stream, _) = parse_canonical_docker_json(line, &mut cache, &mut timestamp_cache)
        .expect("Unicode line uses fast path");
    let parsed: DockerJsonLine<'_> = serde_json::from_slice(line).expect("serde parses line");
    assert_eq!(message.as_ref(), parsed.log);
    assert_eq!(message.as_ref(), "snowman \u{2603} rocket \u{1f680}\n");
    assert_eq!(stream, "stdout");
}

#[test]
fn timestamp_parser_handles_fraction_widths_and_date_rollover() {
    let before_midnight =
        parse_ascii_docker_timestamp(b"2024-02-29T23:59:59.1Z").expect("first timestamp parses");
    let after_midnight = parse_ascii_docker_timestamp(b"2024-03-01T00:00:00.000000001Z")
        .expect("rollover timestamp parses");
    assert_eq!(after_midnight - before_midnight, 900_000_001);
    assert_eq!(
        parse_ascii_docker_timestamp(b"2024-03-01T00:00:00Z")
            .expect("whole-second timestamp parses"),
        after_midnight - 1
    );

    let mut cache = DockerTimestampPrefixCache::default();
    let first = parse_ascii_docker_timestamp_cached(b"2024-03-01T00:00:01.1Z", &mut cache)
        .expect("cached first timestamp parses");
    let second = parse_ascii_docker_timestamp_cached(b"2024-03-01T00:00:01.123456789Z", &mut cache)
        .expect("same-second timestamp parses");
    assert_eq!(second - first, 23_456_789);
    assert_eq!(cache.prefix.as_slice(), b"2024-03-01T00:00:01");
}

#[test]
fn fractional_timestamp_parser_scales_every_width() {
    for (digits, expected) in [
        (b"1".as_slice(), 100_000_000),
        (b"12".as_slice(), 120_000_000),
        (b"123".as_slice(), 123_000_000),
        (b"1234".as_slice(), 123_400_000),
        (b"12345".as_slice(), 123_450_000),
        (b"123456".as_slice(), 123_456_000),
        (b"1234567".as_slice(), 123_456_700),
        (b"12345678".as_slice(), 123_456_780),
        (b"123456789".as_slice(), 123_456_789),
    ] {
        assert_eq!(ascii_fraction_nanos(digits), Some(expected));
    }
    assert_eq!(ascii_fraction_nanos(b""), None);
    assert_eq!(ascii_fraction_nanos(b"1234567890"), None);
    assert_eq!(ascii_fraction_nanos(b"1234x"), None);
    assert_eq!(eight_ascii_digits(b"00000000"), Some(0));
    assert_eq!(eight_ascii_digits(b"99999999"), Some(99_999_999));
    for index in 0..8 {
        for byte in u8::MIN..=u8::MAX {
            if byte.is_ascii_digit() {
                continue;
            }
            let mut invalid = *b"12345678";
            invalid[index] = byte;
            assert_eq!(eight_ascii_digits(&invalid), None);
        }
    }
}

#[test]
fn canonical_parser_finds_every_supported_timestamp_width() {
    for timestamp in [
        "2024-03-01T00:00:00Z",
        "2024-03-01T00:00:00.1Z",
        "2024-03-01T00:00:00.12345Z",
        "2024-03-01T00:00:00.123456789Z",
    ] {
        let line = format!(
            "{{\"log\":\"contains T safely\\\\n\",\"stream\":\"stderr\",\"time\":\"{timestamp}\"}}"
        );
        let mut cache = DockerMessageCache::new();
        let mut timestamp_cache = DockerTimestampPrefixCache::default();
        let (_, stream, parsed_timestamp) =
            parse_canonical_docker_json(line.as_bytes(), &mut cache, &mut timestamp_cache)
                .expect("canonical line uses bounded timestamp probe");
        assert_eq!(stream, "stderr");
        assert_eq!(
            parsed_timestamp,
            parse_docker_timestamp(timestamp).expect("reference timestamp parses")
        );
    }
}

#[test]
fn byte_count_parser_uses_iec_units() {
    assert_eq!(
        parse_byte_count("2GiB").expect("size parses"),
        2 * 1024 * 1024 * 1024
    );
}

#[test]
fn deterministic_spans_skip_partial_edges_without_gaps() {
    let path = std::env::temp_dir().join(format!(
        "shard-telemetry-spans-{}-{}.json",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ));
    let complete = concat!(
        "{\"log\":\"alpha 1\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000001Z\"}\n",
        "{\"log\":\"alpha 2\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000002Z\"}\n",
        "{\"log\":\"alpha 3\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000003Z\"}\n"
    );
    let input = format!("partial prefix\n{complete}partial suffix");
    std::fs::write(&path, input).expect("fixture writes");
    let settings = Settings {
        input: path.clone(),
        report_path: None,
        limit_bytes: u64::MAX,
        block_bytes: 97,
        workers: 2,
        output_dir: None,
        locality_routing: true,
        realtime_dictionary: false,
        persistent_query_index: false,
    };

    let (source_start, spans) = build_block_spans(&settings).expect("spans build");
    let (_, repeated) = build_block_spans(&settings).expect("spans repeat");
    assert_eq!(spans, repeated);
    assert_eq!(source_start, "partial prefix\n".len() as u64);
    assert_eq!(spans.first().expect("first span").start, source_start);
    for (ordinal, span) in spans.iter().enumerate() {
        assert_eq!(span.ordinal, ordinal);
    }
    for adjacent in spans.windows(2) {
        assert_eq!(
            adjacent[0].start + adjacent[0].length as u64,
            adjacent[1].start
        );
    }

    let file = File::open(&path).expect("fixture opens");
    let mut recovered = Vec::new();
    for span in spans {
        let mut bytes = vec![0; span.length];
        file.read_exact_at(&mut bytes, span.start)
            .expect("span reads");
        recovered.extend_from_slice(&bytes);
    }
    assert_eq!(recovered, complete.as_bytes());
    std::fs::remove_file(path).expect("fixture removes");
}

#[test]
fn payload_checksum_detects_changes() {
    let checksum = fnv1a64(b"durable payload");
    assert_eq!(checksum, fnv1a64(b"durable payload"));
    assert_ne!(checksum, fnv1a64(b"durable payloaD"));
}

#[test]
fn dictionary_assignments_are_sparse_run_length_encoded() {
    let directory = std::env::temp_dir().join(format!(
        "shard-telemetry-dictionary-runs-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock is after epoch")
            .as_nanos()
    ));
    std::fs::create_dir(&directory).expect("fixture directory creates");
    let make_entry = |ordinal, dictionary_id| BlockResult {
        ordinal,
        source_offset: 0,
        input_bytes: 1,
        source_bytes: 1,
        record_count: 1,
        rejected_records: 0,
        rejected_bytes: 0,
        structural_bytes: 1,
        embedded_index_bytes: 0,
        structural_stored_bytes: 1,
        pack_worker: 0,
        pack_offset: 0,
        payload_checksum: 0,
        dictionary_id,
        query_index: None,
        structural_compression_time: Duration::ZERO,
    };
    let first_dictionary = DictionaryId::new(7);
    let second_dictionary = DictionaryId::new(8);
    let mut entries = vec![
        make_entry(0, None),
        make_entry(1, Some(first_dictionary)),
        make_entry(2, Some(first_dictionary)),
        make_entry(3, None),
        make_entry(4, Some(second_dictionary)),
    ];
    let bytes = write_dictionary_assignments(&directory, &mut entries).expect("assignments write");
    assert_eq!(bytes, 17 + 2 * 32);

    let mut decoded = (0..entries.len())
        .map(|ordinal| make_entry(ordinal, None))
        .collect::<Vec<_>>();
    read_dictionary_assignments(&directory, &mut decoded).expect("assignments read");
    assert_eq!(
        decoded
            .iter()
            .map(|entry| entry.dictionary_id)
            .collect::<Vec<_>>(),
        vec![
            None,
            Some(first_dictionary),
            Some(first_dictionary),
            None,
            Some(second_dictionary)
        ]
    );
    std::fs::remove_dir_all(directory).expect("fixture directory removes");
}

#[test]
fn benchmark_compressor_adopts_a_validated_realtime_dictionary() {
    let catalog = Arc::new(DictionaryCatalog::new());
    let config = RealtimeDictionaryConfig {
        max_block_sample_bytes: 1024,
        training_sample_bytes: 8 * 1024,
        dictionary_bytes: 1024,
        holdout_blocks: 8,
        queue_blocks: 64,
        max_placements: 4,
        min_net_savings_bytes: 1,
        min_net_savings_bps: 1,
        retrain_after_bytes: u64::MAX,
    };
    let trainer = RealtimeDictionaryTrainer::start(config, ZSTD_LEVEL, Arc::clone(&catalog))
        .expect("trainer starts");
    let mut compressor = BenchmarkCompressor::new(Some(catalog), Some(trainer.observer()))
        .expect("compressor starts");
    let placement_id = CompressionPlacementId::new(88);
    let sample = |index: u64| {
        let mut state = 0x4d59_5df4_d0f3_3173u64;
        let mut bytes = Vec::with_capacity(1024);
        for _ in 0..512 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            bytes.push(state as u8);
        }
        bytes.extend_from_slice(format!(" unique request suffix {index:020}").as_bytes());
        while bytes.len() < 1024 {
            bytes.push(index.wrapping_mul(31).wrapping_add(bytes.len() as u64) as u8);
        }
        bytes
    };
    for index in 0..16 {
        let structural = sample(index);
        compressor
            .compress_log_block(placement_id, structural)
            .expect("training block compresses");
    }
    trainer.flush().expect("trainer flushes");
    assert_eq!(trainer.stats().dictionaries_published, 1);

    let expected = sample(100);
    let compressed = compressor
        .compress_log_block(placement_id, expected.clone())
        .expect("dictionary block compresses");
    assert!(compressed.dictionary_id.is_some());
    let dictionary = compressed
        .dictionary_payload
        .expect("dictionary payload is retained");
    let decoded = zstd::bulk::Decompressor::with_dictionary(&dictionary)
        .expect("decompressor opens")
        .decompress(&compressed.payload, expected.len())
        .expect("dictionary frame decompresses");
    assert_eq!(decoded, expected);
}

#[test]
fn locality_container_round_trips_multiple_ordered_placement_groups() {
    let mut groups = BTreeMap::new();
    groups.insert(
        CompressionPlacementId::new(1),
        vec![DockerStructuralRecord {
            offset: LogicalOffset::new(0),
            timestamp_unix_nanos: 1,
            message: Rc::from("alpha request 1"),
            stream: Cow::Borrowed("stderr"),
        }],
    );
    groups.insert(
        CompressionPlacementId::new(2),
        vec![DockerStructuralRecord {
            offset: LogicalOffset::new(1),
            timestamp_unix_nanos: 2,
            message: Rc::from("beta request 2"),
            stream: Cow::Borrowed("stdout"),
        }],
    );
    let mut compressor = BenchmarkCompressor::new(None, None).expect("compressor");
    let compressed = compress_groups(&groups, &mut compressor).expect("groups compress");
    assert_eq!(compressed.dictionary_id, None);
    assert!(compressed.payload.starts_with(LOCALITY_CONTAINER_MAGIC));
    let mut decoded =
        decode_locality_payload(&compressed.payload, compressed.structural_bytes, None)
            .expect("container decodes");
    decoded.sort_unstable_by_key(|record| record.offset);
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].message.as_ref(), "alpha request 1");
    assert_eq!(decoded[1].message.as_ref(), "beta request 2");
    assert_eq!(decoded[0].fields[0].value.as_ref(), "stderr");
    assert_eq!(decoded[1].fields[0].value.as_ref(), "stdout");
}

#[test]
fn malformed_complete_records_are_counted_and_skipped() {
    let first =
        b"{\"log\":\"one\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000001Z\"}\n";
    let malformed = b"{not-json}\n";
    let second =
        b"{\"log\":\"two\\n\",\"stream\":\"stderr\",\"time\":\"2024-01-01T00:00:00.000000002Z\"}\n";
    let mut raw = Vec::new();
    raw.extend_from_slice(first);
    raw.extend_from_slice(malformed);
    raw.extend_from_slice(second);
    let mut compressor = BenchmarkCompressor::new(None, None).expect("compressor");
    let mut locality = CompressionBlockCollator::new(
        CompressionLocalityConfig {
            enabled: false,
            ..CompressionLocalityConfig::default()
        },
        8 * 1024 * 1024,
    )
    .expect("collator config validates");

    let (result, payload) = process_block(
        RawBlock {
            ordinal: 0,
            source_offset: 0,
            raw: &raw,
            verify: true,
        },
        &mut compressor,
        &mut locality,
        false,
    )
    .expect("block processes");

    assert_eq!(result.record_count, 2);
    assert_eq!(result.rejected_records, 1);
    assert_eq!(result.rejected_bytes, malformed.len() as u64);
    assert_eq!(
        result.source_bytes,
        u64::try_from(first.len() + second.len()).expect("fixture fits")
    );
    let structural =
        zstd::bulk::decompress(&payload, result.structural_bytes as usize).expect("decompresses");
    let decoded = decode_structural_block(&structural).expect("decodes");
    assert_eq!(
        decoded[1].offset.get(),
        u64::try_from(first.len() + malformed.len()).expect("fixture fits")
    );
}
