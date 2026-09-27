//! Structural benchmark ownership: settings, execution, compression, durable output, Docker parsing, and reporting live in the sibling folder.
#[path = "shard-telemetry-structural-bench/settings.rs"]
mod settings;
use settings::*;
#[path = "shard-telemetry-structural-bench/execution.rs"]
mod execution;
use execution::*;
#[path = "shard-telemetry-structural-bench/compression.rs"]
mod compression;
use compression::*;
#[path = "shard-telemetry-structural-bench/output.rs"]
mod output;
use output::*;
#[path = "shard-telemetry-structural-bench/docker.rs"]
mod docker;
use docker::*;
#[path = "shard-telemetry-structural-bench/report.rs"]
mod report;
use report::*;
#[cfg(test)]
#[path = "shard-telemetry-structural-bench/tests.rs"]
mod tests;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

use memmap2::{Mmap, MmapOptions};
use serde::Deserialize;
use shard_stream_core::{LogicalOffset, LogicalPartitionId, TopicId, TopicPartition};
use shard_telemetry::{
    BlockQueryIndex, CompressionBlockCollator, CompressionCohortId, CompressionLocalityConfig,
    CompressionLocalityRecord, CompressionLocalityStats, CompressionPlacementId,
    DecodedStructuralRecord, DictionaryCatalog, DictionaryId, MessageFingerprint,
    PersistentQueryIndex, QueryBlockMetadata, RealtimeDictionaryConfig, RealtimeDictionaryObserver,
    RealtimeDictionaryStats, RealtimeDictionaryTrainer, StructuralRecordView,
    decode_structural_block, encode_indexed_structural_records, fingerprint_message,
};

const DEFAULT_LIMIT_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_BLOCK_BYTES: usize = 8 * 1024 * 1024;
const PROGRESS_BYTES: u64 = 1024 * 1024 * 1024;
const ZSTD_LEVEL: i32 = 1;
const LOCALITY_CONTAINER_MAGIC: &[u8; 8] = b"SLOGLOC1";
const DOCKER_JSON_PREFIX: &[u8] = b"{\"log\":\"";
const DOCKER_JSON_STDERR_SUFFIX: &[u8] = b"\",\"stream\":\"stderr\",\"time\":\"";
const DOCKER_JSON_STDOUT_SUFFIX: &[u8] = b"\",\"stream\":\"stdout\",\"time\":\"";
const DOCKER_MESSAGE_CACHE_ENTRIES: usize = 256;
const QUERY_PARTITION: TopicPartition =
    TopicPartition::new(TopicId::new(0), LogicalPartitionId::new(0));

#[derive(Debug)]
struct Settings {
    input: PathBuf,
    report_path: Option<PathBuf>,
    limit_bytes: u64,
    block_bytes: usize,
    workers: usize,
    output_dir: Option<PathBuf>,
    locality_routing: bool,
    realtime_dictionary: bool,
    persistent_query_index: bool,
}

#[derive(Debug, Deserialize)]
struct DockerJsonLine<'a> {
    #[serde(borrow)]
    log: Cow<'a, str>,
    #[serde(borrow)]
    stream: Cow<'a, str>,
    #[serde(borrow)]
    time: Cow<'a, str>,
}

struct DockerStructuralRecord<'a> {
    offset: LogicalOffset,
    timestamp_unix_nanos: u64,
    message: Rc<str>,
    stream: Cow<'a, str>,
}

impl StructuralRecordView for DockerStructuralRecord<'_> {
    fn structural_offset(&self) -> LogicalOffset {
        self.offset
    }

    fn structural_timestamp_unix_nanos(&self) -> u64 {
        self.timestamp_unix_nanos
    }

    fn structural_message(&self) -> &str {
        &self.message
    }

    fn structural_field_count(&self) -> usize {
        1
    }

    fn structural_field(&self, index: usize) -> Option<(&str, &str)> {
        (index == 0).then_some(("docker.stream", self.stream.as_ref()))
    }
}

struct DockerMessageCacheEntry<'a> {
    raw: &'a [u8],
    decoded: Rc<str>,
}

struct DockerMessageCache<'a> {
    entries: Vec<Option<DockerMessageCacheEntry<'a>>>,
}

impl<'a> DockerMessageCache<'a> {
    fn new() -> Self {
        Self {
            entries: std::iter::repeat_with(|| None)
                .take(DOCKER_MESSAGE_CACHE_ENTRIES)
                .collect(),
        }
    }

    fn decode(&mut self, raw: &'a [u8]) -> Option<Rc<str>> {
        let slot = docker_message_cache_slot(raw);
        if let Some(entry) = &self.entries[slot]
            && entry.raw == raw
        {
            return Some(Rc::clone(&entry.decoded));
        }
        let decoded = decode_common_json_message(raw)?;
        self.entries[slot] = Some(DockerMessageCacheEntry {
            raw,
            decoded: Rc::clone(&decoded),
        });
        Some(decoded)
    }
}

#[derive(Default)]
struct DockerTimestampPrefixCache {
    prefix: [u8; 19],
    base_nanos: u64,
    initialized: bool,
}

#[derive(Debug, Default)]
struct Benchmark {
    input_bytes: u64,
    source_bytes: u64,
    leading_discarded_bytes: u64,
    rejected_records: u64,
    rejected_bytes: u64,
    structural_bytes: u64,
    embedded_index_bytes: u64,
    structural_stored_bytes: u64,
    manifest_bytes: u64,
    query_index_bytes: u64,
    blocks: u64,
    records: u64,
    structural_compression_time: Duration,
    elapsed: Duration,
    verification_elapsed: Duration,
    verified_blocks: u64,
    workers: usize,
    locality: CompressionLocalityStats,
    dictionary_bytes: u64,
    dictionary_stats: RealtimeDictionaryStats,
}

struct RawBlock<'a> {
    ordinal: usize,
    source_offset: u64,
    raw: &'a [u8],
    verify: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockSpan {
    ordinal: usize,
    start: u64,
    length: usize,
}

struct BlockResult {
    ordinal: usize,
    source_offset: u64,
    input_bytes: u64,
    source_bytes: u64,
    record_count: u64,
    rejected_records: u64,
    rejected_bytes: u64,
    structural_bytes: u64,
    embedded_index_bytes: u64,
    structural_stored_bytes: u64,
    pack_worker: usize,
    pack_offset: u64,
    payload_checksum: u64,
    dictionary_id: Option<DictionaryId>,
    query_index: Option<(QueryBlockMetadata, BlockQueryIndex)>,
    structural_compression_time: Duration,
}

struct WorkerResult {
    blocks: Vec<BlockResult>,
    locality: CompressionLocalityStats,
}

struct BenchmarkCompressor {
    zstd: zstd::bulk::Compressor<'static>,
    active_dictionary: Option<DictionaryId>,
    dictionary_catalog: Option<Arc<DictionaryCatalog>>,
    dictionary_observer: Option<RealtimeDictionaryObserver>,
}

impl BenchmarkCompressor {
    fn new(
        dictionary_catalog: Option<Arc<DictionaryCatalog>>,
        dictionary_observer: Option<RealtimeDictionaryObserver>,
    ) -> Result<Self, String> {
        Ok(Self {
            zstd: zstd::bulk::Compressor::new(ZSTD_LEVEL).map_err(|error| error.to_string())?,
            active_dictionary: None,
            dictionary_catalog,
            dictionary_observer,
        })
    }

    fn compress_log_block(
        &mut self,
        placement_id: CompressionPlacementId,
        structural: Vec<u8>,
    ) -> Result<CompressedStructural, String> {
        let dictionary = self
            .dictionary_catalog
            .as_ref()
            .map(|catalog| catalog.snapshot().map_err(|error| error.to_string()))
            .transpose()?
            .and_then(|snapshot| snapshot.dictionary_for(placement_id));
        let dictionary_id = dictionary.as_ref().map(|(dictionary_id, _)| *dictionary_id);
        if self.active_dictionary != dictionary_id {
            let payload = dictionary
                .as_ref()
                .map_or(&[][..], |(_, payload)| payload.as_ref());
            self.zstd
                .set_dictionary(ZSTD_LEVEL, payload)
                .map_err(|error| error.to_string())?;
            self.active_dictionary = dictionary_id;
        }
        let payload = self
            .zstd
            .compress(&structural)
            .map_err(|error| error.to_string())?;
        let structural_len = structural.len();
        if let Some(observer) = &self.dictionary_observer {
            let _ = observer.observe_structural_block(placement_id, structural);
        }
        Ok(CompressedStructural {
            structural_len,
            payload,
            dictionary_id,
            dictionary_payload: dictionary.map(|(_, payload)| payload),
        })
    }
}

struct CompressedStructural {
    structural_len: usize,
    payload: Vec<u8>,
    dictionary_id: Option<DictionaryId>,
    dictionary_payload: Option<Arc<[u8]>>,
}

struct CompressedGroups {
    structural_bytes: u64,
    embedded_index_bytes: u64,
    payload: Vec<u8>,
    dictionary_id: Option<DictionaryId>,
    dictionary_payload: Option<Arc<[u8]>>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let settings = parse_settings()?;
    let benchmark = run_benchmark(&settings)?;
    let report = render_report(&settings, &benchmark);
    if let Some(path) = &settings.report_path {
        let mut output = OpenOptions::new().write(true).create_new(true).open(path)?;
        output.write_all(report.as_bytes())?;
        output.flush()?;
    }
    print!("{report}");
    Ok(())
}
