//! Consuming publication into the real compressed-frame log query index.
//!
//! Existing stripe setup, decompression, objects, journal durability and the
//! application checkpoint transaction are separate producers.
use super::*;
use crate::ingest_pack::{
    IndexedFramePreparationLimits, PreparedIndexedIngestFrames, RetainedIndexedFrameError,
    prepare_indexed_ingest_frames_until,
};
#[cfg(unix)]
use crate::sink::owned_log::replay::AdoptedAppend;
#[cfg(unix)]
use crate::sink_journal::JournalReplayInput;
use crate::{TelemetryEnvelope, TelemetrySignal};
use shard_stream_engine::{
    DurableAppend, DurableAppendDelivery, NativeResumeRefusal, NativeResumeWork,
};
use std::any::Any;
use std::collections::{TryReserveError, hash_map::IntoIter};
use std::error::Error;
use std::fmt;
use std::mem::{replace, take};
use std::num::NonZeroU32;
use std::ops::Range;
use std::panic::{AssertUnwindSafe, catch_unwind};

mod input;
pub(crate) use input::LogPublicationInput;

const HASH_CHUNK: usize = 16 * 1024;
const BYTES_PROMOTION: usize = 3 * size_of::<usize>();
type FrameMap = HashMap<TopicPartition, IndexedFramePartition>;
type ActiveCache = HashMap<Arc<str>, Vec<TopicPartition>>;
type MapEntry = (TopicPartition, IndexedFramePartition);

/// Finite outer publication limits; child preparation debits SAME work as well.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LogIndexPublicationLimits {
    pub(crate) max_envelope_bytes: usize,
    pub(crate) max_context_bytes: usize,
    pub(crate) max_partitions: usize,
    pub(crate) max_appends_per_partition: usize,
    /// Source-visible values and element-slot requests, not allocator RSS.
    pub(crate) max_declared_bytes: usize,
    pub(crate) max_entries: usize,
    pub(crate) max_work_bytes: usize,
    pub(crate) frames: IndexedFramePreparationLimits,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct LogIndexPublicationUsage {
    pub(crate) declared_bytes: usize,
    pub(crate) entries: usize,
    pub(crate) work_bytes: usize,
}
/// Query-index effects only; neither variant is an application checkpoint/ACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogIndexPublicationOutcome {
    Published {
        frames: usize,
        indexed_through: LogicalOffset,
    },
    /// The actual stripe watermark already covers the range. This is not a
    /// content-identity proof or permission to acknowledge a durable append.
    AlreadyCovered { indexed_through: LogicalOffset },
}
#[derive(Debug)]
pub(crate) enum LogIndexPublicationFailure {
    Refused(NativeResumeRefusal),
    Limits,
    #[allow(
        dead_code,
        reason = "retain the exact static identity diagnostic without a refusal allocation"
    )]
    Identity(&'static str),
    Codec(TelemetryError),
    Frames,
    Allocation(TryReserveError),
    Panic,
    State,
}
type Failure = LogIndexPublicationFailure;
type Result<T> = std::result::Result<T, Failure>;
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(Failure::Limits)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(Failure::Limits)
}
fn envelope_error(message: &'static str) -> Failure {
    Failure::Codec(TelemetryError::InvalidTelemetryEnvelope(message))
}

struct Attempt<I> {
    // Complete actual inputs, including old query state, precede every check.
    input: I,
    expected: DurableSinkCheckpoint,
    next: DurableSinkCheckpoint,
    payload_range: Option<Range<usize>>,
    tenant_range: Option<Range<usize>>,
    payload: Option<Bytes>,
    context: Option<Bytes>,
    hasher: Option<blake3::Hasher>,
    hashed_through: usize,
    actual_checksum: Option<[u8; 32]>,
    prepared: Option<std::result::Result<PreparedIndexedIngestFrames, RetainedIndexedFrameError>>,
    index: Option<EmbeddedFrameIndex>,
    index_arc: Option<Arc<EmbeddedFrameIndex>>,
    compressed: Option<Bytes>,
    tenant: Option<Arc<str>>,
    frames: Vec<IndexedIngestFrame>,
    new_append: Option<IndexedFrameAppend>,
    staged_map: Option<FrameMap>,
    old_map: Option<IntoIter<TopicPartition, IndexedFramePartition>>,
    old_capacity: usize,
    current_entry: Option<MapEntry>,
    target: Option<IndexedFramePartition>,
    empty_cache: Option<ActiveCache>,
    retired_cache: Option<ActiveCache>,
    next_frame_id: u64,
    outcome: Option<LogIndexPublicationOutcome>,
    panic: Option<Box<dyn Any + Send>>,
    usage: LogIndexPublicationUsage,
    limits: LogIndexPublicationLimits,
    work: NativeResumeWork,
    // Native compressor/dictionary/object owners outlive all staged/query data.
    stripe: LogStripe,
}
/// Successful actual query publication with its original append/child owners.
pub(crate) struct PublishedLogIndex<I = DurableAppend> {
    retained: Attempt<I>,
}
/// Complete paid staging on the original exclusive stripe; no new watermark
/// or append has been published. Only a consuming commit can publish it.
pub(crate) struct PreparedLogIndexPublication<I = DurableAppend> {
    retained: Attempt<I>,
}
/// Terminal consuming refusal; no extractor can retry a partially moved stripe.
pub(crate) struct RetainedLogIndexPublicationError<I = DurableAppend> {
    pub(crate) cause: Failure,
    retained: Attempt<I>,
}
impl<I: LogPublicationInput> PublishedLogIndex<I> {
    pub(crate) fn outcome(&self) -> Option<LogIndexPublicationOutcome> {
        self.retained.outcome
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original publication diagnostics without extracting retained owners"
    )]
    pub(crate) fn stripe(&self) -> &LogStripe {
        &self.retained.stripe
    }
    #[allow(
        dead_code,
        reason = "preserve read-only original publication diagnostics without extracting retained owners"
    )]
    pub(crate) fn input(&self) -> &I {
        &self.retained.input
    }
    /// Only a successful owner may resume normal exclusive stripe ownership.
    pub(crate) fn into_stripe(self) -> LogStripe {
        self.retained.stripe
    }
}
impl<I: LogPublicationInput> PreparedLogIndexPublication<I> {
    /// Original owned append retained throughout staging and later journal I/O.
    pub(crate) fn input(&self) -> &I {
        &self.retained.input
    }
    /// An existing watermark is an observation, never content or ACK proof.
    pub(crate) fn outcome(&self) -> Option<LogIndexPublicationOutcome> {
        self.retained.outcome
    }
    /// Install the already-paid complete staged append and watermark. A late
    /// error retains the actual committed effect and the SAME original work.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn commit_until(
        mut self,
    ) -> std::result::Result<PublishedLogIndex<I>, RetainedLogIndexPublicationError<I>> {
        let actual = catch_unwind(AssertUnwindSafe(|| {
            self.retained.check()?;
            if self.retained.outcome.is_none() {
                self.retained.commit()?;
                self.retained.observed("published")?;
            }
            Ok(())
        }));
        finish(self.retained, actual).map(|retained| PublishedLogIndex { retained })
    }
}
impl<I: LogPublicationInput> fmt::Debug for PreparedLogIndexPublication<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedLogIndexPublication")
            .field("outcome", &self.retained.outcome)
            .field("usage", &self.retained.usage)
            .finish_non_exhaustive()
    }
}
impl<I: LogPublicationInput> RetainedLogIndexPublicationError<I> {
    #[allow(
        dead_code,
        reason = "preserve read-only partial publication diagnostics without replacing retained owners"
    )]
    pub(crate) fn input(&self) -> &I {
        &self.retained.input
    }
    #[allow(
        dead_code,
        reason = "preserve read-only partial publication diagnostics without replacing retained owners"
    )]
    pub(crate) fn outcome(&self) -> Option<LogIndexPublicationOutcome> {
        self.retained.outcome
    }
    #[allow(
        dead_code,
        reason = "preserve the original refused publication ledger diagnostic without resetting counters"
    )]
    pub(crate) fn usage(&self) -> LogIndexPublicationUsage {
        self.retained.usage
    }
}
impl<I: LogPublicationInput> fmt::Debug for PublishedLogIndex<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishedLogIndex")
            .field("outcome", &self.retained.outcome)
            .field("usage", &self.retained.usage)
            .finish_non_exhaustive()
    }
}
impl<I: LogPublicationInput> fmt::Debug for RetainedLogIndexPublicationError<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedLogIndexPublicationError")
            .field("cause", &self.cause)
            .field("outcome", &self.retained.outcome)
            .field("frames", &self.retained.frames.len())
            .field("usage", &self.retained.usage)
            .field("panicked", &self.retained.panic.is_some())
            .finish_non_exhaustive()
    }
}
impl<I: LogPublicationInput> fmt::Display for RetainedLogIndexPublicationError<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}
impl<I: LogPublicationInput> Error for RetainedLogIndexPublicationError<I> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.cause {
            Failure::Refused(e) => Some(e),
            Failure::Codec(e) => Some(e),
            Failure::Allocation(e) => Some(e),
            Failure::Frames => self
                .retained
                .prepared
                .as_ref()?
                .as_ref()
                .err()
                .map(|e| e as &dyn Error),
            _ => None,
        }
    }
}
impl LogStripe {
    /// Publish one genuinely supplied, contiguous checkpoint edge into the real
    /// log frame index using SAME original work/E. This does not establish WAL
    /// provenance, leader authority, journal durability or application progress.
    ///
    /// Only Publish/non-atomic log appends are supported. Original inputs, old
    /// stripe and actual children enter retained custody before any refusal.
    /// All fallible preparation precedes an allocation-free exclusive commit of
    /// the complete frame append, frame IDs and watermark. Late commit returns
    /// a terminal error retaining the actual published effect. Existing stripe
    /// construction/destruction and later ordinary queries are not bounded here.
    #[allow(
        dead_code,
        reason = "standalone component entry is tested; composing transactions use prepare and commit"
    )]
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn publish_durable_log_until(
        self,
        input: DurableAppend,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
        limits: LogIndexPublicationLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<PublishedLogIndex, RetainedLogIndexPublicationError> {
        self.prepare_durable_log_until(input, expected, next, limits, work)?
            .commit_until()
    }

    /// Stage exactly the real publication, including all final movement/probe
    /// fees, before a composing transaction enters its physical journal write.
    /// Existing query owners and all child results remain retained on failure.
    #[allow(
        clippy::result_large_err,
        reason = "retain complete original owners and native outcomes inline without a refusal allocation"
    )]
    pub(crate) fn prepare_durable_log_until(
        self,
        input: DurableAppend,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
        limits: LogIndexPublicationLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<PreparedLogIndexPublication, RetainedLogIndexPublicationError> {
        self.prepare_log_input_until(input, expected, next, limits, work)
    }
    #[allow(
        clippy::result_large_err,
        reason = "retain complete actual input, native children and partial query owner inline"
    )]
    pub(crate) fn prepare_log_input_until<I: LogPublicationInput>(
        self,
        input: I,
        expected: DurableSinkCheckpoint,
        next: DurableSinkCheckpoint,
        limits: LogIndexPublicationLimits,
        work: &NativeResumeWork,
    ) -> std::result::Result<PreparedLogIndexPublication<I>, RetainedLogIndexPublicationError<I>>
    {
        let mut retained = Attempt {
            input,
            expected,
            next,
            payload_range: None,
            tenant_range: None,
            payload: None,
            context: None,
            hasher: None,
            hashed_through: 0,
            actual_checksum: None,
            prepared: None,
            index: None,
            index_arc: None,
            compressed: None,
            tenant: None,
            frames: Vec::new(),
            new_append: None,
            staged_map: None,
            old_map: None,
            old_capacity: 0,
            current_entry: None,
            target: None,
            empty_cache: None,
            retired_cache: None,
            next_frame_id: 0,
            outcome: None,
            panic: None,
            usage: LogIndexPublicationUsage::default(),
            limits,
            work: work.clone(),
            stripe: self,
        };
        let actual = catch_unwind(AssertUnwindSafe(|| retained.run()));
        finish(retained, actual).map(|retained| PreparedLogIndexPublication { retained })
    }

    /// Actual attached object tier, whose publication needs its own producer.
    pub(crate) fn requires_log_object_publication(&self) -> bool {
        self.tier.is_some()
    }
}
#[allow(
    clippy::result_large_err,
    reason = "retain complete original owners and native outcomes inline without a refusal allocation"
)]
fn finish<I: LogPublicationInput>(
    mut retained: Attempt<I>,
    actual: std::thread::Result<Result<()>>,
) -> std::result::Result<Attempt<I>, RetainedLogIndexPublicationError<I>> {
    let cause = match actual {
        Ok(Ok(())) => return Ok(retained),
        Ok(Err(cause)) => cause,
        Err(payload) => {
            retained.panic = Some(payload);
            Failure::Panic
        }
    };
    Err(RetainedLogIndexPublicationError { cause, retained })
}
impl<I: LogPublicationInput> Attempt<I> {
    fn check(&self) -> Result<()> {
        self.work.check().map_err(Failure::Refused)
    }
    fn pay(&mut self, bytes: usize, entries: usize, work: usize) -> Result<()> {
        let next = LogIndexPublicationUsage {
            declared_bytes: add(self.usage.declared_bytes, bytes)?,
            entries: add(self.usage.entries, entries)?,
            work_bytes: add(self.usage.work_bytes, work)?,
        };
        if next.declared_bytes > self.limits.max_declared_bytes
            || next.entries > self.limits.max_entries
            || next.work_bytes > self.limits.max_work_bytes
        {
            return Err(Failure::Limits);
        }
        self.work
            .reserve_values(bytes, 0, entries, work)
            .map_err(Failure::Refused)?;
        self.usage = next;
        Ok(())
    }
    fn probe<T>(&mut self, capacity: usize) -> Result<()> {
        self.pay(0, 0, mul(add(capacity, 1)?, size_of::<T>().max(1))?)
    }
    fn observed(&self, phase: &'static str) -> Result<()> {
        #[cfg(all(test, any(target_os = "linux", target_os = "android")))]
        tests::after_value(self.work.absolute_expiry(), phase);
        #[cfg(not(all(test, any(target_os = "linux", target_os = "android"))))]
        let _ = phase;
        self.check()
    }
    fn run(&mut self) -> Result<()> {
        self.check()?;
        let l = self.limits;
        if [
            l.max_envelope_bytes,
            l.max_context_bytes,
            l.max_partitions,
            l.max_appends_per_partition,
            l.max_declared_bytes,
            l.max_entries,
            l.max_work_bytes,
        ]
        .contains(&0)
        {
            return Err(Failure::Limits);
        }
        if self.input.payload().len() > l.max_envelope_bytes
            || self
                .input
                .context()
                .is_some_and(|b| b.len() > l.max_context_bytes)
        {
            return Err(Failure::Limits);
        }
        self.pay(
            add(
                size_of::<Self>(),
                add(
                    self.input.payload().len(),
                    self.input.context().map_or(0, Bytes::len),
                )?,
            )?,
            1,
            size_of::<Self>(),
        )?;
        self.identity()?;
        if self.covered()? {
            return self.observed("covered");
        }
        self.envelope()?;
        self.prepare()?;
        self.convert()?;
        self.prepare_map()?;
        self.prepare_append()?;
        self.prepay_commit()
    }
    fn identity(&self) -> Result<()> {
        let Some(input) = self.input.live() else {
            #[cfg(unix)]
            {
                let journal = self.input.journal().ok_or(Failure::State)?;
                let mut expected = journal.expected();
                expected.next_offset = journal.first_offset();
                if journal.anchor().shard_id() != self.stripe.stream_shard_id
                    || self.expected != expected
                    || self.next != journal.next()
                {
                    return Err(Failure::Identity(
                        "journal origin differs from exact index edge",
                    ));
                }
                return Ok(());
            }
            #[cfg(not(unix))]
            return Err(Failure::State);
        };
        let r = input.reservation;
        let tp = input.topic_partition();
        if input.physical_shard_id != self.stripe.stream_shard_id {
            return Err(Failure::Codec(TelemetryError::WrongStripe {
                expected: self.stripe.stream_shard_id,
                observed: input.physical_shard_id,
            }));
        }
        if input.delivery != DurableAppendDelivery::Publish || input.atomic_group.is_some() {
            return Err(Failure::Identity(
                "atomic/staged visibility needs its real decision producer",
            ));
        }
        let last = r
            .first_offset
            .get()
            .checked_add(u64::from(r.record_count.get()) - 1);
        if last != Some(r.last_offset.get())
            || self.expected.topic_partition != tp
            || self.next.topic_partition != tp
            || self.expected.next_offset != r.first_offset
            || self.expected.next_placement_sequence != r.placement.sequence
            || r.last_offset.get().checked_add(1) != Some(self.next.next_offset.get())
            || r.placement.sequence.get().checked_add(1)
                != Some(self.next.next_placement_sequence.get())
        {
            return Err(Failure::Identity(
                "append is not the supplied contiguous checkpoint edge",
            ));
        }
        Ok(())
    }
    fn covered(&mut self) -> Result<bool> {
        let tp = self.input.topic_partition();
        self.probe::<(TopicPartition, PartitionIndex)>(self.stripe.partitions.capacity())?;
        let hot = self
            .stripe
            .partitions
            .get(&tp)
            .and_then(|p| p.indexed_through);
        self.probe::<MapEntry>(self.stripe.indexed_frame_partitions.capacity())?;
        let frames = self
            .stripe
            .indexed_frame_partitions
            .get(&tp)
            .and_then(|p| p.indexed_through);
        if let Some(previous) = hot.max(frames)
            && self.input.first_offset() <= previous
        {
            if self.input.last_offset() <= previous {
                self.outcome = Some(LogIndexPublicationOutcome::AlreadyCovered {
                    indexed_through: previous,
                });
                return Ok(true);
            }
            return Err(Failure::Codec(TelemetryError::OffsetOutOfOrder {
                partition: tp,
                expected: LogicalOffset::new(
                    previous
                        .get()
                        .checked_add(1)
                        .ok_or(Failure::Codec(TelemetryError::OffsetExhausted(tp)))?,
                ),
                observed: self.input.first_offset(),
            }));
        }
        Ok(false)
    }
    fn envelope(&mut self) -> Result<()> {
        self.pay(0, 0, 64)?;
        let p = self.input.payload();
        if p.len() < 64 || p.len() > crate::MAX_TELEMETRY_ENVELOPE_BYTES {
            return Err(envelope_error(
                "envelope length is outside the supported range",
            ));
        }
        if &p[..4] != b"STEL" {
            return Err(envelope_error("missing STEL envelope magic"));
        }
        if p[4] != 1 {
            return Err(envelope_error("unsupported STEL envelope version"));
        }
        if p[6..8] != [0, 0] || p[28..32] != [0; 4] {
            return Err(envelope_error("nonzero reserved envelope bits"));
        }
        self.pay(size_of::<blake3::Hasher>(), 1, size_of::<blake3::Hasher>())?;
        self.hasher = Some(blake3::Hasher::new());
        self.observed("hasher")?;
        // Same STEL checksum domain, with the physical checksum field zeroed.
        while self.hashed_through < self.input.payload().len() {
            let start = self.hashed_through;
            let end = if start < 32 {
                32
            } else if start == 32 {
                64
            } else {
                add(start, HASH_CHUNK)?.min(self.input.payload().len())
            };
            self.pay(0, 0, mul(end - start, 4)?)?;
            let hasher = self.hasher.as_mut().ok_or(Failure::State)?;
            if start == 32 {
                hasher.update(&[0; 32]);
            } else {
                hasher.update(&self.input.payload()[start..end]);
            }
            self.hashed_through = end;
            self.observed("checksum-prefix")?;
        }
        self.pay(0, 0, size_of::<blake3::Hasher>())?;
        self.actual_checksum = Some(
            *self
                .hasher
                .as_ref()
                .ok_or(Failure::State)?
                .finalize()
                .as_bytes(),
        );
        self.observed("checksum")?;
        if self.actual_checksum.as_ref().map(|v| v.as_slice())
            != Some(&self.input.payload()[32..64])
        {
            return Err(envelope_error("envelope checksum mismatch"));
        }
        // This exact ordinary borrowed parser has no section/output allocation;
        // pay its complete possible UTF-8 scan before entry, after real hashing.
        self.pay(0, 0, add(64, self.input.payload().len())?)?;
        let view = TelemetryEnvelope::decode_view_after_validation(self.input.payload())
            .map_err(Failure::Codec)?;
        if view.signal != TelemetrySignal::Logs
            || view.signal.topic_id() != self.input.topic_partition().topic_id
            || view.item_count != self.input.record_count().get()
        {
            return Err(Failure::Identity(
                "STEL signal/topic/count differs from the actual reservation",
            ));
        }
        let payload_start = self.input.payload().len() - view.payload.len();
        self.payload_range = Some(payload_start..self.input.payload().len());
        self.tenant_range = Some(64..64 + view.tenant.len());
        self.observed("envelope")
    }
    fn prepare(&mut self) -> Result<()> {
        self.pay(BYTES_PROMOTION, 1, size_of::<Bytes>())?;
        self.payload = Some(
            self.input
                .payload()
                .slice(self.payload_range.clone().ok_or(Failure::State)?),
        );
        self.observed("payload-owner")?;
        if self.input.context().is_some() {
            self.pay(BYTES_PROMOTION, 1, size_of::<Bytes>())?;
            self.context = self.input.context().cloned();
            self.observed("context-owner")?;
        }
        self.pay(
            size_of::<std::result::Result<PreparedIndexedIngestFrames, RetainedIndexedFrameError>>(
            ),
            1,
            size_of::<Bytes>() * 2,
        )?;
        self.prepared = Some(prepare_indexed_ingest_frames_until(
            self.payload.take().ok_or(Failure::State)?,
            self.context.take(),
            self.input.record_count().get(),
            self.limits.frames,
            &self.work,
        ));
        self.observed("prepared")?;
        if self.prepared.as_ref().is_some_and(|r| r.is_err()) {
            return Err(Failure::Frames);
        }
        Ok(())
    }
    fn prepared(&self) -> Result<&PreparedIndexedIngestFrames> {
        self.prepared
            .as_ref()
            .and_then(|r| r.as_ref().ok())
            .ok_or(Failure::State)
    }
    fn convert(&mut self) -> Result<()> {
        let count = self.prepared()?.frames().len();
        self.next_frame_id = self
            .stripe
            .next_frame_id
            .checked_add(u64::try_from(count).map_err(|_| Failure::Limits)?)
            .ok_or(Failure::Codec(TelemetryError::RecordTooLarge))?;
        let bytes = mul(count, size_of::<IndexedIngestFrame>())?;
        self.pay(bytes, count, bytes)?;
        self.frames
            .try_reserve_exact(count)
            .map_err(Failure::Allocation)?;
        let excess = self.frames.capacity().saturating_sub(count);
        self.pay(
            mul(excess, size_of::<IndexedIngestFrame>())?,
            excess,
            mul(excess, size_of::<IndexedIngestFrame>())?,
        )?;
        self.observed("frame-capacity")?;
        for i in 0..count {
            // ArcInner's two counters plus maximum alignment padding and T.
            // This is a declared allocation request, not allocator metadata/RSS.
            let arc = add(
                add(
                    2 * size_of::<usize>(),
                    std::mem::align_of::<EmbeddedFrameIndex>() - 1,
                )?,
                size_of::<EmbeddedFrameIndex>(),
            )?;
            self.pay(arc, 1, mul(size_of::<EmbeddedFrameIndex>(), 2)?)?;
            let prepared = self
                .prepared
                .as_mut()
                .and_then(|r| r.as_mut().ok())
                .ok_or(Failure::State)?;
            if !prepared.transfer_index_to(i, &mut self.index) {
                return Err(Failure::State);
            }
            self.observed("transferred-index")?;
            self.index_arc = Some(Arc::new(self.index.take().ok_or(Failure::State)?));
            self.observed("index-arc")?;
            self.pay(BYTES_PROMOTION, 1, size_of::<Bytes>())?;
            let range = self.prepared()?.compressed_range(i).ok_or(Failure::State)?;
            self.compressed = Some(self.prepared()?.payload().slice(range));
            self.observed("compressed-owner")?;
            self.pay(0, 0, size_of::<IndexedIngestFrame>())?;
            let f = self.prepared()?.frames().get(i).ok_or(Failure::State)?;
            let (cohort, record_count, structural_bytes, min, max) = (
                f.cohort,
                f.record_count,
                f.structural_bytes,
                f.min_timestamp_unix_nanos,
                f.max_timestamp_unix_nanos,
            );
            self.frames.push(IndexedIngestFrame {
                frame_id: self.stripe.next_frame_id + i as u64,
                cohort,
                record_count,
                structural_bytes,
                min_timestamp_unix_nanos: min,
                max_timestamp_unix_nanos: max,
                compressed: self.compressed.take().ok_or(Failure::State)?,
                index: self.index_arc.take().ok_or(Failure::State)?,
            });
            self.observed("converted-frame")?;
        }
        let tenant = self.tenant_range.clone().ok_or(Failure::State)?;
        self.pay(add(tenant.len(), 3 * size_of::<usize>())?, 1, tenant.len())?;
        self.tenant = Some(Arc::from(
            std::str::from_utf8(&self.input.payload()[tenant]).map_err(|_| Failure::State)?,
        ));
        self.observed("tenant")
    }
    fn prepare_map(&mut self) -> Result<()> {
        self.probe::<MapEntry>(self.stripe.indexed_frame_partitions.capacity())?;
        let missing = !self
            .stripe
            .indexed_frame_partitions
            .contains_key(&self.input.topic_partition());
        let count = add(
            self.stripe.indexed_frame_partitions.len(),
            usize::from(missing),
        )?;
        if count > self.limits.max_partitions {
            return Err(Failure::Limits);
        }
        self.pay(
            2 * size_of::<FrameMap>() + size_of::<ActiveCache>(),
            3,
            3 * size_of::<foldhash::fast::RandomState>(),
        )?;
        self.staged_map = Some(HashMap::with_hasher(
            self.stripe.indexed_frame_partitions.hasher().clone(),
        ));
        self.empty_cache = Some(HashMap::with_hasher(
            self.stripe.active_partition_cache.hasher().clone(),
        ));
        // Reserve only an EMPTY table; no opaque rehash of live query owners.
        let bytes = mul(count, size_of::<MapEntry>())?;
        self.pay(bytes, count, bytes)?;
        self.staged_map
            .as_mut()
            .ok_or(Failure::State)?
            .try_reserve(count)
            .map_err(Failure::Allocation)?;
        let cap = self.staged_map.as_ref().ok_or(Failure::State)?.capacity();
        let excess = cap.saturating_sub(count);
        self.pay(
            mul(excess, size_of::<MapEntry>())?,
            excess,
            mul(excess, size_of::<MapEntry>())?,
        )?;
        self.observed("map-capacity")?;
        // A zero-allocation same-hasher replacement leaves the complete actual
        // old table in a retained iterator before any fallible transfer step.
        self.pay(size_of::<FrameMap>(), 1, size_of::<FrameMap>())?;
        let empty = HashMap::with_hasher(self.stripe.indexed_frame_partitions.hasher().clone());
        self.old_capacity = self.stripe.indexed_frame_partitions.capacity();
        self.old_map = Some(replace(&mut self.stripe.indexed_frame_partitions, empty).into_iter());
        self.observed("old-map")?;
        loop {
            // std map iteration may scan the complete original capacity.
            self.probe::<MapEntry>(self.old_capacity)?;
            self.current_entry = self.old_map.as_mut().ok_or(Failure::State)?.next();
            self.observed("map-entry")?;
            let Some((key, _)) = self.current_entry.as_ref() else {
                break;
            };
            if *key == self.input.topic_partition() {
                self.target = self.current_entry.take().map(|(_, p)| p);
            } else {
                self.probe::<MapEntry>(cap)?;
                let (key, value) = self.current_entry.take().ok_or(Failure::State)?;
                // Reserved empty table fits all old keys plus the new key.
                // TopicPartition Hash/Eq are fixed scalar, allocation-free.
                self.staged_map
                    .as_mut()
                    .ok_or(Failure::State)?
                    .insert(key, value);
            }
            self.observed("moved-partition")?;
        }
        if self.target.is_none() {
            self.target = Some(IndexedFramePartition::default());
        }
        Ok(())
    }
    fn prepare_append(&mut self) -> Result<()> {
        let target = self.target.as_ref().ok_or(Failure::State)?;
        let count = add(target.appends.len(), 1)?;
        if count > self.limits.max_appends_per_partition {
            return Err(Failure::Limits);
        }
        if let Some(last) = target.appends.last().and_then(|a| a.next_checkpoint)
            && (last.next_offset > self.expected.next_offset
                || last.next_placement_sequence > self.expected.next_placement_sequence)
        {
            return Err(Failure::Identity(
                "query append checkpoint regresses actual published coordinates",
            ));
        }
        let bytes = mul(count, size_of::<IndexedFrameAppend>())?;
        self.pay(
            bytes,
            count,
            add(
                bytes,
                mul(
                    self.target.as_ref().ok_or(Failure::State)?.appends.len(),
                    size_of::<IndexedFrameAppend>(),
                )?,
            )?,
        )?;
        self.target
            .as_mut()
            .ok_or(Failure::State)?
            .appends
            .try_reserve_exact(1)
            .map_err(Failure::Allocation)?;
        let excess = self
            .target
            .as_ref()
            .ok_or(Failure::State)?
            .appends
            .capacity()
            .saturating_sub(count);
        self.pay(
            mul(excess, size_of::<IndexedFrameAppend>())?,
            excess,
            mul(excess, size_of::<IndexedFrameAppend>())?,
        )?;
        self.observed("append-capacity")?;
        self.pay(
            size_of::<IndexedFrameAppend>(),
            1,
            size_of::<IndexedFrameAppend>(),
        )?;
        self.new_append = Some(IndexedFrameAppend {
            tenant: self.tenant.take().ok_or(Failure::State)?,
            first_offset: self.input.first_offset(),
            last_offset: self.input.last_offset(),
            record_count: self.input.record_count().get(),
            frames: take(&mut self.frames),
            next_checkpoint: Some(self.next),
        });
        self.observed("staged-append")
    }
    fn prepay_commit(&mut self) -> Result<()> {
        // Pay every commit movement and the actual final insert probe BEFORE
        // removing anything from outside staging. No callbacks/checks/allocation
        // occur between push, watermark, map, cache retirement and outcome save.
        let cap = self.staged_map.as_ref().ok_or(Failure::State)?.capacity();
        self.probe::<MapEntry>(cap)?;
        self.pay(
            0,
            0,
            add(
                size_of::<IndexedFrameAppend>(),
                add(3 * size_of::<FrameMap>(), 2 * size_of::<ActiveCache>())?,
            )?,
        )?;
        if self.new_append.is_none()
            || self.target.is_none()
            || self.empty_cache.is_none()
            || self.staged_map.is_none()
        {
            return Err(Failure::State);
        }
        self.check()
    }
    fn commit(&mut self) -> Result<()> {
        self.observed("before-publication")?;
        let mut target = self.target.take().ok_or(Failure::State)?;
        let append = self.new_append.take().ok_or(Failure::State)?;
        let frames = append.frames.len();
        target.appends.push(append); // admitted actual capacity, cannot allocate
        target.indexed_through = Some(self.input.last_offset());
        // The only native table methods here hash fixed integer keys with the
        // retained foldhash state; neither user code nor allocation is entered.
        let mut map = self.staged_map.take().ok_or(Failure::State)?;
        map.insert(self.input.topic_partition(), target);
        self.stripe.indexed_frame_partitions = map;
        self.stripe.next_frame_id = self.next_frame_id;
        self.retired_cache = self
            .empty_cache
            .take()
            .map(|empty| replace(&mut self.stripe.active_partition_cache, empty));
        self.outcome = Some(LogIndexPublicationOutcome::Published {
            frames,
            indexed_through: self.input.last_offset(),
        });
        Ok(())
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests;

#[cfg(test)]
impl LogStripe {
    pub(crate) fn replay_frame_ids(&self, partition: TopicPartition) -> Vec<u64> {
        self.indexed_frame_partitions
            .get(&partition)
            .into_iter()
            .flat_map(|partition| partition.appends.iter())
            .flat_map(|append| append.frames.iter().map(|frame| frame.frame_id))
            .collect()
    }
}
