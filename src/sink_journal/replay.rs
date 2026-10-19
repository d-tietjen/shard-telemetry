//! Exact immutable journal-origin input, distinct from a live WAL append.

use super::*;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Private construction retains the actual anchored row. No reservation,
/// producer identity, atomic decision or delivery metadata is invented.
pub(crate) struct JournalReplayInput {
    anchor: Arc<AnchoredSinkJournal>,
    row: usize,
    records: NonZeroU32,
}
impl std::fmt::Debug for JournalReplayInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalReplayInput")
            .field("row", &self.row)
            .finish_non_exhaustive()
    }
}
impl AnchoredSinkJournal {
    /// Source-visible replay capacities for the composing bounded census.
    pub(crate) fn replay_capacity(&self) -> usize {
        self.recovered_capacity()
    }
    /// Derive only an exact supported journal edge. The caller prepays this
    /// value and its Arc ownership before entry and retains it before post-E.
    pub(crate) fn replay_input(self: &Arc<Self>, row: usize) -> Option<JournalReplayInput> {
        let transaction = self.recovered().get(row)?;
        if transaction.appends.len() != 1 {
            return None;
        }
        let append = &transaction.appends[0];
        let expected = transaction.expected;
        let next = transaction.next;
        if append.topic_partition != expected.topic_partition
            || next.topic_partition != expected.topic_partition
            || append.topic_partition.topic_id != crate::TelemetrySignal::Logs.topic_id()
            || append.first_offset < expected.next_offset
            || expected.next_placement_sequence.get().checked_add(1)
                != Some(next.next_placement_sequence.get())
        {
            return None;
        }
        let records = NonZeroU32::new(
            u32::try_from(
                next.next_offset
                    .get()
                    .checked_sub(append.first_offset.get())?,
            )
            .ok()?,
        )?;
        Some(JournalReplayInput {
            anchor: Arc::clone(self),
            row,
            records,
        })
    }
}
impl JournalReplayInput {
    pub(crate) fn anchor(&self) -> &Arc<AnchoredSinkJournal> {
        &self.anchor
    }
    pub(crate) fn expected(&self) -> DurableSinkCheckpoint {
        self.anchor.recovered()[self.row].expected
    }
    pub(crate) fn next(&self) -> DurableSinkCheckpoint {
        self.anchor.recovered()[self.row].next
    }
    pub(crate) fn topic_partition(&self) -> TopicPartition {
        self.expected().topic_partition
    }
    pub(crate) fn first_offset(&self) -> LogicalOffset {
        self.anchor.recovered()[self.row].appends[0].first_offset
    }
    pub(crate) fn last_offset(&self) -> LogicalOffset {
        LogicalOffset::new(self.next().next_offset.get() - 1)
    }
    pub(crate) fn record_count(&self) -> NonZeroU32 {
        self.records
    }
    pub(crate) fn payload(&self) -> &Bytes {
        &self.anchor.recovered()[self.row].appends[0].payload
    }
}
