//! Sealed source views preserve live provenance and actual journal origins.
use super::*;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::DurableAppend {}
    #[cfg(unix)]
    impl Sealed for super::JournalReplayInput {}
    #[cfg(unix)]
    impl Sealed for super::AdoptedAppend {}
}

pub(crate) enum LogInputRef<'a> {
    Live(&'a DurableAppend),
    #[cfg(unix)]
    Journal(&'a JournalReplayInput),
}

/// Only the actual live append and the closed anchored replay producer can
/// supply a source. These views confer no WAL identity or current authority.
pub(crate) trait LogPublicationInput: sealed::Sealed + Send + 'static {
    fn source(&self) -> LogInputRef<'_>;
    fn live(&self) -> Option<&DurableAppend> {
        match self.source() {
            LogInputRef::Live(input) => Some(input),
            #[cfg(unix)]
            LogInputRef::Journal(_) => None,
        }
    }
    #[cfg(unix)]
    fn journal(&self) -> Option<&JournalReplayInput> {
        match self.source() {
            LogInputRef::Journal(input) => Some(input),
            LogInputRef::Live(_) => None,
        }
    }
    fn payload(&self) -> &Bytes {
        match self.source() {
            LogInputRef::Live(input) => &input.payload,
            #[cfg(unix)]
            LogInputRef::Journal(input) => input.payload(),
        }
    }
    fn context(&self) -> Option<&Bytes> {
        match self.source() {
            LogInputRef::Live(input) => input.transient_context.as_ref(),
            #[cfg(unix)]
            LogInputRef::Journal(_) => None,
        }
    }
    fn topic_partition(&self) -> TopicPartition {
        match self.source() {
            LogInputRef::Live(input) => input.topic_partition(),
            #[cfg(unix)]
            LogInputRef::Journal(input) => input.topic_partition(),
        }
    }
    fn first_offset(&self) -> LogicalOffset {
        match self.source() {
            LogInputRef::Live(input) => input.reservation.first_offset,
            #[cfg(unix)]
            LogInputRef::Journal(input) => input.first_offset(),
        }
    }
    fn last_offset(&self) -> LogicalOffset {
        match self.source() {
            LogInputRef::Live(input) => input.reservation.last_offset,
            #[cfg(unix)]
            LogInputRef::Journal(input) => input.last_offset(),
        }
    }
    fn record_count(&self) -> NonZeroU32 {
        match self.source() {
            LogInputRef::Live(input) => input.reservation.record_count,
            #[cfg(unix)]
            LogInputRef::Journal(input) => input.record_count(),
        }
    }
    #[cfg(unix)]
    fn adopted_for(
        &self,
        _: &Arc<crate::AnchoredSinkJournal>,
        _: &Arc<Mutex<std::collections::HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    ) -> bool {
        false
    }
}
impl LogPublicationInput for DurableAppend {
    fn source(&self) -> LogInputRef<'_> {
        LogInputRef::Live(self)
    }
}
#[cfg(unix)]
impl LogPublicationInput for JournalReplayInput {
    fn source(&self) -> LogInputRef<'_> {
        LogInputRef::Journal(self)
    }
}
#[cfg(unix)]
impl LogPublicationInput for AdoptedAppend {
    fn source(&self) -> LogInputRef<'_> {
        LogInputRef::Live(self.input())
    }
    fn adopted_for(
        &self,
        anchor: &Arc<crate::AnchoredSinkJournal>,
        checkpoints: &Arc<Mutex<std::collections::HashMap<TopicPartition, DurableSinkCheckpoint>>>,
    ) -> bool {
        self.adoption().permits(anchor, checkpoints)
    }
}
