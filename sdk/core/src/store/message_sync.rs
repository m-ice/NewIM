//! Typed state for bounded message history recovery.

use super::{Message, MessageWrite, Pending, StoreError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSyncLookup {
    pub server_id: String,
    pub sender_id: String,
    pub client_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSyncQuery {
    pub conversation_id: String,
    pub lookups: Vec<MessageSyncLookup>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSyncSnapshot {
    pub revision: u64,
    pub conversation_id: String,
    pub visible: bool,
    pub state: MessageSyncState,
    pub messages: Vec<Message>,
    pub pending: Vec<Pending>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageSyncState {
    pub continuous_seq: i64,
    pub history_after_seq: i64,
    pub highest_observed_seq: i64,
    pub job: Option<GapJob>,
}

impl MessageSyncState {
    pub fn after_seq(&self) -> i64 {
        self.continuous_seq.max(self.history_after_seq)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GapJob {
    pub started_at_ms: u64,
    pub failures: u8,
    pub limit: usize,
    pub status: GapStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GapStatus {
    Waiting { until_ms: u64 },
    InFlight(GapAttempt),
    AuthRecovery,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GapAttempt {
    pub operation_id: u64,
    pub generation: u64,
    pub after_seq: i64,
    pub limit: usize,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GapFailure {
    Temporary,
    InvalidCursor,
    Authentication,
    PageCapacity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSyncBatch {
    pub expected_revision: u64,
    pub conversation_id: String,
    pub now_ms: u64,
    pub mutation: MessageSyncMutation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageSyncMutation {
    Observe(MessageWrite),
    StartAttempt,
    CommitPage {
        attempt: GapAttempt,
        page: MessageSyncPage,
    },
    FailAttempt {
        attempt: GapAttempt,
        failure: GapFailure,
    },
    Resume,
    ObserveTarget {
        published_round: u64,
        through_seq: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSyncPage {
    pub history_after_seq: i64,
    pub latest_seq: i64,
    pub has_next: bool,
    pub next_after_seq: Option<i64>,
    pub messages: Vec<MessageWrite>,
}

impl MessageSyncPage {
    pub fn validate(&self, conversation_id: &str, attempt: &GapAttempt) -> Result<(), StoreError> {
        let mut sequence = attempt.after_seq.max(self.history_after_seq);
        if attempt.after_seq < 0
            || self.history_after_seq < 0
            || sequence > self.latest_seq
            || self.messages.len() > attempt.limit
        {
            return Err(StoreError::InvalidInput);
        }
        let mut last_seq = None;
        for write in &self.messages {
            let message = match write {
                MessageWrite::Insert(message) | MessageWrite::PreserveExisting(message) => message,
            };
            sequence = sequence.checked_add(1).ok_or(StoreError::InvalidInput)?;
            if message.conversation_id != conversation_id
                || message.sequence != sequence
                || sequence > self.latest_seq
                || !super::validation::message(message, matches!(write, MessageWrite::Insert(_)))
            {
                return Err(StoreError::InvalidInput);
            }
            last_seq = Some(sequence);
        }
        if self.next_after_seq != last_seq
            || (self.has_next && (last_seq.is_none() || sequence == self.latest_seq))
            || (!self.has_next && sequence != self.latest_seq)
        {
            return Err(StoreError::InvalidInput);
        }
        Ok(())
    }
}
