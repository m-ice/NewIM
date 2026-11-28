//! Typed state for bounded message history recovery.

use super::{Blob, Message, MessageWrite, Pending, StoreError};

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
    pub visibility_revision: i64,
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
    ApplyVisibility(VisibilityPage),
}

pub const HIDDEN_MESSAGE_TYPE: &str = "message_hidden";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VisibilityAction {
    Recall,
    DeleteForMe,
    AdminRemove,
}

impl VisibilityAction {
    pub const fn code(self) -> u8 {
        match self {
            Self::Recall => 1,
            Self::DeleteForMe => 2,
            Self::AdminRemove => 3,
        }
    }

    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Recall),
            2 => Some(Self::DeleteForMe),
            3 => Some(Self::AdminRemove),
            _ => None,
        }
    }

    pub fn from_payload(payload: &Blob) -> Option<Self> {
        [Self::Recall, Self::DeleteForMe, Self::AdminRemove]
            .into_iter()
            .find(|action| action.payload() == payload.0)
    }

    pub fn hide(self, message: &mut Message) {
        message.message_type = HIDDEN_MESSAGE_TYPE.into();
        message.schema_version = 1;
        message.payload = Some(Blob(self.payload().to_vec()));
    }

    const fn payload(self) -> &'static [u8] {
        match self {
            Self::Recall => br#"{"action":"recall","countAsUnread":false}"#,
            Self::DeleteForMe => br#"{"action":"deleteForMe","countAsUnread":false}"#,
            Self::AdminRemove => br#"{"action":"adminRemove","countAsUnread":false}"#,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibilityEvent {
    pub server_id: String,
    pub sequence: i64,
    pub revision: i64,
    pub action: VisibilityAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibilityPage {
    pub published_round: u64,
    pub after_revision: i64,
    pub next_revision: i64,
    pub through_revision: i64,
    pub events: Vec<VisibilityEvent>,
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
