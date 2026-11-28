//! Host-driven message recovery over a wire-neutral local store.

use crate::outbox::{OutboxRecord, identity_matches};
use crate::store::{
    Action, Blob, Fence, GapAttempt, GapFailure, GapStatus, MAX_ITEMS, Message, MessageSyncBatch,
    MessageSyncLookup, MessageSyncMutation, MessageSyncPage, MessageSyncQuery, MessageSyncSnapshot,
    MessageWrite, StoreError,
};

pub struct IncomingMessage {
    pub protocol_version: u32,
    pub message: Message,
}

pub struct Page {
    pub history_after_seq: i64,
    pub latest_seq: i64,
    pub has_next: bool,
    pub next_after_seq: Option<i64>,
    pub messages: Vec<IncomingMessage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    pub conversation_id: String,
    pub after_seq: i64,
    pub limit: usize,
}

pub type ComparePayload = fn(&Blob, &Blob) -> Result<bool, StoreError>;

pub fn snapshot_query(
    conversation_id: &str,
    incoming: &[IncomingMessage],
) -> Result<Action, StoreError> {
    if incoming.len() > MAX_ITEMS {
        return Err(StoreError::CapacityExceeded);
    }
    let mut lookups = Vec::with_capacity(incoming.len());
    for item in incoming {
        let message = &item.message;
        if message.conversation_id != conversation_id || message.sequence <= 0 {
            return Err(StoreError::InvalidInput);
        }
        lookups.push(MessageSyncLookup {
            server_id: message.server_id.clone(),
            sender_id: message.sender_id.clone(),
            client_id: message.client_id.clone(),
        });
    }
    Ok(Action::MessageSyncSnapshot(MessageSyncQuery {
        conversation_id: conversation_id.to_owned(),
        lookups,
    }))
}

pub fn observe(
    fence: &Fence,
    snapshot: &MessageSyncSnapshot,
    incoming: IncomingMessage,
    now_ms: u64,
    compare_payload: ComparePayload,
) -> Result<Action, StoreError> {
    let write = merge_message(fence, snapshot, incoming, compare_payload)?;
    batch(snapshot, now_ms, MessageSyncMutation::Observe(write))
}

/// Schedule history from a published directory target without observing a message.
pub fn observe_target(
    snapshot: &MessageSyncSnapshot,
    now_ms: u64,
    published_round: u64,
    through_seq: i64,
) -> Result<Action, StoreError> {
    if !(1..=i64::MAX as u64).contains(&published_round) || through_seq < 0 {
        return Err(StoreError::InvalidInput);
    }
    batch(
        snapshot,
        now_ms,
        MessageSyncMutation::ObserveTarget {
            published_round,
            through_seq,
        },
    )
}

pub fn schedule(snapshot: &MessageSyncSnapshot, now_ms: u64) -> Result<Option<Action>, StoreError> {
    if !snapshot.visible {
        return Ok(None);
    }
    let state = &snapshot.state;
    let due = match state.job.as_ref().map(|job| &job.status) {
        None => state.highest_observed_seq > state.after_seq(),
        Some(GapStatus::Waiting { until_ms }) => now_ms >= *until_ms,
        Some(GapStatus::InFlight(attempt)) => now_ms >= attempt.expires_at_ms,
        Some(GapStatus::Stopped | GapStatus::AuthRecovery) => false,
    };
    due.then(|| batch(snapshot, now_ms, MessageSyncMutation::StartAttempt))
        .transpose()
}

pub fn resume(snapshot: &MessageSyncSnapshot, now_ms: u64) -> Result<Action, StoreError> {
    batch(snapshot, now_ms, MessageSyncMutation::Resume)
}

/// A transport request may be dispatched after its attempt has been durably recorded.
pub struct InFlightPage {
    fence: Fence,
    conversation_id: String,
    attempt: GapAttempt,
}

impl InFlightPage {
    pub fn started(
        fence: &Fence,
        snapshot: &MessageSyncSnapshot,
        operation_id: u64,
    ) -> Option<Self> {
        let GapStatus::InFlight(attempt) = &snapshot.state.job.as_ref()?.status else {
            return None;
        };
        (snapshot.visible
            && attempt.operation_id == operation_id
            && attempt.generation == fence.generation)
            .then(|| Self {
                fence: fence.clone(),
                conversation_id: snapshot.conversation_id.clone(),
                attempt: attempt.clone(),
            })
    }

    pub fn request(&self) -> PageRequest {
        PageRequest {
            conversation_id: self.conversation_id.clone(),
            after_seq: self.attempt.after_seq,
            limit: self.attempt.limit,
        }
    }

    pub fn apply(
        &self,
        fence: &Fence,
        snapshot: &MessageSyncSnapshot,
        page: Page,
        now_ms: u64,
        compare_payload: ComparePayload,
    ) -> Result<Action, StoreError> {
        self.check_current(fence, snapshot, now_ms)?;
        if page.messages.len() > self.attempt.limit {
            return Err(StoreError::InvalidInput);
        }
        let messages = page
            .messages
            .into_iter()
            .map(|message| merge_message(fence, snapshot, message, compare_payload))
            .collect::<Result<Vec<_>, _>>()?;
        let page = MessageSyncPage {
            history_after_seq: page.history_after_seq,
            latest_seq: page.latest_seq,
            has_next: page.has_next,
            next_after_seq: page.next_after_seq,
            messages,
        };
        page.validate(&self.conversation_id, &self.attempt)?;
        batch(
            snapshot,
            now_ms,
            MessageSyncMutation::CommitPage {
                attempt: self.attempt.clone(),
                page,
            },
        )
    }

    pub fn fail(
        &self,
        fence: &Fence,
        snapshot: &MessageSyncSnapshot,
        now_ms: u64,
        failure: GapFailure,
    ) -> Result<Action, StoreError> {
        self.check_current(fence, snapshot, now_ms)?;
        batch(
            snapshot,
            now_ms,
            MessageSyncMutation::FailAttempt {
                attempt: self.attempt.clone(),
                failure,
            },
        )
    }

    fn check_current(
        &self,
        fence: &Fence,
        snapshot: &MessageSyncSnapshot,
        now_ms: u64,
    ) -> Result<(), StoreError> {
        if self.fence != *fence {
            return Err(StoreError::StaleGeneration);
        }
        if snapshot.conversation_id != self.conversation_id
            || !snapshot.visible
            || now_ms >= self.attempt.expires_at_ms
            || !matches!(&snapshot.state.job, Some(job)
                if matches!(&job.status, GapStatus::InFlight(attempt) if attempt == &self.attempt))
        {
            return Err(StoreError::StaleRevision);
        }
        Ok(())
    }
}

fn batch(
    snapshot: &MessageSyncSnapshot,
    now_ms: u64,
    mutation: MessageSyncMutation,
) -> Result<Action, StoreError> {
    if !snapshot.visible {
        return Err(StoreError::StaleRevision);
    }
    Ok(Action::ApplyMessageSync(MessageSyncBatch {
        expected_revision: snapshot.revision,
        conversation_id: snapshot.conversation_id.clone(),
        now_ms,
        mutation,
    }))
}

fn merge_message(
    fence: &Fence,
    snapshot: &MessageSyncSnapshot,
    incoming: IncomingMessage,
    compare_payload: ComparePayload,
) -> Result<MessageWrite, StoreError> {
    let mut message = incoming.message;
    message.validate()?;
    if message.conversation_id != snapshot.conversation_id || message.sequence <= 0 {
        return Err(StoreError::InvalidInput);
    }
    let wire = message.payload.as_ref().ok_or(StoreError::InvalidInput)?;
    if let Some(pending) = snapshot.pending.iter().find(|pending| {
        pending.sender_id == message.sender_id && pending.client_id == message.client_id
    }) {
        let record = OutboxRecord::decode(pending).map_err(|_| StoreError::Corrupt)?;
        if record.fence.account != fence.account
            || record.fence.instance != fence.instance
            || record.intent.conversation_id != message.conversation_id
            || record.intent.protocol_version != incoming.protocol_version
            || record.intent.schema_version != message.schema_version
            || record.intent.message_type != message.message_type
            || (wire != &record.intent.payload && !compare_payload(wire, &record.intent.payload)?)
        {
            return Err(StoreError::IdentityConflict);
        }
        message.payload = Some(record.intent.payload);
    }
    if let Some(existing) = snapshot
        .messages
        .iter()
        .find(|existing| existing.server_id == message.server_id)
    {
        if !identity_matches(existing, &message) {
            return Err(StoreError::IdentityConflict);
        }
        if let Some(payload) = &existing.payload {
            let incoming = message.payload.as_ref().ok_or(StoreError::InvalidInput)?;
            if payload != incoming && !compare_payload(payload, incoming)? {
                return Err(StoreError::IdentityConflict);
            }
        }
        return Ok(MessageWrite::PreserveExisting(existing.clone()));
    }
    Ok(MessageWrite::Insert(message))
}
