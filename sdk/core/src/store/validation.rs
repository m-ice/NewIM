use super::*;

fn id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
pub(crate) fn valid_identity(value: &str) -> bool {
    id(value)
}

fn blob(b: &Blob) -> bool {
    b.0.len() <= MAX_VALUE_BYTES
}
fn pending(p: &Pending) -> bool {
    id(&p.sender_id) && id(&p.client_id) && id(&p.conversation_id) && blob(&p.payload)
}
pub(crate) fn message(m: &Message, insert: bool) -> bool {
    id(&m.server_id)
        && id(&m.sender_id)
        && id(&m.client_id)
        && id(&m.conversation_id)
        && m.sequence >= 0
        && m.server_time >= 0
        && (1..=i32::MAX as u32).contains(&m.schema_version)
        && !m.message_type.is_empty()
        && m.message_type.len() <= 64
        && m.message_type.as_bytes()[0].is_ascii_lowercase()
        && m.message_type
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && m.payload.as_ref().is_none_or(blob)
        && (!insert || m.payload.is_some())
}
fn limit(n: usize) -> bool {
    (1..=MAX_ITEMS).contains(&n)
}

fn message_lookup(query: &MessageSyncQuery) -> bool {
    id(&query.conversation_id)
        && query.lookups.len() <= MAX_ITEMS
        && query
            .lookups
            .iter()
            .all(|key| id(&key.server_id) && id(&key.sender_id) && id(&key.client_id))
}

fn gap_attempt(attempt: &GapAttempt) -> bool {
    (1..=i64::MAX as u64).contains(&attempt.operation_id)
        && (1..=i64::MAX as u64).contains(&attempt.generation)
        && attempt.after_seq >= 0
        && attempt.expires_at_ms <= i64::MAX as u64
        && limit(attempt.limit)
}

fn message_sync_batch(batch: &MessageSyncBatch) -> bool {
    batch.expected_revision <= i64::MAX as u64
        && batch.now_ms <= i64::MAX as u64
        && id(&batch.conversation_id)
        && match &batch.mutation {
            MessageSyncMutation::Observe(write) => {
                let observed = match write {
                    MessageWrite::Insert(message) | MessageWrite::PreserveExisting(message) => {
                        message
                    }
                };
                observed.conversation_id == batch.conversation_id
                    && observed.sequence > 0
                    && message(observed, matches!(write, MessageWrite::Insert(_)))
            }
            MessageSyncMutation::StartAttempt | MessageSyncMutation::Resume => true,
            MessageSyncMutation::CommitPage { attempt, page } => {
                gap_attempt(attempt) && page.validate(&batch.conversation_id, attempt).is_ok()
            }
            MessageSyncMutation::FailAttempt { attempt, .. } => gap_attempt(attempt),
        }
}

fn sync_cursor(cursor: &Blob) -> bool {
    (1..=MAX_SYNC_CURSOR_BYTES).contains(&cursor.0.len())
}

fn sync_position(position: &ConversationSyncPosition) -> bool {
    (1..=i64::MAX as u64).contains(&position.round)
        && match &position.phase {
            ConversationSyncPhase::BootstrapStart => true,
            ConversationSyncPhase::BootstrapPage(cursor)
            | ConversationSyncPhase::DeltaCheckpoint(cursor)
            | ConversationSyncPhase::DeltaPage(cursor) => sync_cursor(cursor),
        }
}

pub(super) fn sync_item(item: &ConversationSyncItem) -> bool {
    id(&item.conversation_id)
        && (1..=i64::MAX as u64).contains(&item.revision)
        && item.known_clear_through_seq <= i64::MAX as u64
        && match &item.change {
            ConversationSyncChange::Remove => true,
            ConversationSyncChange::Upsert(summary) => {
                let latest = summary.latest_conversation_seq;
                latest <= i64::MAX as u64
                    && (latest == 0) == summary.latest_server_msg_id.is_none()
                    && summary
                        .latest_server_msg_id
                        .as_ref()
                        .is_none_or(|key| id(key))
                    && summary
                        .last_read_seq
                        .is_none_or(|seq| seq <= i64::MAX as u64 && (latest == 0 || seq <= latest))
                    && summary.settings.as_ref().is_none_or(|settings| {
                        settings.version <= i64::MAX as u64
                            && settings.clear_through_seq <= i64::MAX as u64
                            && settings.draft.len() <= MAX_CONVERSATION_DRAFT_BYTES
                    })
            }
        }
}

fn sync_query(query: &ConversationSyncQuery) -> bool {
    match query {
        ConversationSyncQuery::State => true,
        ConversationSyncQuery::Merge { ids } => {
            ids.len() <= MAX_ITEMS
                && ids.iter().all(|key| id(key))
                && ids.windows(2).all(|pair| pair[0] < pair[1])
        }
        ConversationSyncQuery::Published { after, limit: n } => {
            limit(*n) && after.as_ref().is_none_or(|key| id(key))
        }
    }
}

fn sync_batch(batch: &ConversationSyncBatch) -> bool {
    batch.expected_revision <= i64::MAX as u64
        && sync_position(&batch.expected_position)
        && match &batch.mutation {
            ConversationSyncMutation::BeginBootstrap => true,
            ConversationSyncMutation::CommitPage {
                next_cursor,
                has_more,
                items,
            } => {
                let repeats_cursor = match &batch.expected_position.phase {
                    ConversationSyncPhase::BootstrapStart => false,
                    ConversationSyncPhase::BootstrapPage(cursor)
                    | ConversationSyncPhase::DeltaCheckpoint(cursor)
                    | ConversationSyncPhase::DeltaPage(cursor) => cursor == next_cursor,
                };
                sync_cursor(next_cursor)
                    && (!has_more || !repeats_cursor)
                    && items.len() <= MAX_ITEMS
                    && items.iter().all(sync_item)
                    && items
                        .windows(2)
                        .all(|pair| pair[0].conversation_id < pair[1].conversation_id)
            }
        }
}

/// 先验证界限再分配请求序列 / Validate limits before allocating request identity bytes.
pub fn validate_request(r: &Request) -> Result<(), StoreError> {
    let valid = id(&r.fence.account)
        && id(&r.fence.instance)
        && (1..=i64::MAX as u64).contains(&r.fence.generation)
        && (1..=i64::MAX as u64).contains(&r.operation_id)
        && match &r.action {
            Action::Snapshot { conversation } => conversation.as_ref().is_none_or(|s| id(s)),
            Action::Lookup { server_id } => id(server_id),
            Action::Messages {
                conversation,
                after,
                limit: n,
            } => id(conversation) && after.is_none_or(|n| n >= 0) && limit(*n),
            Action::Pending { after, limit: n } => {
                after.as_ref().is_none_or(|s| {
                    s.len() <= 257
                        && s.bytes().all(|b| {
                            b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b':'
                        })
                }) && limit(*n)
            }
            Action::Enqueue(p) => pending(p),
            Action::Apply(b) => {
                b.expected_revision <= i64::MAX as u64
                    && b.messages.len() <= MAX_ITEMS
                    && b.conversations.len() <= MAX_ITEMS
                    && b.resolve_pending.len() <= MAX_ITEMS
                    && unique_batch(b)
                    && b.messages.iter().all(|w| match w {
                        MessageWrite::Insert(m) => message(m, true),
                        MessageWrite::PreserveExisting(m) => message(m, false),
                    })
                    && b.conversations
                        .iter()
                        .all(|c| id(&c.id) && c.unread <= i64::MAX as u64 && blob(&c.summary))
                    && b.cursor.as_ref().is_none_or(blob)
                    && b.resolve_pending
                        .iter()
                        .all(|p| id(&p.sender_id) && id(&p.client_id) && id(&p.server_id))
            }
            Action::Trim {
                conversation,
                through,
                limit: n,
            } => id(conversation) && *through >= 0 && limit(*n),
            Action::Compact { budget_ms } => (1..=30_000).contains(budget_ms),
            Action::AdvanceGeneration | Action::Metrics => true,
            Action::ConversationSyncSnapshot(query) => sync_query(query),
            Action::ApplyConversationSync(batch) => sync_batch(batch),
            Action::MessageSyncSnapshot(query) => message_lookup(query),
            Action::ApplyMessageSync(batch) => message_sync_batch(batch),
            Action::ApplyConversationSyncWithRemovals { batch, removed } => {
                sync_batch(batch)
                    && !batch.expected_position.is_bootstrap()
                    && limit(removed.len())
                    && removed.windows(2).all(|pair| pair[0] < pair[1])
                    && matches!(&batch.mutation, ConversationSyncMutation::CommitPage { items, .. }
                        if removed.iter().all(|id| items.iter().any(|item|
                            &item.conversation_id == id && item.change != ConversationSyncChange::Remove)))
            }
        };
    if !valid {
        return Err(StoreError::InvalidInput);
    }
    // At most 128 bounded payloads can reach this calculation; avoid cloning them.
    if encoded_len(r) > MAX_BATCH_BYTES {
        return Err(StoreError::CapacityExceeded);
    }
    Ok(())
}

struct Writer {
    bytes: Vec<u8>,
    len: usize,
    count_only: bool,
}
impl Writer {
    fn conversation_sync_batch(&mut self, batch: &ConversationSyncBatch) {
        self.number(batch.expected_revision);
        self.sync_position(&batch.expected_position);
        match &batch.mutation {
            ConversationSyncMutation::BeginBootstrap => self.number(0),
            ConversationSyncMutation::CommitPage {
                next_cursor,
                has_more,
                items,
            } => {
                self.number(1);
                self.data(&next_cursor.0);
                self.number(u64::from(*has_more));
                self.number(items.len() as u64);
                for item in items {
                    self.sync_item(item);
                }
            }
        }
    }

    fn message_sync_attempt(&mut self, attempt: &GapAttempt) {
        self.number(attempt.operation_id);
        self.number(attempt.generation);
        self.number(attempt.after_seq as u64);
        self.number(attempt.limit as u64);
        self.number(attempt.expires_at_ms);
    }

    fn message_write(&mut self, write: &MessageWrite) {
        match write {
            MessageWrite::Insert(message) => {
                self.number(0);
                self.message(message);
            }
            MessageWrite::PreserveExisting(message) => {
                self.number(1);
                self.message(message);
            }
        }
    }

    fn number(&mut self, n: u64) {
        self.raw(&n.to_le_bytes());
    }
    fn raw(&mut self, s: &[u8]) {
        self.len = self.len.saturating_add(s.len());
        if !self.count_only {
            self.bytes.extend_from_slice(s);
        }
    }
    fn data(&mut self, s: &[u8]) {
        self.number(s.len() as u64);
        self.raw(s);
    }
    fn text(&mut self, s: &str) {
        self.data(s.as_bytes());
    }
    fn optional(&mut self, b: &Option<Blob>) {
        self.number(u64::from(b.is_some()));
        if let Some(b) = b {
            self.data(&b.0);
        }
    }
    fn message(&mut self, m: &Message) {
        self.text(&m.server_id);
        self.text(&m.sender_id);
        self.text(&m.client_id);
        self.text(&m.conversation_id);
        self.number(m.sequence as u64);
        self.number(m.server_time as u64);
        self.number(m.schema_version as u64);
        self.text(&m.message_type);
        self.optional(&m.payload);
    }
    fn pending(&mut self, p: &Pending) {
        self.text(&p.sender_id);
        self.text(&p.client_id);
        self.text(&p.conversation_id);
        self.data(&p.payload.0);
    }
    fn sync_position(&mut self, position: &ConversationSyncPosition) {
        self.number(position.round);
        match &position.phase {
            ConversationSyncPhase::BootstrapStart => self.number(0),
            ConversationSyncPhase::BootstrapPage(cursor) => {
                self.number(1);
                self.data(&cursor.0);
            }
            ConversationSyncPhase::DeltaCheckpoint(cursor) => {
                self.number(2);
                self.data(&cursor.0);
            }
            ConversationSyncPhase::DeltaPage(cursor) => {
                self.number(3);
                self.data(&cursor.0);
            }
        }
    }
    fn sync_item(&mut self, item: &ConversationSyncItem) {
        self.text(&item.conversation_id);
        self.number(item.revision);
        self.number(item.known_clear_through_seq);
        match &item.change {
            ConversationSyncChange::Remove => self.number(0),
            ConversationSyncChange::Upsert(summary) => {
                self.number(1);
                self.number(summary.latest_conversation_seq);
                self.optional_text(summary.latest_server_msg_id.as_deref());
                self.number(u64::from(summary.last_read_seq.is_some()));
                if let Some(sequence) = summary.last_read_seq {
                    self.number(sequence);
                }
                self.number(u64::from(summary.settings.is_some()));
                if let Some(settings) = &summary.settings {
                    self.number(settings.version);
                    self.number(u64::from(settings.pinned));
                    self.number(u64::from(settings.muted));
                    self.text(&settings.draft);
                    self.number(settings.clear_through_seq);
                }
            }
        }
    }
    fn optional_text(&mut self, value: Option<&str>) {
        self.number(u64::from(value.is_some()));
        if let Some(text) = value {
            self.text(text);
        }
    }
    fn request(&mut self, r: &Request) {
        self.text(&r.fence.account);
        self.text(&r.fence.instance);
        self.number(r.fence.generation);
        self.number(r.operation_id);
        match &r.action {
            Action::Snapshot { conversation } => {
                self.number(0);
                self.number(u64::from(conversation.is_some()));
                if let Some(s) = conversation {
                    self.text(s);
                }
            }
            Action::Lookup { server_id } => {
                self.number(1);
                self.text(server_id);
            }
            Action::Messages {
                conversation,
                after,
                limit,
            } => {
                self.number(2);
                self.text(conversation);
                self.number(u64::from(after.is_some()));
                self.number(after.unwrap_or(0) as u64);
                self.number(*limit as u64);
            }
            Action::Pending { after, limit } => {
                self.number(3);
                self.number(u64::from(after.is_some()));
                if let Some(s) = after {
                    self.text(s);
                }
                self.number(*limit as u64);
            }
            Action::Enqueue(p) => {
                self.number(4);
                self.pending(p);
            }
            Action::Apply(b) => {
                self.number(5);
                self.number(b.expected_revision);
                self.number(b.messages.len() as u64);
                for w in &b.messages {
                    self.message_write(w);
                }
                self.number(b.conversations.len() as u64);
                for c in &b.conversations {
                    self.text(&c.id);
                    self.number(c.unread);
                    self.data(&c.summary.0);
                }
                self.optional(&b.cursor);
                self.number(b.resolve_pending.len() as u64);
                for p in &b.resolve_pending {
                    self.text(&p.sender_id);
                    self.text(&p.client_id);
                    self.text(&p.server_id);
                }
            }
            Action::AdvanceGeneration => self.number(6),
            Action::Trim {
                conversation,
                through,
                limit,
            } => {
                self.number(7);
                self.text(conversation);
                self.number(*through as u64);
                self.number(*limit as u64);
            }
            Action::Metrics => self.number(8),
            Action::Compact { budget_ms } => {
                self.number(9);
                self.number(*budget_ms);
            }
            Action::ConversationSyncSnapshot(query) => {
                self.number(10);
                match query {
                    ConversationSyncQuery::State => self.number(0),
                    ConversationSyncQuery::Merge { ids } => {
                        self.number(1);
                        self.number(ids.len() as u64);
                        for id in ids {
                            self.text(id);
                        }
                    }
                    ConversationSyncQuery::Published { after, limit } => {
                        self.number(2);
                        self.optional_text(after.as_deref());
                        self.number(*limit as u64);
                    }
                }
            }
            Action::ApplyConversationSync(batch) => {
                self.number(11);
                self.conversation_sync_batch(batch);
            }
            Action::MessageSyncSnapshot(query) => {
                self.number(12);
                self.text(&query.conversation_id);
                self.number(query.lookups.len() as u64);
                for key in &query.lookups {
                    self.text(&key.server_id);
                    self.text(&key.sender_id);
                    self.text(&key.client_id);
                }
            }
            Action::ApplyMessageSync(batch) => {
                self.number(13);
                self.number(batch.expected_revision);
                self.text(&batch.conversation_id);
                self.number(batch.now_ms);
                match &batch.mutation {
                    MessageSyncMutation::Observe(write) => {
                        self.number(0);
                        self.message_write(write);
                    }
                    MessageSyncMutation::StartAttempt => self.number(1),
                    MessageSyncMutation::CommitPage { attempt, page } => {
                        self.number(2);
                        self.message_sync_attempt(attempt);
                        self.number(page.history_after_seq as u64);
                        self.number(page.latest_seq as u64);
                        self.number(u64::from(page.has_next));
                        self.number(u64::from(page.next_after_seq.is_some()));
                        self.number(page.next_after_seq.unwrap_or(0) as u64);
                        self.number(page.messages.len() as u64);
                        for write in &page.messages {
                            self.message_write(write);
                        }
                    }
                    MessageSyncMutation::FailAttempt { attempt, failure } => {
                        self.number(3);
                        self.message_sync_attempt(attempt);
                        self.number(match failure {
                            GapFailure::Temporary => 0,
                            GapFailure::InvalidCursor => 1,
                            GapFailure::Authentication => 2,
                            GapFailure::PageCapacity => 3,
                        });
                    }
                    MessageSyncMutation::Resume => self.number(4),
                }
            }
            Action::ApplyConversationSyncWithRemovals { batch, removed } => {
                self.number(14);
                self.conversation_sync_batch(batch);
                self.number(removed.len() as u64);
                for id in removed {
                    self.text(id);
                }
            }
        }
    }
}
fn encoded_len(r: &Request) -> usize {
    let mut w = Writer {
        bytes: Vec::new(),
        len: 0,
        count_only: true,
    };
    w.request(r);
    w.len
}
/// 用于最新操作重试的精确有界身份，不是JSON规范化 / Exact retry identity, not JSON canonicalization.
pub fn request_bytes(r: &Request) -> Result<Vec<u8>, StoreError> {
    validate_request(r)?;
    let mut w = Writer {
        bytes: Vec::with_capacity(encoded_len(r)),
        len: 0,
        count_only: false,
    };
    w.request(r);
    Ok(w.bytes)
}

// A duplicated in-batch insert must not return an Existing snapshot that will roll back.
fn unique_batch(b: &Batch) -> bool {
    use std::collections::BTreeSet;
    let mut servers = BTreeSet::new();
    let mut clients = BTreeSet::new();
    let mut sequences = BTreeSet::new();
    for write in &b.messages {
        let m = match write {
            MessageWrite::Insert(m) | MessageWrite::PreserveExisting(m) => m,
        };
        if !servers.insert(&m.server_id)
            || !clients.insert((&m.sender_id, &m.client_id))
            || !sequences.insert((&m.conversation_id, m.sequence))
        {
            return false;
        }
    }
    let mut conversations = BTreeSet::new();
    let mut resolutions = BTreeSet::new();
    b.conversations.iter().all(|c| conversations.insert(&c.id))
        && b.resolve_pending
            .iter()
            .all(|p| resolutions.insert((&p.sender_id, &p.client_id)))
}
