use super::*;

fn id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn blob(b: &Blob) -> bool {
    b.0.len() <= MAX_VALUE_BYTES
}
fn pending(p: &Pending) -> bool {
    id(&p.sender_id) && id(&p.client_id) && id(&p.conversation_id) && blob(&p.payload)
}
fn message(m: &Message, insert: bool) -> bool {
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
                    match w {
                        MessageWrite::Insert(m) => {
                            self.number(0);
                            self.message(m);
                        }
                        MessageWrite::PreserveExisting(m) => {
                            self.number(1);
                            self.message(m);
                        }
                    }
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
