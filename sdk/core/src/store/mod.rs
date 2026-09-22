//! 原子本地存储端口 / Atomic local storage port, independent of physical storage.

mod validation;
pub use validation::{request_bytes, validate_request};

use std::fmt;

pub const MAX_ITEMS: usize = 64;
pub const MAX_VALUE_BYTES: usize = 65_536;
pub const MAX_BATCH_BYTES: usize = 262_144;
pub const MAX_PENDING: usize = 256;

/// 原始字节；Debug 不泄露正文 / Opaque bytes with content-free diagnostics.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Blob(pub Vec<u8>);
impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Blob")
            .field("bytes", &self.0.len())
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Fence {
    pub account: String,
    pub instance: String,
    pub generation: u64,
}
impl fmt::Debug for Fence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fence")
            .field("identity", &"redacted")
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreError {
    InvalidInput,
    IdentityConflict,
    StaleRevision,
    StaleGeneration,
    OperationConflict,
    OperationExpired,
    Busy,
    CapacityExceeded,
    StorageFull,
    Io,
    Corrupt,
    UnsupportedSchema,
    UnsupportedEngine,
    RecoveryRequired,
    CommitOutcomeUnknown,
    Cancelled,
}
impl StoreError {
    /// 稳定且无输入内容的错误码 / Stable codes without input data.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "STORE_INVALID_INPUT",
            Self::IdentityConflict => "STORE_IDENTITY_CONFLICT",
            Self::StaleRevision => "STORE_STALE_REVISION",
            Self::StaleGeneration => "STORE_STALE_GENERATION",
            Self::OperationConflict => "STORE_OPERATION_CONFLICT",
            Self::OperationExpired => "STORE_OPERATION_EXPIRED",
            Self::Busy => "STORE_BUSY",
            Self::CapacityExceeded => "STORE_CAPACITY_EXCEEDED",
            Self::StorageFull => "STORE_FULL",
            Self::Io => "STORE_IO",
            Self::Corrupt => "STORE_CORRUPT",
            Self::UnsupportedSchema => "STORE_UNSUPPORTED_SCHEMA",
            Self::UnsupportedEngine => "STORE_UNSUPPORTED_ENGINE",
            Self::RecoveryRequired => "STORE_RECOVERY_REQUIRED",
            Self::CommitOutcomeUnknown => "STORE_COMMIT_OUTCOME_UNKNOWN",
            Self::Cancelled => "STORE_CANCELLED",
        }
    }
}
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for StoreError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub server_id: String,
    pub sender_id: String,
    pub client_id: String,
    pub conversation_id: String,
    pub sequence: i64,
    pub server_time: i64,
    pub schema_version: u32,
    pub message_type: String,
    /// None 表示已裁剪缓存 / None means the payload cache was trimmed.
    pub payload: Option<Blob>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    pub sender_id: String,
    pub client_id: String,
    pub conversation_id: String,
    pub payload: Blob,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conversation {
    pub id: String,
    pub unread: u64,
    pub summary: Blob,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MessageWrite {
    /// 仅插入新身份；既存返回 Existing / Insert new identity or return Existing.
    Insert(Message),
    /// 明确保留读取过的完整快照 / Preserve an explicitly read storage snapshot.
    PreserveExisting(Message),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingResolution {
    pub sender_id: String,
    pub client_id: String,
    pub server_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    pub expected_revision: u64,
    pub messages: Vec<MessageWrite>,
    /// 去重后计算的绝对值，不是增量 / Absolute values computed after deduplication.
    pub conversations: Vec<Conversation>,
    pub cursor: Option<Blob>,
    /// 调用方已验证权威确认；存储不解释 ACK / Caller verifies authority, not storage.
    pub resolve_pending: Vec<PendingResolution>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Snapshot {
        conversation: Option<String>,
    },
    Lookup {
        server_id: String,
    },
    Messages {
        conversation: String,
        after: Option<i64>,
        limit: usize,
    },
    Pending {
        after: Option<String>,
        limit: usize,
    },
    Enqueue(Pending),
    Apply(Batch),
    AdvanceGeneration,
    Trim {
        conversation: String,
        through: i64,
        limit: usize,
    },
    Metrics,
    Compact {
        budget_ms: u64,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    pub fence: Fence,
    pub operation_id: u64,
    pub action: Action,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub revision: u64,
    pub cursor: Blob,
    pub conversation: Option<Conversation>,
    pub recovery_required: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub revision: u64,
    pub affected: u32,
    pub replayed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessagePage {
    pub items: Vec<Message>,
    /// 下一页从最后返回的序列继续 / Continue after the last returned sequence.
    pub next: Option<i64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPage {
    pub items: Vec<Pending>,
    /// 有序复合键；不可解析为业务ID / Opaque ordered compound key.
    pub next: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Metrics {
    pub revision: u64,
    pub page_count: u64,
    pub free_pages: u64,
    pub page_size: u64,
    pub database_bytes: u64,
    pub wal_bytes: u64,
    pub pending_count: u64,
    pub recovery_required: bool,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Snapshot(Snapshot),
    Found(Option<Message>),
    Messages(MessagePage),
    Pending(PendingPage),
    Committed(Receipt),
    Existing(Message),
    ExistingPending(Pending),
    Generation(Fence),
    Metrics(Metrics),
    Compacted,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Completion {
    pub fence: Fence,
    pub operation_id: u64,
    pub result: Result<Response, StoreError>,
}
impl Completion {
    /// 旧实例完成不可发布 / Do not publish completions for a replaced instance.
    pub fn belongs_to(&self, current: &Fence) -> bool {
        &self.fence == current
    }
}

/// 宿主调度请求/完成；不要求线程或运行时 / Host-dispatched requests/completions.
/// Native submit may block: dispatch it on the host storage worker.
pub trait LocalStore {
    fn submit(&mut self, request: Request) -> Result<(), StoreError>;
    fn take_completion(&mut self) -> Option<Completion>;
}

/// 待处理 outbox 的精确三字段身份 / Exact three-field identity for one pending row.
#[derive(Clone, PartialEq, Eq)]
pub struct PendingIdentity {
    pub sender_id: String,
    pub client_id: String,
    pub conversation_id: String,
}
impl PendingIdentity {
    pub fn validate(&self) -> Result<(), StoreError> {
        if validation::valid_identity(&self.sender_id)
            && validation::valid_identity(&self.client_id)
            && validation::valid_identity(&self.conversation_id)
        {
            Ok(())
        } else {
            Err(StoreError::InvalidInput)
        }
    }
}
impl fmt::Debug for PendingIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingIdentity")
            .field("sender_id", &"redacted")
            .field("client_id", &"redacted")
            .field("conversation_id", &"redacted")
            .finish()
    }
}

/// 替换一条 pending payload 的 CAS 请求 / Revision-CAS replacement of one pending payload.
#[derive(Clone, PartialEq, Eq)]
pub struct PendingMutation {
    pub expected_revision: u64,
    pub identity: PendingIdentity,
    pub payload: Blob,
}
impl PendingMutation {
    pub fn validate(&self) -> Result<(), StoreError> {
        self.identity.validate()?;
        if self.expected_revision > i64::MAX as u64 || self.payload.0.len() > MAX_VALUE_BYTES {
            return Err(StoreError::InvalidInput);
        }
        Ok(())
    }
}
impl fmt::Debug for PendingMutation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingMutation")
            .field("expected_revision", &self.expected_revision)
            .field("identity", &self.identity)
            .field("payload_len", &self.payload.0.len())
            .finish()
    }
}

/// 显式移除一条 terminal/auth-recovery pending 的 CAS 请求。
/// Revision-CAS removal reserved for explicit terminal/auth-recovery dismissal.
#[derive(Clone, PartialEq, Eq)]
pub struct PendingRemoval {
    pub expected_revision: u64,
    pub identity: PendingIdentity,
}
impl PendingRemoval {
    pub fn validate(&self) -> Result<(), StoreError> {
        self.identity.validate()?;
        if self.expected_revision > i64::MAX as u64 {
            return Err(StoreError::InvalidInput);
        }
        Ok(())
    }
}
impl fmt::Debug for PendingRemoval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingRemoval")
            .field("expected_revision", &self.expected_revision)
            .field("identity", &self.identity)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingReceipt {
    pub revision: u64,
}

/// Additive pending mutation port; existing `LocalStore` and exhaustive `Action` stay unchanged.
/// 增量 pending 变更端口；不改既有 LocalStore 与穷尽 Action。
pub trait PendingMutationStore {
    fn update_pending(&mut self, mutation: PendingMutation) -> Result<PendingReceipt, StoreError>;
    fn remove_pending(&mut self, removal: PendingRemoval) -> Result<PendingReceipt, StoreError>;
}
