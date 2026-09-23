//! Platform-neutral send outbox state machine.
//! 平台中立的发送 outbox 状态机；不解析 JSON、不持有连接或 SQLite。

mod envelope;

use crate::message::{
    ConnectionGeneration, PersistedAck, SendContext, SendDisposition, SendFailure, SendIntent,
};
use crate::store::{
    Batch, Blob, Fence, LocalStore, Message, MessageWrite, Pending, PendingIdentity,
    PendingMutation, PendingMutationStore, PendingReceipt, PendingRemoval, PendingResolution,
    Request, Response, StoreError,
};
pub use envelope::{MAX_ENVELOPE_BYTES, decode_envelope, encode_envelope};
use std::fmt;

pub const MAX_ATTEMPTS: u8 = 8;
pub const MAX_RETRY_AGE_MS: u64 = 24 * 60 * 60 * 1_000;
pub const BASE_RETRY_DELAY_MS: u64 = 1_000;
pub const MAX_RETRY_DELAY_MS: u64 = 5 * 60 * 1_000;
pub const IN_FLIGHT_TIMEOUT_MS: u64 = 5 * 60 * 1_000;

pub const RETRY_EXHAUSTED: &str = "OUTBOX_RETRY_EXHAUSTED";
pub const RECORD_INVALID: &str = "OUTBOX_RECORD_INVALID";
pub const ACK_INTENT_UNVERIFIED: &str = "OUTBOX_ACK_INTENT_UNVERIFIED";
pub const STORE_GENERATION_CHANGED: &str = "STORE_GENERATION_CHANGED";
pub const AUTH_RECOVERY_RESUMED: &str = "AUTH_RECOVERY_RESUMED";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxState {
    Ready,
    InFlight,
    RetryWait,
    AuthRecovery,
    PermanentFailure,
}
impl OutboxState {
    pub const fn as_byte(self) -> u8 {
        match self {
            Self::Ready => 0,
            Self::InFlight => 1,
            Self::RetryWait => 2,
            Self::AuthRecovery => 3,
            Self::PermanentFailure => 4,
        }
    }
    pub const fn from_byte(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Ready),
            1 => Some(Self::InFlight),
            2 => Some(Self::RetryWait),
            3 => Some(Self::AuthRecovery),
            4 => Some(Self::PermanentFailure),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxOperation {
    Enqueue,
    PlanDispatch,
    Failure,
    Resume,
    Ack,
    Remove,
    Load,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxObservation {
    pub operation: OutboxOperation,
    pub state: Option<OutboxState>,
    pub error_class: Option<String>,
    pub attempt_bucket: u8,
    pub elapsed_bucket: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutboxError {
    RecordInvalid,
    RecordTooLarge,
    InvalidInput,
    GenerationMismatch,
    TransitionInvalid,
    RetryExhausted,
    TerminalRequired,
    AckIntentUnverified,
    AckCorrelationMismatch,
    AckResultConflict,
    FailureCorrelationMismatch,
    UnexpectedResponse,
    Store(StoreError),
}
impl OutboxError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::RecordInvalid => RECORD_INVALID,
            Self::RecordTooLarge => "OUTBOX_RECORD_TOO_LARGE",
            Self::InvalidInput => "OUTBOX_INVALID_INPUT",
            Self::GenerationMismatch => "OUTBOX_GENERATION_MISMATCH",
            Self::TransitionInvalid => "OUTBOX_TRANSITION_INVALID",
            Self::RetryExhausted => RETRY_EXHAUSTED,
            Self::TerminalRequired => "OUTBOX_TERMINAL_REQUIRED",
            Self::AckIntentUnverified => ACK_INTENT_UNVERIFIED,
            Self::AckCorrelationMismatch => "OUTBOX_ACK_CORRELATION_MISMATCH",
            Self::AckResultConflict => "OUTBOX_ACK_RESULT_CONFLICT",
            Self::FailureCorrelationMismatch => "OUTBOX_FAILURE_CORRELATION_MISMATCH",
            Self::UnexpectedResponse => "OUTBOX_UNEXPECTED_RESPONSE",
            Self::Store(error) => error.code(),
        }
    }
}
impl fmt::Display for OutboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for OutboxError {}
impl From<StoreError> for OutboxError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OutboxRecord {
    /// Trusted sender, always equal to the one-account store fence account.
    pub sender_id: String,
    pub intent: SendIntent,
    /// Original enqueue fence; retained for audit after explicit recovery.
    pub fence: Fence,
    /// Original enqueue connection generation; retained for audit.
    pub connection_generation: ConnectionGeneration,
    /// Fence used for dispatch eligibility after an explicit recovery.
    pub active_fence: Fence,
    /// Connection generation used for dispatch eligibility after recovery.
    pub active_connection_generation: ConnectionGeneration,
    pub state: OutboxState,
    pub attempts: u8,
    pub created_at_ms: u64,
    pub deadline_ms: u64,
    pub last_code: String,
}
impl fmt::Debug for OutboxRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboxRecord")
            .field("sender_id", &"redacted")
            .field("intent", &self.intent)
            .field("fence", &"redacted")
            .field("connection_generation", &"redacted")
            .field("active_fence", &"redacted")
            .field("active_connection_generation", &"redacted")
            .field("state", &self.state)
            .field("attempts", &self.attempts)
            .field("created_at_ms", &self.created_at_ms)
            .field("deadline_ms", &self.deadline_ms)
            .field("last_code", &self.last_code)
            .finish()
    }
}
impl OutboxRecord {
    pub fn new(
        intent: SendIntent,
        context: &SendContext,
        now_ms: u64,
    ) -> Result<Self, OutboxError> {
        intent.validate().map_err(|_| OutboxError::InvalidInput)?;
        context.validate().map_err(|_| OutboxError::InvalidInput)?;
        let fence = context.fence.clone();
        Ok(Self {
            sender_id: context.sender_id.clone(),
            intent,
            fence: fence.clone(),
            connection_generation: context.connection_generation,
            active_fence: fence,
            active_connection_generation: context.connection_generation,
            state: OutboxState::Ready,
            attempts: 0,
            created_at_ms: now_ms,
            deadline_ms: now_ms,
            last_code: String::new(),
        })
    }

    pub fn decode(pending: &Pending) -> Result<Self, OutboxError> {
        let record = decode_envelope(&pending.payload.0)?;
        if record.intent.client_id != pending.client_id
            || record.intent.conversation_id != pending.conversation_id
            || pending.sender_id != record.fence.account
            || record.active_fence.account != record.fence.account
            || record.active_fence.instance != record.fence.instance
        {
            return Err(OutboxError::RecordInvalid);
        }
        let decoded = Self {
            sender_id: pending.sender_id.clone(),
            ..record
        };
        decoded
            .identity()
            .validate()
            .map_err(|_| OutboxError::RecordInvalid)?;
        Ok(decoded)
    }

    pub fn encode(&self) -> Result<Blob, OutboxError> {
        Ok(Blob(encode_envelope(self)?))
    }

    pub fn identity(&self) -> PendingIdentity {
        PendingIdentity {
            sender_id: self.sender_id.clone(),
            client_id: self.intent.client_id.clone(),
            conversation_id: self.intent.conversation_id.clone(),
        }
    }

    /// 仅返回有界分类元数据 / Return bounded classification metadata only.
    pub fn observation(&self, operation: OutboxOperation, now_ms: u64) -> OutboxObservation {
        let elapsed_ms = now_ms.saturating_sub(self.created_at_ms);
        let elapsed_bucket = match elapsed_ms {
            0 => 0,
            1..=999 => 1,
            1_000..=4_999 => 2,
            5_000..=29_999 => 3,
            30_000..=299_999 => 4,
            300_000..=3_599_999 => 5,
            _ => 6,
        };
        OutboxObservation {
            operation,
            state: Some(self.state),
            error_class: (!self.last_code.is_empty()).then(|| self.last_code.clone()),
            attempt_bucket: self.attempts,
            elapsed_bucket,
        }
    }

    pub fn age_deadline(&self) -> Result<u64, OutboxError> {
        self.created_at_ms
            .checked_add(MAX_RETRY_AGE_MS)
            .ok_or(OutboxError::RetryExhausted)
    }

    /// ACK/resume/removal authority: same account/instance and trusted sender, no connection gen.
    fn ensure_identity(&self, context: &SendContext) -> Result<(), OutboxError> {
        context.validate().map_err(|_| OutboxError::InvalidInput)?;
        if self.sender_id != context.sender_id
            || self.sender_id != self.fence.account
            || self.fence.account != context.fence.account
            || self.fence.instance != context.fence.instance
            || self.active_fence.account != self.fence.account
            || self.active_fence.instance != self.fence.instance
        {
            return Err(OutboxError::GenerationMismatch);
        }
        Ok(())
    }

    fn store_generation_changed(&self, context: &SendContext, current_fence: &Fence) -> bool {
        self.active_fence != *current_fence
            || self.active_fence.generation != context.fence.generation
    }

    fn connection_generation_matches(&self, context: &SendContext) -> bool {
        self.active_connection_generation == context.connection_generation
    }

    fn with_terminal(&self, code: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.state = OutboxState::PermanentFailure;
        next.deadline_ms = 0;
        next.last_code = code.into();
        next
    }

    fn with_retry_wait(&self, deadline_ms: u64, code: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.state = OutboxState::RetryWait;
        next.deadline_ms = deadline_ms;
        next.last_code = code.into();
        next
    }

    fn with_auth_recovery(&self, code: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.state = OutboxState::AuthRecovery;
        next.deadline_ms = 0;
        next.last_code = code.into();
        next
    }

    fn retry_deadline(&self, now_ms: u64, delay_ms: u64) -> Result<u64, OutboxError> {
        let age_deadline = self.age_deadline()?;
        let floor = self
            .created_at_ms
            .checked_add(1)
            .ok_or(OutboxError::RetryExhausted)?;
        let candidate = now_ms
            .checked_add(delay_ms)
            .ok_or(OutboxError::RetryExhausted)?;
        let deadline = candidate.max(floor);
        if deadline >= age_deadline {
            Err(OutboxError::RetryExhausted)
        } else {
            Ok(deadline)
        }
    }

    fn with_in_flight(&self, now_ms: u64) -> Result<Self, OutboxError> {
        let age_deadline = self.age_deadline()?;
        if now_ms < self.created_at_ms || now_ms >= age_deadline || self.attempts >= MAX_ATTEMPTS {
            return Err(OutboxError::RetryExhausted);
        }
        let deadline = now_ms
            .checked_add(IN_FLIGHT_TIMEOUT_MS)
            .ok_or(OutboxError::RetryExhausted)?
            .min(age_deadline);
        if deadline <= self.created_at_ms {
            return Err(OutboxError::RetryExhausted);
        }
        let mut next = self.clone();
        next.state = OutboxState::InFlight;
        next.attempts = self
            .attempts
            .checked_add(1)
            .ok_or(OutboxError::RetryExhausted)?;
        next.deadline_ms = deadline;
        next.last_code.clear();
        Ok(next)
    }

    fn dispatch_plan(
        &self,
        context: &SendContext,
        current_fence: &Fence,
        now_ms: u64,
    ) -> Result<Plan, OutboxError> {
        self.ensure_identity(context)?;
        if self.state == OutboxState::PermanentFailure {
            return Ok(Plan::Terminal(self.last_code.clone()));
        }
        if self.state == OutboxState::AuthRecovery {
            return Ok(Plan::AuthRecovery(self.last_code.clone()));
        }
        if self.store_generation_changed(context, current_fence) {
            return Ok(Plan::PersistAuthRecovery(
                self.with_auth_recovery(STORE_GENERATION_CHANGED),
            ));
        }
        if !self.connection_generation_matches(context) {
            return Err(OutboxError::GenerationMismatch);
        }
        let age_deadline = match self.age_deadline() {
            Ok(deadline) => deadline,
            Err(_) => return Ok(Plan::TerminalRecord(self.with_terminal(RETRY_EXHAUSTED))),
        };
        if self.attempts >= MAX_ATTEMPTS || now_ms >= age_deadline {
            return Ok(Plan::TerminalRecord(self.with_terminal(RETRY_EXHAUSTED)));
        }
        if (self.state == OutboxState::InFlight || self.state == OutboxState::RetryWait)
            && now_ms < self.deadline_ms
        {
            return Ok(Plan::Wait(self.deadline_ms));
        }
        if now_ms < self.created_at_ms {
            return Ok(Plan::Wait(self.deadline_ms.max(self.created_at_ms)));
        }
        match self.with_in_flight(now_ms) {
            Ok(next) => Ok(Plan::Send(next)),
            Err(OutboxError::RetryExhausted) => {
                Ok(Plan::TerminalRecord(self.with_terminal(RETRY_EXHAUSTED)))
            }
            Err(error) => Err(error),
        }
    }

    fn failure_plan(
        &self,
        context: &SendContext,
        current_fence: &Fence,
        failure: &SendFailure,
        now_ms: u64,
    ) -> Result<Plan, OutboxError> {
        self.ensure_identity(context)?;
        failure.validate().map_err(|_| OutboxError::InvalidInput)?;
        if failure.client_id != self.intent.client_id
            || failure.conversation_id != self.intent.conversation_id
        {
            return Err(OutboxError::FailureCorrelationMismatch);
        }
        if self.state == OutboxState::PermanentFailure {
            return Ok(Plan::Terminal(self.last_code.clone()));
        }
        if self.state == OutboxState::AuthRecovery {
            return Ok(Plan::AuthRecovery(self.last_code.clone()));
        }
        if self.store_generation_changed(context, current_fence) {
            return Ok(Plan::PersistAuthRecovery(
                self.with_auth_recovery(STORE_GENERATION_CHANGED),
            ));
        }
        if !self.connection_generation_matches(context) {
            return Err(OutboxError::GenerationMismatch);
        }
        if self.state != OutboxState::InFlight {
            return Err(OutboxError::TransitionInvalid);
        }
        if failure.disposition == SendDisposition::PermanentFailure {
            return Ok(Plan::Persist(self.with_terminal(failure.code.clone())));
        }
        if failure.disposition == SendDisposition::AuthRecovery {
            return Ok(Plan::Persist(self.with_auth_recovery(failure.code.clone())));
        }
        let age_deadline = match self.age_deadline() {
            Ok(deadline) => deadline,
            Err(_) => return Ok(Plan::Persist(self.with_terminal(RETRY_EXHAUSTED))),
        };
        if self.attempts >= MAX_ATTEMPTS || now_ms >= age_deadline {
            return Ok(Plan::Persist(self.with_terminal(RETRY_EXHAUSTED)));
        }
        let shift = self.attempts.saturating_sub(1).min(31);
        let delay = BASE_RETRY_DELAY_MS
            .checked_shl(u32::from(shift))
            .ok_or(OutboxError::RetryExhausted)?
            .min(MAX_RETRY_DELAY_MS);
        let deadline = match self.retry_deadline(now_ms, delay) {
            Ok(deadline) => deadline,
            Err(_) => return Ok(Plan::Persist(self.with_terminal(RETRY_EXHAUSTED))),
        };
        Ok(Plan::Persist(
            self.with_retry_wait(deadline, failure.code.clone()),
        ))
    }
}

#[derive(Clone, PartialEq, Eq)]
enum Plan {
    Send(OutboxRecord),
    Persist(OutboxRecord),
    PersistAuthRecovery(OutboxRecord),
    Wait(u64),
    AuthRecovery(String),
    Terminal(String),
    TerminalRecord(OutboxRecord),
}
impl fmt::Debug for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Send(_) => f.write_str("Send"),
            Self::Persist(_) => f.write_str("Persist"),
            Self::PersistAuthRecovery(_) => f.write_str("PersistAuthRecovery"),
            Self::Wait(deadline) => f.debug_tuple("Wait").field(deadline).finish(),
            Self::AuthRecovery(code) => f.debug_tuple("AuthRecovery").field(code).finish(),
            Self::Terminal(code) => f.debug_tuple("Terminal").field(code).finish(),
            Self::TerminalRecord(_) => f.write_str("TerminalRecord"),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum OutboxTransition {
    Send { intent: SendIntent },
    Wait { until_ms: u64 },
    AuthRecovery { code: String },
    Terminal { code: String },
}
impl fmt::Debug for OutboxTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Send { .. } => f.write_str("Send"),
            Self::Wait { until_ms } => f.debug_struct("Wait").field("until_ms", until_ms).finish(),
            Self::AuthRecovery { code } => {
                f.debug_struct("AuthRecovery").field("code", code).finish()
            }
            Self::Terminal { code } => f.debug_struct("Terminal").field("code", code).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckResolution {
    Resolved(PendingReceipt),
}

fn execute<S: LocalStore>(store: &mut S, request: Request) -> Result<Response, OutboxError> {
    store.submit(request.clone())?;
    let completion = store
        .take_completion()
        .ok_or(OutboxError::Store(StoreError::CommitOutcomeUnknown))?;
    if completion.fence != request.fence || completion.operation_id != request.operation_id {
        return Err(OutboxError::Store(StoreError::CommitOutcomeUnknown));
    }
    completion.result.map_err(OutboxError::Store)
}

fn persist<S: PendingMutationStore>(
    store: &mut S,
    record: &OutboxRecord,
    expected_revision: u64,
) -> Result<PendingReceipt, OutboxError> {
    let payload = record.encode()?;
    store
        .update_pending(PendingMutation {
            expected_revision,
            identity: record.identity(),
            payload,
        })
        .map_err(Into::into)
}

/// Persist one new immutable intent using the existing LocalStore enqueue action.
pub fn enqueue<S: LocalStore>(
    store: &mut S,
    operation_id: u64,
    context: &SendContext,
    intent: SendIntent,
    now_ms: u64,
) -> Result<OutboxRecord, OutboxError> {
    context.validate().map_err(|_| OutboxError::InvalidInput)?;
    let record = OutboxRecord::new(intent, context, now_ms)?;
    let request = Request {
        fence: context.fence.clone(),
        operation_id,
        action: crate::store::Action::Enqueue(Pending {
            sender_id: context.sender_id.clone(),
            client_id: record.intent.client_id.clone(),
            conversation_id: record.intent.conversation_id.clone(),
            payload: record.encode()?,
        }),
    };
    match execute(store, request)? {
        Response::Committed(_) => Ok(record),
        Response::ExistingPending(existing) => {
            if existing.conversation_id != record.intent.conversation_id {
                return Err(OutboxError::InvalidInput);
            }
            let decoded = OutboxRecord::decode(&existing)?;
            decoded.ensure_identity(context)?;
            if decoded.intent == record.intent {
                Ok(decoded)
            } else {
                Err(OutboxError::InvalidInput)
            }
        }
        _ => Err(OutboxError::UnexpectedResponse),
    }
}

/// Persist an InFlight attempt with revision CAS before returning send intent.
pub fn plan_dispatch<S: PendingMutationStore>(
    store: &mut S,
    pending: &Pending,
    expected_revision: u64,
    context: &SendContext,
    now_ms: u64,
) -> Result<OutboxTransition, OutboxError> {
    let record = OutboxRecord::decode(pending)?;
    let current_fence = store.current_fence().clone();
    match record.dispatch_plan(context, &current_fence, now_ms)? {
        Plan::Send(next) => {
            let intent = next.intent.clone();
            persist(store, &next, expected_revision)?;
            Ok(OutboxTransition::Send { intent })
        }
        Plan::PersistAuthRecovery(next) => {
            let code = next.last_code.clone();
            persist(store, &next, expected_revision)?;
            Ok(OutboxTransition::AuthRecovery { code })
        }
        Plan::Persist(_) => Err(OutboxError::UnexpectedResponse),
        Plan::Wait(until_ms) => Ok(OutboxTransition::Wait { until_ms }),
        Plan::AuthRecovery(code) => Ok(OutboxTransition::AuthRecovery { code }),
        Plan::Terminal(code) => Ok(OutboxTransition::Terminal { code }),
        Plan::TerminalRecord(next) => {
            persist(store, &next, expected_revision)?;
            Ok(OutboxTransition::Terminal {
                code: RETRY_EXHAUSTED.into(),
            })
        }
    }
}

/// Apply a classified send failure and persist the next bounded state.
pub fn apply_failure<S: PendingMutationStore>(
    store: &mut S,
    pending: &Pending,
    expected_revision: u64,
    context: &SendContext,
    failure: &SendFailure,
    now_ms: u64,
) -> Result<OutboxTransition, OutboxError> {
    let record = OutboxRecord::decode(pending)?;
    let current_fence = store.current_fence().clone();
    match record.failure_plan(context, &current_fence, failure, now_ms)? {
        Plan::Persist(next) => {
            let transition = match next.state {
                OutboxState::RetryWait => OutboxTransition::Wait {
                    until_ms: next.deadline_ms,
                },
                OutboxState::AuthRecovery => OutboxTransition::AuthRecovery {
                    code: next.last_code.clone(),
                },
                OutboxState::PermanentFailure => OutboxTransition::Terminal {
                    code: next.last_code.clone(),
                },
                _ => return Err(OutboxError::TransitionInvalid),
            };
            persist(store, &next, expected_revision)?;
            Ok(transition)
        }
        Plan::PersistAuthRecovery(next) => {
            let code = next.last_code.clone();
            persist(store, &next, expected_revision)?;
            Ok(OutboxTransition::AuthRecovery { code })
        }
        Plan::Terminal(code) => Ok(OutboxTransition::Terminal { code }),
        Plan::AuthRecovery(code) => Ok(OutboxTransition::AuthRecovery { code }),
        _ => Err(OutboxError::TransitionInvalid),
    }
}

fn require_current_fence<S: PendingMutationStore>(
    store: &S,
    context: &SendContext,
) -> Result<(), OutboxError> {
    if context.fence == *store.current_fence() {
        Ok(())
    } else {
        Err(OutboxError::GenerationMismatch)
    }
}

/// Explicit host recovery: bind a non-terminal record to the current connection generation.
///
/// This is a one-row revision-CAS transition. It requires the same trusted sender and exact
/// current active store account/instance/generation fence, accepts only Ready/InFlight/RetryWait, and changes
/// only the active connection binding. The immutable intent, client identity, attempts,
/// original enqueue time, deadline and last code are preserved. Terminal/auth states must use
/// their existing resume/remove paths instead.
pub fn rebind_connection<S: PendingMutationStore>(
    store: &mut S,
    pending: &Pending,
    expected_revision: u64,
    context: &SendContext,
) -> Result<PendingReceipt, OutboxError> {
    require_current_fence(store, context)?;
    let record = OutboxRecord::decode(pending)?;
    record.ensure_identity(context)?;
    if record.active_fence != context.fence {
        return Err(OutboxError::GenerationMismatch);
    }
    if !matches!(
        record.state,
        OutboxState::Ready | OutboxState::InFlight | OutboxState::RetryWait
    ) {
        return Err(OutboxError::TransitionInvalid);
    }
    if record.active_connection_generation == context.connection_generation {
        return Err(OutboxError::TransitionInvalid);
    }
    let mut next = record;
    next.active_connection_generation = context.connection_generation;
    persist(store, &next, expected_revision)
}

/// Explicitly resume auth recovery; no automatic retry occurs before this call.
pub fn resume_auth<S: PendingMutationStore>(
    store: &mut S,
    pending: &Pending,
    expected_revision: u64,
    context: &SendContext,
    now_ms: u64,
) -> Result<OutboxTransition, OutboxError> {
    require_current_fence(store, context)?;
    let record = OutboxRecord::decode(pending)?;
    record.ensure_identity(context)?;
    if record.state != OutboxState::AuthRecovery {
        return if record.state == OutboxState::PermanentFailure {
            Ok(OutboxTransition::Terminal {
                code: record.last_code,
            })
        } else {
            Err(OutboxError::TransitionInvalid)
        };
    }
    let exhausted = match record.age_deadline() {
        Ok(deadline) => record.attempts >= MAX_ATTEMPTS || now_ms >= deadline,
        Err(_) => true,
    };
    if exhausted {
        let next = record.with_terminal(RETRY_EXHAUSTED);
        persist(store, &next, expected_revision)?;
        return Ok(OutboxTransition::Terminal {
            code: RETRY_EXHAUSTED.into(),
        });
    }
    let deadline = match record.retry_deadline(now_ms, 0) {
        Ok(deadline) => deadline,
        Err(_) => {
            let next = record.with_terminal(RETRY_EXHAUSTED);
            persist(store, &next, expected_revision)?;
            return Ok(OutboxTransition::Terminal {
                code: RETRY_EXHAUSTED.into(),
            });
        }
    };
    let mut next = record.clone();
    next.active_fence = context.fence.clone();
    next.active_connection_generation = context.connection_generation;
    next.state = OutboxState::RetryWait;
    next.deadline_ms = deadline;
    next.last_code = AUTH_RECOVERY_RESUMED.into();
    persist(store, &next, expected_revision)?;
    Ok(OutboxTransition::Wait { until_ms: deadline })
}

/// Remove only an explicitly terminal/auth-recovery record; queued work cannot be deleted.
pub fn remove_terminal<S: PendingMutationStore>(
    store: &mut S,
    pending: &Pending,
    expected_revision: u64,
    context: &SendContext,
) -> Result<PendingReceipt, OutboxError> {
    require_current_fence(store, context)?;
    let record = OutboxRecord::decode(pending)?;
    record.ensure_identity(context)?;
    if !matches!(
        record.state,
        OutboxState::PermanentFailure | OutboxState::AuthRecovery
    ) {
        return Err(OutboxError::TerminalRequired);
    }
    store
        .remove_pending(PendingRemoval {
            expected_revision,
            identity: record.identity(),
        })
        .map_err(Into::into)
}

/// Resolve one exact persisted ACK through a single existing store Batch.
pub fn resolve_ack<S: LocalStore>(
    store: &mut S,
    operation_id: u64,
    pending: &Pending,
    expected_revision: u64,
    context: &SendContext,
    ack: &PersistedAck,
) -> Result<AckResolution, OutboxError> {
    let record = OutboxRecord::decode(pending)?;
    // ACKs are authoritative for the same account/instance even after reconnect.
    record.ensure_identity(context)?;
    ack.validate().map_err(|_| OutboxError::InvalidInput)?;
    if ack.sender_id != context.sender_id
        || ack.client_id != record.intent.client_id
        || ack.conversation_id != record.intent.conversation_id
        || ack.protocol_version != record.intent.protocol_version
        || ack.schema_version != record.intent.schema_version
        || ack.message_type != record.intent.message_type
        || ack.payload != record.intent.payload
    {
        return Err(OutboxError::AckCorrelationMismatch);
    }

    let message = Message {
        server_id: ack.server_id.clone(),
        sender_id: context.sender_id.clone(),
        client_id: record.intent.client_id.clone(),
        conversation_id: record.intent.conversation_id.clone(),
        sequence: ack.sequence,
        server_time: ack.server_time,
        schema_version: ack.schema_version,
        message_type: ack.message_type.clone(),
        payload: Some(record.intent.payload.clone()),
    };
    let request = Request {
        fence: context.fence.clone(),
        operation_id,
        action: crate::store::Action::Apply(Batch {
            expected_revision,
            messages: vec![MessageWrite::Insert(message.clone())],
            conversations: vec![],
            cursor: None,
            resolve_pending: vec![PendingResolution {
                sender_id: context.sender_id.clone(),
                client_id: record.intent.client_id.clone(),
                server_id: ack.server_id.clone(),
            }],
        }),
    };
    match execute(store, request) {
        Ok(Response::Committed(receipt)) => Ok(AckResolution::Resolved(PendingReceipt {
            revision: receipt.revision,
        })),
        Ok(Response::Existing(existing)) => {
            if !identity_matches(&existing, &message) {
                return Err(OutboxError::AckResultConflict);
            }
            match &existing.payload {
                None => Err(OutboxError::AckIntentUnverified),
                Some(payload) if payload == &record.intent.payload => {
                    let request = Request {
                        fence: context.fence.clone(),
                        operation_id,
                        action: crate::store::Action::Apply(Batch {
                            expected_revision,
                            messages: vec![MessageWrite::PreserveExisting(existing)],
                            conversations: vec![],
                            cursor: None,
                            resolve_pending: vec![PendingResolution {
                                sender_id: context.sender_id.clone(),
                                client_id: record.intent.client_id.clone(),
                                server_id: ack.server_id.clone(),
                            }],
                        }),
                    };
                    match execute(store, request)? {
                        Response::Committed(receipt) => {
                            Ok(AckResolution::Resolved(PendingReceipt {
                                revision: receipt.revision,
                            }))
                        }
                        _ => Err(OutboxError::UnexpectedResponse),
                    }
                }
                Some(_) => Err(OutboxError::AckResultConflict),
            }
        }
        Ok(_) => Err(OutboxError::UnexpectedResponse),
        Err(error) => Err(error),
    }
}

fn identity_matches(existing: &Message, expected: &Message) -> bool {
    existing.server_id == expected.server_id
        && existing.sender_id == expected.sender_id
        && existing.client_id == expected.client_id
        && existing.conversation_id == expected.conversation_id
        && existing.sequence == expected.sequence
        && existing.server_time == expected.server_time
        && existing.schema_version == expected.schema_version
        && existing.message_type == expected.message_type
}

#[cfg(test)]
mod conformance;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_bytes_are_closed() {
        for byte in 0..=4 {
            assert!(OutboxState::from_byte(byte).is_some());
        }
        assert!(OutboxState::from_byte(5).is_none());
    }
}
