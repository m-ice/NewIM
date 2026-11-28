use crate::message::{ConnectionGeneration, SendContext};
use crate::outbox::{BASE_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS};
use crate::store::{
    ConversationSyncChange, ConversationSyncPhase, ConversationSyncSnapshot, Fence,
    MessageSyncSnapshot, StoreError,
};
use std::fmt;

/// Time at READY required to reset the connection retry interval.
pub const STABLE_READY_MS: u64 = 30_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub fence: Fence,
    pub connection_generation: ConnectionGeneration,
}

impl Ticket {
    fn send_context(&self) -> SendContext {
        SendContext {
            sender_id: self.fence.account.clone(),
            fence: self.fence.clone(),
            connection_generation: self.connection_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthCause {
    Missing,
    Expired,
    Rejected,
    Revoked,
    DeviceLimit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Temporary,
    ConversationForbidden,
    Authentication(AuthCause),
    Protocol,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Subscription {
    conversation_id: String,
    published_round: u64,
    after_seq: i64,
    through_seq: i64,
}

impl Subscription {
    pub fn conversation_id(&self) -> &str {
        &self.conversation_id
    }

    pub const fn published_round(&self) -> u64 {
        self.published_round
    }

    pub const fn after_seq(&self) -> i64 {
        self.after_seq
    }

    pub const fn through_seq(&self) -> i64 {
        self.through_seq
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("published_round", &self.published_round)
            .field("after_seq", &self.after_seq)
            .field("through_seq", &self.through_seq)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedDirectory(Option<Subscription>);

impl CommittedDirectory {
    /// Both snapshots must be read through the ticket's store fence.
    pub fn from_store(
        directory: &ConversationSyncSnapshot,
        selected: Option<&MessageSyncSnapshot>,
    ) -> Result<Self, StoreError> {
        let round = directory
            .published_round
            .filter(|&round| round > 0 && round == directory.position.round)
            .filter(|_| {
                matches!(
                    directory.position.phase,
                    ConversationSyncPhase::DeltaCheckpoint(_)
                )
            })
            .ok_or(StoreError::StaleRevision)?;
        let Some(messages) = selected else {
            return Ok(Self(None));
        };
        let row = directory
            .items
            .iter()
            .find(|row| row.conversation_id == messages.conversation_id)
            .ok_or(StoreError::StaleRevision)?;
        let ConversationSyncChange::Upsert(summary) = &row.change else {
            return Err(StoreError::StaleRevision);
        };
        if !messages.visible || messages.revision != directory.revision {
            return Err(StoreError::StaleRevision);
        }
        row.validate()?;
        let after_seq = messages.state.after_seq();
        let through_seq =
            i64::try_from(summary.latest_conversation_seq).map_err(|_| StoreError::InvalidInput)?;
        if after_seq < 0 {
            return Err(StoreError::InvalidInput);
        }
        Ok(Self(Some(Subscription {
            conversation_id: messages.conversation_id.clone(),
            published_round: round,
            after_seq,
            through_seq,
        })))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    RecoverCredentials {
        ticket: Ticket,
        cause: AuthCause,
    },
    AdvanceFence(Fence),
    Unavailable(StoreError),
    LoadDirectory(Ticket),
    OpenSocket(Ticket),
    Subscribe {
        ticket: Ticket,
        target: Subscription,
    },
    RecoverMessages {
        ticket: Ticket,
        target: Subscription,
    },
    Cancel(Ticket),
    Ready(Ticket),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Offline,
    Idle,
    Backoff { until_ms: u64 },
    Credentials(AuthCause),
    AdvancingFence,
    Directory,
    Opening,
    Subscribing,
    Recovering,
    Ready,
    Closed,
}

enum Phase {
    Idle,
    Backoff(u64),
    Credentials(AuthCause),
    AdvancingFence(Option<AuthCause>),
    Directory,
    Opening(Subscription),
    Subscribing(Subscription),
    Recovering(Subscription),
    Ready(u64),
    Closed,
    Faulted,
}

pub enum Event<'a> {
    NetworkAvailable,
    NetworkChanged,
    NetworkLost,
    Tick,
    Resume,
    RefreshDirectory,
    Logout,
    FenceAdvanced {
        previous: &'a Fence,
        next: Fence,
    },
    Credentials {
        ticket: &'a Ticket,
        expires_at_ms: u64,
    },
    Directory {
        ticket: &'a Ticket,
        committed: CommittedDirectory,
    },
    SocketOpened(&'a Ticket),
    Subscribed(&'a Ticket),
    MessageProgress {
        ticket: &'a Ticket,
        snapshot: &'a MessageSyncSnapshot,
    },
    Failed {
        ticket: &'a Ticket,
        failure: Failure,
    },
}

/// One coordinator per store instance. A live host owns the credential, timers,
/// and transport. It returns completions with the ticket from each effect.
pub struct Connection {
    fence: Fence,
    last_generation: u64,
    active: Option<Ticket>,
    phase: Phase,
    online: bool,
    expires_at_ms: Option<u64>,
    auth_cause: AuthCause,
    failures: u8,
}

impl Connection {
    /// Pass the last issued generation when recreating the coordinator while host work can survive.
    pub fn new(fence: Fence, last_generation: u64) -> Result<Self, StoreError> {
        Ticket {
            fence: fence.clone(),
            connection_generation: ConnectionGeneration(last_generation.max(1)),
        }
        .send_context()
        .validate()?;
        Ok(Self {
            fence,
            last_generation,
            active: None,
            phase: Phase::Idle,
            online: false,
            expires_at_ms: None,
            auth_cause: AuthCause::Missing,
            failures: 0,
        })
    }

    pub fn status(&self) -> Status {
        match self.phase {
            Phase::AdvancingFence(_) => Status::AdvancingFence,
            Phase::Closed | Phase::Faulted => Status::Closed,
            _ if !self.online => Status::Offline,
            Phase::Idle => Status::Idle,
            Phase::Backoff(until_ms) => Status::Backoff { until_ms },
            Phase::Credentials(cause) => Status::Credentials(cause),
            Phase::Directory => Status::Directory,
            Phase::Opening(_) => Status::Opening,
            Phase::Subscribing(_) => Status::Subscribing,
            Phase::Recovering(_) => Status::Recovering,
            Phase::Ready(_) => Status::Ready,
        }
    }

    pub fn send_context(&self) -> Option<SendContext> {
        match (&self.phase, &self.active) {
            (Phase::Ready(_), Some(ticket)) => Some(ticket.send_context()),
            _ => None,
        }
    }

    /// The host passes milliseconds from a non-decreasing clock and fresh random bits on failure.
    pub fn update(
        &mut self,
        event: Event<'_>,
        now_ms: u64,
        random: u64,
    ) -> Result<Vec<Effect>, StoreError> {
        let mut effects = Vec::new();
        if let Phase::Ready(since_ms) = self.phase
            && now_ms.saturating_sub(since_ms) >= STABLE_READY_MS
        {
            self.failures = 0;
        }
        let auth_event = match &event {
            Event::Logout => true,
            Event::Failed {
                ticket,
                failure: Failure::Authentication(_),
            } => self.accepts(ticket),
            _ => false,
        };
        if !auth_event
            && (!matches!(self.phase, Phase::Closed) || matches!(event, Event::Resume))
            && self.expires_at_ms.is_some_and(|expiry| now_ms >= expiry)
        {
            self.advance_fence(Some(AuthCause::Expired), &mut effects);
        }
        match event {
            Event::NetworkAvailable => {
                self.online = true;
                self.start_if_due(now_ms, &mut effects);
            }
            Event::NetworkChanged => {
                self.online = true;
                if self.active.is_some() {
                    self.retry(now_ms, random, &mut effects);
                } else {
                    self.start_if_due(now_ms, &mut effects);
                }
            }
            Event::NetworkLost => {
                self.online = false;
                if self.active.is_some() {
                    self.cancel(&mut effects);
                    self.phase = Phase::Idle;
                }
            }
            Event::Tick => self.start_if_due(now_ms, &mut effects),
            Event::Resume => {
                if matches!(self.phase, Phase::Closed) {
                    self.phase = Phase::Idle;
                    self.auth_cause = AuthCause::Missing;
                }
                self.start_if_due(now_ms, &mut effects);
            }
            Event::RefreshDirectory if self.active.is_some() && self.expires_at_ms.is_some() => {
                self.cancel(&mut effects);
                self.phase = Phase::Idle;
                self.start_if_due(now_ms, &mut effects);
            }
            Event::Logout
                if !matches!(self.phase, Phase::Closed) || self.expires_at_ms.is_some() =>
            {
                self.advance_fence(None, &mut effects);
            }
            Event::FenceAdvanced { previous, next } if previous == &self.fence => {
                if let Phase::AdvancingFence(resume) = self.phase {
                    if next.account != previous.account
                        || next.instance != previous.instance
                        || next.generation != previous.generation + 1
                    {
                        return Err(StoreError::StaleGeneration);
                    }
                    self.fence = next;
                    self.phase = Phase::Closed;
                    if let Some(cause) = resume {
                        self.auth_cause = cause;
                        self.phase = Phase::Idle;
                        self.start_if_due(now_ms, &mut effects);
                    }
                }
            }
            Event::Credentials {
                ticket,
                expires_at_ms,
            } if self.accepts(ticket) => {
                if let Phase::Credentials(_) = self.phase {
                    if expires_at_ms <= now_ms {
                        self.auth_cause = AuthCause::Expired;
                        self.retry(now_ms, random, &mut effects);
                    } else {
                        self.expires_at_ms = Some(expires_at_ms);
                        self.phase = Phase::Directory;
                        effects.push(Effect::LoadDirectory(ticket.clone()));
                    }
                }
            }
            Event::Directory { ticket, committed }
                if self.accepts(ticket) && matches!(self.phase, Phase::Directory) =>
            {
                if let Some(target) = committed.0 {
                    self.phase = Phase::Opening(target);
                    effects.push(Effect::OpenSocket(ticket.clone()));
                } else {
                    self.ready(ticket, now_ms, &mut effects);
                }
            }
            Event::SocketOpened(ticket) if self.accepts(ticket) => {
                if let Phase::Opening(target) = &self.phase {
                    effects.push(Effect::Subscribe {
                        ticket: ticket.clone(),
                        target: target.clone(),
                    });
                    self.phase = Phase::Subscribing(target.clone());
                }
            }
            Event::Subscribed(ticket) if self.accepts(ticket) => {
                if let Phase::Subscribing(target) = &self.phase {
                    if target.after_seq < target.through_seq {
                        effects.push(Effect::RecoverMessages {
                            ticket: ticket.clone(),
                            target: target.clone(),
                        });
                        self.phase = Phase::Recovering(target.clone());
                    } else {
                        self.ready(ticket, now_ms, &mut effects);
                    }
                }
            }
            Event::MessageProgress { ticket, snapshot } if self.accepts(ticket) => {
                if let Phase::Recovering(target) = &self.phase
                    && snapshot.visible
                    && snapshot.conversation_id == target.conversation_id
                    && snapshot.state.after_seq() >= target.through_seq
                {
                    self.ready(ticket, now_ms, &mut effects);
                }
            }
            Event::Failed { ticket, failure } if self.accepts(ticket) => match failure {
                Failure::Authentication(cause) if self.expires_at_ms.is_some() => {
                    self.advance_fence(Some(cause), &mut effects);
                }
                Failure::Authentication(cause) => {
                    self.auth_cause = cause;
                    self.retry(now_ms, random, &mut effects);
                }
                Failure::Temporary | Failure::ConversationForbidden => {
                    self.retry(now_ms, random, &mut effects);
                }
                Failure::Protocol => {
                    self.cancel(&mut effects);
                    self.phase = Phase::Closed;
                }
            },
            _ => {}
        }
        Ok(effects)
    }

    /// Check before accepting incoming data or committing asynchronous sync results.
    /// Generation checking, clock updates and storage effects are serialized by the host.
    /// Persisted send ACKs use `accepts_ack` and the outbox's original correlation checks.
    pub fn accepts(&self, ticket: &Ticket) -> bool {
        self.active.as_ref() == Some(ticket)
    }

    /// Authorize ACK reconciliation from the same auth session after a transport change.
    /// Check and commit in the host's serialized event loop.
    pub fn accepts_ack(&self, context: &SendContext, now_ms: u64) -> bool {
        self.expires_at_ms.is_some_and(|expiry| now_ms < expiry)
            && !matches!(self.phase, Phase::Closed | Phase::Faulted)
            && context.fence == self.fence
            && context.validate().is_ok()
    }

    fn start_if_due(&mut self, now_ms: u64, effects: &mut Vec<Effect>) {
        if !self.online
            || !matches!(self.phase, Phase::Idle | Phase::Backoff(_))
            || matches!(self.phase, Phase::Backoff(until_ms) if now_ms < until_ms)
        {
            return;
        }
        if self.last_generation == i64::MAX as u64 {
            self.stop_at_capacity(effects);
            return;
        }
        let next = self.last_generation + 1;
        self.last_generation = next;
        let ticket = Ticket {
            fence: self.fence.clone(),
            connection_generation: ConnectionGeneration(next),
        };
        self.active = Some(ticket.clone());
        if self.expires_at_ms.is_some() {
            self.phase = Phase::Directory;
            effects.push(Effect::LoadDirectory(ticket));
        } else {
            self.phase = Phase::Credentials(self.auth_cause);
            effects.push(Effect::RecoverCredentials {
                ticket,
                cause: self.auth_cause,
            });
        }
    }

    fn ready(&mut self, ticket: &Ticket, now_ms: u64, effects: &mut Vec<Effect>) {
        self.phase = Phase::Ready(now_ms);
        effects.push(Effect::Ready(ticket.clone()));
    }

    fn cancel(&mut self, effects: &mut Vec<Effect>) {
        if let Some(ticket) = self.active.take() {
            effects.push(Effect::Cancel(ticket));
        }
    }

    fn retry(&mut self, now_ms: u64, random: u64, effects: &mut Vec<Effect>) {
        self.cancel(effects);
        let window = (BASE_RETRY_DELAY_MS << self.failures.min(16)).min(MAX_RETRY_DELAY_MS);
        let floor = window / 2;
        let delay = floor + random % (window - floor + 1);
        self.failures = self.failures.saturating_add(1);
        self.phase = Phase::Backoff(now_ms.saturating_add(delay));
    }

    fn advance_fence(&mut self, resume: Option<AuthCause>, effects: &mut Vec<Effect>) {
        if let Phase::AdvancingFence(pending) = &mut self.phase {
            if resume.is_none() {
                *pending = None;
            }
            return;
        }
        if matches!(self.phase, Phase::Faulted) {
            return;
        }
        self.cancel(effects);
        self.expires_at_ms = None;
        if self.fence.generation == i64::MAX as u64 {
            self.stop_at_capacity(effects);
        } else {
            self.phase = Phase::AdvancingFence(resume);
            effects.push(Effect::AdvanceFence(self.fence.clone()));
        }
    }

    fn stop_at_capacity(&mut self, effects: &mut Vec<Effect>) {
        self.expires_at_ms = None;
        self.phase = Phase::Faulted;
        effects.push(Effect::Unavailable(StoreError::CapacityExceeded));
    }
}
