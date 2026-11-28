use crate::{Result, Status, abi::MAX_EVENT_BYTES, codec, history, id, outbound::SendFlight};
use newim_protocol::gateway::{self, GATEWAY_PATH, GATEWAY_SUBPROTOCOL, GatewaySubscription};
use newim_sdk_core::{
    connection::{AuthCause, CommittedDirectory, Connection, Effect, Event, Failure, Ticket},
    conversation_sync::InFlightPage,
    message::SendContext,
    message_sync as history_core,
    observability::SDK_REPORT_PATH,
    push,
    store::*,
};
use newim_store_sqlite::{Limits, SqliteStore};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    now_ms: u64,
    #[serde(default)]
    random: u64,
    event: HostEvent,
}

#[derive(Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum HostEvent {
    NetworkAvailable,
    NetworkChanged,
    NetworkLost,
    RefreshDirectory,
    Tick,
    Resume,
    Logout,
    SelectConversation {
        conversation_id: Option<String>,
    },
    Notification {
        marker: String,
    },
    QueueSend {
        body: String,
    },
    ResumeSend {
        client_id: String,
    },
    TakeMetrics,
    ListPending {
        after: Option<String>,
        limit: usize,
    },
    DismissSend {
        client_id: String,
    },
    ListMessages {
        conversation_id: String,
        after: Option<String>,
        limit: usize,
    },
    ListConversations {
        after: Option<String>,
        limit: usize,
    },
    Credentials {
        operation: String,
        account: String,
        verified: bool,
        expires_at_ms: u64,
    },
    Http {
        operation: String,
        status: u16,
        verified: bool,
        body: String,
        trace_id: Option<String>,
    },
    SocketOpened {
        operation: String,
        verified: bool,
        subprotocol: String,
    },
    SocketClosed {
        operation: String,
        code: u16,
        reason: String,
    },
    SocketSubscribed {
        operation: String,
    },
    Frame {
        operation: String,
        body: String,
    },
    TransportFailed {
        operation: String,
        reason: TransportFailure,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransportFailure {
    Temporary,
    Unauthorized,
    Revoked,
    Expired,
    DeviceLimit,
    ConversationForbidden,
    Protocol,
}

impl From<TransportFailure> for Failure {
    fn from(failure: TransportFailure) -> Self {
        match failure {
            TransportFailure::Temporary => Self::Temporary,
            TransportFailure::Unauthorized => Self::Authentication(AuthCause::Rejected),
            TransportFailure::Revoked => Self::Authentication(AuthCause::Revoked),
            TransportFailure::Expired => Self::Authentication(AuthCause::Expired),
            TransportFailure::DeviceLimit => Self::Authentication(AuthCause::DeviceLimit),
            TransportFailure::ConversationForbidden => Self::ConversationForbidden,
            TransportFailure::Protocol => Self::Protocol,
        }
    }
}

#[derive(Clone)]
pub(crate) enum DirectoryPurpose {
    Connect,
    Visibility { through: i64 },
}

pub(crate) enum Work {
    Credentials(Ticket),
    Directory(Ticket, InFlightPage, DirectoryPurpose),
    History(Ticket, history_core::InFlightPage),
    Visibility(Ticket, history::VisibilityRequest),
}

impl Work {
    pub(crate) fn ticket(&self) -> &Ticket {
        match self {
            Self::Credentials(ticket)
            | Self::Directory(ticket, ..)
            | Self::History(ticket, _)
            | Self::Visibility(ticket, _) => ticket,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SocketPhase {
    Opening,
    Subscribing,
    Active,
}

struct Socket {
    operation: u64,
    ticket: Ticket,
    phase: SocketPhase,
    conversation: Option<String>,
}

pub(crate) struct Client {
    pub(crate) store: SqliteStore,
    pub(crate) connection: Connection,
    pub(crate) requests: BTreeMap<u64, Work>,
    pub(crate) selected: Option<String>,
    pub(crate) last_context: Option<SendContext>,
    pub(crate) sending: Option<SendFlight>,
    pub(crate) now_ms: u64,
    random: u64,
    auth_expires_at: Option<u64>,
    next_store_operation: u64,
    output: VecDeque<Vec<u8>>,
    socket: Option<Socket>,
    advance_pending: Option<Fence>,
}

fn operation(value: &str) -> Result<u64> {
    let id = value.parse::<u64>().map_err(|_| Status::InvalidInput)?;
    if id == 0 || id.to_string() != value {
        return Err(Status::InvalidInput);
    }
    Ok(id)
}

impl Client {
    pub(crate) fn open(root: &Path, account: &str) -> Result<Self> {
        let store = SqliteStore::open(root, account, Limits::default())?;
        let next_store_operation = store
            .last_operation_id()?
            .checked_add(1)
            .filter(|id| *id <= i64::MAX as u64)
            .ok_or(Status::StoreUnavailable)?;
        let connection = Connection::new(store.fence().clone(), 0)?;
        Ok(Self {
            store,
            connection,
            requests: BTreeMap::new(),
            selected: None,
            last_context: None,
            sending: None,
            now_ms: 0,
            random: 0,
            auth_expires_at: None,
            next_store_operation,
            output: VecDeque::new(),
            socket: None,
            advance_pending: None,
        })
    }

    pub(crate) fn peek(&self) -> Option<&[u8]> {
        self.output.front().map(Vec::as_slice)
    }

    pub(crate) fn pop(&mut self) {
        self.output.pop_front();
    }

    pub(crate) fn emit(&mut self, event: Value) -> Result<()> {
        let bytes = serde_json::to_vec(&event).map_err(|_| Status::InternalFailure)?;
        if bytes.len() > MAX_EVENT_BYTES {
            return Err(Status::InternalFailure);
        }
        self.output.push_back(bytes);
        Ok(())
    }

    pub(crate) fn next_store_id(&mut self) -> Result<u64> {
        let current = self.next_store_operation;
        if current > i64::MAX as u64 {
            return Err(Status::StoreUnavailable);
        }
        self.next_store_operation += 1;
        Ok(current)
    }

    pub(crate) fn read(&mut self, action: Action) -> Result<Response> {
        Ok(self.execute(action)?.result?)
    }

    pub(crate) fn execute(&mut self, action: Action) -> Result<Completion> {
        let request = Request {
            fence: self.store.fence().clone(),
            operation_id: self.next_store_id()?,
            action,
        };
        let result = self.store.execute(request.clone());
        if let Some(socket) = &self.socket {
            self.connection
                .observe_store_completion(&socket.ticket, &request, &result);
        }
        Ok(result)
    }

    pub(crate) fn commit(&mut self, action: Action) -> Result<Receipt> {
        match self.read(action)? {
            Response::Committed(receipt) => Ok(receipt),
            _ => Err(Status::InternalFailure),
        }
    }

    pub(crate) fn start_http(
        &mut self,
        work: Work,
        path: &'static str,
        body: String,
    ) -> Result<()> {
        let operation = id()?;
        self.emit(json!({
            "type": "http", "operation": operation.to_string(), "method": "POST",
            "path": path, "body": body,
        }))?;
        self.requests.insert(operation, work);
        Ok(())
    }

    pub(crate) fn submit(&mut self, bytes: &[u8]) -> Result<()> {
        if !self.output.is_empty() {
            return Err(Status::Busy);
        }
        let input: Input = serde_json::from_slice(bytes).map_err(|_| Status::InvalidInput)?;
        if input.now_ms < self.now_ms || input.now_ms > i64::MAX as u64 {
            return Err(Status::InvalidInput);
        }
        self.now_ms = input.now_ms;
        self.random = input.random;
        if self.advance_pending.is_some() {
            match input.event {
                HostEvent::Tick => {}
                HostEvent::Logout => self.update(Event::Logout)?,
                HostEvent::NetworkAvailable => self.update(Event::NetworkAvailable)?,
                HostEvent::NetworkChanged => self.update(Event::NetworkChanged)?,
                HostEvent::NetworkLost => self.update(Event::NetworkLost)?,
                _ => return Err(Status::StoreUnavailable),
            }
            return self.advance();
        }
        let is_completion = matches!(
            &input.event,
            HostEvent::Http { .. }
                | HostEvent::SocketOpened { .. }
                | HostEvent::SocketSubscribed { .. }
                | HostEvent::SocketClosed { .. }
                | HostEvent::Frame { .. }
                | HostEvent::TransportFailed { .. }
        );
        if is_completion && self.auth_expires_at.is_some_and(|at| input.now_ms >= at) {
            return self.update(Event::Tick);
        }
        match input.event {
            HostEvent::NetworkAvailable => self.update(Event::NetworkAvailable),
            HostEvent::NetworkChanged => self.update(Event::NetworkChanged),
            HostEvent::NetworkLost => self.update(Event::NetworkLost),
            HostEvent::RefreshDirectory => self.update(Event::RefreshDirectory),
            HostEvent::Resume => self.update(Event::Resume),
            HostEvent::Logout => self.update(Event::Logout),
            HostEvent::Tick => {
                self.update(Event::Tick)?;
                if let Some(ticket) = self.active_socket_ticket() {
                    self.schedule_history(&ticket)?;
                }
                self.dispatch_outbox()
            }
            HostEvent::SelectConversation { conversation_id } => {
                if let Some(conversation) = &conversation_id {
                    self.message_snapshot(conversation, &[])?;
                }
                if self.selected != conversation_id {
                    self.selected = conversation_id;
                    self.update(Event::RefreshDirectory)?;
                }
                Ok(())
            }
            HostEvent::Notification { marker } => {
                let effects = push::open_notification(
                    &mut self.connection,
                    &marker,
                    self.now_ms,
                    self.random,
                )?;
                self.effects(effects)
            }
            HostEvent::QueueSend { body } => self.queue_send(&body),
            HostEvent::ResumeSend { client_id } => self.resume_send(&client_id),
            HostEvent::ListPending { after, limit } => self.list_pending(after, limit),
            HostEvent::DismissSend { client_id } => self.dismiss_send(&client_id),
            HostEvent::ListMessages {
                conversation_id,
                after,
                limit,
            } => self.list_messages(&conversation_id, after.as_deref(), limit),
            HostEvent::ListConversations { after, limit } => self.list_conversations(after, limit),
            HostEvent::TakeMetrics => {
                self.read(Action::Metrics)?;
                if let Some(report) = self.connection.take_metrics_report() {
                    self.emit(json!({"type":"metrics","method":"POST","path":SDK_REPORT_PATH,"body":report.encode()}))?;
                }
                Ok(())
            }
            HostEvent::Credentials {
                operation: token,
                account,
                verified,
                expires_at_ms,
            } => {
                if !verified
                    || self.store.fence().account != account
                    || expires_at_ms > i64::MAX as u64
                {
                    return Err(Status::InvalidInput);
                }
                let op = operation(&token)?;
                let Some(Work::Credentials(ticket)) = self.requests.get(&op) else {
                    return Err(Status::InvalidInput);
                };
                let ticket = ticket.clone();
                self.requests.remove(&op);
                self.auth_expires_at = (expires_at_ms > self.now_ms).then_some(expires_at_ms);
                self.update(Event::Credentials {
                    ticket: &ticket,
                    expires_at_ms,
                })
            }
            HostEvent::Http {
                operation: token,
                status,
                verified,
                body,
                trace_id,
            } => {
                if !verified || !(100..600).contains(&status) {
                    return Err(Status::InvalidInput);
                }
                let op = operation(&token)?;
                if self
                    .sending
                    .as_ref()
                    .is_some_and(|flight| flight.operation == op)
                {
                    return self.send_response(status, &body, trace_id.as_deref());
                }
                self.http(op, status, &body)
            }
            HostEvent::SocketOpened {
                operation: token,
                verified,
                subprotocol,
            } => {
                if !verified || subprotocol != GATEWAY_SUBPROTOCOL {
                    return Err(Status::InvalidInput);
                }
                let ticket = self.socket_ticket(operation(&token)?, SocketPhase::Opening)?;
                self.update(Event::SocketOpened(&ticket))
            }
            HostEvent::SocketClosed {
                operation: token,
                code,
                reason,
            } => {
                let op = operation(&token)?;
                let socket = self
                    .socket
                    .as_ref()
                    .filter(|socket| {
                        socket.operation == op
                            && socket.phase != SocketPhase::Opening
                            && self.connection.accepts(&socket.ticket)
                    })
                    .ok_or(Status::InvalidInput)?;
                let ticket = socket.ticket.clone();
                use gateway::GatewayClose;
                let failure = match gateway::classify_gateway_close(code, &reason) {
                    GatewayClose::SessionRevoked => Failure::Authentication(AuthCause::Revoked),
                    GatewayClose::DeviceLimit => Failure::Authentication(AuthCause::DeviceLimit),
                    GatewayClose::ConversationForbidden => Failure::ConversationForbidden,
                    GatewayClose::InvalidSubscription => Failure::Protocol,
                    GatewayClose::ServerDraining
                    | GatewayClose::SubscriptionTimeout
                    | GatewayClose::TemporarilyUnavailable
                    | GatewayClose::Unknown => Failure::Temporary,
                };
                self.update(Event::Failed {
                    ticket: &ticket,
                    failure,
                })
            }
            HostEvent::SocketSubscribed { operation: token } => {
                let op = operation(&token)?;
                let ticket = self.socket_ticket(op, SocketPhase::Subscribing)?;
                self.socket.as_mut().ok_or(Status::InternalFailure)?.phase = SocketPhase::Active;
                self.update(Event::Subscribed(&ticket))
            }
            HostEvent::Frame {
                operation: token,
                body,
            } => {
                let ticket = self.socket_ticket(operation(&token)?, SocketPhase::Active)?;
                let conversation = self
                    .socket
                    .as_ref()
                    .and_then(|value| value.conversation.clone())
                    .ok_or(Status::InvalidInput)?;
                self.gateway_frame(&ticket, &conversation, &body)
            }
            HostEvent::TransportFailed {
                operation: token,
                reason,
            } => {
                let op = operation(&token)?;
                if self
                    .sending
                    .as_ref()
                    .is_some_and(|flight| flight.operation == op)
                {
                    return match reason.into() {
                        Failure::Authentication(cause) => {
                            self.fail_send("AUTH_REQUIRED", Some(cause))
                        }
                        _ => self.fail_send("SERVER_TEMPORARY_UNAVAILABLE", None),
                    };
                }
                let ticket = match self.requests.get(&op) {
                    Some(work) => work.ticket().clone(),
                    None => self
                        .socket
                        .as_ref()
                        .filter(|socket| socket.operation == op)
                        .map(|socket| socket.ticket.clone())
                        .ok_or(Status::InvalidInput)?,
                };
                self.update(Event::Failed {
                    ticket: &ticket,
                    failure: reason.into(),
                })
            }
        }
    }

    fn socket_ticket(&self, operation: u64, phase: SocketPhase) -> Result<Ticket> {
        self.socket
            .as_ref()
            .filter(|socket| {
                socket.operation == operation
                    && socket.phase == phase
                    && self.connection.accepts(&socket.ticket)
            })
            .map(|socket| socket.ticket.clone())
            .ok_or(Status::InvalidInput)
    }

    fn active_socket_ticket(&self) -> Option<Ticket> {
        self.socket
            .as_ref()
            .filter(|socket| {
                socket.phase == SocketPhase::Active && self.connection.accepts(&socket.ticket)
            })
            .map(|socket| socket.ticket.clone())
    }

    pub(crate) fn transition(&mut self, event: Event<'_>) -> Result<Vec<Effect>> {
        Ok(self.connection.update(event, self.now_ms, self.random)?)
    }

    pub(crate) fn update(&mut self, event: Event<'_>) -> Result<()> {
        let effects = self.transition(event)?;
        self.effects(effects)
    }

    pub(crate) fn effects(&mut self, effects: Vec<Effect>) -> Result<()> {
        let mut cleanup = Ok(());
        for effect in effects {
            match effect {
                Effect::RecoverCredentials { ticket, cause } => {
                    let op = id()?;
                    let cause = match cause {
                        AuthCause::Missing => "missing",
                        AuthCause::Expired => "expired",
                        AuthCause::Rejected => "rejected",
                        AuthCause::Revoked => "revoked",
                        AuthCause::DeviceLimit => "device_limit",
                    };
                    self.emit(json!({"type": "credentials_required", "operation": op.to_string(), "cause": cause}))?;
                    self.requests.insert(op, Work::Credentials(ticket));
                }
                Effect::LoadDirectory(ticket) => {
                    self.load_directory(ticket, DirectoryPurpose::Connect)?
                }
                Effect::OpenSocket(ticket) => {
                    let operation = id()?;
                    self.emit(
                        json!({"type":"open_socket","operation":operation.to_string(),
                                    "path":GATEWAY_PATH,"subprotocol":GATEWAY_SUBPROTOCOL}),
                    )?;
                    self.socket = Some(Socket {
                        operation,
                        ticket,
                        phase: SocketPhase::Opening,
                        conversation: None,
                    });
                }
                Effect::Subscribe { ticket, target } => {
                    let socket = self
                        .socket
                        .as_mut()
                        .filter(|socket| socket.ticket == ticket)
                        .ok_or(Status::InternalFailure)?;
                    let operation = socket.operation;
                    let body = gateway::encode_gateway_subscription(&GatewaySubscription {
                        conversation_id: target.conversation_id().to_owned(),
                        after_seq: target.after_seq(),
                        after_visibility_revision: target.gateway_after_visibility_revision(),
                    })
                    .map_err(|_| Status::InternalFailure)?;
                    socket.phase = SocketPhase::Subscribing;
                    socket.conversation = Some(target.conversation_id().to_owned());
                    self.emit(json!({"type":"subscribe","operation":operation.to_string(),"body":codec::utf8(body)?}))?;
                }
                Effect::RecoverMessages { ticket, target } => {
                    self.recover_messages(&ticket, &target)?
                }
                Effect::RefreshVisibility {
                    ticket,
                    through_revision,
                } => {
                    self.load_directory(
                        ticket,
                        DirectoryPurpose::Visibility {
                            through: through_revision,
                        },
                    )?;
                }
                Effect::Cancel(ticket) => {
                    // An optional gap update must not suppress a following fence change.
                    cleanup = self.cancel(&ticket).and(cleanup);
                }
                Effect::AdvanceFence(previous) => {
                    self.advance_pending = Some(previous);
                    self.advance()?;
                }
                Effect::Unavailable(_) => {
                    self.emit(json!({"type":"unavailable","code":"store_unavailable"}))?
                }
                Effect::Ready(_) => {
                    self.last_context = self.connection.send_context();
                    self.emit(json!({"type":"ready"}))?;
                    self.dispatch_outbox()?;
                }
            }
        }
        cleanup
    }

    fn advance(&mut self) -> Result<()> {
        let previous = self
            .advance_pending
            .clone()
            .ok_or(Status::InternalFailure)?;
        let Response::Generation(next) = self.read(Action::AdvanceGeneration)? else {
            return Err(Status::InternalFailure);
        };
        self.advance_pending = None;
        self.auth_expires_at = None;
        self.last_context = None;
        if let Some(flight) = self.sending.take() {
            self.emit(json!({"type":"cancel","operation":flight.operation.to_string()}))?;
        }
        self.next_store_operation = 1;
        self.requests.clear();
        self.update(Event::FenceAdvanced {
            previous: &previous,
            next,
        })
    }

    fn cancel(&mut self, ticket: &Ticket) -> Result<()> {
        let mut cleanup = Ok(());
        let cancelled: Vec<u64> = self
            .requests
            .iter()
            .filter(|(_, work)| work.ticket() == ticket)
            .map(|(&op, _)| op)
            .collect();
        for op in cancelled {
            if let Some(Work::History(_, flight)) = self.requests.remove(&op) {
                cleanup = self.interrupt_history(&flight).and(cleanup);
            }
            self.emit(json!({"type":"cancel","operation":op.to_string()}))?;
        }
        if self
            .socket
            .as_ref()
            .is_some_and(|socket| socket.ticket == *ticket)
            && let Some(socket) = self.socket.take()
        {
            self.emit(json!({"type":"cancel","operation":socket.operation.to_string()}))?;
        }
        cleanup
    }

    pub(crate) fn directory_snapshot(
        &mut self,
        query: ConversationSyncQuery,
    ) -> Result<ConversationSyncSnapshot> {
        match self.read(Action::ConversationSyncSnapshot(query))? {
            Response::ConversationSyncSnapshot(snapshot) => Ok(snapshot),
            _ => Err(Status::InternalFailure),
        }
    }

    fn load_directory(&mut self, ticket: Ticket, purpose: DirectoryPurpose) -> Result<()> {
        let current = self.directory_snapshot(ConversationSyncQuery::State)?;
        let flight = InFlightPage::new(self.store.fence(), &current);
        let request = codec::directory_request(&flight)?;
        let body = codec::utf8(request.encode().map_err(|_| Status::InternalFailure)?)?;
        self.start_http(
            Work::Directory(ticket, flight, purpose),
            request.path(),
            body,
        )
    }

    fn http(&mut self, op: u64, status: u16, body: &str) -> Result<()> {
        let work = self.requests.get(&op).ok_or(Status::InvalidInput)?;
        if !self.connection.accepts(work.ticket()) || matches!(work, Work::Credentials(_)) {
            return Err(Status::InvalidInput);
        }
        let ticket = work.ticket().clone();
        if let Work::Directory(_, flight, purpose) = work {
            return self.directory_response(
                op,
                status,
                body,
                ticket,
                flight.clone(),
                purpose.clone(),
            );
        }
        match self.requests.remove(&op).ok_or(Status::InternalFailure)? {
            Work::History(_, flight) => self.history_response(&ticket, flight, status, body),
            Work::Visibility(_, request) => {
                self.visibility_response(&ticket, request, status, body)
            }
            _ => Err(Status::InternalFailure),
        }
    }

    fn directory_response(
        &mut self,
        op: u64,
        status: u16,
        body: &str,
        ticket: Ticket,
        flight: InFlightPage,
        purpose: DirectoryPurpose,
    ) -> Result<()> {
        if status == 409
            && newim_protocol::conversation_sync::decode_error_code(body.as_bytes())
                .is_ok_and(|code| code == "SYNC_CURSOR_EXPIRED")
        {
            let current = self.directory_snapshot(ConversationSyncQuery::State)?;
            let action =
                flight.restart(self.store.fence(), &current, status, "SYNC_CURSOR_EXPIRED")?;
            self.commit(action)?;
            self.requests.remove(&op);
            return self.load_directory(ticket, purpose);
        }
        if status != 200 {
            return self.network_response(&ticket, status);
        }
        let page = match codec::directory_page(body) {
            Ok(page) => page,
            Err(_) => {
                return self.update(Event::Failed {
                    ticket: &ticket,
                    failure: Failure::Protocol,
                });
            }
        };
        let more = page.has_more;
        let snapshot = self.directory_snapshot(flight.snapshot_query(&page)?)?;
        let action = flight.apply(self.store.fence(), &snapshot, &page)?;
        self.commit(action)?;
        self.requests.remove(&op);
        if more {
            return self.load_directory(ticket, purpose);
        }
        self.emit(json!({"type":"directory_changed"}))?;
        match purpose {
            DirectoryPurpose::Connect => {
                let directory = self.selected_directory()?;
                self.update(Event::Directory {
                    ticket: &ticket,
                    committed: directory,
                })
            }
            DirectoryPurpose::Visibility { through } => {
                self.start_live_visibility(&ticket, through)
            }
        }
    }

    pub(crate) fn selected_directory(&mut self) -> Result<CommittedDirectory> {
        let Some(selected) = self.selected.clone() else {
            let directory = self.directory_snapshot(ConversationSyncQuery::State)?;
            return Ok(CommittedDirectory::from_store(&directory, None)?);
        };
        let mut after = None;
        loop {
            let directory = self.directory_snapshot(ConversationSyncQuery::Published {
                after,
                limit: MAX_ITEMS,
            })?;
            if directory
                .items
                .iter()
                .any(|row| row.conversation_id == selected)
            {
                let snapshot = self.message_snapshot(&selected, &[])?;
                return Ok(CommittedDirectory::from_store(&directory, Some(&snapshot))?);
            }
            after = directory.next.clone();
            if after.is_none() {
                self.selected = None;
                return Ok(CommittedDirectory::from_store(&directory, None)?);
            }
        }
    }

    pub(crate) fn network_response(&mut self, ticket: &Ticket, status: u16) -> Result<()> {
        let failure = match status {
            401 => Failure::Authentication(AuthCause::Rejected),
            403 | 404 => Failure::ConversationForbidden,
            429 | 500..=599 => Failure::Temporary,
            _ => Failure::Protocol,
        };
        self.update(Event::Failed { ticket, failure })
    }
}
