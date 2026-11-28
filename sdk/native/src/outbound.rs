use crate::{Result, Status, client::Client, codec, id};
use newim_protocol::send::{self, Send, ServerFrame};
use newim_sdk_core::{
    connection::AuthCause,
    message::{PersistedAck, SendContext, SendFailure, SendIntent},
    observability::AckFlight,
    outbox::{self, OutboxError, OutboxRecord, OutboxState, OutboxTransition},
    store::*,
};
use serde_json::{json, value::RawValue};
use std::time::Instant;

const SEND_PATH: &str = "/api/v1/messages";

pub(crate) struct SendFlight {
    pub(crate) operation: u64,
    context: SendContext,
    pending: Pending,
    sent: Send,
    attempted_at_ms: u64,
    deadline_ms: u64,
}

impl From<OutboxError> for Status {
    fn from(error: OutboxError) -> Self {
        match error {
            OutboxError::Store(error) => error.into(),
            OutboxError::UnexpectedResponse => Self::InternalFailure,
            _ => Self::InvalidInput,
        }
    }
}

fn to_wire(intent: &SendIntent) -> Result<Send> {
    let payload = String::from_utf8(intent.payload.0.clone()).map_err(|_| Status::InvalidInput)?;
    Ok(Send {
        protocol_version: intent.protocol_version,
        client_msg_id: intent.client_id.clone(),
        conversation_id: intent.conversation_id.clone(),
        version: intent.schema_version,
        message_type: intent.message_type.clone(),
        payload: RawValue::from_string(payload).map_err(|_| Status::InvalidInput)?,
    })
}

fn failure_code(status: u16, body: &str) -> String {
    match status {
        401 => "AUTH_REQUIRED".into(),
        408 | 429 | 500..=599 => "SERVER_TEMPORARY_UNAVAILABLE".into(),
        _ => newim_protocol::conversation_sync::decode_error_code(body.as_bytes())
            .unwrap_or_else(|_| "OUTBOX_PROTOCOL_FAILURE".into()),
    }
}

impl Client {
    pub(crate) fn queue_send(&mut self, body: &str) -> Result<()> {
        let context = self.last_context.clone().ok_or(Status::Busy)?;
        if !self.connection.accepts_ack(&context, self.now_ms) {
            return Err(Status::Busy);
        }
        let wire = send::decode_send(body.as_bytes()).map_err(|_| Status::InvalidInput)?;
        if !wire.supported() {
            return Err(Status::InvalidInput);
        }
        let intent = SendIntent {
            protocol_version: wire.protocol_version,
            client_id: wire.client_msg_id,
            conversation_id: wire.conversation_id,
            schema_version: wire.version,
            message_type: wire.message_type,
            payload: Blob(wire.payload.get().as_bytes().to_vec()),
        };
        let operation = self.next_store_id()?;
        let record = outbox::enqueue(&mut self.store, operation, &context, intent, self.now_ms)?;
        self.emit(
            json!({"type":"send_queued","clientMsgId":record.intent.client_id,
                        "conversationId":record.intent.conversation_id}),
        )?;
        self.dispatch_outbox()
    }

    fn pending_page(&mut self, after: Option<String>) -> Result<PendingPage> {
        match self.read(Action::Pending {
            after,
            limit: MAX_ITEMS,
        })? {
            Response::Pending(page) => Ok(page),
            _ => Err(Status::InternalFailure),
        }
    }

    fn pending_in_page(&mut self, after: Option<String>, pending: &Pending) -> Result<Pending> {
        self.pending_page(after)?
            .items
            .into_iter()
            .find(|item| {
                item.sender_id == pending.sender_id
                    && item.client_id == pending.client_id
                    && item.conversation_id == pending.conversation_id
            })
            .ok_or(Status::InternalFailure)
    }

    fn revision(&mut self) -> Result<u64> {
        match self.read(Action::Snapshot { conversation: None })? {
            Response::Snapshot(snapshot) if !snapshot.recovery_required => Ok(snapshot.revision),
            _ => Err(Status::StoreUnavailable),
        }
    }

    pub(crate) fn dispatch_outbox(&mut self) -> Result<()> {
        let Some(context) = self.connection.send_context() else {
            return Ok(());
        };
        if let Some(flight) = &self.sending {
            if self.now_ms < flight.deadline_ms {
                return Ok(());
            }
            let op = flight.operation.to_string();
            self.sending = None;
            self.emit(json!({"type":"cancel","operation":op}))?;
        }
        let mut after = None;
        loop {
            let page = self.pending_page(after.clone())?;
            for mut pending in page.items {
                let record = OutboxRecord::decode(&pending)?;
                if matches!(
                    record.state,
                    OutboxState::PermanentFailure | OutboxState::AuthRecovery
                ) {
                    continue;
                }
                if self.now_ms < record.created_at_ms
                    || (record.active_fence == context.fence
                        && matches!(record.state, OutboxState::InFlight | OutboxState::RetryWait)
                        && self.now_ms < record.deadline_ms)
                {
                    continue;
                }
                if record.active_fence == context.fence
                    && record.active_connection_generation != context.connection_generation
                {
                    let revision = self.revision()?;
                    outbox::rebind_connection(&mut self.store, &pending, revision, &context)?;
                    pending = self.pending_in_page(after.clone(), &pending)?;
                }
                let revision = self.revision()?;
                let transition = outbox::plan_dispatch(
                    &mut self.store,
                    &pending,
                    revision,
                    &context,
                    self.now_ms,
                )?;
                match transition {
                    OutboxTransition::Send { intent } => {
                        let pending = self.pending_in_page(after, &pending)?;
                        let deadline_ms = OutboxRecord::decode(&pending)?.deadline_ms;
                        let sent = to_wire(&intent)?;
                        let body = codec::utf8(
                            send::encode_send(&sent).map_err(|_| Status::InternalFailure)?,
                        )?;
                        let operation = id()?;
                        self.emit(
                            json!({"type":"http","operation":operation.to_string(),"method":"POST",
                                        "path":SEND_PATH,"body":body}),
                        )?;
                        self.sending = Some(SendFlight {
                            operation,
                            context,
                            pending,
                            sent,
                            attempted_at_ms: self.now_ms,
                            deadline_ms,
                        });
                        return Ok(());
                    }
                    OutboxTransition::Wait { .. } => continue,
                    other => {
                        self.emit_send_transition(&pending, other)?;
                        return Ok(());
                    }
                }
            }
            after = page.next;
            if after.is_none() {
                return Ok(());
            }
        }
    }

    fn emit_send_transition(
        &mut self,
        pending: &Pending,
        transition: OutboxTransition,
    ) -> Result<()> {
        let message = &pending.client_id;
        let conversation = &pending.conversation_id;
        match transition {
            OutboxTransition::Wait { until_ms } => self.emit(json!({"type":"send_wait","clientMsgId":message,
                                                                   "conversationId":conversation,"untilMs":until_ms})),
            OutboxTransition::AuthRecovery { code } | OutboxTransition::Terminal { code } => {
                self.emit(json!({"type":"send_failed","clientMsgId":message,"conversationId":conversation,"code":code}))
            }
            OutboxTransition::Send { .. } => Err(Status::InternalFailure),
        }
    }

    pub(crate) fn send_response(
        &mut self,
        status: u16,
        body: &str,
        trace: Option<&str>,
    ) -> Result<()> {
        if status != 200 {
            let auth = (status == 401).then_some(AuthCause::Rejected);
            return self.fail_send(&failure_code(status, body), auth);
        }
        let flight = self.sending.as_ref().ok_or(Status::InvalidInput)?;
        let frame = send::decode_server_frame(body.as_bytes()).map_err(|_| Status::InvalidInput)?;
        send::correlate(&flight.sent, &flight.context.sender_id, &frame)
            .map_err(|_| Status::InvalidInput)?;
        let ServerFrame::Ack(wire) = frame else {
            return Err(Status::InvalidInput);
        };
        let ack = PersistedAck {
            sender_id: wire.sender_id,
            client_id: wire.client_msg_id,
            conversation_id: wire.conversation_id,
            server_id: wire.server_msg_id,
            sequence: wire
                .conversation_seq
                .parse()
                .map_err(|_| Status::InvalidInput)?,
            server_time: wire.server_time.parse().map_err(|_| Status::InvalidInput)?,
            protocol_version: flight.sent.protocol_version,
            schema_version: flight.sent.version,
            message_type: flight.sent.message_type.clone(),
            payload: Blob(flight.sent.payload.get().as_bytes().to_vec()),
        };
        let expected_revision = self.revision()?;
        let operation_id = self.next_store_id()?;
        let flight = self.sending.as_ref().ok_or(Status::InternalFailure)?;
        let now = self.now_ms;
        let start = Instant::now();
        let header = trace.map(|value| [value]);
        let headers = header.as_ref().map_or(&[][..], |value| &value[..]);
        let (_, trace) = self.connection.resolve_http_ack(
            &mut self.store,
            AckFlight {
                context: &flight.context,
                pending: &flight.pending,
                expected_revision,
                operation_id,
                attempted_at_ms: flight.attempted_at_ms,
            },
            &ack,
            headers,
            || now.saturating_add(start.elapsed().as_millis() as u64),
        )?;
        self.sending = None;
        self.emit(json!({
            "type":"send_ack","clientMsgId":ack.client_id,"conversationId":ack.conversation_id,
            "serverMsgId":ack.server_id,"conversationSeq":ack.sequence.to_string(),
            "traceId":trace.map(|trace| trace.trace_id)
        }))?;
        self.dispatch_outbox()
    }

    pub(crate) fn fail_send(
        &mut self,
        code: &str,
        authentication: Option<AuthCause>,
    ) -> Result<()> {
        let Some(cause) = authentication else {
            self.record_send_failure(code)?;
            return self.dispatch_outbox();
        };
        let flight = self.sending.as_ref().ok_or(Status::InvalidInput)?;
        let effects = self
            .connection
            .reject_send_credentials(&flight.context, cause, self.now_ms);
        if effects.is_empty() {
            return Err(Status::InvalidInput);
        }
        // Record the failure under the old store fence before applying the new fence.
        let recorded = self.record_send_failure(code);
        self.effects(effects).and(recorded)
    }

    fn record_send_failure(&mut self, code: &str) -> Result<()> {
        let revision = self.revision()?;
        let flight = self.sending.as_ref().ok_or(Status::InvalidInput)?;
        let failure = SendFailure::from_validated_code(
            &flight.pending.client_id,
            &flight.pending.conversation_id,
            code,
        )
        .map_err(|_| Status::InvalidInput)?;
        let transition = outbox::apply_failure(
            &mut self.store,
            &flight.pending,
            revision,
            &flight.context,
            &failure,
            self.now_ms,
        )?;
        let flight = self.sending.take().ok_or(Status::InternalFailure)?;
        self.emit_send_transition(&flight.pending, transition)
    }

    fn pending(&mut self, client_id: &str) -> Result<Pending> {
        let mut after = None;
        loop {
            let page = self.pending_page(after)?;
            if let Some(pending) = page
                .items
                .into_iter()
                .find(|item| item.client_id == client_id)
            {
                return Ok(pending);
            }
            after = page.next;
            if after.is_none() {
                return Err(Status::InvalidInput);
            }
        }
    }

    pub(crate) fn resume_send(&mut self, client_id: &str) -> Result<()> {
        let context = self.connection.send_context().ok_or(Status::Busy)?;
        let pending = self.pending(client_id)?;
        let revision = self.revision()?;
        let transition =
            outbox::resume_auth(&mut self.store, &pending, revision, &context, self.now_ms)?;
        self.emit_send_transition(&pending, transition)
    }

    pub(crate) fn dismiss_send(&mut self, client_id: &str) -> Result<()> {
        let context = self.connection.send_context().ok_or(Status::Busy)?;
        let pending = self.pending(client_id)?;
        let revision = self.revision()?;
        outbox::remove_terminal(&mut self.store, &pending, revision, &context)?;
        self.emit(
            json!({"type":"send_dismissed","clientMsgId":pending.client_id,
                        "conversationId":pending.conversation_id}),
        )
    }

    pub(crate) fn list_pending(&mut self, after: Option<String>, limit: usize) -> Result<()> {
        self.authorize_local_read()?;
        let Response::Pending(page) = self.read(Action::Pending { after, limit })? else {
            return Err(Status::InternalFailure);
        };
        let mut items = Vec::with_capacity(page.items.len());
        for pending in page.items {
            let record = OutboxRecord::decode(&pending)?;
            let state = match record.state {
                OutboxState::Ready => "ready",
                OutboxState::InFlight => "in_flight",
                OutboxState::RetryWait => "retry_wait",
                OutboxState::AuthRecovery => "auth_recovery",
                OutboxState::PermanentFailure => "permanent_failure",
            };
            items.push(
                json!({"clientMsgId":pending.client_id,"conversationId":pending.conversation_id,
                "state":state,"attempts":record.attempts,"deadlineMs":record.deadline_ms,
                "lastCode":record.last_code,"messageType":record.intent.message_type,
                "payload":codec::utf8(record.intent.payload.0)?}),
            );
        }
        self.emit(json!({"type":"pending","items":items,"next":page.next}))
    }
}
