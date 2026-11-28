use crate::{
    Result, Status,
    client::{Client, Work},
    codec,
};
use newim_protocol::{
    Message as WireMessage,
    gateway::{self, GatewayFrame},
    message_delta as delta, send, visibility,
};
use newim_sdk_core::{
    connection::{Event, Failure, Subscription, Ticket},
    message_sync::{self as core, InFlightPage, IncomingMessage},
    store::*,
};
use serde_json::json;

pub(crate) struct VisibilityRequest {
    request: visibility::PageRequest,
    published_round: u64,
    announced: Option<i64>,
}

fn incoming(wire: WireMessage) -> Result<IncomingMessage> {
    Ok(IncomingMessage {
        protocol_version: wire.protocol_version,
        message: Message {
            server_id: wire.server_msg_id,
            sender_id: wire.sender_id,
            client_id: wire.client_msg_id,
            conversation_id: wire.conversation_id,
            sequence: wire
                .conversation_seq
                .parse()
                .map_err(|_| Status::InvalidInput)?,
            server_time: wire.server_time.parse().map_err(|_| Status::InvalidInput)?,
            schema_version: wire.version,
            message_type: wire.message_type,
            payload: Some(Blob(wire.payload.get().as_bytes().to_vec())),
        },
    })
}

fn compare(left: &Blob, right: &Blob) -> std::result::Result<bool, StoreError> {
    send::same_message_payload(&left.0, &right.0).map_err(|_| StoreError::InvalidInput)
}

fn wire_history_request(flight: &InFlightPage) -> delta::MessageDeltaRequest {
    let page = flight.request();
    delta::MessageDeltaRequest {
        conversation_id: page.conversation_id,
        after_seq: page.after_seq,
        limit: page.limit,
    }
}

impl Client {
    pub(crate) fn message_snapshot(
        &mut self,
        conversation: &str,
        items: &[IncomingMessage],
    ) -> Result<MessageSyncSnapshot> {
        match self.read(core::snapshot_query(conversation, items)?)? {
            Response::MessageSyncSnapshot(snapshot) => Ok(snapshot),
            _ => Err(Status::InternalFailure),
        }
    }

    pub(crate) fn recover_messages(
        &mut self,
        ticket: &Ticket,
        target: &Subscription,
    ) -> Result<()> {
        let conversation = target.conversation_id();
        let mut snapshot = self.message_snapshot(conversation, &[])?;
        if snapshot.state.after_seq() < target.through_seq() {
            if snapshot
                .state
                .job
                .as_ref()
                .is_some_and(|job| matches!(job.status, GapStatus::AuthRecovery))
            {
                self.commit(core::resume(&snapshot, self.now_ms)?)?;
                snapshot = self.message_snapshot(conversation, &[])?;
            }
            self.commit(core::observe_target(
                &snapshot,
                self.now_ms,
                target.published_round(),
                target.through_seq(),
            )?)?;
            snapshot = self.message_snapshot(conversation, &[])?;
        }
        if snapshot.state.visibility_revision < target.through_visibility_revision() {
            self.request_visibility(snapshot, target, ticket, None)?;
        }
        self.schedule_history(ticket)
    }

    pub(crate) fn schedule_history(&mut self, ticket: &Ticket) -> Result<()> {
        let Some(conversation) = self.selected.clone() else {
            return Ok(());
        };
        let snapshot = self.message_snapshot(&conversation, &[])?;
        let existing = self.requests.iter().find_map(|(&operation, work)| {
            matches!(work, Work::History(current, _) if current == ticket).then_some(operation)
        });
        if let Some(operation) = existing {
            if snapshot.state.job.as_ref().is_some_and(|job|
                matches!(&job.status, GapStatus::InFlight(attempt) if self.now_ms < attempt.expires_at_ms)
            ) {
                return Ok(());
            }
            self.requests.remove(&operation);
            self.emit(json!({"type":"cancel","operation":operation.to_string()}))?;
        }
        if let Some(action) = core::schedule(&snapshot, self.now_ms)? {
            let completion = self.execute(action)?;
            if !matches!(completion.result?, Response::Committed(_)) {
                return Err(Status::InternalFailure);
            }
            let next = self.message_snapshot(&conversation, &[])?;
            if let Some(flight) =
                InFlightPage::started(self.store.fence(), &next, completion.operation_id)
            {
                let wire = wire_history_request(&flight);
                let bytes = delta::encode_message_delta_request(&wire)
                    .map_err(|_| Status::InternalFailure)?;
                self.start_http(
                    Work::History(ticket.clone(), flight),
                    delta::MESSAGE_DELTA_PATH,
                    codec::utf8(bytes)?,
                )?;
            }
        }
        Ok(())
    }

    fn fail_history(&mut self, flight: &InFlightPage, failure: GapFailure) -> Result<()> {
        let snapshot = self.message_snapshot(&flight.request().conversation_id, &[])?;
        match flight.fail(self.store.fence(), &snapshot, self.now_ms, failure) {
            Ok(action) => {
                self.commit(action)?;
                Ok(())
            }
            Err(StoreError::StaleRevision | StoreError::StaleGeneration) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn interrupt_history(&mut self, flight: &InFlightPage) -> Result<()> {
        self.fail_history(flight, GapFailure::Temporary)
    }

    pub(crate) fn history_response(
        &mut self,
        ticket: &Ticket,
        flight: InFlightPage,
        status: u16,
        body: &str,
    ) -> Result<()> {
        let request = wire_history_request(&flight);
        if status != 200 {
            let failure = match status {
                401 => GapFailure::Authentication,
                409 => GapFailure::InvalidCursor,
                _ => GapFailure::Temporary,
            };
            self.fail_history(&flight, failure)?;
            return match status {
                401 | 403 | 404 => self.network_response(ticket, status),
                _ => Ok(()),
            };
        }
        let page = match delta::decode_message_delta_page(&request, body.as_bytes()) {
            Ok(page) => page,
            Err(_) => {
                self.fail_history(&flight, GapFailure::Temporary)?;
                return self.update(Event::Failed {
                    ticket,
                    failure: Failure::Protocol,
                });
            }
        };
        let page = core::Page {
            history_after_seq: page.history_after_seq,
            latest_seq: page.latest_seq,
            has_next: page.has_next,
            next_after_seq: page.next_after_seq,
            messages: page
                .items
                .into_iter()
                .map(incoming)
                .collect::<Result<_>>()?,
        };
        let snapshot = self.message_snapshot(&request.conversation_id, &page.messages)?;
        match flight.apply(self.store.fence(), &snapshot, page, self.now_ms, compare) {
            Ok(action) => {
                self.commit(action)?;
                self.emit(
                    json!({"type":"messages_changed","conversationId":request.conversation_id}),
                )?;
                self.message_progress(ticket, &request.conversation_id)?;
            }
            Err(StoreError::StaleRevision) => {}
            Err(error) => return Err(error.into()),
        }
        self.schedule_history(ticket)
    }

    fn message_progress(&mut self, ticket: &Ticket, conversation: &str) -> Result<()> {
        let snapshot = self.message_snapshot(conversation, &[])?;
        self.update(Event::MessageProgress {
            ticket,
            snapshot: &snapshot,
        })
    }

    pub(crate) fn gateway_frame(
        &mut self,
        ticket: &Ticket,
        conversation: &str,
        body: &str,
    ) -> Result<()> {
        let frame =
            gateway::decode_gateway_frame(body.as_bytes()).map_err(|_| Status::InvalidInput)?;
        match frame {
            GatewayFrame::Message(wire) if wire.conversation_id == conversation => {
                let item = incoming(wire)?;
                let snapshot = self.message_snapshot(conversation, std::slice::from_ref(&item))?;
                let action =
                    core::observe(self.store.fence(), &snapshot, item, self.now_ms, compare)?;
                let receipt = self.commit(action)?;
                if receipt.affected > 0 {
                    self.emit(json!({"type":"messages_changed","conversationId":conversation}))?;
                }
                self.message_progress(ticket, conversation)?;
                self.schedule_history(ticket)
            }
            GatewayFrame::VisibilityChanged(notice) if notice.conversation_id == conversation => {
                self.update(Event::VisibilityChanged {
                    ticket,
                    conversation_id: conversation,
                    revision: notice.revision,
                })
            }
            _ => Err(Status::InvalidInput),
        }
    }

    pub(crate) fn start_live_visibility(&mut self, ticket: &Ticket, through: i64) -> Result<()> {
        let Some(target) = self.selected_directory()?.into_subscription() else {
            return self.update(Event::Failed {
                ticket,
                failure: Failure::ConversationForbidden,
            });
        };
        if target.through_visibility_revision() < through {
            return self.update(Event::Failed {
                ticket,
                failure: Failure::Temporary,
            });
        }
        let snapshot = self.message_snapshot(target.conversation_id(), &[])?;
        if snapshot.state.visibility_revision >= through {
            return self.update(Event::VisibilityRefreshed {
                ticket,
                through_revision: through,
                snapshot: &snapshot,
            });
        }
        self.request_visibility(snapshot, &target, ticket, Some(through))
    }

    fn request_visibility(
        &mut self,
        snapshot: MessageSyncSnapshot,
        target: &Subscription,
        ticket: &Ticket,
        announced: Option<i64>,
    ) -> Result<()> {
        let request = visibility::PageRequest {
            conversation_id: snapshot.conversation_id,
            after_revision: snapshot.state.visibility_revision,
            through_revision: target.through_visibility_revision(),
            limit: visibility::MAX_PAGE_ITEMS,
        };
        self.send_visibility(
            ticket,
            VisibilityRequest {
                request,
                published_round: target.published_round(),
                announced,
            },
        )
    }

    fn send_visibility(&mut self, ticket: &Ticket, work: VisibilityRequest) -> Result<()> {
        let bytes = work.request.encode().map_err(|_| Status::InternalFailure)?;
        self.start_http(
            Work::Visibility(ticket.clone(), work),
            visibility::VISIBILITY_PATH,
            codec::utf8(bytes)?,
        )
    }

    pub(crate) fn visibility_response(
        &mut self,
        ticket: &Ticket,
        mut work: VisibilityRequest,
        status: u16,
        body: &str,
    ) -> Result<()> {
        if status != 200 {
            return self.network_response(ticket, status);
        }
        if work.announced.is_some_and(|announced| {
            !self
                .connection
                .accepts_visibility_refresh(ticket, announced)
        }) {
            return Err(Status::InvalidInput);
        }
        let wire = match visibility::decode_page(&work.request, body.as_bytes()) {
            Ok(page) => page,
            Err(_) => {
                return self.update(Event::Failed {
                    ticket,
                    failure: Failure::Protocol,
                });
            }
        };
        let conversation = work.request.conversation_id.clone();
        let snapshot = self.message_snapshot(&conversation, &[])?;
        let page = VisibilityPage {
            published_round: work.published_round,
            after_revision: work.request.after_revision,
            next_revision: wire.next_after_revision,
            through_revision: work.request.through_revision,
            events: wire
                .items
                .into_iter()
                .map(|item| VisibilityEvent {
                    server_id: item.server_msg_id,
                    sequence: item.conversation_seq,
                    revision: item.revision,
                    action: match item.action {
                        visibility::Action::Recall => VisibilityAction::Recall,
                        visibility::Action::DeleteForMe => VisibilityAction::DeleteForMe,
                        visibility::Action::AdminRemove => VisibilityAction::AdminRemove,
                    },
                })
                .collect(),
        };
        self.commit(core::apply_visibility(&snapshot, self.now_ms, page)?)?;
        self.emit(json!({"type":"messages_changed","conversationId":conversation}))?;
        if wire.has_next {
            work.request.after_revision = wire.next_after_revision;
            return self.send_visibility(ticket, work);
        }
        if let Some(through_revision) = work.announced {
            let snapshot = self.message_snapshot(&conversation, &[])?;
            self.update(Event::VisibilityRefreshed {
                ticket,
                through_revision,
                snapshot: &snapshot,
            })
        } else {
            self.message_progress(ticket, &conversation)
        }
    }
}
