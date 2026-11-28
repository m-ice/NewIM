//! Host-local counters and correlation for validated, committed receipts.

use crate::connection::{Connection, Effect, Ticket};
use crate::message::{PersistedAck, SendContext};
use crate::outbox::{self, AckResolution, OutboxError};
use crate::store::{
    Action, Completion, LocalStore, MessageSyncMutation, MessageSyncSnapshot, Pending, Request,
    Response,
};
use std::fmt;

const MAX_COUNT: u32 = 1024;
const MAX_ACK_MS: u64 = 600_000;
const MAX_PENDING: u64 = 10_000;

pub const SDK_REPORT_PATH: &str = "/api/v1/observability/sdk";
pub const TRACE_HEADER: &str = "X-NewIM-Trace-Id";

#[derive(Clone, Default, PartialEq, Eq)]
pub struct MetricsReport {
    socket_attempt: u32,
    ready: u32,
    reconnect: u32,
    ack_committed: u32,
    ack_failed: u32,
    ack_millis: u32,
    gap_detected: u32,
    page_committed: u32,
    page_failed: u32,
    message_committed: u32,
    outbox_samples: u32,
    outbox_pending: u32,
}

impl MetricsReport {
    /// Encode the fixed v1 aggregate. This payload carries no IDs or request data.
    pub fn encode(&self) -> String {
        format!(
            concat!(
                "{{\"version\":1,\"socket_attempt\":{},\"ready\":{},\"reconnect\":{},",
                "\"ack_committed\":{},\"ack_failed\":{},\"ack_millis\":{},\"gap_detected\":{},",
                "\"page_committed\":{},\"page_failed\":{},\"message_committed\":{},",
                "\"outbox_samples\":{},\"outbox_pending\":{}}}"
            ),
            self.socket_attempt,
            self.ready,
            self.reconnect,
            self.ack_committed,
            self.ack_failed,
            self.ack_millis,
            self.gap_detected,
            self.page_committed,
            self.page_failed,
            self.message_committed,
            self.outbox_samples,
            self.outbox_pending,
        )
    }
}

#[derive(Default)]
pub(crate) struct Metrics {
    report: MetricsReport,
    dialed: bool,
    gap: Option<(String, u64)>,
    last_store_revision: Option<u64>,
}

fn increment(count: &mut u32) -> bool {
    if *count == MAX_COUNT {
        return false;
    }
    *count += 1;
    true
}

impl Metrics {
    pub(crate) fn connection_effects(&mut self, effects: &[Effect]) {
        for effect in effects {
            match effect {
                Effect::OpenSocket(_) => {
                    increment(&mut self.report.socket_attempt);
                    if self.dialed {
                        increment(&mut self.report.reconnect);
                    }
                    self.dialed = true;
                }
                Effect::Ready(_) => {
                    increment(&mut self.report.ready);
                }
                Effect::AdvanceFence(_) => *self = Self::default(),
                _ => {}
            }
        }
    }

    pub(crate) fn observe_gap(&mut self, snapshot: &MessageSyncSnapshot) {
        let job = snapshot.state.job.as_ref().filter(|_| {
            snapshot.visible && snapshot.state.after_seq() < snapshot.state.highest_observed_seq
        });
        if let Some(job) = job {
            let id = &snapshot.conversation_id;
            let start = job.started_at_ms;
            if !self
                .gap
                .as_ref()
                .is_some_and(|(room, previous)| room == id && *previous == start)
            {
                increment(&mut self.report.gap_detected);
                self.gap = Some((id.clone(), start));
            }
        } else if self
            .gap
            .as_ref()
            .is_some_and(|(room, _)| room == &snapshot.conversation_id)
        {
            self.gap = None;
        }
    }

    fn complete(&mut self, request: &Request, response: &Response) {
        if let (Action::Metrics, Response::Metrics(metrics)) = (&request.action, response) {
            if !metrics.recovery_required && increment(&mut self.report.outbox_samples) {
                self.report.outbox_pending += metrics.pending_count.min(MAX_PENDING) as u32;
            }
        } else if let (Action::ApplyMessageSync(batch), Response::Committed(receipt)) =
            (&request.action, response)
            && !receipt.replayed
            && self
                .last_store_revision
                .is_none_or(|last| receipt.revision > last)
        {
            self.last_store_revision = Some(receipt.revision);
            match batch.mutation {
                MessageSyncMutation::Observe(_) if receipt.affected > 0 => {
                    increment(&mut self.report.message_committed);
                }
                MessageSyncMutation::CommitPage { .. } => {
                    increment(&mut self.report.page_committed);
                }
                MessageSyncMutation::FailAttempt { .. } => {
                    increment(&mut self.report.page_failed);
                }
                _ => {}
            }
        }
    }
}

/// The persisted intent and original authority for an in-flight HTTP send.
pub struct AckFlight<'a> {
    pub context: &'a SendContext,
    pub pending: &'a Pending,
    pub expected_revision: u64,
    pub operation_id: u64,
    pub attempted_at_ms: u64,
}

/// A diagnostic association returned to the host after a verified receipt commit.
pub struct ReceiptTrace<'a> {
    pub trace_id: &'a str,
    pub client_msg_id: &'a str,
    pub server_msg_id: &'a str,
}

impl fmt::Debug for ReceiptTrace<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReceiptTrace { redacted }")
    }
}

fn single_trace<'a>(headers: &[&'a str]) -> Option<&'a str> {
    let [id] = headers else {
        return None;
    };
    (id.len() == 32
        && id
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)))
    .then_some(*id)
}

impl Connection {
    /// Observe a real completion on the host's serialized storage queue.
    pub fn observe_store_completion(
        &mut self,
        ticket: &Ticket,
        request: &Request,
        completion: &Completion,
    ) {
        if self.accepts(ticket)
            && request.fence == ticket.fence
            && completion.belongs_to(&ticket.fence)
            && request.operation_id == completion.operation_id
            && let Ok(response) = &completion.result
        {
            self.metrics.complete(request, response);
        }
    }

    /// The protocol adapter must decode the ACK and take headers from the same verified TLS response.
    /// If the optional trace is absent or malformed, normal ACK reconciliation still applies.
    pub fn resolve_http_ack<'a, S: LocalStore>(
        &mut self,
        store: &mut S,
        flight: AckFlight<'_>,
        ack: &'a PersistedAck,
        trace_headers: &[&'a str],
        now_ms: u64,
    ) -> Result<(AckResolution, Option<ReceiptTrace<'a>>), OutboxError> {
        if !self.accepts_ack(flight.context, now_ms) {
            return Err(OutboxError::GenerationMismatch);
        }
        let committed = outbox::resolve_ack(
            store,
            flight.operation_id,
            flight.pending,
            flight.expected_revision,
            flight.context,
            ack,
        );
        match committed {
            Ok(receipt) => {
                if increment(&mut self.metrics.report.ack_committed) {
                    self.metrics.report.ack_millis += now_ms
                        .saturating_sub(flight.attempted_at_ms)
                        .min(MAX_ACK_MS)
                        as u32;
                }
                let trace = single_trace(trace_headers).map(|trace_id| ReceiptTrace {
                    trace_id,
                    client_msg_id: &ack.client_id,
                    server_msg_id: &ack.server_id,
                });
                Ok((receipt, trace))
            }
            Err(error) => {
                increment(&mut self.metrics.report.ack_failed);
                Err(error)
            }
        }
    }

    /// Take a best-effort delta while this connection can send authenticated requests.
    pub fn take_metrics_report(&mut self) -> Option<MetricsReport> {
        self.send_context()?;
        if self.metrics.report == MetricsReport::default() {
            return None;
        }
        Some(std::mem::take(&mut self.metrics.report))
    }
}
