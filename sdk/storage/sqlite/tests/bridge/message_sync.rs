use super::*;
use newim_protocol::{Message as WireMessage, message_delta as delta, send as protocol};
use newim_sdk_core::{
    message::{ConnectionGeneration, PersistedAck, SendContext, SendIntent},
    message_sync as flow, outbox,
};
use std::{
    path::Path,
    thread,
    time::{Duration, Instant},
};

const GUARD_TIMEOUT: Duration = Duration::from_secs(10);

struct Native {
    store: SqliteStore,
    next: u64,
}

impl Native {
    fn open(path: &Path, next: u64) -> Self {
        Self {
            store: SqliteStore::open(path, ACCOUNT, Limits::default()).unwrap(),
            next,
        }
    }

    fn snapshot(&mut self, messages: &[flow::IncomingMessage]) -> MessageSyncSnapshot {
        let action = flow::snapshot_query(ROOM, messages).unwrap();
        let Response::MessageSyncSnapshot(snapshot) = run(&mut self.store, 1, action).unwrap()
        else {
            panic!("expected SQLite message progress");
        };
        snapshot
    }

    fn observe(&mut self, incoming: flow::IncomingMessage, now: u64) {
        let snapshot = self.snapshot(std::slice::from_ref(&incoming));
        let action = flow::observe(self.store.fence(), &snapshot, incoming, now, compare).unwrap();
        commit(&mut self.store, &mut self.next, action);
    }

    fn start(&mut self, now: u64) -> Option<flow::InFlightPage> {
        let snapshot = self.snapshot(&[]);
        let action = flow::schedule(&snapshot, now).unwrap()?;
        let operation = self.next;
        commit(&mut self.store, &mut self.next, action);
        let snapshot = self.snapshot(&[]);
        flow::InFlightPage::started(self.store.fence(), &snapshot, operation)
    }

    fn receive(&mut self, flight: &flow::InFlightPage, page: flow::Page, now: u64) {
        let snapshot = self.snapshot(&page.messages);
        let action = flight
            .apply(self.store.fence(), &snapshot, page, now, compare)
            .unwrap();
        commit(&mut self.store, &mut self.next, action);
    }

    fn queue(&mut self, intent: SendIntent, now: u64) -> (Pending, SendContext) {
        let context = SendContext {
            sender_id: ACCOUNT.into(),
            fence: self.store.fence().clone(),
            connection_generation: ConnectionGeneration(1),
        };
        let record = outbox::OutboxRecord::new(intent, &context, now).unwrap();
        let pending = Pending {
            sender_id: ACCOUNT.into(),
            client_id: record.intent.client_id.clone(),
            conversation_id: ROOM.into(),
            payload: record.encode().unwrap(),
        };
        commit(
            &mut self.store,
            &mut self.next,
            Action::Enqueue(pending.clone()),
        );
        (pending, context)
    }

    fn accept_ack(&mut self, pending: &Pending, context: &SendContext, ack: &PersistedAck) {
        let revision = self.snapshot(&[]).revision;
        let result =
            outbox::resolve_ack(&mut self.store, self.next, pending, revision, context, ack);
        assert!(
            matches!(result, Ok(outbox::AckResolution::Resolved(_))),
            "{result:?}"
        );
        self.next += 1;
    }
}

fn incoming(value: WireMessage) -> flow::IncomingMessage {
    flow::IncomingMessage {
        protocol_version: value.protocol_version,
        message: Message {
            server_id: value.server_msg_id,
            sender_id: value.sender_id,
            client_id: value.client_msg_id,
            conversation_id: value.conversation_id,
            sequence: value.conversation_seq.parse().unwrap(),
            server_time: value.server_time.parse().unwrap(),
            schema_version: value.version,
            message_type: value.message_type,
            payload: Some(Blob(value.payload.get().as_bytes().to_vec())),
        },
    }
}

fn compare(left: &Blob, right: &Blob) -> Result<bool, StoreError> {
    protocol::same_message_payload(&left.0, &right.0).map_err(|_| StoreError::InvalidInput)
}

fn frame(bridge: &mut Bridge, sequence: i64) -> flow::IncomingMessage {
    bridge.send(&format!("FRAME\t{sequence}"));
    let line = bridge.line();
    let raw = decode_hex(line.strip_prefix("FRAME\t").expect("missing WSS message"));
    let ServerFrame::Message(message) = decode_server_frame(&raw).unwrap() else {
        panic!("expected persisted WSS message");
    };
    let value = incoming(message);
    assert_eq!(value.message.sequence, sequence);
    value
}

fn get_page(bridge: &mut Bridge, flight: &flow::InFlightPage) -> flow::Page {
    let request = flight.request();
    let wire = delta::MessageDeltaRequest {
        conversation_id: request.conversation_id,
        after_seq: request.after_seq,
        limit: request.limit,
    };
    let reply = bridge.call(
        "phone",
        "POST",
        delta::MESSAGE_DELTA_PATH,
        &delta::encode_message_delta_request(&wire).unwrap(),
    );
    reply.require_status(200, "message delta");
    let page = delta::decode_message_delta_page(&wire, &reply.bytes).unwrap();
    flow::Page {
        history_after_seq: page.history_after_seq,
        latest_seq: page.latest_seq,
        has_next: page.has_next,
        next_after_seq: page.next_after_seq,
        messages: page.items.into_iter().map(incoming).collect(),
    }
}

fn send(bridge: &mut Bridge, device: &str, client: &str, payload: &[u8]) -> protocol::Ack {
    let wire = protocol::Send {
        protocol_version: 1,
        client_msg_id: client.into(),
        conversation_id: ROOM.into(),
        version: 1,
        message_type: "text".into(),
        payload: serde_json::from_slice(payload).unwrap(),
    };
    let reply = bridge.call(
        device,
        "POST",
        "/api/v1/messages",
        &protocol::encode_send(&wire).unwrap(),
    );
    reply.require_status(200, "send fixture message");
    let ServerFrame::Ack(value) = decode_server_frame(&reply.bytes).unwrap() else {
        panic!("expected server acknowledgment");
    };
    value
}

fn queued_ack(
    native: &mut Native,
    bridge: &mut Bridge,
    client: &str,
    now: u64,
) -> (Pending, SendContext, PersistedAck) {
    let raw = Blob(br#"{ "text" : "own message" }"#.to_vec());
    let (pending, context) = native.queue(
        SendIntent {
            protocol_version: 1,
            client_id: client.into(),
            conversation_id: ROOM.into(),
            schema_version: 1,
            message_type: "text".into(),
            payload: raw.clone(),
        },
        now,
    );
    let ack = send(bridge, "phone", client, br#"{"text":"own message"}"#);
    let ack = PersistedAck {
        sender_id: ack.sender_id,
        client_id: ack.client_msg_id,
        conversation_id: ack.conversation_id,
        server_id: ack.server_msg_id,
        sequence: ack.conversation_seq.parse().unwrap(),
        server_time: ack.server_time.parse().unwrap(),
        protocol_version: 1,
        schema_version: 1,
        message_type: "text".into(),
        payload: raw,
    };
    (pending, context, ack)
}

fn drain(native: &mut Native, bridge: &mut Bridge, mut now: u64) {
    for _ in 0..8 {
        let Some(flight) = native.start(now) else {
            return;
        };
        let page = get_page(bridge, &flight);
        now += 1;
        native.receive(&flight, page, now);
        now += 1;
    }
    panic!("fixture message recovery did not finish");
}

#[test]
#[ignore = "run with make message-sdk-sync-check"]
fn tls_sqlite_message_sync() {
    let mut bridge = Bridge::start_fixture("TestMessageSDKBridge");
    let dir = Directory::new();
    let mut native = Native::open(&dir.0, 1);
    complete_bootstrap(&mut bridge, &mut native.store, &mut native.next);
    native.observe(frame(&mut bridge, 70), 1_000);
    native.observe(frame(&mut bridge, 2), 1_001);
    native.observe(frame(&mut bridge, 70), 1_002);
    let first = native.start(1_003).unwrap();
    assert_eq!(first.request().after_seq, 0);
    let page = get_page(&mut bridge, &first);
    assert!(page.has_next);
    assert_eq!(page.next_after_seq, Some(64));
    native.receive(&first, page, 1_004);
    let second = native.start(1_005).unwrap();
    let terminal = get_page(&mut bridge, &second);
    assert_eq!(second.request().after_seq, 64);
    assert!(!terminal.has_next);
    assert_eq!(terminal.next_after_seq, Some(70));
    send(
        &mut bridge,
        "peer",
        "gap_late_071",
        br#"{"text":"first new message"}"#,
    );
    send(
        &mut bridge,
        "peer",
        "gap_late_072",
        br#"{"text":"second new message"}"#,
    );
    native.observe(frame(&mut bridge, 72), 1_006);
    native.receive(&second, terminal, 1_007);
    let state = native.snapshot(&[]).state;
    assert_eq!(state.continuous_seq, 70);
    assert_eq!(state.highest_observed_seq, 72);
    assert!(state.job.is_some());
    drain(&mut native, &mut bridge, 1_008);
    let state = native.snapshot(&[]).state;
    assert_eq!(state.continuous_seq, 72);
    assert!(state.job.is_none());
    assert_eq!(
        cached_room_sequences(&mut native.store, Some(64)),
        (65..=72).collect::<Vec<_>>()
    );
    bridge.finish();
}

#[test]
#[ignore = "run with make message-sdk-sync-convergence"]
fn tls_sqlite_message_convergence() {
    let mut bridge = Bridge::start_fixture("TestMessageSDKBridge");
    let dir = Directory::new();
    let mut native = Native::open(&dir.0, 1);
    complete_bootstrap(&mut bridge, &mut native.store, &mut native.next);
    let (pending, context, ack) = queued_ack(&mut native, &mut bridge, "gap_ack_first", 1_000);
    native.accept_ack(&pending, &context, &ack);
    assert_eq!(ack.sequence, 71);
    let next = native.next;
    drop(native);
    let mut native = Native::open(&dir.0, next);
    let high = native.snapshot(&[]).state;
    assert_eq!(high.highest_observed_seq, 71);
    assert_eq!(high.continuous_seq, 0);
    drain(&mut native, &mut bridge, 1_001);
    let (pending, context, ack) = queued_ack(&mut native, &mut bridge, "gap_receive_first", 1_100);
    assert_eq!(ack.sequence, 72);
    let received = frame(&mut bridge, 72);
    assert_ne!(received.message.payload.as_ref(), Some(&ack.payload));
    native.observe(received, 1_101);
    assert_eq!(
        native.snapshot(&[frame(&mut bridge, 72)]).pending,
        vec![pending.clone()]
    );
    for _ in 0..2 {
        commit(
            &mut native.store,
            &mut native.next,
            Action::Trim {
                conversation: ROOM.into(),
                through: 72,
                limit: 64,
            },
        );
    }
    assert_eq!(
        native.snapshot(&[frame(&mut bridge, 72)]).messages[0]
            .payload
            .as_ref(),
        Some(&ack.payload)
    );
    native.accept_ack(&pending, &context, &ack);
    let replay = frame(&mut bridge, 72);
    let snapshot = native.snapshot(std::slice::from_ref(&replay));
    assert!(snapshot.pending.is_empty());
    assert_eq!(snapshot.messages[0].payload.as_ref(), Some(&ack.payload));
    assert_eq!(snapshot.state.continuous_seq, 72);
    native.observe(replay, 1_102);
    commit(
        &mut native.store,
        &mut native.next,
        Action::Trim {
            conversation: ROOM.into(),
            through: 72,
            limit: 64,
        },
    );
    native.observe(frame(&mut bridge, 72), 1_103);
    let snapshot = native.snapshot(&[frame(&mut bridge, 72)]);
    assert!(snapshot.messages[0].payload.is_none());
    bridge.finish();
}

#[test]
#[ignore = "run with make message-sdk-sync-recovery"]
fn tls_sqlite_message_recovery() {
    let mut bridge = Bridge::start_fixture("TestMessageSDKBridge");
    let dir = Directory::new();
    let mut native = Native::open(&dir.0, 1);
    complete_bootstrap(&mut bridge, &mut native.store, &mut native.next);
    native.observe(frame(&mut bridge, 70), 1_000);
    let next = native.next;
    drop(native);
    stop_during_attempt(&dir.0, next);
    let mut native = Native::open(&dir.0, next + 1);
    let snapshot = native.snapshot(&[]);
    let Some(GapJob {
        status: GapStatus::InFlight(attempt),
        ..
    }) = snapshot.state.job
    else {
        panic!("attempt did not survive process stop");
    };
    let expired = attempt.expires_at_ms;
    assert!(native.start(expired).is_none());
    let snapshot = native.snapshot(&[]);
    let job = snapshot.state.job.unwrap();
    assert_eq!(job.failures, 1);
    let GapStatus::Waiting { until_ms } = job.status else {
        panic!("expired attempt did not enter backoff");
    };
    assert!(native.start(expired).is_none());
    assert_eq!(native.snapshot(&[]).state.job.unwrap().failures, 1);
    let flight = native.start(until_ms).unwrap();
    bridge
        .call(
            "phone",
            "POST",
            &format!("/api/v1/conversations/{ROOM}/clear"),
            br#"{"expectedVersion":"0"}"#,
        )
        .require_status(200, "clear account history");
    let page = get_page(&mut bridge, &flight);
    assert_eq!(page.history_after_seq, 70);
    assert!(page.messages.is_empty());
    native.receive(&flight, page, until_ms + 1);
    let snapshot = native.snapshot(&[]);
    assert_eq!(snapshot.state.history_after_seq, 70);
    assert_eq!(snapshot.state.continuous_seq, 0);
    assert!(snapshot.state.job.is_none());
    native.observe(frame(&mut bridge, 70), until_ms + 2);
    assert!(cached_room_sequences(&mut native.store, None).is_empty());
    send(
        &mut bridge,
        "peer",
        "gap_visible_071",
        br#"{"text":"after personal clear"}"#,
    );
    native.observe(frame(&mut bridge, 71), until_ms + 3);
    assert_eq!(cached_room_sequences(&mut native.store, None), vec![71]);
    bridge.finish();
}

fn stop_during_attempt(path: &Path, operation: u64) {
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "message_sync::start_attempt_in_child",
        ])
        .env("NEWIM_MESSAGE_SYNC_CHILD_DIR", path)
        .env("NEWIM_MESSAGE_SYNC_CHILD_OPERATION", operation.to_string())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + GUARD_TIMEOUT;
    let marker = path.join("attempt-ready");
    while !marker.is_file() && Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("SQLite attempt process exited early: {status}");
        }
        thread::sleep(Duration::from_millis(20));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    assert!(
        marker.is_file(),
        "SQLite process did not commit the request attempt"
    );
}

#[test]
#[ignore = "subprocess of the native message recovery test"]
fn start_attempt_in_child() {
    let path = std::env::var_os("NEWIM_MESSAGE_SYNC_CHILD_DIR").unwrap();
    let operation = std::env::var("NEWIM_MESSAGE_SYNC_CHILD_OPERATION")
        .unwrap()
        .parse()
        .unwrap();
    let path = Path::new(&path);
    let mut native = Native::open(path, operation);
    assert!(native.start(1_001).is_some());
    std::fs::write(path.join("attempt-ready"), b"ready").unwrap();
    thread::park_timeout(GUARD_TIMEOUT * 2);
}
