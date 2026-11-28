mod common;
#[path = "bridge/connection.rs"]
mod connection;
#[path = "bridge/message_sync.rs"]
mod message_sync;

use common::{Directory, batch, run, snapshot};
use newim_protocol::conversation_sync as wire;
use newim_protocol::send::{ServerFrame, decode_server_frame};
use newim_sdk_core::{conversation_sync as sdk, store::*};
use newim_store_sqlite::{Limits, SqliteStore};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, BufWriter, Read, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
};

const ACCOUNT: &str = "sdk_bridge_alice";
const ROOM: &str = "sdk_bridge_group_00";
const DEPARTED: &str = "sdk_bridge_departed";
const DRAFT: &str = "draft from the other device";
const MAX_REPLY_LINE: usize = 2 * 65_536 + 32;

struct Bridge {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
}

struct Reply {
    status: u16,
    bytes: Vec<u8>,
}

impl Reply {
    fn require_status(&self, expected: u16, action: &str) {
        if self.status != expected {
            let code = wire::decode_error_code(&self.bytes)
                .unwrap_or_else(|_| "unknown response".to_owned());
            panic!(
                "{action} returned HTTP {} ({code}), expected {expected}",
                self.status
            );
        }
    }
}

impl Bridge {
    fn start() -> Self {
        Self::start_fixture("TestAccountSDKBridge")
    }

    fn start_fixture(name: &str) -> Self {
        let container = std::env::var("NEWIM_ACCOUNT_SDK_BRIDGE_CONTAINER")
            .expect("start the owned TLS fixture through its Make target");
        assert!(container.strip_prefix("newim-db-").is_some_and(|id| {
            id.len() == 32
                && id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }));
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                "-e",
                "NEWIM_GATEWAY_BINARY=/tmp/newim-gateway",
                "-e",
                "NEWIM_SERVER_BINARY=/tmp/newim-server",
                "-e",
                "NEWIM_SYNC_BACKFILL_BINARY=/tmp/newim-sync-backfill",
                &container,
                "/tmp/conversationsynchttp.test",
                "-test.run",
                &format!("^{name}$"),
                "-test.count=1",
                "-test.timeout=120s",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("could not start owned TLS fixture transport");
        let input = BufWriter::new(child.stdin.take().unwrap());
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut bridge = Self {
            child,
            input,
            output,
        };
        assert!(bridge.line() == "READY", "TLS fixture did not become ready");
        bridge
    }

    fn send(&mut self, line: &str) {
        writeln!(self.input, "{line}").expect("could not write to TLS fixture");
        self.input
            .flush()
            .expect("could not flush TLS fixture request");
    }

    fn line(&mut self) -> String {
        self.line_with_limit(MAX_REPLY_LINE)
    }

    fn line_with_limit(&mut self, max: usize) -> String {
        let mut bytes = Vec::new();
        (&mut self.output)
            .take(max as u64 + 1)
            .read_until(b'\n', &mut bytes)
            .expect("could not read TLS fixture response");
        assert!(
            !bytes.is_empty() && bytes.len() <= max && bytes.pop() == Some(b'\n'),
            "TLS fixture response is missing or exceeds the transport limit"
        );
        let line = String::from_utf8(bytes).expect("TLS fixture transport returned invalid UTF-8");
        if let Some(phase) = line.strip_prefix("FAILED\t") {
            match phase {
                "database" | "gateway" | "server" | "accounts" | "groups" | "messages"
                | "transport" | "shutdown" | "leave" | "backfill" | "restore" => {
                    panic!("TLS fixture failed during {phase}");
                }
                _ => panic!("TLS fixture did not finish"),
            }
        }
        line
    }

    fn call(&mut self, device: &str, method: &str, path: &str, body: &[u8]) -> Reply {
        self.send(&format!(
            "HTTP\t{device}\t{method}\t{path}\t{}",
            encode_hex(body)
        ));
        let line = if path == newim_protocol::message_delta::MESSAGE_DELTA_PATH {
            self.line_with_limit(
                2 * newim_protocol::message_delta::MAX_MESSAGE_DELTA_PAGE_BYTES + 32,
            )
        } else {
            self.line()
        };
        let mut parts = line.splitn(3, '\t');
        assert!(
            parts.next() == Some("HTTP"),
            "TLS fixture did not return a response"
        );
        let status = parts
            .next()
            .and_then(|part| part.parse().ok())
            .expect("TLS fixture did not return an HTTP status");
        let bytes = decode_hex(parts.next().expect("TLS fixture omitted its HTTP body"));
        Reply { status, bytes }
    }

    fn page(&mut self, flight: &sdk::InFlightPage) -> Reply {
        self.page_for("phone", flight)
    }

    fn page_for(&mut self, device: &str, flight: &sdk::InFlightPage) -> Reply {
        let request = match flight.request() {
            sdk::PageRequest::Bootstrap { limit } => wire::PageRequest::Bootstrap { limit },
            sdk::PageRequest::ContinueBootstrap { cursor } => {
                wire::PageRequest::ContinueBootstrap {
                    cursor: String::from_utf8(cursor.0).unwrap(),
                }
            }
            sdk::PageRequest::Delta { checkpoint, limit } => wire::PageRequest::Delta {
                checkpoint: String::from_utf8(checkpoint.0).unwrap(),
                limit,
            },
            sdk::PageRequest::ContinueDelta { cursor } => wire::PageRequest::ContinueDelta {
                cursor: String::from_utf8(cursor.0).unwrap(),
            },
        };
        self.call(device, "POST", request.path(), &request.encode().unwrap())
    }

    fn reset(&mut self) {
        self.send("RESET");
        assert!(self.line() == "RESET_OK", "server backfill did not finish");
    }

    fn restore(&mut self) {
        self.send("RESTORE");
        assert!(
            self.line() == "RESTORE_OK",
            "new message did not restore the conversation"
        );
    }

    fn finish(mut self) {
        self.send("QUIT");
        assert!(self.line() == "DONE", "TLS fixture did not finish cleanly");
        assert!(
            self.child.wait().unwrap().success(),
            "TLS fixture process failed"
        );
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        hex.push(char::from(DIGITS[(byte >> 4) as usize]));
        hex.push(char::from(DIGITS[(byte & 15) as usize]));
    }
    hex
}

fn decode_hex(value: &str) -> Vec<u8> {
    fn digit(byte: u8) -> u8 {
        match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => panic!("TLS fixture returned invalid hex"),
        }
    }

    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(
        remainder.is_empty(),
        "TLS fixture returned an incomplete hex pair"
    );
    pairs
        .iter()
        .map(|pair| digit(pair[0]) << 4 | digit(pair[1]))
        .collect()
}

fn decode_page(reply: Reply) -> sdk::Page {
    reply.require_status(200, "read sync page");
    let page =
        wire::decode_page(&reply.bytes).expect("TLS sync response did not match the Rust codec");
    let items = page
        .items
        .into_iter()
        .map(|item| ConversationSyncItem {
            conversation_id: item.conversation_id,
            revision: item.revision,
            known_clear_through_seq: 0,
            change: match item.change {
                wire::Change::Remove => ConversationSyncChange::Remove,
                wire::Change::Upsert(summary) => {
                    ConversationSyncChange::Upsert(ConversationSyncSummary {
                        latest_conversation_seq: summary.latest_conversation_seq,
                        latest_server_msg_id: summary.latest_server_msg_id,
                        last_read_seq: summary.last_read_seq,
                        unread: summary.unread.map(|unread| ConversationUnread {
                            count: unread.count,
                            visibility_revision: unread.visibility_revision,
                        }),
                        settings: summary.settings.map(|value| ConversationSyncSettings {
                            version: value.version,
                            pinned: value.pinned,
                            muted: value.muted,
                            draft: value.draft,
                            clear_through_seq: value.clear_through_seq,
                        }),
                    })
                }
            },
        })
        .collect();
    sdk::Page {
        items,
        next_cursor: Blob(page.next_cursor.into_bytes()),
        has_more: page.has_more,
    }
}

fn read_sync(store: &mut SqliteStore, query: ConversationSyncQuery) -> ConversationSyncSnapshot {
    let Response::ConversationSyncSnapshot(snapshot) =
        run(store, 1, Action::ConversationSyncSnapshot(query)).unwrap()
    else {
        panic!("SQLite did not return a synchronization snapshot");
    };
    snapshot
}

fn state(store: &mut SqliteStore) -> ConversationSyncSnapshot {
    read_sync(store, ConversationSyncQuery::State)
}

fn published(store: &mut SqliteStore) -> ConversationSyncSnapshot {
    read_sync(
        store,
        ConversationSyncQuery::Published {
            after: None,
            limit: 64,
        },
    )
}

fn dispatch(
    store: &mut SqliteStore,
    next: &mut u64,
    action: Action,
) -> Result<Response, StoreError> {
    let operation = *next;
    *next += 1;
    run(store, operation, action)
}

fn commit(store: &mut SqliteStore, next: &mut u64, action: Action) {
    let result = dispatch(store, next, action);
    assert!(
        matches!(result, Ok(Response::Committed(_))),
        "SQLite write failed: {result:?}"
    );
}

fn capture(store: &mut SqliteStore) -> sdk::InFlightPage {
    let current = state(store);
    sdk::InFlightPage::new(store.fence(), &current)
}

fn apply_page(store: &mut SqliteStore, flight: &sdk::InFlightPage, page: &sdk::Page) -> Action {
    let merge = read_sync(store, flight.snapshot_query(page).unwrap());
    flight.apply(store.fence(), &merge, page).unwrap()
}

fn complete_bootstrap(bridge: &mut Bridge, store: &mut SqliteStore, next: &mut u64) {
    for _ in 0..3 {
        let flight = capture(store);
        assert!(matches!(
            flight.request(),
            sdk::PageRequest::Bootstrap { .. } | sdk::PageRequest::ContinueBootstrap { .. }
        ));
        let page = decode_page(bridge.page(&flight));
        let action = apply_page(store, &flight, &page);
        commit(store, next, action);
        if !page.has_more {
            return;
        }
    }
    panic!("fixture bootstrap did not finish");
}

fn pending(client: &str) -> Action {
    Action::Enqueue(Pending {
        sender_id: ACCOUNT.into(),
        client_id: client.into(),
        conversation_id: DEPARTED.into(),
        payload: Blob(b"local draft".to_vec()),
    })
}

fn room_summary(directory: &ConversationSyncSnapshot) -> &ConversationSyncSummary {
    let row = directory
        .items
        .iter()
        .find(|row| row.conversation_id == ROOM)
        .unwrap();
    let ConversationSyncChange::Upsert(summary) = &row.change else {
        panic!("fixture room is absent from published directory");
    };
    summary
}

fn read_room_messages(bridge: &mut Bridge) -> Vec<Message> {
    let request =
        serde_json::to_vec(&json!({"conversationId":ROOM,"afterSeq":"0","limit":20})).unwrap();
    let reply = bridge.call("phone", "POST", "/api/v1/sync/messages", &request);
    reply.require_status(200, "read persisted room messages");
    let page: Value = serde_json::from_slice(&reply.bytes).unwrap();
    page["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|frame| {
            let wire = serde_json::to_vec(frame).unwrap();
            let ServerFrame::Message(item) = decode_server_frame(&wire).unwrap() else {
                panic!("room history did not contain a persisted message");
            };
            assert_eq!(item.conversation_id, ROOM);
            Message {
                server_id: item.server_msg_id,
                sender_id: item.sender_id,
                client_id: item.client_msg_id,
                conversation_id: item.conversation_id,
                sequence: item.conversation_seq.parse().unwrap(),
                server_time: item.server_time.parse().unwrap(),
                schema_version: item.version,
                message_type: item.message_type,
                payload: Some(Blob(item.payload.get().as_bytes().to_vec())),
            }
        })
        .collect()
}

fn cached_room_sequences(store: &mut SqliteStore, after: Option<i64>) -> Vec<i64> {
    let Response::Messages(page) = run(
        store,
        1,
        Action::Messages {
            conversation: ROOM.into(),
            after,
            limit: 10,
        },
    )
    .unwrap() else {
        panic!("room cache could not be read");
    };
    page.items
        .into_iter()
        .map(|message| message.sequence)
        .collect()
}

#[test]
#[ignore = "run with make conversation-account-sdk-check"]
fn tls_sqlite_conversation_account_sync() {
    let mut bridge = Bridge::start();
    let dir = Directory::new();
    let mut store = SqliteStore::open(&dir.0, ACCOUNT, Limits::default()).unwrap();
    let mut next = 1;
    let legacy_cursor = Blob(vec![0xf0, 0, 0x80]);
    let legacy_room = Conversation {
        id: DEPARTED.into(),
        unread: 17,
        summary: Blob(vec![0xff, 0, 0x80]),
    };
    commit(
        &mut store,
        &mut next,
        Action::Apply(Batch {
            expected_revision: 0,
            messages: vec![MessageWrite::Insert(Message {
                server_id: "local_message".into(),
                sender_id: ACCOUNT.into(),
                client_id: "local_committed".into(),
                conversation_id: DEPARTED.into(),
                sequence: 1,
                server_time: 0,
                schema_version: 1,
                message_type: "text".into(),
                payload: Some(Blob(b"local message".to_vec())),
            })],
            conversations: vec![legacy_room.clone()],
            cursor: Some(legacy_cursor.clone()),
            resolve_pending: vec![],
        }),
    );

    let first = capture(&mut store);
    assert!(matches!(
        first.request(),
        sdk::PageRequest::Bootstrap { .. }
    ));
    let page = decode_page(bridge.page(&first));
    assert!(page.has_more);
    assert_eq!(page.items.len(), sdk::PAGE_LIMIT);
    let stale = apply_page(&mut store, &first, &page);
    commit(&mut store, &mut next, pending("local_pending"));
    assert_eq!(
        dispatch(&mut store, &mut next, stale),
        Err(StoreError::StaleRevision)
    );
    assert_eq!(
        state(&mut store).position,
        ConversationSyncPosition::initial()
    );
    assert!(published(&mut store).items.is_empty());
    let fresh = apply_page(&mut store, &first, &page);
    commit(&mut store, &mut next, fresh);
    let staged = state(&mut store);
    assert_eq!(
        staged.position.phase,
        ConversationSyncPhase::BootstrapPage(page.next_cursor)
    );
    assert_eq!(staged.published_round, None);
    let fence = store.fence().clone();
    drop(store);

    let mut store = SqliteStore::open(&dir.0, ACCOUNT, Limits::default()).unwrap();
    assert_eq!(store.fence(), &fence);
    assert_eq!(state(&mut store), staged);
    complete_bootstrap(&mut bridge, &mut store, &mut next);
    let directory = published(&mut store);
    assert_eq!(directory.items.len(), 34);
    assert!(
        directory
            .items
            .iter()
            .any(|item| item.conversation_id == DEPARTED)
    );

    let mut cached = read_room_messages(&mut bridge);
    assert_eq!(cached.len(), 3);
    let late = cached.remove(1);
    assert_eq!(late.sequence, 2);
    let revision = snapshot(&mut store).revision;
    commit(&mut store, &mut next, batch(revision, cached));
    assert_eq!(cached_room_sequences(&mut store, None), [1, 3]);

    let settings_path = format!("/api/v1/conversations/{ROOM}/settings");
    let settings = bridge.call("tablet", "GET", &settings_path, &[]);
    settings.require_status(200, "read tablet settings");
    let settings: Value = serde_json::from_slice(&settings.bytes).unwrap();
    let version = settings["settingsVersion"].as_str().unwrap();
    let patch = serde_json::to_vec(&json!({"expectedVersion":version,"draft":DRAFT,"pinned":true}))
        .unwrap();
    bridge
        .call("tablet", "PATCH", &settings_path, &patch)
        .require_status(200, "change tablet settings");
    let read_path = format!("/api/v1/conversations/{ROOM}/read");
    bridge
        .call("tablet", "POST", &read_path, br#"{"lastReadSeq":"2"}"#)
        .require_status(200, "advance tablet read watermark");

    let delta = capture(&mut store);
    assert!(matches!(delta.request(), sdk::PageRequest::Delta { .. }));
    let page = decode_page(bridge.page(&delta));
    assert_eq!(page.items.len(), 2);
    assert!(page.items.iter().all(|item| item.conversation_id == ROOM));
    let write = apply_page(&mut store, &delta, &page);
    let Action::ApplyConversationSync(ConversationSyncBatch {
        mutation: ConversationSyncMutation::CommitPage { items, .. },
        ..
    }) = &write
    else {
        panic!("SDK did not produce one atomic sync page");
    };
    assert_eq!(items.len(), 1);
    commit(&mut store, &mut next, write);
    let directory = published(&mut store);
    let summary = room_summary(&directory);
    assert_eq!(summary.last_read_seq, Some(2));
    let settings = summary.settings.as_ref().unwrap();
    assert!(settings.pinned && settings.draft == DRAFT);

    let clear_path = format!("/api/v1/conversations/{ROOM}/clear");
    let clear =
        serde_json::to_vec(&json!({"expectedVersion": settings.version.to_string()})).unwrap();
    bridge
        .call("tablet", "POST", &clear_path, &clear)
        .require_status(200, "clear tablet history");
    let delta = capture(&mut store);
    let page = decode_page(bridge.page(&delta));
    let write = apply_page(&mut store, &delta, &page);
    commit(&mut store, &mut next, write);
    let directory = published(&mut store);
    let room = directory
        .items
        .iter()
        .find(|item| item.conversation_id == ROOM)
        .unwrap();
    assert!(!room.allows_message(3));
    assert!(room.allows_message(4));
    let summary = room_summary(&directory);
    assert_eq!(summary.last_read_seq, Some(2));
    let late_server_id = late.server_id.clone();
    let revision = snapshot(&mut store).revision;
    commit(&mut store, &mut next, batch(revision, vec![late]));
    assert!(cached_room_sequences(&mut store, None).is_empty());
    assert!(matches!(
        run(&mut store, 1, Action::Lookup { server_id: late_server_id }),
        Ok(Response::Found(Some(message))) if message.sequence == 2
    ));
    let version = summary.settings.as_ref().unwrap().version;
    let delete_path = format!("/api/v1/conversations/{ROOM}/delete");
    let delete = serde_json::to_vec(&json!({"expectedVersion":version.to_string()})).unwrap();
    bridge
        .call("tablet", "POST", &delete_path, &delete)
        .require_status(200, "delete tablet conversation");
    let delta = capture(&mut store);
    let page = decode_page(bridge.page(&delta));
    let write = apply_page(&mut store, &delta, &page);
    commit(&mut store, &mut next, write);
    assert!(
        published(&mut store)
            .items
            .iter()
            .all(|item| item.conversation_id != ROOM)
    );

    let old = capture(&mut store);
    let delayed = decode_page(bridge.page(&old));
    bridge.reset();
    let expired = bridge.page(&old);
    expired.require_status(409, "read expired cursor");
    let code = wire::decode_error_code(&expired.bytes).unwrap();
    let before = state(&mut store);
    let restart = old
        .restart(store.fence(), &before, expired.status, &code)
        .unwrap();
    commit(&mut store, &mut next, restart);
    let after = state(&mut store);
    assert_eq!(after.position.round, before.position.round + 1);
    assert_eq!(after.position.phase, ConversationSyncPhase::BootstrapStart);
    assert_eq!(
        old.apply(store.fence(), &after, &delayed),
        Err(StoreError::StaleRevision)
    );
    assert_eq!(published(&mut store).items.len(), 33);
    complete_bootstrap(&mut bridge, &mut store, &mut next);
    let rebuilt = published(&mut store);
    assert_eq!(rebuilt.items.len(), 32);
    assert!(
        rebuilt
            .items
            .iter()
            .all(|item| item.conversation_id != DEPARTED && item.conversation_id != ROOM)
    );
    let fence = store.fence().clone();
    drop(store);
    let mut store = SqliteStore::open(&dir.0, ACCOUNT, Limits::default()).unwrap();
    assert_eq!(store.fence(), &fence);
    assert!(cached_room_sequences(&mut store, None).is_empty());
    bridge.restore();
    let delta = capture(&mut store);
    let page = decode_page(bridge.page_for("legacy-phone", &delta));
    let write = apply_page(&mut store, &delta, &page);
    commit(&mut store, &mut next, write);
    let restored = published(&mut store);
    assert_eq!(restored.items.len(), 33);
    let summary = room_summary(&restored);
    assert_eq!(summary.last_read_seq, Some(2));
    assert!(summary.settings.is_none());
    let room = restored
        .items
        .iter()
        .find(|item| item.conversation_id == ROOM)
        .unwrap();
    assert_eq!(room.known_clear_through_seq, 3);
    assert!(!room.allows_message(3));
    assert!(room.allows_message(4));
    let current = read_room_messages(&mut bridge);
    assert_eq!(
        current
            .iter()
            .map(|message| message.sequence)
            .collect::<Vec<_>>(),
        [4]
    );
    let revision = snapshot(&mut store).revision;
    commit(&mut store, &mut next, batch(revision, current));
    assert_eq!(cached_room_sequences(&mut store, Some(1)), [4]);

    let Response::Snapshot(saved) = run(
        &mut store,
        1,
        Action::Snapshot {
            conversation: Some(DEPARTED.into()),
        },
    )
    .unwrap() else {
        panic!("legacy SQLite summary snapshot unavailable");
    };
    assert_eq!(saved.cursor, legacy_cursor);
    assert_eq!(saved.conversation, Some(legacy_room));
    assert!(matches!(
        run(
            &mut store,
            1,
            Action::Lookup {
                server_id: "local_message".into()
            }
        ),
        Ok(Response::Found(Some(_)))
    ));
    assert!(matches!(
        run(&mut store, 1, Action::Pending { after: None, limit: 64 }),
        Ok(Response::Pending(PendingPage { items, .. })) if items.iter().any(|row| row.client_id == "local_pending")
    ));
    assert_eq!(snapshot(&mut store).cursor, legacy_cursor);
    bridge.finish();
}
