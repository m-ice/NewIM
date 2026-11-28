use super::message_sync::{Native, get_page_for, queued_ack_with};
use super::*;
use newim_protocol::gateway::{
    GATEWAY_SUBPROTOCOL, GatewayClose, GatewayFrame, GatewaySubscription, VisibilityNotice,
    classify_gateway_close, decode_gateway_frame, encode_gateway_subscription,
};
use newim_protocol::visibility as visibility_wire;
use newim_sdk_core::{
    connection::{
        AuthCause, CommittedDirectory, Connection, Effect, Event, Failure, Status, Subscription,
        Ticket,
    },
    message::ConnectionGeneration,
    message_sync as recovery,
};

const EMPTY_ROOM: &str = "sdk_bridge_group_empty";
const NOW: u64 = 10_000;

fn event(connection: &mut Connection, event: Event<'_>, now: u64) -> Vec<Effect> {
    connection.update(event, now, 0).unwrap()
}

fn directory_ticket(effects: Vec<Effect>) -> Ticket {
    let [Effect::LoadDirectory(ticket)]: [Effect; 1] = effects.try_into().unwrap() else {
        panic!("expected directory sync");
    };
    ticket
}

fn credential_ticket(effects: Vec<Effect>, expected: AuthCause) -> Ticket {
    let [Effect::RecoverCredentials { ticket, cause }]: [Effect; 1] = effects.try_into().unwrap()
    else {
        panic!("expected recovery of credentials");
    };
    assert_eq!(cause, expected);
    ticket
}

fn authorize(connection: &mut Connection, ticket: &Ticket, now: u64) -> Ticket {
    directory_ticket(event(
        connection,
        Event::Credentials {
            ticket,
            expires_at_ms: now + 90_000,
        },
        now,
    ))
}

fn progress(native: &mut Native, room: &str) -> MessageSyncSnapshot {
    let action = recovery::snapshot_query(room, &[]).unwrap();
    let Response::MessageSyncSnapshot(snapshot) = run(&mut native.store, 1, action).unwrap() else {
        panic!("expected durable message progress");
    };
    snapshot
}

fn sync_directory(
    bridge: &mut Bridge,
    native: &mut Native,
    device: &str,
) -> ConversationSyncSnapshot {
    for _ in 0..8 {
        let flight = capture(&mut native.store);
        let page = decode_page(bridge.page_for(device, &flight));
        let action = apply_page(&mut native.store, &flight, &page);
        commit(&mut native.store, &mut native.next, action);
        if !page.has_more {
            return published(&mut native.store);
        }
    }
    panic!("TLS directory did not reach a checkpoint");
}

fn socket(bridge: &mut Bridge, device: &str, expected: u16) {
    bridge.send(&format!("SOCKET\t{device}"));
    let subprotocol = if expected == 101 {
        GATEWAY_SUBPROTOCOL
    } else {
        ""
    };
    assert_eq!(bridge.line(), format!("SOCKET\t{expected}\t{subprotocol}"));
}

fn close(bridge: &mut Bridge) {
    bridge.send("CLOSE");
    assert_eq!(bridge.line(), "CLOSE_OK");
}

fn bind(
    bridge: &mut Bridge,
    native: &mut Native,
    connection: &mut Connection,
    ticket: &Ticket,
    device: &str,
    target: (&str, i64),
    now: u64,
) -> Vec<Effect> {
    let (room, after_seq) = target;
    let directory = sync_directory(bridge, native, device);
    let start = progress(native, room);
    assert_eq!(start.state.after_seq(), after_seq);
    let committed = CommittedDirectory::from_store(&directory, Some(&start)).unwrap();
    assert_eq!(
        event(connection, Event::Directory { ticket, committed }, now),
        vec![Effect::OpenSocket(ticket.clone())],
    );
    socket(bridge, device, 101);
    let [
        Effect::Subscribe {
            ticket: callback,
            target,
        },
    ]: [Effect; 1] = event(connection, Event::SocketOpened(ticket), now)
        .try_into()
        .unwrap()
    else {
        panic!("expected WSS subscription");
    };
    assert_eq!(&callback, ticket);
    assert_eq!(target.after_seq(), after_seq);
    let wire = GatewaySubscription {
        conversation_id: target.conversation_id().to_owned(),
        after_seq: target.after_seq(),
        after_visibility_revision: target.gateway_after_visibility_revision(),
    };
    bridge.send(&format!(
        "SUBSCRIBE\t{}",
        encode_hex(&encode_gateway_subscription(&wire).unwrap())
    ));
    assert_eq!(bridge.line(), "SUBSCRIBED");
    event(connection, Event::Subscribed(ticket), now)
}

fn advance(
    native: &mut Native,
    connection: &mut Connection,
    old: &Ticket,
    now: u64,
) -> Vec<Effect> {
    assert!(!connection.accepts(old));
    let result = dispatch(
        &mut native.store,
        &mut native.next,
        Action::AdvanceGeneration,
    )
    .unwrap();
    let Response::Generation(next) = result else {
        panic!("expected committed auth generation");
    };
    let stale = Request {
        fence: old.fence.clone(),
        operation_id: native.next,
        action: Action::Snapshot { conversation: None },
    };
    assert_eq!(
        native.store.execute(stale).result,
        Err(StoreError::StaleGeneration)
    );
    event(
        connection,
        Event::FenceAdvanced {
            previous: &old.fence,
            next,
        },
        now,
    )
}

fn close_cause(bridge: &mut Bridge) -> GatewayClose {
    for _ in 0..30 {
        bridge.send("POLL");
        let line = bridge.line();
        if line == "EMPTY" {
            continue;
        }
        let fields = line.splitn(3, '\t').collect::<Vec<_>>();
        let ["CLOSED", code, reason] = fields.as_slice() else {
            panic!("expected a gateway close");
        };
        return classify_gateway_close(code.parse().unwrap(), reason);
    }
    panic!("gateway did not close the evicted socket");
}

#[test]
#[ignore = "run with make sdk-connection-integration"]
fn tls_sdk_connection() {
    let mut bridge = Bridge::start_fixture("TestConnectionSDKBridge");
    let path = Directory::new();
    let mut native = Native::open(&path.0, 1);
    let mut connection = Connection::new(native.store.fence().clone(), 0).unwrap();
    let first = credential_ticket(
        event(&mut connection, Event::NetworkAvailable, NOW),
        AuthCause::Missing,
    );
    assert_eq!(authorize(&mut connection, &first, NOW), first);
    assert_eq!(
        bind(
            &mut bridge,
            &mut native,
            &mut connection,
            &first,
            "phone",
            (EMPTY_ROOM, 0),
            NOW
        ),
        vec![Effect::Ready(first.clone())],
    );
    bridge.send("POLL");
    assert_eq!(bridge.line(), "EMPTY");

    let [Effect::Cancel(old), Effect::LoadDirectory(history)]: [Effect; 2] =
        event(&mut connection, Event::RefreshDirectory, NOW)
            .try_into()
            .unwrap()
    else {
        panic!("expected a new subscription generation");
    };
    close(&mut bridge);
    assert_eq!(first, old);
    let [Effect::RecoverMessages { ticket, target }]: [Effect; 1] = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &history,
        "phone",
        (ROOM, 0),
        NOW,
    )
    .try_into()
    .unwrap() else {
        panic!("expected recovery from the committed directory tail");
    };
    assert_eq!(ticket, history);
    assert_eq!(target.through_seq(), 70);
    let start = progress(&mut native, ROOM);
    let action =
        recovery::observe_target(&start, NOW, target.published_round(), target.through_seq())
            .unwrap();
    commit(&mut native.store, &mut native.next, action);
    let mut effects = Vec::new();
    for (index, expected_after) in [0, 64].into_iter().enumerate() {
        let now = NOW + 2 * index as u64 + 1;
        let flight = native.start(now).unwrap();
        assert_eq!(flight.request().after_seq, expected_after);
        let page = get_page_for(&mut bridge, "phone", &flight);
        native.receive(&flight, page, now + 1);
        let snapshot = progress(&mut native, ROOM);
        effects = event(
            &mut connection,
            Event::MessageProgress {
                ticket: &history,
                snapshot: &snapshot,
            },
            now + 1,
        );
        assert_eq!(
            connection.status(),
            if index == 0 {
                Status::Recovering
            } else {
                Status::Ready
            }
        );
    }
    assert_eq!(effects, vec![Effect::Ready(history.clone())]);

    let original = connection.send_context().unwrap();
    let (pending, ack) = queued_ack_with(
        &mut native,
        &mut bridge,
        &original,
        "phone",
        "connection_ack",
        NOW + 10,
    );
    let switched_at = NOW + 20;
    assert_eq!(
        event(&mut connection, Event::NetworkChanged, switched_at),
        vec![Effect::Cancel(history.clone())]
    );
    assert!(!connection.accepts(&history));
    close(&mut bridge);
    assert!(event(&mut connection, Event::SocketOpened(&history), switched_at).is_empty());
    assert!(event(&mut connection, Event::Tick, switched_at + 499).is_empty());
    let next = directory_ticket(event(&mut connection, Event::Tick, switched_at + 500));
    assert_eq!(next.fence, history.fence);
    assert_ne!(next.connection_generation, history.connection_generation);
    assert!(connection.accepts_ack(&original, switched_at + 500));
    native.accept_ack(&pending, &original, &ack);
    let now = switched_at + 510;
    let [Effect::RecoverMessages { target, .. }]: [Effect; 1] = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &next,
        "phone",
        (ROOM, 70),
        now,
    )
    .try_into()
    .unwrap() else {
        panic!("the server tail includes the message whose ACK was committed");
    };
    let snapshot = progress(&mut native, ROOM);
    let action = recovery::observe_target(
        &snapshot,
        now,
        target.published_round(),
        target.through_seq(),
    )
    .unwrap();
    commit(&mut native.store, &mut native.next, action);
    // The confirmed send already stored sequence 71. The scheduler uses that
    // cached row to advance continuity without issuing another page request.
    assert!(native.start(now + 1).is_none());
    let snapshot = progress(&mut native, ROOM);
    assert_eq!(snapshot.state.after_seq(), 71);
    assert_eq!(
        event(
            &mut connection,
            Event::MessageProgress {
                ticket: &next,
                snapshot: &snapshot
            },
            now + 2
        ),
        vec![Effect::Ready(next.clone())],
    );

    close(&mut bridge);
    socket(&mut bridge, "expired", 401);
    assert_eq!(
        event(
            &mut connection,
            Event::Failed {
                ticket: &next,
                failure: Failure::Authentication(AuthCause::Expired)
            },
            now + 3
        ),
        vec![
            Effect::Cancel(next.clone()),
            Effect::AdvanceFence(next.fence.clone())
        ]
    );
    assert_eq!(native.store.fence(), &original.fence);
    assert!(!connection.accepts_ack(&original, now + 3));
    let renewed = credential_ticket(
        advance(&mut native, &mut connection, &next, now + 3),
        AuthCause::Expired,
    );
    assert_eq!(authorize(&mut connection, &renewed, now + 3), renewed);
    assert_eq!(
        bind(
            &mut bridge,
            &mut native,
            &mut connection,
            &renewed,
            "renewed",
            (ROOM, 71),
            now + 3
        ),
        vec![Effect::Ready(renewed.clone())],
    );

    close(&mut bridge);
    let next_operation = native.next;
    drop(native);
    let mut native = Native::open(&path.0, next_operation);
    assert_eq!(native.store.fence(), &renewed.fence);
    assert_eq!(progress(&mut native, ROOM).state.after_seq(), 71);
    let mut reopened = Connection::new(
        native.store.fence().clone(),
        renewed.connection_generation.0,
    )
    .unwrap();
    let phone = credential_ticket(
        event(&mut reopened, Event::NetworkAvailable, now + 4),
        AuthCause::Missing,
    );
    assert_eq!(
        phone.connection_generation,
        ConnectionGeneration(renewed.connection_generation.0 + 1)
    );
    assert!(!reopened.accepts(&renewed));
    authorize(&mut reopened, &phone, now + 4);
    assert_eq!(
        bind(
            &mut bridge,
            &mut native,
            &mut reopened,
            &phone,
            "renewed",
            (ROOM, 71),
            now + 4
        ),
        vec![Effect::Ready(phone.clone())],
    );

    bridge.send("KICK");
    assert_eq!(bridge.line(), "KICKED");
    assert_eq!(close_cause(&mut bridge), GatewayClose::DeviceLimit);
    assert_eq!(
        event(
            &mut reopened,
            Event::Failed {
                ticket: &phone,
                failure: Failure::Authentication(AuthCause::DeviceLimit)
            },
            now + 5
        ),
        vec![
            Effect::Cancel(phone.clone()),
            Effect::AdvanceFence(phone.fence.clone())
        ]
    );
    close(&mut bridge);
    socket(&mut bridge, "renewed", 401);
    socket(&mut bridge, "phone", 401);
    let _ = credential_ticket(
        advance(&mut native, &mut reopened, &phone, now + 5),
        AuthCause::DeviceLimit,
    );
    assert_eq!(
        reopened.status(),
        Status::Credentials(AuthCause::DeviceLimit)
    );
    assert!(reopened.send_context().is_none());

    let tablet_path = Directory::new();
    let mut tablet_store = Native::open(&tablet_path.0, 1);
    let mut tablet_connection = Connection::new(tablet_store.store.fence().clone(), 0).unwrap();
    let tablet = credential_ticket(
        event(&mut tablet_connection, Event::NetworkAvailable, now + 5),
        AuthCause::Missing,
    );
    assert_eq!(tablet.fence.account, phone.fence.account);
    assert_ne!(tablet.fence.instance, phone.fence.instance);
    authorize(&mut tablet_connection, &tablet, now + 5);
    assert_eq!(
        bind(
            &mut bridge,
            &mut tablet_store,
            &mut tablet_connection,
            &tablet,
            "next",
            (EMPTY_ROOM, 0),
            now + 5
        ),
        vec![Effect::Ready(tablet)],
    );
    bridge.finish();
}

#[test]
#[ignore = "run with make message-event-integration"]
fn tls_sdk_message_visibility() {
    let mut bridge = Bridge::start_fixture("TestMessageVisibilitySDKBridge");
    let path = Directory::new();
    let mut native = Native::open(&path.0, 1);
    let mut connection = Connection::new(native.store.fence().clone(), 0).unwrap();

    let revision = progress(&mut native, ROOM).revision;
    commit(
        &mut native.store,
        &mut native.next,
        Action::Apply(Batch {
            expected_revision: revision,
            messages: Vec::new(),
            conversations: vec![Conversation {
                id: ROOM.into(),
                unread: 7,
                summary: Blob(vec![1]),
            }],
            cursor: None,
            resolve_pending: Vec::new(),
        }),
    );
    let first = credential_ticket(
        event(&mut connection, Event::NetworkAvailable, NOW),
        AuthCause::Missing,
    );
    authorize(&mut connection, &first, NOW);
    let [Effect::RecoverMessages { target, .. }]: [Effect; 1] = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &first,
        "phone",
        (ROOM, 0),
        NOW,
    )
    .try_into()
    .unwrap() else {
        panic!("expected initial message recovery");
    };
    assert_eq!(target.through_seq(), 3);
    assert_eq!(target.through_visibility_revision(), 0);
    assert_eq!(unread_count(&mut native), 3);
    let start = progress(&mut native, ROOM);
    let action =
        recovery::observe_target(&start, NOW, target.published_round(), target.through_seq())
            .unwrap();
    commit(&mut native.store, &mut native.next, action);
    let flight = native.start(NOW + 1).unwrap();
    let page = get_page_for(&mut bridge, "phone", &flight);
    native.receive(&flight, page, NOW + 2);
    let snapshot = progress(&mut native, ROOM);
    assert_eq!(
        event(
            &mut connection,
            Event::MessageProgress {
                ticket: &first,
                snapshot: &snapshot
            },
            NOW + 2
        ),
        vec![Effect::Ready(first.clone())]
    );

    let before = room_messages(&mut native);
    assert_eq!(before.len(), 3);
    assert!(before.iter().all(|message| !message.is_hidden()));

    // The phone already displayed the third message and stays on the same socket.
    change_visibility(&mut bridge, "peer", &before[2], "sdk_live_recall", "recall");
    let (announced, live_target) =
        start_live_visibility(&mut bridge, &mut native, &mut connection, &first, NOW + 3);
    assert_eq!(announced, 1);
    assert_eq!(unread_count(&mut native), 2);
    assert_eq!(live_target.after_seq(), live_target.through_seq());
    assert_eq!(
        commit_visibility_page(&mut bridge, &mut native, &live_target, 0, NOW + 3),
        (1, 1),
    );
    complete_live_visibility(&mut native, &mut connection, &first, announced, NOW + 3);
    let offline_cache = room_messages(&mut native);
    assert!(offline_cache[2].same_position(&before[2]));
    assert!(offline_cache[2].is_hidden());
    assert!(
        offline_cache[..2]
            .iter()
            .all(|message| !message.is_hidden())
    );

    assert_eq!(
        event(&mut connection, Event::NetworkLost, NOW + 4),
        vec![Effect::Cancel(first.clone())]
    );
    close(&mut bridge);
    change_visibility(&mut bridge, "peer", &before[0], "sdk_recall", "recall");
    change_visibility(&mut bridge, "next", &before[1], "sdk_hide", "deleteForMe");
    assert_eq!(unread_count(&mut native), 2);
    assert_eq!(room_messages(&mut native), offline_cache);

    let next_operation = native.next;
    drop(native);
    let mut native = Native::open(&path.0, next_operation);
    let current = directory_ticket(event(&mut connection, Event::NetworkAvailable, NOW + 5));
    assert!(!connection.accepts(&first));
    let [Effect::RecoverMessages { target, .. }]: [Effect; 1] = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &current,
        "phone",
        (ROOM, 3),
        NOW + 5,
    )
    .try_into()
    .unwrap() else {
        panic!("visibility must catch up even when the message tail stayed put");
    };
    assert_eq!(target.after_seq(), target.through_seq());
    assert_eq!(target.through_visibility_revision(), 3);
    assert_eq!(unread_count(&mut native), 0);

    for expected_revision in 2..=3 {
        assert!(connection.accepts(&current));
        let now = NOW + 5 + expected_revision as u64;
        assert_eq!(
            commit_visibility_page(&mut bridge, &mut native, &target, 1, now),
            (expected_revision, 1),
        );
        let snapshot = progress(&mut native, ROOM);
        let effects = event(
            &mut connection,
            Event::MessageProgress {
                ticket: &current,
                snapshot: &snapshot,
            },
            now,
        );
        if expected_revision == 2 {
            assert!(effects.is_empty());
            assert_eq!(connection.status(), Status::Recovering);
        } else {
            assert_eq!(effects, vec![Effect::Ready(current.clone())]);
        }
    }

    let delayed = recovery::IncomingMessage {
        protocol_version: 1,
        message: before[0].clone(),
    };
    let query = recovery::snapshot_query(ROOM, std::slice::from_ref(&delayed)).unwrap();
    let Response::MessageSyncSnapshot(snapshot) = run(&mut native.store, 1, query).unwrap() else {
        unreachable!()
    };
    let action = recovery::observe(
        native.store.fence(),
        &snapshot,
        delayed,
        NOW + 9,
        |left, right| Ok(left == right),
    )
    .unwrap();
    commit(&mut native.store, &mut native.next, action);

    let next_operation = native.next;
    drop(native);
    let mut native = Native::open(&path.0, next_operation);
    let after = room_messages(&mut native);
    assert_eq!(after.len(), 3);
    assert_eq!(progress(&mut native, ROOM).state.visibility_revision, 3);
    for (record, original, expected) in [
        (&after[0], &before[0], VisibilityAction::Recall),
        (&after[1], &before[1], VisibilityAction::DeleteForMe),
        (&after[2], &before[2], VisibilityAction::Recall),
    ] {
        assert!(record.same_position(original));
        assert!(record.is_hidden());
        assert_eq!(
            record
                .payload
                .as_ref()
                .and_then(VisibilityAction::from_payload),
            Some(expected)
        );
        assert_ne!(record.payload, original.payload);
    }
    assert_eq!(unread_count(&mut native), 0);

    // Use the HTTP default, with more than one full SQLite page already committed.
    bridge.send("BULK_HIDE");
    assert_eq!(bridge.line(), "BULK_HIDDEN");
    let (announced, bulk_target) = start_live_visibility(
        &mut bridge,
        &mut native,
        &mut connection,
        &current,
        NOW + 10,
    );
    assert_eq!(visibility_wire::MAX_PAGE_ITEMS, MAX_ITEMS);
    let full = visibility_wire::MAX_PAGE_ITEMS;
    let terminal = 3 + full as i64 + 1;
    assert_eq!(bulk_target.through_visibility_revision(), terminal);
    assert_eq!(unread_count(&mut native), 0);
    assert_eq!(
        commit_visibility_page(&mut bridge, &mut native, &bulk_target, 0, NOW + 11),
        (terminal - 1, full),
    );
    assert_eq!(
        commit_visibility_page(&mut bridge, &mut native, &bulk_target, 0, NOW + 12),
        (terminal, 1),
    );
    complete_live_visibility(&mut native, &mut connection, &current, announced, NOW + 12);
    bridge.finish();
}

fn poll_visibility_notice(bridge: &mut Bridge) -> VisibilityNotice {
    for _ in 0..82 {
        bridge.send("POLL");
        let line = bridge.line();
        if line == "EMPTY" {
            continue;
        }
        let raw = decode_hex(
            line.strip_prefix("FRAME\t")
                .expect("socket must stay connected"),
        );
        if let GatewayFrame::VisibilityChanged(notice) = decode_gateway_frame(&raw).unwrap() {
            return notice;
        }
    }
    panic!("live gateway did not send a visibility notice");
}

fn start_live_visibility(
    bridge: &mut Bridge,
    native: &mut Native,
    connection: &mut Connection,
    ticket: &Ticket,
    now: u64,
) -> (i64, Subscription) {
    let notice = poll_visibility_notice(bridge);
    assert_eq!(notice.conversation_id, ROOM);
    assert_eq!(
        event(
            connection,
            Event::VisibilityChanged {
                ticket,
                conversation_id: &notice.conversation_id,
                revision: notice.revision,
            },
            now
        ),
        vec![Effect::RefreshVisibility {
            ticket: ticket.clone(),
            through_revision: notice.revision,
        }],
    );
    assert_eq!(connection.status(), Status::Ready);
    assert!(connection.send_context().is_some());
    assert!(connection.accepts_visibility_refresh(ticket, notice.revision));
    let directory = sync_directory(bridge, native, "phone");
    let snapshot = progress(native, ROOM);
    let committed = CommittedDirectory::from_store(&directory, Some(&snapshot)).unwrap();
    let target = committed.into_subscription().unwrap();
    assert!(target.through_visibility_revision() >= notice.revision);
    (notice.revision, target)
}

fn complete_live_visibility(
    native: &mut Native,
    connection: &mut Connection,
    ticket: &Ticket,
    announced: i64,
    now: u64,
) {
    let snapshot = progress(native, ROOM);
    assert!(
        event(
            connection,
            Event::VisibilityRefreshed {
                ticket,
                through_revision: announced,
                snapshot: &snapshot,
            },
            now
        )
        .is_empty()
    );
    assert_eq!(connection.status(), Status::Ready);
    assert!(connection.accepts(ticket));
    assert!(!connection.accepts_visibility_refresh(ticket, announced));
}

fn commit_visibility_page(
    bridge: &mut Bridge,
    native: &mut Native,
    target: &Subscription,
    limit: usize,
    now: u64,
) -> (i64, usize) {
    let snapshot = progress(native, ROOM);
    let request = visibility_wire::PageRequest {
        conversation_id: ROOM.into(),
        after_revision: snapshot.state.visibility_revision,
        through_revision: target.through_visibility_revision(),
        limit,
    };
    let reply = bridge.call(
        "phone",
        "POST",
        visibility_wire::VISIBILITY_PATH,
        &request.encode().unwrap(),
    );
    reply.require_status(200, "visibility page");
    let page = visibility_wire::decode_page(&request, &reply.bytes).unwrap();
    let result = (page.next_after_revision, page.items.len());
    let action = recovery::apply_visibility(
        &snapshot,
        now,
        VisibilityPage {
            published_round: target.published_round(),
            after_revision: request.after_revision,
            next_revision: page.next_after_revision,
            through_revision: request.through_revision,
            events: page
                .items
                .into_iter()
                .map(|item| VisibilityEvent {
                    server_id: item.server_msg_id,
                    sequence: item.conversation_seq,
                    revision: item.revision,
                    action: match item.action {
                        visibility_wire::Action::Recall => VisibilityAction::Recall,
                        visibility_wire::Action::DeleteForMe => VisibilityAction::DeleteForMe,
                        visibility_wire::Action::AdminRemove => VisibilityAction::AdminRemove,
                    },
                })
                .collect(),
        },
    )
    .unwrap();
    commit(&mut native.store, &mut native.next, action);
    result
}

fn room_messages(native: &mut Native) -> Vec<Message> {
    let Response::Messages(page) = run(
        &mut native.store,
        1,
        Action::Messages {
            conversation: ROOM.into(),
            after: None,
            limit: 16,
        },
    )
    .unwrap() else {
        panic!("message cache returned an unexpected response")
    };
    assert!(page.next.is_none());
    page.items
}

fn unread_count(native: &mut Native) -> u64 {
    let Response::Snapshot(snapshot) = run(
        &mut native.store,
        1,
        Action::Snapshot {
            conversation: Some(ROOM.into()),
        },
    )
    .unwrap() else {
        panic!("missing cached conversation")
    };
    snapshot.conversation.unwrap().unread
}

fn change_visibility(
    bridge: &mut Bridge,
    device: &str,
    message: &Message,
    request: &str,
    action: &str,
) {
    let wire = serde_json::to_vec(&json!({
        "conversationId": ROOM, "serverMsgId": message.server_id, "requestId": request, "action": action,
    })).unwrap();
    bridge
        .call(device, "POST", "/api/v1/messages/visibility", &wire)
        .require_status(200, "change visibility");
}

#[test]
#[ignore = "run with make push-sdk-check"]
fn tls_sdk_push_click() {
    use newim_sdk_core::push::open_notification;

    let mut bridge = Bridge::start_fixture("TestPushSDKBridge");
    let path = Directory::new();
    let mut native = Native::open(&path.0, 1);
    let mut connection = Connection::new(native.store.fence().clone(), 0).unwrap();
    let original = credential_ticket(
        event(&mut connection, Event::NetworkAvailable, NOW),
        AuthCause::Missing,
    );
    authorize(&mut connection, &original, NOW);
    let initial = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &original,
        "phone",
        (ROOM, 0),
        NOW,
    );
    reconcile_push_target(
        &mut bridge,
        &mut native,
        &mut connection,
        &original,
        initial,
        1,
        NOW,
    );
    let initial_messages = room_messages(&mut native);
    assert_eq!(initial_messages.len(), 1);

    // The host has not processed the new WSS frame when the notification is opened.
    bridge.send("PUSH_NEXT");
    assert_eq!(bridge.line(), "PUSH_COMMITTED");
    assert!(
        open_notification(&mut connection, "injected_room", NOW + 3, 0)
            .unwrap()
            .is_empty()
    );
    let [Effect::Cancel(old), Effect::LoadDirectory(current)]: [Effect; 2] =
        open_notification(&mut connection, "sync", NOW + 3, 0)
            .unwrap()
            .try_into()
            .unwrap()
    else {
        panic!("expected a normal authorized directory refresh");
    };
    assert_eq!(old, original);
    assert!(!connection.accepts(&old));
    assert!(connection.accepts(&current));
    assert_eq!(old.fence, current.fence);
    assert_eq!(room_messages(&mut native), initial_messages);
    assert_eq!(progress(&mut native, ROOM).state.after_seq(), 1);
    close(&mut bridge);

    let resumed = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &current,
        "phone",
        (ROOM, 1),
        NOW + 3,
    );
    reconcile_push_target(
        &mut bridge,
        &mut native,
        &mut connection,
        &current,
        resumed,
        2,
        NOW + 3,
    );

    let next_op = native.next;
    drop(native);
    let mut native = Native::open(&path.0, next_op);
    let messages = room_messages(&mut native);
    assert_eq!(progress(&mut native, ROOM).state.after_seq(), 2);
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0], initial_messages[0]);
    assert_eq!(messages[1].client_id, "opened_from_notification");
    bridge.finish();
}

fn reconcile_push_target(
    bridge: &mut Bridge,
    native: &mut Native,
    connection: &mut Connection,
    ticket: &Ticket,
    effects: Vec<Effect>,
    through: i64,
    now: u64,
) {
    let [
        Effect::RecoverMessages {
            ticket: callback,
            target,
        },
    ]: [Effect; 1] = effects.try_into().unwrap()
    else {
        panic!("push refresh must use the normal message recovery worker");
    };
    assert_eq!(&callback, ticket);
    assert_eq!(target.through_seq(), through);
    let snapshot = progress(native, ROOM);
    let action =
        recovery::observe_target(&snapshot, now, target.published_round(), through).unwrap();
    commit(&mut native.store, &mut native.next, action);
    let flight = native.start(now + 1).unwrap();
    let page = get_page_for(bridge, "phone", &flight);
    native.receive(&flight, page, now + 2);
    let snapshot = progress(native, ROOM);
    assert_eq!(
        event(
            connection,
            Event::MessageProgress {
                ticket,
                snapshot: &snapshot
            },
            now + 2
        ),
        vec![Effect::Ready(ticket.clone())]
    );
}

#[test]
#[ignore = "run with make sdk-observability-check"]
fn tls_sdk_observability() {
    use newim_sdk_core::message::{PersistedAck, SendIntent};
    use newim_sdk_core::observability::AckFlight;
    use newim_sdk_core::outbox::OutboxRecord;

    let mut bridge = Bridge::start_fixture("TestSDKObservabilityBridge");
    let path = Directory::new();
    let mut native = Native::open(&path.0, 1);
    let mut connection = Connection::new(native.store.fence().clone(), 0).unwrap();
    let first = credential_ticket(
        event(&mut connection, Event::NetworkAvailable, NOW),
        AuthCause::Missing,
    );
    authorize(&mut connection, &first, NOW);
    let effects = bind(
        &mut bridge,
        &mut native,
        &mut connection,
        &first,
        "phone",
        (ROOM, 0),
        NOW,
    );
    let [Effect::RecoverMessages { target, .. }]: [Effect; 1] = effects.try_into().unwrap() else {
        panic!("expected bootstrap gap");
    };
    let before = progress(&mut native, ROOM);
    let action =
        recovery::observe_target(&before, NOW, target.published_round(), target.through_seq())
            .unwrap();
    observed_store_action(&mut native, &mut connection, &first, action);
    let gap = progress(&mut native, ROOM);
    assert!(gap.state.job.is_some());
    assert!(
        event(
            &mut connection,
            Event::MessageProgress {
                ticket: &first,
                snapshot: &gap
            },
            NOW
        )
        .is_empty()
    );

    let flight = native.start(NOW + 1).unwrap();
    let page = get_page_for(&mut bridge, "phone", &flight);
    let query = recovery::snapshot_query(ROOM, &page.messages).unwrap();
    let Response::MessageSyncSnapshot(snapshot) = run(&mut native.store, 1, query).unwrap() else {
        panic!("expected current page snapshot");
    };
    let action = flight
        .apply(
            native.store.fence(),
            &snapshot,
            page,
            NOW + 2,
            compare_observed_payload,
        )
        .unwrap();
    observed_store_action(&mut native, &mut connection, &first, action);
    let caught = progress(&mut native, ROOM);
    assert_eq!(
        event(
            &mut connection,
            Event::MessageProgress {
                ticket: &first,
                snapshot: &caught
            },
            NOW + 2
        ),
        vec![Effect::Ready(first.clone())]
    );

    let context = connection.send_context().unwrap();
    let payload = Blob(br#"{"text":"observable message"}"#.to_vec());
    let record = OutboxRecord::new(
        SendIntent {
            protocol_version: 1,
            client_id: "observed_outbound".into(),
            conversation_id: ROOM.into(),
            schema_version: 1,
            message_type: "text".into(),
            payload: payload.clone(),
        },
        &context,
        NOW + 3,
    )
    .unwrap();
    let pending = Pending {
        sender_id: ACCOUNT.into(),
        client_id: "observed_outbound".into(),
        conversation_id: ROOM.into(),
        payload: record.encode().unwrap(),
    };
    observed_store_action(
        &mut native,
        &mut connection,
        &first,
        Action::Enqueue(pending.clone()),
    );
    observed_store_action(&mut native, &mut connection, &first, Action::Metrics);

    bridge.send("OBS_SEND");
    let line = bridge.line();
    let parts: Vec<_> = line.splitn(3, '\t').collect();
    let ["OBS_ACK", header, raw] = parts.as_slice() else {
        panic!("trace must originate from the same verified response");
    };
    let ServerFrame::Ack(ack) = decode_server_frame(&decode_hex(raw)).unwrap() else {
        panic!("expected a persisted ACK frame");
    };
    let persisted = PersistedAck {
        sender_id: ack.sender_id,
        client_id: ack.client_msg_id,
        conversation_id: ack.conversation_id,
        server_id: ack.server_msg_id,
        sequence: ack.conversation_seq.parse().unwrap(),
        server_time: ack.server_time.parse().unwrap(),
        protocol_version: 1,
        schema_version: 1,
        message_type: "text".into(),
        payload,
    };
    let revision = progress(&mut native, ROOM).revision;
    let operation_id = native.next;
    native.next += 1;
    let flight = || AckFlight {
        context: &context,
        pending: &pending,
        expected_revision: revision,
        operation_id,
        attempted_at_ms: NOW + 3,
    };
    let mut clock = [NOW + 13, NOW + 15].into_iter();
    let (resolution, Some(trace)) = connection
        .resolve_http_ack(&mut native.store, flight(), &persisted, &[header], || {
            clock.next().unwrap()
        })
        .unwrap()
    else {
        panic!("the committed receipt did not retain diagnostic metadata");
    };
    assert!(clock.next().is_none());
    assert_eq!(trace.trace_id, *header);
    assert_eq!(trace.client_msg_id, "observed_outbound");
    assert_eq!(trace.server_msg_id, persisted.server_id);
    assert!(!format!("{trace:?}").contains(*header));
    let (replayed, duplicate_header) = connection
        .resolve_http_ack(
            &mut native.store,
            flight(),
            &persisted,
            &[header, header],
            || NOW + 15,
        )
        .unwrap();
    assert_eq!(replayed, resolution);
    assert!(duplicate_header.is_none());
    observed_store_action(&mut native, &mut connection, &first, Action::Metrics);

    bridge.send("OBS_NEXT");
    assert_eq!(bridge.line(), "OBS_COMMITTED");
    let incoming = observed_message(&mut bridge);
    let query = recovery::snapshot_query(ROOM, std::slice::from_ref(&incoming)).unwrap();
    let Response::MessageSyncSnapshot(snapshot) = run(&mut native.store, 1, query).unwrap() else {
        panic!("expected WSS message position");
    };
    let action = recovery::observe(
        native.store.fence(),
        &snapshot,
        incoming,
        NOW + 16,
        compare_observed_payload,
    )
    .unwrap();
    observed_store_action(&mut native, &mut connection, &first, action);
    assert_eq!(room_messages(&mut native).len(), 3);

    assert_eq!(
        event(&mut connection, Event::NetworkLost, NOW + 17),
        vec![Effect::Cancel(first)]
    );
    close(&mut bridge);
    let current = directory_ticket(event(&mut connection, Event::NetworkAvailable, NOW + 18));
    assert_eq!(
        bind(
            &mut bridge,
            &mut native,
            &mut connection,
            &current,
            "phone",
            (ROOM, 3),
            NOW + 18
        ),
        vec![Effect::Ready(current.clone())]
    );

    let report = connection.take_metrics_report().unwrap().encode();
    let aggregate: Value = serde_json::from_str(&report).unwrap();
    for (field, count) in [
        ("socket_attempt", 2),
        ("reconnect", 1),
        ("ready", 2),
        ("ack_committed", 1),
        ("ack_millis", 12),
        ("gap_detected", 1),
        ("page_committed", 1),
        ("message_committed", 1),
        ("outbox_samples", 2),
        ("outbox_pending", 1),
    ] {
        assert_eq!(aggregate[field], count, "{field}");
    }
    assert!(connection.take_metrics_report().is_none());
    for private in [
        ACCOUNT,
        ROOM,
        header,
        &persisted.server_id,
        "observed_outbound",
        "observable message",
    ] {
        assert!(!report.contains(private));
    }

    bridge.send(&format!("OBS_REPORT\t{}", encode_hex(report.as_bytes())));
    assert_eq!(bridge.line(), "OBS_RECORDED");
    let mut observed = None;
    for _ in 0..20 {
        bridge.send("OBS_SCRAPE");
        let line = bridge.line();
        let metrics =
            String::from_utf8(decode_hex(line.strip_prefix("OBS_METRICS\t").unwrap())).unwrap();
        if metrics.contains("newim_sdk_events_total{event=\"ack_committed\"} 1")
            && metrics.contains("newim_gateway_active_subscriptions 1")
        {
            observed = Some(metrics);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let metrics = observed.expect("SDK report was not published");
    for sample in [
        "newim_gateway_active_subscriptions 1",
        "newim_gateway_operations_total{operation=\"delivery\",result=\"success\"}",
        "newim_gateway_operations_total{operation=\"directory_bootstrap\",result=\"success\"}",
        "newim_gateway_operations_total{operation=\"message_page\",result=\"success\"}",
        "newim_message_lifecycle_duration_seconds_count{component=\"send\",stage=\"persisted\"}",
        "newim_sdk_events_total{event=\"gap_detected\"} 1",
        "newim_sdk_events_total{event=\"reconnect\"} 1",
        "newim_sdk_ack_duration_seconds_sum 0.012",
        "newim_sdk_outbox_depth_count 2",
        "newim_sdk_outbox_depth_sum 1",
    ] {
        assert!(metrics.contains(sample), "absent sample {sample}");
    }
    for private in [
        ACCOUNT,
        ROOM,
        header,
        &persisted.server_id,
        "observed_outbound",
        "observable message",
    ] {
        assert!(!metrics.contains(private));
    }
    bridge.finish();
}

fn observed_store_action(
    native: &mut Native,
    connection: &mut Connection,
    ticket: &Ticket,
    action: Action,
) {
    let request = Request {
        fence: native.store.fence().clone(),
        operation_id: native.next,
        action,
    };
    native.next += 1;
    let completion = native.store.execute(request.clone());
    assert!(
        completion.result.is_ok(),
        "observation fixture store action failed"
    );
    connection.observe_store_completion(ticket, &request, &completion);
}

fn compare_observed_payload(left: &Blob, right: &Blob) -> Result<bool, StoreError> {
    newim_protocol::send::same_message_payload(&left.0, &right.0)
        .map_err(|_| StoreError::InvalidInput)
}

fn observed_message(bridge: &mut Bridge) -> recovery::IncomingMessage {
    for _ in 0..40 {
        bridge.send("POLL");
        let line = bridge.line();
        if line == "EMPTY" {
            continue;
        }
        let raw = decode_hex(line.strip_prefix("FRAME\t").unwrap());
        let GatewayFrame::Message(wire) = decode_gateway_frame(&raw).unwrap() else {
            continue;
        };
        if wire.client_msg_id == "observed_inbound" {
            return recovery::IncomingMessage {
                protocol_version: wire.protocol_version,
                message: Message {
                    server_id: wire.server_msg_id,
                    sender_id: wire.sender_id,
                    client_id: wire.client_msg_id,
                    conversation_id: wire.conversation_id,
                    sequence: wire.conversation_seq.parse().unwrap(),
                    server_time: wire.server_time.parse().unwrap(),
                    schema_version: wire.version,
                    message_type: wire.message_type,
                    payload: Some(Blob(wire.payload.get().as_bytes().to_vec())),
                },
            };
        }
    }
    panic!("no live message reached the SDK socket");
}
