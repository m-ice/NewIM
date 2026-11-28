use super::message_sync::{Native, get_page_for, queued_ack_with};
use super::*;
use newim_protocol::gateway::{
    GATEWAY_SUBPROTOCOL, GatewayClose, GatewaySubscription, classify_gateway_close,
    encode_gateway_subscription,
};
use newim_sdk_core::{
    connection::{
        AuthCause, CommittedDirectory, Connection, Effect, Event, Failure, Status, Ticket,
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
    for _ in 0..4 {
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
