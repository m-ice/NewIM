use newim_sdk_core::connection::{
    AuthCause, CommittedDirectory, Connection, Effect, Event, Failure, STABLE_READY_MS, Status,
    Ticket,
};
use newim_sdk_core::message::ConnectionGeneration;
use newim_sdk_core::outbox::{BASE_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS};
use newim_sdk_core::store::{
    Blob, ConversationSyncChange, ConversationSyncItem, ConversationSyncPhase,
    ConversationSyncPosition, ConversationSyncSnapshot, ConversationSyncSummary, Fence,
    MessageSyncSnapshot, MessageSyncState, StoreError,
};

const NOW: u64 = 10_000;
const ROOM: &str = "room";

fn fence(generation: u64) -> Fence {
    Fence {
        account: "account".into(),
        instance: "local_install".into(),
        generation,
    }
}

fn directory(sequence: u64) -> ConversationSyncSnapshot {
    ConversationSyncSnapshot {
        revision: 10,
        position: ConversationSyncPosition {
            round: 1,
            phase: ConversationSyncPhase::DeltaCheckpoint(Blob(b"checkpoint".to_vec())),
        },
        published_round: Some(1),
        items: vec![ConversationSyncItem {
            conversation_id: ROOM.into(),
            revision: 1,
            known_clear_through_seq: 0,
            change: ConversationSyncChange::Upsert(ConversationSyncSummary {
                latest_conversation_seq: sequence,
                latest_server_msg_id: (sequence > 0).then(|| "message".into()),
                last_read_seq: None,
                settings: None,
            }),
        }],
        history_floors: Default::default(),
        next: None,
    }
}

fn messages(after: i64) -> MessageSyncSnapshot {
    MessageSyncSnapshot {
        revision: 10,
        conversation_id: ROOM.into(),
        visible: true,
        state: MessageSyncState {
            continuous_seq: after,
            ..Default::default()
        },
        messages: Vec::new(),
        pending: Vec::new(),
    }
}

fn event(connection: &mut Connection, event: Event<'_>, time: u64) -> Vec<Effect> {
    connection.update(event, time, 0).unwrap()
}

fn credential(effects: Vec<Effect>, cause: AuthCause) -> Ticket {
    let [
        Effect::RecoverCredentials {
            ticket,
            cause: actual,
        },
    ]: [Effect; 1] = effects.try_into().unwrap()
    else {
        panic!("expected credential effect");
    };
    assert_eq!(actual, cause);
    ticket
}

fn online(connection: &mut Connection, expiry: u64) -> Ticket {
    assert_eq!(connection.status(), Status::Offline);
    assert!(event(connection, Event::Tick, NOW).is_empty());
    let ticket = credential(
        event(connection, Event::NetworkAvailable, NOW),
        AuthCause::Missing,
    );
    assert_eq!(
        event(
            connection,
            Event::Credentials {
                ticket: &ticket,
                expires_at_ms: expiry
            },
            NOW
        ),
        vec![Effect::LoadDirectory(ticket.clone())],
    );
    ticket
}

#[test]
fn readiness_requires_directory_subscription_and_durable_target() {
    let mut connection = Connection::new(fence(1), 0).unwrap();
    let ticket = online(&mut connection, NOW + 10_000);
    let directory = directory(70);
    let mut staged = directory.clone();
    staged.position.phase = ConversationSyncPhase::DeltaPage(Blob(b"staged".to_vec()));
    assert_eq!(
        CommittedDirectory::from_store(&staged, Some(&messages(10))),
        Err(StoreError::StaleRevision)
    );
    let mut inconsistent = messages(10);
    inconsistent.revision -= 1;
    assert_eq!(
        CommittedDirectory::from_store(&directory, Some(&inconsistent)),
        Err(StoreError::StaleRevision)
    );
    let mut start = messages(4);
    start.state.history_after_seq = 10;
    let committed = CommittedDirectory::from_store(&directory, Some(&start)).unwrap();
    assert!(event(&mut connection, Event::Subscribed(&ticket), NOW).is_empty());
    assert_eq!(
        event(
            &mut connection,
            Event::Directory {
                ticket: &ticket,
                committed
            },
            NOW
        ),
        vec![Effect::OpenSocket(ticket.clone())]
    );
    let [
        Effect::Subscribe {
            ticket: callback,
            target,
        },
    ]: [Effect; 1] = event(&mut connection, Event::SocketOpened(&ticket), NOW)
        .try_into()
        .unwrap()
    else {
        panic!("expected subscription");
    };
    assert_eq!(callback, ticket);
    assert_eq!(
        (target.conversation_id(), target.published_round()),
        (ROOM, 1)
    );
    assert_eq!((target.after_seq(), target.through_seq()), (10, 70));
    assert_eq!(
        event(&mut connection, Event::Subscribed(&ticket), NOW),
        vec![Effect::RecoverMessages {
            ticket: ticket.clone(),
            target
        }]
    );
    assert!(connection.send_context().is_none());
    let mut wrong_room = messages(71);
    wrong_room.conversation_id = "another_room".into();
    for snapshot in [&messages(69), &wrong_room] {
        assert!(
            event(
                &mut connection,
                Event::MessageProgress {
                    ticket: &ticket,
                    snapshot
                },
                NOW
            )
            .is_empty()
        );
    }
    let mut removed = messages(71);
    removed.visible = false;
    assert!(
        event(
            &mut connection,
            Event::MessageProgress {
                ticket: &ticket,
                snapshot: &removed
            },
            NOW
        )
        .is_empty()
    );
    assert_eq!(
        event(
            &mut connection,
            Event::MessageProgress {
                ticket: &ticket,
                snapshot: &messages(70)
            },
            NOW
        ),
        vec![Effect::Ready(ticket.clone())],
    );
    assert_eq!(connection.status(), Status::Ready);
    let send = connection.send_context().unwrap();
    assert_eq!(send.fence, ticket.fence);
    assert_eq!(send.connection_generation, ticket.connection_generation);
    assert!(event(&mut connection, Event::Subscribed(&ticket), NOW).is_empty());
}

#[test]
fn empty_directory_and_empty_room_are_available_without_a_message() {
    let mut connection = Connection::new(fence(1), 29).unwrap();
    let ticket = online(&mut connection, NOW + 10_000);
    assert_eq!(ticket.connection_generation, ConnectionGeneration(30));
    let mut empty = directory(0);
    empty.items.clear();
    let committed = CommittedDirectory::from_store(&empty, None).unwrap();
    assert_eq!(
        event(
            &mut connection,
            Event::Directory {
                ticket: &ticket,
                committed
            },
            NOW
        ),
        vec![Effect::Ready(ticket.clone())],
    );
    let [Effect::Cancel(old), Effect::LoadDirectory(next)]: [Effect; 2] =
        event(&mut connection, Event::RefreshDirectory, NOW)
            .try_into()
            .unwrap()
    else {
        panic!("expected directory refresh");
    };
    assert_eq!(old, ticket);
    assert_eq!(next.connection_generation, ConnectionGeneration(31));
    assert!(!connection.accepts(&old));
    let committed = CommittedDirectory::from_store(&directory(0), Some(&messages(0))).unwrap();
    assert_eq!(
        event(
            &mut connection,
            Event::Directory {
                ticket: &next,
                committed
            },
            NOW
        ),
        vec![Effect::OpenSocket(next.clone())],
    );
    assert!(matches!(
        event(&mut connection, Event::SocketOpened(&next), NOW).as_slice(),
        [Effect::Subscribe { .. }],
    ));
    assert_eq!(
        event(&mut connection, Event::Subscribed(&next), NOW),
        vec![Effect::Ready(next)]
    );
}

#[test]
fn network_loss_and_switch_cancel_old_callbacks_and_keep_the_store_fence() {
    let mut connection = Connection::new(fence(4), 0).unwrap();
    let first = online(&mut connection, NOW + 90_000);
    assert_eq!(
        event(&mut connection, Event::NetworkChanged, NOW),
        vec![Effect::Cancel(first.clone())]
    );
    assert_eq!(
        connection.status(),
        Status::Backoff {
            until_ms: NOW + 500
        }
    );
    assert!(event(&mut connection, Event::Tick, NOW + 499).is_empty());
    assert!(event(&mut connection, Event::NetworkLost, NOW + 500).is_empty());
    assert!(event(&mut connection, Event::Tick, NOW + 900).is_empty());
    let [Effect::LoadDirectory(second)]: [Effect; 1] =
        event(&mut connection, Event::NetworkAvailable, NOW + 900)
            .try_into()
            .unwrap()
    else {
        panic!("expected reconnect");
    };
    assert_eq!(second.fence, first.fence);
    assert_ne!(second.connection_generation, first.connection_generation);
    assert!(!connection.accepts(&first));
    assert!(
        event(
            &mut connection,
            Event::Failed {
                ticket: &first,
                failure: Failure::Authentication(AuthCause::Revoked)
            },
            NOW + 900,
        )
        .is_empty()
    );
    assert!(event(&mut connection, Event::SocketOpened(&first), NOW + 900).is_empty());
    assert_eq!(connection.status(), Status::Directory);
    assert_eq!(
        event(&mut connection, Event::NetworkLost, NOW + 900),
        vec![Effect::Cancel(second)],
    );
    assert_eq!(connection.status(), Status::Offline);
}

#[test]
fn expiry_and_device_limit_wait_for_a_committed_fence_before_credentials() {
    let mut connection = Connection::new(fence(7), 0).unwrap();
    let first = online(&mut connection, NOW + 50);
    assert_eq!(
        event(&mut connection, Event::Tick, NOW + 50),
        vec![
            Effect::Cancel(first.clone()),
            Effect::AdvanceFence(fence(7))
        ],
    );
    assert!(event(&mut connection, Event::NetworkLost, NOW + 50).is_empty());
    assert!(!connection.accepts(&first));
    assert_eq!(connection.status(), Status::AdvancingFence);
    let mut other = fence(8);
    other.account = "other_account".into();
    assert_eq!(
        connection.update(
            Event::FenceAdvanced {
                previous: &first.fence,
                next: other
            },
            NOW + 50,
            0
        ),
        Err(StoreError::StaleGeneration),
    );
    assert!(
        event(
            &mut connection,
            Event::FenceAdvanced {
                previous: &first.fence,
                next: fence(8)
            },
            NOW + 50
        )
        .is_empty()
    );
    let next = credential(
        event(&mut connection, Event::NetworkAvailable, NOW + 50),
        AuthCause::Expired,
    );
    assert_eq!(next.fence, fence(8));
    assert_eq!(
        event(
            &mut connection,
            Event::Credentials {
                ticket: &next,
                expires_at_ms: NOW + 80
            },
            NOW + 51
        ),
        vec![Effect::LoadDirectory(next.clone())],
    );
    assert_eq!(
        event(
            &mut connection,
            Event::Failed {
                ticket: &next,
                failure: Failure::Authentication(AuthCause::DeviceLimit)
            },
            NOW + 80
        ),
        vec![Effect::Cancel(next.clone()), Effect::AdvanceFence(fence(8))],
    );
    let admitted = credential(
        event(
            &mut connection,
            Event::FenceAdvanced {
                previous: &next.fence,
                next: fence(9),
            },
            NOW + 80,
        ),
        AuthCause::DeviceLimit,
    );
    assert_eq!(admitted.fence, fence(9));
    assert!(!connection.accepts(&next));
}

#[test]
fn logout_during_fence_commit_supersedes_recovery() {
    let mut connection = Connection::new(fence(1), 0).unwrap();
    let first = online(&mut connection, NOW + 50);
    event(&mut connection, Event::Tick, NOW + 50);
    assert!(event(&mut connection, Event::Logout, NOW + 51).is_empty());
    assert!(
        event(
            &mut connection,
            Event::FenceAdvanced {
                previous: &first.fence,
                next: fence(2)
            },
            NOW + 51
        )
        .is_empty()
    );
    assert_eq!(connection.status(), Status::Closed);
    assert!(event(&mut connection, Event::Logout, NOW + 51).is_empty());
    assert!(event(&mut connection, Event::NetworkAvailable, NOW + 52).is_empty());
    let next = credential(
        event(&mut connection, Event::Resume, NOW + 52),
        AuthCause::Missing,
    );
    assert_eq!(next.fence, fence(2));
}

#[test]
fn retry_window_caps_and_resets_after_stable_ready() {
    let mut connection = Connection::new(fence(1), 0).unwrap();
    let expiry = NOW + 10_000_000;
    let mut current = online(&mut connection, expiry);
    let mut now = NOW;
    for attempt in 0..20 {
        let window = (BASE_RETRY_DELAY_MS << attempt).min(MAX_RETRY_DELAY_MS);
        assert_eq!(
            connection
                .update(
                    Event::Failed {
                        ticket: &current,
                        failure: Failure::Temporary
                    },
                    now,
                    window / 2
                )
                .unwrap(),
            vec![Effect::Cancel(current)],
        );
        assert_eq!(
            connection.status(),
            Status::Backoff {
                until_ms: now + window
            }
        );
        assert!(event(&mut connection, Event::Tick, now + window - 1).is_empty());
        now += window;
        let [Effect::LoadDirectory(ticket)]: [Effect; 1] =
            event(&mut connection, Event::Tick, now).try_into().unwrap()
        else {
            panic!("expected another attempt");
        };
        current = ticket;
    }
    let committed = CommittedDirectory::from_store(&directory(0), None).unwrap();
    event(
        &mut connection,
        Event::Directory {
            ticket: &current,
            committed,
        },
        now,
    );
    now += STABLE_READY_MS;
    event(
        &mut connection,
        Event::Failed {
            ticket: &current,
            failure: Failure::ConversationForbidden,
        },
        now,
    );
    assert_eq!(
        connection.status(),
        Status::Backoff {
            until_ms: now + BASE_RETRY_DELAY_MS / 2
        }
    );
}

#[test]
fn exhausted_store_fence_closes_the_active_connection() {
    let mut connection = Connection::new(fence(i64::MAX as u64), 0).unwrap();
    let ticket = online(&mut connection, NOW + 10_000);
    let committed = CommittedDirectory::from_store(&directory(0), None).unwrap();
    event(
        &mut connection,
        Event::Directory {
            ticket: &ticket,
            committed,
        },
        NOW,
    );
    assert!(connection.send_context().is_some());
    assert_eq!(
        event(&mut connection, Event::Logout, NOW),
        vec![
            Effect::Cancel(ticket.clone()),
            Effect::Unavailable(StoreError::CapacityExceeded)
        ],
    );
    assert_eq!(connection.status(), Status::Closed);
    assert!(connection.send_context().is_none());
    assert!(!connection.accepts(&ticket));
    assert!(event(&mut connection, Event::Resume, NOW).is_empty());
}

#[test]
fn exhausted_connection_generation_delivers_cancel_and_closes() {
    let mut connection = Connection::new(fence(1), i64::MAX as u64 - 1).unwrap();
    let ticket = online(&mut connection, NOW + 10_000);
    assert_eq!(
        ticket.connection_generation,
        ConnectionGeneration(i64::MAX as u64)
    );
    assert_eq!(
        event(&mut connection, Event::RefreshDirectory, NOW),
        vec![
            Effect::Cancel(ticket.clone()),
            Effect::Unavailable(StoreError::CapacityExceeded)
        ],
    );
    assert_eq!(connection.status(), Status::Closed);
    assert!(!connection.accepts(&ticket));
    assert!(event(&mut connection, Event::Resume, NOW).is_empty());
}

#[test]
fn acknowledgments_can_survive_transport_loss_but_cannot_cross_auth_loss() {
    let mut connection = Connection::new(fence(1), 0).unwrap();
    let expiry = NOW + 10_000;
    let ticket = online(&mut connection, expiry);
    let committed = CommittedDirectory::from_store(&directory(0), None).unwrap();
    event(
        &mut connection,
        Event::Directory {
            ticket: &ticket,
            committed,
        },
        NOW,
    );
    let sent = connection.send_context().unwrap();
    event(&mut connection, Event::NetworkLost, NOW + 1);
    assert!(!connection.accepts(&ticket));
    assert!(connection.accepts_ack(&sent, expiry - 1));
    assert!(!connection.accepts_ack(&sent, expiry));
    assert_eq!(
        event(&mut connection, Event::Tick, expiry),
        vec![Effect::AdvanceFence(fence(1))]
    );
    assert!(!connection.accepts_ack(&sent, expiry));
    event(
        &mut connection,
        Event::FenceAdvanced {
            previous: &ticket.fence,
            next: fence(2),
        },
        expiry,
    );
    let next = credential(
        event(&mut connection, Event::NetworkAvailable, expiry),
        AuthCause::Expired,
    );
    event(
        &mut connection,
        Event::Credentials {
            ticket: &next,
            expires_at_ms: expiry + 500,
        },
        expiry,
    );
    assert!(!connection.accepts_ack(&sent, expiry));
}
