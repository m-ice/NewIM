mod common;

use common::*;
use newim_sdk_core::conversation_sync::{InFlightPage, Page};
use newim_sdk_core::message_sync;
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore};

fn state(store: &mut SqliteStore) -> MessageSyncSnapshot {
    let query = MessageSyncQuery {
        conversation_id: "room".into(),
        lookups: vec![],
    };
    match run(store, 1, Action::MessageSyncSnapshot(query)).unwrap() {
        Response::MessageSyncSnapshot(snapshot) => snapshot,
        _ => panic!("unexpected message sync response"),
    }
}

fn directory(store: &mut SqliteStore, operation: u64, change: ConversationSyncChange) {
    let Response::ConversationSyncSnapshot(snapshot) = run(
        store,
        1,
        Action::ConversationSyncSnapshot(ConversationSyncQuery::State),
    )
    .unwrap() else {
        panic!("unexpected directory response");
    };
    let action = Action::ApplyConversationSync(ConversationSyncBatch {
        expected_revision: snapshot.revision,
        expected_position: snapshot.position,
        mutation: ConversationSyncMutation::CommitPage {
            next_cursor: Blob(operation.to_be_bytes().to_vec()),
            has_more: false,
            items: vec![ConversationSyncItem {
                conversation_id: "room".into(),
                revision: operation,
                known_clear_through_seq: 0,
                change,
            }],
        },
    });
    assert!(matches!(
        run(store, operation, action),
        Ok(Response::Committed(_))
    ));
}

fn publish(store: &mut SqliteStore, operation: u64) {
    directory(
        store,
        operation,
        ConversationSyncChange::Upsert(ConversationSyncSummary {
            latest_conversation_seq: 0,
            latest_server_msg_id: None,
            last_read_seq: None,
            settings: None,
        }),
    );
}

fn action(store: &mut SqliteStore, now_ms: u64, mutation: MessageSyncMutation) -> Action {
    Action::ApplyMessageSync(MessageSyncBatch {
        expected_revision: state(store).revision,
        conversation_id: "room".into(),
        now_ms,
        mutation,
    })
}

fn commit(store: &mut SqliteStore, id: u64, now: u64, mutation: MessageSyncMutation) {
    let action = action(store, now, mutation);
    assert!(matches!(run(store, id, action), Ok(Response::Committed(_))));
}

fn attempt(store: &mut SqliteStore) -> GapAttempt {
    let Some(GapJob {
        status: GapStatus::InFlight(attempt),
        ..
    }) = state(store).state.job
    else {
        panic!("missing active attempt");
    };
    attempt
}

fn observe(n: i64) -> MessageSyncMutation {
    MessageSyncMutation::Observe(MessageWrite::Insert(msg(n)))
}

fn directory_state(
    store: &mut SqliteStore,
    query: ConversationSyncQuery,
) -> ConversationSyncSnapshot {
    let Response::ConversationSyncSnapshot(value) =
        run(store, 1, Action::ConversationSyncSnapshot(query)).unwrap()
    else {
        panic!("expected directory state");
    };
    value
}

fn upsert_at(revision: u64, floor: u64) -> ConversationSyncItem {
    ConversationSyncItem {
        conversation_id: "room".into(),
        revision,
        known_clear_through_seq: floor,
        change: ConversationSyncChange::Upsert(ConversationSyncSummary {
            latest_conversation_seq: 8,
            latest_server_msg_id: Some("s8".into()),
            last_read_seq: None,
            settings: Some(ConversationSyncSettings {
                version: revision,
                pinned: false,
                muted: false,
                draft: String::new(),
                clear_through_seq: floor,
            }),
        }),
    }
}

fn directory_page(store: &mut SqliteStore, operation: u64, items: Vec<ConversationSyncItem>) {
    let current = directory_state(store, ConversationSyncQuery::State);
    let flight = InFlightPage::new(store.fence(), &current);
    let page = Page {
        items,
        next_cursor: Blob(operation.to_be_bytes().to_vec()),
        has_more: false,
    };
    let merge = directory_state(store, flight.snapshot_query(&page).unwrap());
    let action = flight.apply(store.fence(), &merge, &page).unwrap();
    assert!(matches!(
        run(store, operation, action),
        Ok(Response::Committed(_))
    ));
}

#[test]
fn published_target_starts_history_without_a_live_message() {
    let dir = Directory::new();
    let mut store = dir.open();
    publish(&mut store, 1);
    let empty = state(&mut store);
    let target = message_sync::observe_target(&empty, 10, 1, 0).unwrap();
    assert!(run(&mut store, 2, target).is_ok());
    assert_eq!(
        message_sync::schedule(&state(&mut store), 10).unwrap(),
        None
    );
    directory_page(&mut store, 3, vec![upsert_at(2, 0)]);
    let initial = state(&mut store);
    assert_eq!(initial.state.highest_observed_seq, 0);
    let target = message_sync::observe_target(&initial, 20, 1, 8).unwrap();
    let request = req(&store, 4, target);
    assert!(store.execute(request.clone()).result.is_ok());
    assert!(matches!(
        store.execute(request).result,
        Ok(Response::Committed(Receipt { replayed: true, .. }))
    ));
    let scheduled = state(&mut store).state;
    assert_eq!(
        (scheduled.continuous_seq, scheduled.highest_observed_seq),
        (0, 8)
    );
    assert!(matches!(
        scheduled.job.as_ref().unwrap().status,
        GapStatus::Waiting { until_ms: 20 }
    ));

    let duplicate = message_sync::observe_target(&state(&mut store), 21, 1, 7).unwrap();
    assert!(run(&mut store, 5, duplicate).is_ok());
    assert_eq!(state(&mut store).state, scheduled);
    drop(store);
    let mut store = dir.open();
    let start = message_sync::schedule(&state(&mut store), 22)
        .unwrap()
        .unwrap();
    assert!(run(&mut store, 6, start).is_ok());
    let current = attempt(&mut store);
    assert_eq!(current.after_seq, 0);
    commit(
        &mut store,
        7,
        23,
        MessageSyncMutation::CommitPage {
            attempt: current,
            page: MessageSyncPage {
                history_after_seq: 0,
                latest_seq: 8,
                has_next: false,
                next_after_seq: Some(8),
                messages: (1..=8).map(|seq| MessageWrite::Insert(msg(seq))).collect(),
            },
        },
    );
    let settled = state(&mut store).state;
    assert_eq!(
        (settled.continuous_seq, settled.highest_observed_seq),
        (8, 8)
    );
    assert!(settled.job.is_none());
}

#[test]
fn directory_target_requires_the_current_published_item_and_tail() {
    let dir = Directory::new();
    let mut store = dir.open();
    directory_page(&mut store, 1, vec![upsert_at(1, 0)]);
    let published = directory_state(&mut store, ConversationSyncQuery::State);
    let restart = InFlightPage::new(store.fence(), &published)
        .restart(store.fence(), &published, 409, "SYNC_CURSOR_EXPIRED")
        .unwrap();
    assert!(run(&mut store, 2, restart).is_ok());
    let staging = directory_state(&mut store, ConversationSyncQuery::State);
    assert_eq!(staging.position.round, 2);
    assert_eq!(staging.published_round, Some(1));
    let staged_item = Action::ApplyConversationSync(ConversationSyncBatch {
        expected_revision: staging.revision,
        expected_position: staging.position,
        mutation: ConversationSyncMutation::CommitPage {
            next_cursor: Blob(vec![3]),
            has_more: true,
            items: vec![upsert_at(2, 0)],
        },
    });
    assert!(run(&mut store, 3, staged_item).is_ok());
    let original = state(&mut store);
    for (round, through) in [(2, 8), (1, 9)] {
        let target = message_sync::observe_target(&original, 20, round, through).unwrap();
        assert_eq!(run(&mut store, 4, target), Err(StoreError::StaleRevision));
    }
    assert_eq!(state(&mut store), original);

    directory_page(&mut store, 4, vec![]);
    let snapshot = state(&mut store);
    let outdated = message_sync::observe_target(&snapshot, 21, 1, 8).unwrap();
    assert_eq!(run(&mut store, 5, outdated), Err(StoreError::StaleRevision));
    let target = message_sync::observe_target(&snapshot, 21, 2, 8).unwrap();
    assert!(run(&mut store, 5, target).is_ok());
    directory(&mut store, 6, ConversationSyncChange::Remove);
    let hidden = state(&mut store);
    assert!(!hidden.visible);
    assert!(hidden.state.job.is_none());
    let target = action(
        &mut store,
        22,
        MessageSyncMutation::ObserveTarget {
            published_round: 2,
            through_seq: 8,
        },
    );
    assert_eq!(run(&mut store, 7, target), Err(StoreError::StaleRevision));
    assert_eq!(state(&mut store), hidden);
}

#[test]
fn directory_advance_resets_retry_budget() {
    let dir = Directory::new();
    let mut store = dir.open();
    publish(&mut store, 1);
    let mut now = 100;
    let mut operation = 2;
    commit(&mut store, operation, now, observe(8));
    operation += 1;
    for failures in 1..8 {
        commit(
            &mut store,
            operation,
            now,
            MessageSyncMutation::StartAttempt,
        );
        operation += 1;
        let current = attempt(&mut store);
        assert_eq!(current.after_seq, 0);
        commit(
            &mut store,
            operation,
            now,
            MessageSyncMutation::FailAttempt {
                attempt: current,
                failure: GapFailure::Temporary,
            },
        );
        operation += 1;
        let job = state(&mut store).state.job.unwrap();
        assert_eq!(job.failures, failures);
        let GapStatus::Waiting { until_ms } = job.status else {
            panic!("expected retry delay");
        };
        now = until_ms;
    }
    directory_page(&mut store, operation, vec![upsert_at(2, 1)]);
    operation += 1;
    commit(
        &mut store,
        operation,
        now,
        MessageSyncMutation::StartAttempt,
    );
    operation += 1;
    let current = attempt(&mut store);
    assert_eq!(current.after_seq, 1);
    commit(
        &mut store,
        operation,
        now + 1,
        MessageSyncMutation::FailAttempt {
            attempt: current,
            failure: GapFailure::Temporary,
        },
    );
    operation += 1;
    let job = state(&mut store).state.job.unwrap();
    assert_eq!(job.failures, 1);
    let GapStatus::Waiting { until_ms } = job.status else {
        panic!("new position must still have retry budget");
    };
    commit(
        &mut store,
        operation,
        until_ms,
        MessageSyncMutation::StartAttempt,
    );
    operation += 1;
    let current = attempt(&mut store);
    directory_page(&mut store, operation, vec![upsert_at(3, 2)]);
    operation += 1;
    commit(
        &mut store,
        operation,
        until_ms + 1,
        MessageSyncMutation::FailAttempt {
            attempt: current,
            failure: GapFailure::Authentication,
        },
    );
    let job = state(&mut store).state.job.unwrap();
    assert_eq!(job.failures, 0);
    assert_eq!(job.status, GapStatus::AuthRecovery);
}

#[test]
fn directory_rejoin_and_rebootstrap_discard_prior_attempts() {
    let dir = Directory::new();
    let mut store = dir.open();
    publish(&mut store, 1);
    commit(&mut store, 2, 100, observe(8));
    commit(&mut store, 3, 101, MessageSyncMutation::StartAttempt);
    let previous = attempt(&mut store);
    let remove = ConversationSyncItem {
        conversation_id: "room".into(),
        revision: 2,
        known_clear_through_seq: 0,
        change: ConversationSyncChange::Remove,
    };
    directory_page(&mut store, 4, vec![remove.clone(), upsert_at(3, 1)]);
    let snapshot = state(&mut store);
    assert!(snapshot.visible && snapshot.state.job.is_none());
    let stale = action(
        &mut store,
        102,
        MessageSyncMutation::FailAttempt {
            attempt: previous,
            failure: GapFailure::Temporary,
        },
    );
    assert_eq!(run(&mut store, 5, stale), Err(StoreError::StaleRevision));
    commit(&mut store, 5, 103, MessageSyncMutation::StartAttempt);
    let current = attempt(&mut store);
    assert_eq!(current.after_seq, 1);
    directory_page(&mut store, 6, vec![remove, upsert_at(4, 1)]);
    assert_eq!(attempt(&mut store), current);

    let snapshot = directory_state(&mut store, ConversationSyncQuery::State);
    let restart = InFlightPage::new(store.fence(), &snapshot)
        .restart(store.fence(), &snapshot, 409, "SYNC_CURSOR_EXPIRED")
        .unwrap();
    assert!(matches!(
        run(&mut store, 7, restart),
        Ok(Response::Committed(_))
    ));
    assert_eq!(attempt(&mut store), current);
    directory_page(&mut store, 8, vec![upsert_at(5, 2)]);
    let snapshot = state(&mut store);
    assert!(snapshot.visible);
    assert!(matches!(
        snapshot.state.job.unwrap().status,
        GapStatus::Waiting { .. }
    ));
    let stale = action(
        &mut store,
        104,
        MessageSyncMutation::FailAttempt {
            attempt: current,
            failure: GapFailure::Temporary,
        },
    );
    assert_eq!(run(&mut store, 9, stale), Err(StoreError::StaleRevision));
    commit(&mut store, 9, 105, MessageSyncMutation::StartAttempt);
    assert_eq!(attempt(&mut store).after_seq, 2);
}

#[test]
fn late_failure_and_expiry_keep_new_position_budget() {
    let dir = Directory::new();
    let mut store = dir.open();
    publish(&mut store, 1);
    commit(&mut store, 2, 100, observe(8));
    commit(&mut store, 3, 101, MessageSyncMutation::StartAttempt);
    let first = attempt(&mut store);
    commit(&mut store, 4, 102, observe(1));
    commit(
        &mut store,
        5,
        103,
        MessageSyncMutation::FailAttempt {
            attempt: first,
            failure: GapFailure::Temporary,
        },
    );
    let waiting = state(&mut store).state.job.unwrap();
    assert_eq!(waiting.failures, 0);
    assert_eq!(waiting.status, GapStatus::Waiting { until_ms: 103 });
    commit(&mut store, 6, 104, MessageSyncMutation::StartAttempt);
    let second = attempt(&mut store);
    assert_eq!(second.after_seq, 1);
    commit(&mut store, 7, 105, observe(2));
    commit(
        &mut store,
        8,
        second.expires_at_ms,
        MessageSyncMutation::StartAttempt,
    );
    let waiting = state(&mut store).state.job.unwrap();
    assert_eq!(waiting.failures, 0);
    assert_eq!(
        waiting.status,
        GapStatus::Waiting {
            until_ms: second.expires_at_ms
        }
    );
    commit(
        &mut store,
        9,
        second.expires_at_ms + 1,
        MessageSyncMutation::StartAttempt,
    );
    assert_eq!(attempt(&mut store).after_seq, 2);
}

#[test]
fn progress_can_continue_past_eight_pages() {
    let dir = Directory::new();
    let mut store = dir.open();
    publish(&mut store, 1);
    let mut operation = 2;
    let mut now = 100;
    let mut apply = |store: &mut SqliteStore, mutation| {
        commit(store, operation, now, mutation);
        operation += 1;
        now += 1;
    };
    apply(&mut store, observe(11));
    for expected in [64, 32, 16, 8, 4, 2] {
        apply(&mut store, MessageSyncMutation::StartAttempt);
        let current = attempt(&mut store);
        assert_eq!(current.limit, expected);
        apply(
            &mut store,
            MessageSyncMutation::FailAttempt {
                attempt: current,
                failure: GapFailure::PageCapacity,
            },
        );
    }
    for sequence in 1..=10 {
        apply(&mut store, MessageSyncMutation::StartAttempt);
        let current = attempt(&mut store);
        assert_eq!((current.limit, current.after_seq), (1, sequence - 1));
        apply(
            &mut store,
            MessageSyncMutation::CommitPage {
                attempt: current,
                page: MessageSyncPage {
                    history_after_seq: 0,
                    latest_seq: 11,
                    has_next: true,
                    next_after_seq: Some(sequence),
                    messages: vec![MessageWrite::Insert(msg(sequence))],
                },
            },
        );
    }
    let state = state(&mut store).state;
    assert_eq!(state.continuous_seq, 11);
    assert!(state.job.is_none());
}

#[test]
fn old_ack_gap_and_older_terminal_page_preserve_new_target() {
    let d = Directory::new();
    let mut store = d.open();
    publish(&mut store, 1);
    assert!(run(&mut store, 2, batch(1, vec![msg(4)])).is_ok());
    drop(store);
    let mut store = d.open();
    let discovered = state(&mut store);
    assert_eq!(discovered.conversation_id, "room");
    assert_eq!(discovered.state.highest_observed_seq, 4);
    assert!(discovered.state.job.is_none());
    commit(&mut store, 3, 100, MessageSyncMutation::StartAttempt);
    let current = attempt(&mut store);
    assert_eq!(
        (
            current.operation_id,
            current.after_seq,
            current.expires_at_ms
        ),
        (3, 0, 30_100)
    );
    commit(&mut store, 4, 101, observe(8));
    commit(
        &mut store,
        5,
        102,
        MessageSyncMutation::CommitPage {
            attempt: current,
            page: MessageSyncPage {
                history_after_seq: 0,
                latest_seq: 4,
                has_next: false,
                next_after_seq: Some(4),
                messages: (1..=4).map(|n| MessageWrite::Insert(msg(n))).collect(),
            },
        },
    );
    let waiting = state(&mut store).state;
    assert_eq!(
        (waiting.continuous_seq, waiting.highest_observed_seq),
        (4, 8)
    );
    assert!(matches!(
        waiting.job.unwrap().status,
        GapStatus::Waiting { .. }
    ));
    commit(&mut store, 6, 103, MessageSyncMutation::StartAttempt);
    let current = attempt(&mut store);
    assert_eq!(current.after_seq, 4);
    commit(
        &mut store,
        7,
        104,
        MessageSyncMutation::CommitPage {
            attempt: current,
            page: MessageSyncPage {
                history_after_seq: 9,
                latest_seq: 9,
                has_next: false,
                next_after_seq: None,
                messages: vec![],
            },
        },
    );
    commit(&mut store, 8, 105, observe(6));
    assert!(found(&mut store, 6).is_none());
    let settled = state(&mut store).state;
    assert_eq!((settled.continuous_seq, settled.history_after_seq), (4, 9));
    assert!(settled.job.is_none());
}

#[test]
fn restart_counts_expired_attempt_once_and_checks_retry_limit() {
    let d = Directory::new();
    let mut store = d.open();
    publish(&mut store, 1);
    commit(&mut store, 2, 1, observe(10));
    commit(&mut store, 3, 1, MessageSyncMutation::StartAttempt);
    drop(store);
    let mut store = d.open();
    let reaped = action(&mut store, 30_001, MessageSyncMutation::StartAttempt);
    let req = req(&store, 4, reaped);
    assert!(store.execute(req.clone()).result.is_ok());
    assert!(matches!(
        store.execute(req).result,
        Ok(Response::Committed(Receipt { replayed: true, .. }))
    ));
    let job = state(&mut store).state.job.unwrap();
    assert_eq!(job.failures, 1);
    assert_eq!(job.status, GapStatus::Waiting { until_ms: 31_001 });
    let mut operation = 5;
    for expected_failures in 2..=8 {
        let GapStatus::Waiting { until_ms } = state(&mut store).state.job.unwrap().status else {
            panic!("unexpected retry state");
        };
        commit(
            &mut store,
            operation,
            until_ms,
            MessageSyncMutation::StartAttempt,
        );
        let current = attempt(&mut store);
        commit(
            &mut store,
            operation + 1,
            until_ms + 1,
            MessageSyncMutation::FailAttempt {
                attempt: current,
                failure: GapFailure::Temporary,
            },
        );
        assert_eq!(
            state(&mut store).state.job.unwrap().failures,
            expected_failures
        );
        operation += 2;
    }
    assert_eq!(
        state(&mut store).state.job.unwrap().status,
        GapStatus::Stopped
    );
    commit(&mut store, operation, 300_000, observe(11));
    assert_eq!(
        state(&mut store).state.job.unwrap().status,
        GapStatus::Stopped
    );
    commit(
        &mut store,
        operation + 1,
        300_001,
        MessageSyncMutation::Resume,
    );
    assert_eq!(state(&mut store).state.job.unwrap().failures, 0);
}

#[test]
fn page_capacity_rollback_and_removal_invalidate_attempt() {
    let d = Directory::new();
    let mut store = SqliteStore::open(
        &d.0,
        "alice",
        Limits {
            records: 2,
            ..Limits::default()
        },
    )
    .unwrap();
    publish(&mut store, 1);
    commit(&mut store, 2, 1, observe(4));
    commit(&mut store, 3, 2, MessageSyncMutation::StartAttempt);
    let current = attempt(&mut store);
    let original = state(&mut store);
    let page = MessageSyncMutation::CommitPage {
        attempt: current.clone(),
        page: MessageSyncPage {
            history_after_seq: 0,
            latest_seq: 4,
            has_next: true,
            next_after_seq: Some(2),
            messages: vec![MessageWrite::Insert(msg(1)), MessageWrite::Insert(msg(2))],
        },
    };
    let failed = action(&mut store, 3, page.clone());
    assert_eq!(
        run(&mut store, 4, failed),
        Err(StoreError::CapacityExceeded)
    );
    assert_eq!(original, state(&mut store));
    assert!(found(&mut store, 1).is_none());
    commit(
        &mut store,
        4,
        4,
        MessageSyncMutation::FailAttempt {
            attempt: current.clone(),
            failure: GapFailure::PageCapacity,
        },
    );
    assert_eq!(state(&mut store).state.job.unwrap().limit, MAX_ITEMS / 2);
    commit(&mut store, 5, 5, MessageSyncMutation::StartAttempt);
    let replacement = attempt(&mut store);
    assert_eq!(replacement.after_seq, current.after_seq);
    directory(&mut store, 6, ConversationSyncChange::Remove);
    let hidden = state(&mut store);
    assert!(!hidden.visible);
    assert!(hidden.state.job.is_none());
    assert!(found(&mut store, 4).is_some());
    publish(&mut store, 7);
    let stale = action(
        &mut store,
        8,
        MessageSyncMutation::FailAttempt {
            attempt: replacement,
            failure: GapFailure::Temporary,
        },
    );
    assert_eq!(run(&mut store, 8, stale), Err(StoreError::StaleRevision));
}

#[test]
fn new_generation_resumes_and_missing_bootstrap_invalidates_attempt() {
    let d = Directory::new();
    let mut store = d.open();
    publish(&mut store, 1);
    commit(&mut store, 2, 1, observe(3));
    commit(&mut store, 3, 2, MessageSyncMutation::StartAttempt);
    let old = attempt(&mut store);
    assert!(matches!(
        run(&mut store, 4, Action::AdvanceGeneration),
        Ok(Response::Generation(_))
    ));
    let stale = action(
        &mut store,
        3,
        MessageSyncMutation::FailAttempt {
            attempt: old.clone(),
            failure: GapFailure::Temporary,
        },
    );
    assert_eq!(run(&mut store, 1, stale), Err(StoreError::StaleGeneration));
    commit(&mut store, 1, 3, MessageSyncMutation::Resume);
    commit(&mut store, 2, 4, MessageSyncMutation::StartAttempt);
    let current = attempt(&mut store);
    assert_ne!(current.generation, old.generation);
    assert_eq!(current.after_seq, old.after_seq);

    let apply_directory = |store: &mut SqliteStore, operation, mutation| {
        let Response::ConversationSyncSnapshot(snapshot) = run(
            store,
            1,
            Action::ConversationSyncSnapshot(ConversationSyncQuery::State),
        )
        .unwrap() else {
            panic!("unexpected directory response");
        };
        let action = Action::ApplyConversationSync(ConversationSyncBatch {
            expected_revision: snapshot.revision,
            expected_position: snapshot.position,
            mutation,
        });
        assert!(run(store, operation, action).is_ok());
    };
    apply_directory(&mut store, 3, ConversationSyncMutation::BeginBootstrap);
    assert_eq!(attempt(&mut store), current);
    apply_directory(
        &mut store,
        4,
        ConversationSyncMutation::CommitPage {
            next_cursor: Blob(vec![1]),
            has_more: false,
            items: vec![],
        },
    );
    let hidden = state(&mut store);
    assert!(!hidden.visible);
    assert!(hidden.state.job.is_none());
    assert!(found(&mut store, 3).is_some());
}
