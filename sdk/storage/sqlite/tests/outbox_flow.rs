mod common;

use common::*;
use newim_sdk_core::message::{
    ConnectionGeneration, PersistedAck, SendContext, SendFailure, SendIntent,
};
use newim_sdk_core::outbox::{
    self, AUTH_RECOVERY_RESUMED, AckResolution, MAX_RETRY_AGE_MS, OutboxError, OutboxRecord,
    OutboxState, OutboxTransition, RETRY_EXHAUSTED, STORE_GENERATION_CHANGED,
};
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore};
use rusqlite::params;

fn send_context(store: &SqliteStore) -> SendContext {
    SendContext {
        sender_id: "alice".into(),
        fence: store.fence().clone(),
        connection_generation: ConnectionGeneration(11),
    }
}

fn intent_for(client_id: &str) -> SendIntent {
    SendIntent {
        protocol_version: 1,
        client_id: client_id.into(),
        conversation_id: "room".into(),
        schema_version: 1,
        message_type: "text".into(),
        payload: Blob(br#"{"text":"hello"}"#.to_vec()),
    }
}

fn intent() -> SendIntent {
    intent_for("stable")
}

fn revision(store: &mut SqliteStore) -> u64 {
    match run(store, 1, Action::Snapshot { conversation: None }).unwrap() {
        Response::Snapshot(snapshot) => snapshot.revision,
        _ => panic!("snapshot response expected"),
    }
}

fn pending(store: &mut SqliteStore, client_id: &str) -> Pending {
    let response = run(
        store,
        1,
        Action::Pending {
            after: None,
            limit: MAX_ITEMS,
        },
    )
    .unwrap();
    match response {
        Response::Pending(page) => page
            .items
            .into_iter()
            .find(|item| item.client_id == client_id)
            .expect("pending row"),
        _ => panic!("pending response expected"),
    }
}

fn pending_count(directory: &Directory) -> i64 {
    directory
        .db()
        .query_row("SELECT count(*) FROM pending_outbox", [], |row| row.get(0))
        .unwrap()
}

fn message_count(directory: &Directory, server_id: &str) -> i64 {
    directory
        .db()
        .query_row(
            "SELECT count(*) FROM messages WHERE server_id=?",
            [server_id],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn restart_resumes_same_pending() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 1_000).unwrap();
    assert_eq!(created.intent.client_id, "stable");
    let before = pending(&mut store, "stable");
    assert_eq!(
        OutboxRecord::decode(&before).unwrap().state,
        OutboxState::Ready
    );

    let rev = revision(&mut store);
    let transition = outbox::plan_dispatch(&mut store, &before, rev, &context, 1_000).unwrap();
    assert!(matches!(transition, OutboxTransition::Send { .. }));
    drop(store);

    let mut reopened = directory.open();
    let after = pending(&mut reopened, "stable");
    let decoded = OutboxRecord::decode(&after).unwrap();
    assert_eq!(decoded.intent.client_id, "stable");
    assert_eq!(decoded.intent, intent());
    assert_eq!(decoded.state, OutboxState::InFlight);
    assert_eq!(decoded.attempts, 1);
    assert_eq!(pending_count(&directory), 1);
}

#[test]
fn retry_wait_rebinds_after_reopen_and_dispatches() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &row, rev, &context, 0).unwrap(),
        OutboxTransition::Send { .. }
    ));
    let in_flight = pending(&mut store, "stable");
    let in_flight_rev = revision(&mut store);
    let failure =
        SendFailure::from_validated_code("stable", "room", "SERVER_TEMPORARY_UNAVAILABLE").unwrap();
    assert!(matches!(
        outbox::apply_failure(&mut store, &in_flight, in_flight_rev, &context, &failure, 0)
            .unwrap(),
        OutboxTransition::Wait { .. }
    ));
    let before = OutboxRecord::decode(&pending(&mut store, "stable")).unwrap();
    assert_eq!(before.state, OutboxState::RetryWait);
    assert_eq!(before.attempts, 1);
    drop(store);

    let mut reopened = directory.open();
    let mut rebound_context = send_context(&reopened);
    rebound_context.connection_generation = ConnectionGeneration(99);
    let row = pending(&mut reopened, "stable");
    let rev = revision(&mut reopened);
    let receipt = outbox::rebind_connection(&mut reopened, &row, rev, &rebound_context).unwrap();
    let persisted = OutboxRecord::decode(&pending(&mut reopened, "stable")).unwrap();
    assert_eq!(persisted.state, OutboxState::RetryWait);
    assert_eq!(persisted.intent, before.intent);
    assert_eq!(persisted.attempts, before.attempts);
    assert_eq!(persisted.created_at_ms, before.created_at_ms);
    assert_eq!(persisted.deadline_ms, before.deadline_ms);
    assert_eq!(persisted.last_code, before.last_code);
    assert_eq!(
        persisted.active_connection_generation,
        rebound_context.connection_generation
    );

    let rev = receipt.revision;
    let pending_before_deadline = pending(&mut reopened, "stable");
    assert!(matches!(
        outbox::plan_dispatch(
            &mut reopened,
            &pending_before_deadline,
            rev,
            &rebound_context,
            persisted.deadline_ms - 1
        )
        .unwrap(),
        OutboxTransition::Wait { .. }
    ));
    let pending_at_deadline = pending(&mut reopened, "stable");
    assert!(matches!(
        outbox::plan_dispatch(
            &mut reopened,
            &pending_at_deadline,
            rev,
            &rebound_context,
            persisted.deadline_ms
        )
        .unwrap(),
        OutboxTransition::Send { .. }
    ));
    let sent = OutboxRecord::decode(&pending(&mut reopened, "stable")).unwrap();
    assert_eq!(sent.intent, before.intent);
    assert_eq!(sent.attempts, 2);
    assert_eq!(
        sent.active_connection_generation,
        rebound_context.connection_generation
    );
    drop(reopened);

    let mut final_store = directory.open();
    let final_record = OutboxRecord::decode(&pending(&mut final_store, "stable")).unwrap();
    assert_eq!(final_record.state, OutboxState::InFlight);
    assert_eq!(final_record.intent, before.intent);
    assert_eq!(final_record.attempts, 2);
    assert_eq!(
        final_record.active_connection_generation,
        ConnectionGeneration(99)
    );
}

#[test]
fn in_flight_rebinds_after_reopen_and_preserves_deadline() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &row, rev, &context, 0).unwrap(),
        OutboxTransition::Send { .. }
    ));
    let before = OutboxRecord::decode(&pending(&mut store, "stable")).unwrap();
    assert_eq!(before.state, OutboxState::InFlight);
    assert_eq!(before.attempts, 1);
    drop(store);

    let mut reopened = directory.open();
    let mut rebound_context = send_context(&reopened);
    rebound_context.connection_generation = ConnectionGeneration(100);
    let row = pending(&mut reopened, "stable");
    let rev = revision(&mut reopened);
    let receipt = outbox::rebind_connection(&mut reopened, &row, rev, &rebound_context).unwrap();
    let persisted = OutboxRecord::decode(&pending(&mut reopened, "stable")).unwrap();
    assert_eq!(persisted.state, OutboxState::InFlight);
    assert_eq!(persisted.intent, before.intent);
    assert_eq!(persisted.attempts, before.attempts);
    assert_eq!(persisted.created_at_ms, before.created_at_ms);
    assert_eq!(persisted.deadline_ms, before.deadline_ms);
    assert_eq!(persisted.last_code, before.last_code);
    assert_eq!(
        persisted.active_connection_generation,
        rebound_context.connection_generation
    );

    let pending_before_deadline = pending(&mut reopened, "stable");
    assert!(matches!(
        outbox::plan_dispatch(
            &mut reopened,
            &pending_before_deadline,
            receipt.revision,
            &rebound_context,
            persisted.deadline_ms - 1
        )
        .unwrap(),
        OutboxTransition::Wait { .. }
    ));
    let pending_at_deadline = pending(&mut reopened, "stable");
    assert!(matches!(
        outbox::plan_dispatch(
            &mut reopened,
            &pending_at_deadline,
            receipt.revision,
            &rebound_context,
            persisted.deadline_ms
        )
        .unwrap(),
        OutboxTransition::Send { .. }
    ));
    let sent = OutboxRecord::decode(&pending(&mut reopened, "stable")).unwrap();
    assert_eq!(sent.intent, before.intent);
    assert_eq!(sent.attempts, 2);
    assert_eq!(
        sent.active_connection_generation,
        rebound_context.connection_generation
    );
}

#[test]
fn ack_after_connection_generation_change_is_reconcilable() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 1_000).unwrap();
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let ack = PersistedAck {
        sender_id: "alice".into(),
        client_id: created.intent.client_id.clone(),
        conversation_id: created.intent.conversation_id.clone(),
        server_id: "server-after-reconnect".into(),
        sequence: 1,
        server_time: 1,
        protocol_version: created.intent.protocol_version,
        schema_version: created.intent.schema_version,
        message_type: created.intent.message_type.clone(),
        payload: created.intent.payload.clone(),
    };
    let mut changed = context.clone();
    changed.connection_generation = ConnectionGeneration(99);
    let result = outbox::resolve_ack(&mut store, 2, &row, rev, &changed, &ack).unwrap();
    assert!(matches!(result, AckResolution::Resolved(_)));
    assert_eq!(pending_count(&directory), 0);
    assert_eq!(message_count(&directory, "server-after-reconnect"), 1);
    drop(store);

    let mut reopened = directory.open();
    let found = run(
        &mut reopened,
        3,
        Action::Lookup {
            server_id: "server-after-reconnect".into(),
        },
    )
    .unwrap();
    assert!(matches!(found, Response::Found(Some(_))));
}

#[test]
fn store_generation_change_persists_auth_recovery_and_resume() {
    let directory = Directory::new();
    let mut store = directory.open();
    let original = send_context(&store);
    outbox::enqueue(&mut store, 1, &original, intent(), 1_000).unwrap();
    assert!(matches!(
        run(&mut store, 2, Action::AdvanceGeneration),
        Ok(Response::Generation(_))
    ));
    let current = send_context(&store);
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let transition = outbox::plan_dispatch(&mut store, &row, rev, &current, 1_000).unwrap();
    assert!(matches!(
        transition,
        OutboxTransition::AuthRecovery { ref code } if code == STORE_GENERATION_CHANGED
    ));
    let recovered = pending(&mut store, "stable");
    let recovered = OutboxRecord::decode(&recovered).unwrap();
    assert_eq!(recovered.state, OutboxState::AuthRecovery);
    assert_eq!(recovered.fence.generation, 1);
    assert_eq!(recovered.active_fence.generation, 1);

    let recovered_rev = revision(&mut store);
    let recovered_pending = pending(&mut store, "stable");
    let resumed = outbox::resume_auth(
        &mut store,
        &recovered_pending,
        recovered_rev,
        &current,
        1_000,
    )
    .unwrap();
    let OutboxTransition::Wait { until_ms } = resumed else {
        panic!("resume wait expected");
    };
    let resumed = pending(&mut store, "stable");
    let resumed = OutboxRecord::decode(&resumed).unwrap();
    assert_eq!(resumed.fence.generation, 1);
    assert_eq!(resumed.active_fence.generation, 2);
    assert_eq!(resumed.last_code, AUTH_RECOVERY_RESUMED);

    let resumed_rev = revision(&mut store);
    let resumed_pending = pending(&mut store, "stable");
    assert!(matches!(
        outbox::plan_dispatch(
            &mut store,
            &resumed_pending,
            resumed_rev,
            &current,
            until_ms
        )
        .unwrap(),
        OutboxTransition::Send { .. }
    ));
    let in_flight = OutboxRecord::decode(&pending(&mut store, "stable")).unwrap();
    assert_eq!(in_flight.state, OutboxState::InFlight);
    assert_eq!(in_flight.fence.generation, 1);
    assert_eq!(in_flight.active_fence.generation, 2);
}

#[test]
fn retry_deadline_overflow_persists_terminal_across_reopen() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let created_at = u64::MAX - MAX_RETRY_AGE_MS;
    let mut overflow = OutboxRecord::decode(&row).unwrap();
    overflow.created_at_ms = created_at;
    overflow.deadline_ms = created_at + 1;
    overflow.state = OutboxState::InFlight;
    overflow.attempts = 1;
    overflow.last_code.clear();
    let payload = overflow.encode().unwrap();
    let receipt = store
        .update_pending(PendingMutation {
            expected_revision: rev,
            identity: overflow.identity(),
            payload: payload.clone(),
        })
        .unwrap();
    let overflow_pending = Pending { payload, ..row };
    let failure =
        SendFailure::from_validated_code("stable", "room", "SERVER_TEMPORARY_UNAVAILABLE").unwrap();
    let transition = outbox::apply_failure(
        &mut store,
        &overflow_pending,
        receipt.revision,
        &context,
        &failure,
        u64::MAX - 500,
    )
    .unwrap();
    assert!(
        matches!(transition, OutboxTransition::Terminal { ref code } if code == RETRY_EXHAUSTED)
    );
    assert_eq!(pending_count(&directory), 1);
    drop(store);

    let mut reopened = directory.open();
    let persisted = OutboxRecord::decode(&pending(&mut reopened, "stable")).unwrap();
    assert_eq!(persisted.state, OutboxState::PermanentFailure);
    assert_eq!(persisted.last_code, RETRY_EXHAUSTED);
}

#[test]
fn sender_account_mismatch_is_rejected_before_write() {
    let directory = Directory::new();
    let mut store = directory.open();
    let mut mismatch = send_context(&store);
    mismatch.sender_id = "mallory".into();
    assert_eq!(
        outbox::enqueue(&mut store, 1, &mismatch, intent(), 0).unwrap_err(),
        OutboxError::InvalidInput
    );
    assert_eq!(pending_count(&directory), 0);
}

#[test]
fn retry_state_survives_reopen_and_exhausts() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let mut row = pending(&mut store, "stable");
    let mut rev = revision(&mut store);
    let mut now = 0u64;
    for attempt in 1..=8 {
        assert!(matches!(
            outbox::plan_dispatch(&mut store, &row, rev, &context, now).unwrap(),
            OutboxTransition::Send { .. }
        ));
        row = pending(&mut store, "stable");
        rev = revision(&mut store);
        let failure =
            SendFailure::from_validated_code("stable", "room", "SERVER_TEMPORARY_UNAVAILABLE")
                .unwrap();
        let transition =
            outbox::apply_failure(&mut store, &row, rev, &context, &failure, now).unwrap();
        row = pending(&mut store, "stable");
        rev = revision(&mut store);
        if attempt == 8 {
            let terminal = OutboxRecord::decode(&row).unwrap();
            assert_eq!(terminal.state, OutboxState::PermanentFailure);
            assert_eq!(terminal.last_code, RETRY_EXHAUSTED);
            assert!(matches!(transition, OutboxTransition::Terminal { .. }));
            break;
        }
        let OutboxTransition::Wait { until_ms } = transition else {
            panic!("retry wait expected");
        };
        assert!(until_ms > now);
        assert!(matches!(
            outbox::plan_dispatch(&mut store, &row, rev, &context, until_ms - 1).unwrap(),
            OutboxTransition::Wait { .. }
        ));
        now = until_ms;
    }
    drop(store);
    let mut reopened = directory.open();
    let persisted = pending(&mut reopened, "stable");
    let decoded = OutboxRecord::decode(&persisted).unwrap();
    assert_eq!(decoded.state, OutboxState::PermanentFailure);
    assert_eq!(decoded.last_code, RETRY_EXHAUSTED);
    assert_eq!(pending_count(&directory), 1);
}

#[test]
fn ack_is_atomic_and_existing_is_reconciled() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let original = intent();
    let ack = PersistedAck {
        sender_id: "alice".into(),
        client_id: "stable".into(),
        conversation_id: "room".into(),
        server_id: "server-1".into(),
        sequence: 1,
        server_time: 1,
        protocol_version: 1,
        schema_version: 1,
        message_type: "text".into(),
        payload: original.payload,
    };
    let result = outbox::resolve_ack(&mut store, 2, &row, rev, &context, &ack).unwrap();
    assert!(matches!(result, AckResolution::Resolved(_)));
    assert_eq!(pending_count(&directory), 0);
    assert_eq!(message_count(&directory, "server-1"), 1);
    drop(store);

    let mut store = directory.open();
    let context = send_context(&store);
    let second = intent_for("stable2");
    outbox::enqueue(&mut store, 3, &context, second.clone(), 0).unwrap();
    let ack2 = PersistedAck {
        sender_id: "alice".into(),
        client_id: second.client_id.clone(),
        conversation_id: second.conversation_id.clone(),
        server_id: "server-2".into(),
        sequence: 2,
        server_time: 2,
        protocol_version: second.protocol_version,
        schema_version: second.schema_version,
        message_type: second.message_type.clone(),
        payload: second.payload.clone(),
    };
    drop(store);
    directory
        .db()
        .execute(
            "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,NULL)",
            params![
                ack2.server_id,
                ack2.sender_id,
                ack2.client_id,
                ack2.conversation_id,
                ack2.sequence,
                ack2.server_time,
                ack2.schema_version,
                ack2.message_type
            ],
        )
        .unwrap();
    let mut store = directory.open();
    let context = send_context(&store);
    let row = pending(&mut store, "stable2");
    let rev = revision(&mut store);
    let err = outbox::resolve_ack(&mut store, 4, &row, rev, &context, &ack2).unwrap_err();
    assert_eq!(err, OutboxError::AckIntentUnverified);
    assert_eq!(pending_count(&directory), 1);
    assert_eq!(message_count(&directory, "server-2"), 1);
    drop(store);

    let mut store = directory.open();
    let context = send_context(&store);
    let third = intent_for("stable3");
    outbox::enqueue(&mut store, 5, &context, third.clone(), 0).unwrap();
    let ack3 = PersistedAck {
        sender_id: "alice".into(),
        client_id: third.client_id.clone(),
        conversation_id: third.conversation_id.clone(),
        server_id: "server-3".into(),
        sequence: 3,
        server_time: 3,
        protocol_version: third.protocol_version,
        schema_version: third.schema_version,
        message_type: third.message_type.clone(),
        payload: third.payload.clone(),
    };
    drop(store);
    directory
        .db()
        .execute(
            "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?)",
            params![
                ack3.server_id,
                ack3.sender_id,
                ack3.client_id,
                ack3.conversation_id,
                ack3.sequence,
                ack3.server_time,
                ack3.schema_version,
                ack3.message_type,
                ack3.payload.0
            ],
        )
        .unwrap();
    let mut store = directory.open();
    let context = send_context(&store);
    let row = pending(&mut store, "stable3");
    let rev = revision(&mut store);
    let result = outbox::resolve_ack(&mut store, 6, &row, rev, &context, &ack3).unwrap();
    assert!(matches!(result, AckResolution::Resolved(_)));
    assert_eq!(pending_count(&directory), 1);
    assert_eq!(message_count(&directory, "server-3"), 1);
    drop(store);

    let mut store = directory.open();
    let context = send_context(&store);
    let fourth = intent_for("stable4");
    outbox::enqueue(&mut store, 7, &context, fourth.clone(), 0).unwrap();
    let ack4 = PersistedAck {
        sender_id: "alice".into(),
        client_id: fourth.client_id.clone(),
        conversation_id: fourth.conversation_id.clone(),
        server_id: "server-4".into(),
        sequence: 4,
        server_time: 4,
        protocol_version: fourth.protocol_version,
        schema_version: fourth.schema_version,
        message_type: fourth.message_type.clone(),
        payload: fourth.payload.clone(),
    };
    drop(store);
    directory
        .db()
        .execute(
            "INSERT INTO messages VALUES('server-4','alice','stable4','room',4,4,1,'text',X'01')",
            [],
        )
        .unwrap();
    let mut store = directory.open();
    let context = send_context(&store);
    let row = pending(&mut store, "stable4");
    let rev = revision(&mut store);
    let err = outbox::resolve_ack(&mut store, 8, &row, rev, &context, &ack4).unwrap_err();
    assert_eq!(err, OutboxError::AckResultConflict);
    assert_eq!(pending_count(&directory), 2);
    assert_eq!(message_count(&directory, "server-4"), 1);
}

#[test]
fn pending_cas_is_exact_and_revision_checked() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let identity = PendingIdentity {
        sender_id: "alice".into(),
        client_id: "stable".into(),
        conversation_id: "room".into(),
    };
    let stale = PendingMutation {
        expected_revision: rev - 1,
        identity: identity.clone(),
        payload: Blob(vec![9]),
    };
    assert_eq!(
        store.update_pending(stale).unwrap_err(),
        StoreError::StaleRevision
    );
    assert_eq!(pending(&mut store, "stable").payload, row.payload);
    let wrong = PendingMutation {
        expected_revision: rev,
        identity: PendingIdentity {
            conversation_id: "other".into(),
            ..identity.clone()
        },
        payload: Blob(vec![9]),
    };
    assert_eq!(
        store.update_pending(wrong).unwrap_err(),
        StoreError::IdentityConflict
    );
    assert_eq!(pending(&mut store, "stable").payload, row.payload);
    let receipt = store
        .update_pending(PendingMutation {
            expected_revision: rev,
            identity,
            payload: Blob(vec![9]),
        })
        .unwrap();
    assert_eq!(receipt.revision, rev + 1);
    assert_eq!(pending(&mut store, "stable").payload, Blob(vec![9]));
}

#[test]
fn rollback_preserves_pending_and_terminal_cas_removes_exactly() {
    let directory = Directory::new();
    let mut store = directory.open();
    let context = send_context(&store);
    outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    drop(store);

    // Fill the one-record test limit with an unrelated authoritative row. The ACK insert must
    // fail before touching pending, proving rollback rather than a partial resolution.
    directory
        .db()
        .execute(
            "INSERT INTO messages VALUES('fill','alice','fill-client','fill-room',1,1,1,'text',X'01')",
            [],
        )
        .unwrap();
    let mut store = SqliteStore::open(
        &directory.0,
        "alice",
        Limits {
            records: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    let context = send_context(&store);
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let ack = PersistedAck {
        sender_id: "alice".into(),
        client_id: "stable".into(),
        conversation_id: "room".into(),
        server_id: "server-full".into(),
        sequence: 2,
        server_time: 2,
        protocol_version: 1,
        schema_version: 1,
        message_type: "text".into(),
        payload: intent().payload,
    };
    let err = outbox::resolve_ack(&mut store, 2, &row, rev, &context, &ack).unwrap_err();
    assert_eq!(err, OutboxError::Store(StoreError::CapacityExceeded));
    assert_eq!(pending_count(&directory), 1);
    assert_eq!(message_count(&directory, "server-full"), 0);
    drop(store);

    let mut store = directory.open();
    let context = send_context(&store);
    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let auth = SendFailure::from_validated_code("stable", "room", "AUTH_REQUIRED").unwrap();
    let in_flight = match outbox::plan_dispatch(&mut store, &row, rev, &context, 0).unwrap() {
        OutboxTransition::Send { intent: sent } => {
            assert_eq!(sent, intent());
            pending(&mut store, "stable")
        }
        _ => panic!("dispatch expected"),
    };
    let in_flight_rev = revision(&mut store);
    outbox::apply_failure(&mut store, &in_flight, in_flight_rev, &context, &auth, 0).unwrap();
    drop(store);

    let mut reopened = directory.open();
    let context = send_context(&reopened);
    let auth_row = pending(&mut reopened, "stable");
    assert_eq!(
        OutboxRecord::decode(&auth_row).unwrap().state,
        OutboxState::AuthRecovery
    );
    let stale_rev = revision(&mut reopened) - 1;
    assert_eq!(
        outbox::remove_terminal(&mut reopened, &auth_row, stale_rev, &context).unwrap_err(),
        OutboxError::Store(StoreError::StaleRevision)
    );
    assert_eq!(pending_count(&directory), 1);
    let current_rev = revision(&mut reopened);
    outbox::remove_terminal(&mut reopened, &auth_row, current_rev, &context).unwrap();
    assert_eq!(pending_count(&directory), 0);
}

struct UnknownCommitStore {
    inner: SqliteStore,
    drop_completion: bool,
}
impl LocalStore for UnknownCommitStore {
    fn submit(&mut self, request: Request) -> Result<(), StoreError> {
        self.inner.submit(request)?;
        if self.drop_completion {
            self.drop_completion = false;
            let _ = self.inner.take_completion();
        }
        Ok(())
    }
    fn take_completion(&mut self) -> Option<Completion> {
        self.inner.take_completion()
    }
}

#[test]
fn commit_outcome_unknown_requires_authoritative_reload() {
    let directory = Directory::new();
    let mut inner = directory.open();
    let context = send_context(&inner);
    outbox::enqueue(&mut inner, 1, &context, intent(), 0).unwrap();
    let row = pending(&mut inner, "stable");
    let rev = revision(&mut inner);
    let ack = PersistedAck {
        sender_id: "alice".into(),
        client_id: "stable".into(),
        conversation_id: "room".into(),
        server_id: "server-unknown".into(),
        sequence: 1,
        server_time: 1,
        protocol_version: 1,
        schema_version: 1,
        message_type: "text".into(),
        payload: intent().payload,
    };
    let mut store = UnknownCommitStore {
        inner,
        drop_completion: true,
    };
    let err = outbox::resolve_ack(&mut store, 2, &row, rev, &context, &ack).unwrap_err();
    assert_eq!(err, OutboxError::Store(StoreError::CommitOutcomeUnknown));
    drop(store);

    let mut reloaded = directory.open();
    assert_eq!(pending_count(&directory), 0);
    assert_eq!(message_count(&directory, "server-unknown"), 1);
    let found = run(
        &mut reloaded,
        1,
        Action::Lookup {
            server_id: "server-unknown".into(),
        },
    )
    .unwrap();
    assert!(matches!(found, Response::Found(Some(_))));
}

#[test]
fn stale_store_generation_cannot_dispatch() {
    let directory = Directory::new();
    let mut store = directory.open();
    let stale_context = send_context(&store);
    outbox::enqueue(&mut store, 1, &stale_context, intent(), 1_000).unwrap();

    assert!(matches!(
        run(&mut store, 2, Action::AdvanceGeneration),
        Ok(Response::Generation(_))
    ));
    let current = send_context(&store);
    assert_ne!(current.fence, stale_context.fence);

    let row = pending(&mut store, "stable");
    let rev = revision(&mut store);
    let transition = outbox::plan_dispatch(&mut store, &row, rev, &stale_context, 1_000).unwrap();
    assert!(
        matches!(transition, OutboxTransition::AuthRecovery { ref code } if code == STORE_GENERATION_CHANGED),
        "stale store generation must not dispatch: {transition:?}"
    );
}
