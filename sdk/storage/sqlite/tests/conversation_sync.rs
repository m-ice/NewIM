mod common;

use common::*;
use newim_sdk_core::conversation_sync::{InFlightPage, Page};
use newim_sdk_core::store::*;
use newim_store_sqlite::SqliteStore;
use rusqlite::OptionalExtension;

fn read(s: &mut SqliteStore, query: ConversationSyncQuery) -> ConversationSyncSnapshot {
    match run(s, 1, Action::ConversationSyncSnapshot(query)).unwrap() {
        Response::ConversationSyncSnapshot(snapshot) => snapshot,
        _ => panic!("unexpected store response"),
    }
}

fn current(s: &mut SqliteStore) -> ConversationSyncSnapshot {
    read(s, ConversationSyncQuery::State)
}

fn published(s: &mut SqliteStore, after: Option<String>, limit: usize) -> ConversationSyncSnapshot {
    read(s, ConversationSyncQuery::Published { after, limit })
}

fn change(snapshot: &ConversationSyncSnapshot, mutation: ConversationSyncMutation) -> Action {
    Action::ApplyConversationSync(ConversationSyncBatch {
        expected_revision: snapshot.revision,
        expected_position: snapshot.position.clone(),
        mutation,
    })
}

fn page(
    snapshot: &ConversationSyncSnapshot,
    cursor: u8,
    has_more: bool,
    items: Vec<ConversationSyncItem>,
) -> Action {
    change(
        snapshot,
        ConversationSyncMutation::CommitPage {
            next_cursor: Blob(vec![cursor]),
            has_more,
            items,
        },
    )
}

fn upsert(id: &str, revision: u64) -> ConversationSyncItem {
    ConversationSyncItem {
        conversation_id: id.into(),
        revision,
        known_clear_through_seq: 0,
        change: ConversationSyncChange::Upsert(ConversationSyncSummary {
            latest_conversation_seq: 0,
            latest_server_msg_id: None,
            last_read_seq: None,
            unread: None,
            settings: None,
        }),
    }
}

fn from_server(
    store: &mut SqliteStore,
    operation: u64,
    has_more: bool,
    items: Vec<ConversationSyncItem>,
) {
    let fence = store.fence().clone();
    let flight = InFlightPage::new(&fence, &current(store));
    let page = Page {
        items,
        next_cursor: Blob(operation.to_be_bytes().to_vec()),
        has_more,
    };
    let snapshot = read(store, flight.snapshot_query(&page).unwrap());
    let action = flight.apply(&fence, &snapshot, &page).unwrap();
    assert!(matches!(
        run(store, operation, action),
        Ok(Response::Committed(_))
    ));
}

fn cleared(id: &str, revision: u64, floor: u64) -> ConversationSyncItem {
    let mut item = upsert(id, revision);
    let ConversationSyncChange::Upsert(summary) = &mut item.change else {
        unreachable!()
    };
    summary.settings = Some(ConversationSyncSettings {
        version: revision,
        pinned: true,
        muted: true,
        draft: "private draft".into(),
        clear_through_seq: floor,
    });
    item
}

fn remove(id: &str, revision: u64) -> ConversationSyncItem {
    ConversationSyncItem {
        conversation_id: id.into(),
        revision,
        known_clear_through_seq: 0,
        change: ConversationSyncChange::Remove,
    }
}

fn expect_legacy_history(item: &ConversationSyncItem, floor: u64) {
    assert_eq!(item.known_clear_through_seq, floor);
    assert!(!item.allows_message(floor));
    assert!(item.allows_message(floor + 1));
    let ConversationSyncChange::Upsert(summary) = &item.change else {
        panic!("expected a visible conversation");
    };
    assert_eq!(summary.settings, None);
}

fn message_page(
    store: &mut SqliteStore,
    conversation: &str,
    after: Option<i64>,
    limit: usize,
) -> MessagePage {
    let Response::Messages(page) = run(
        store,
        1,
        Action::Messages {
            conversation: conversation.into(),
            after,
            limit,
        },
    )
    .unwrap() else {
        panic!("expected a message page");
    };
    page
}

fn archived_floor(dir: &Directory, id: &str) -> Option<i64> {
    dir.db()
        .query_row(
            "SELECT clear_through_seq FROM conversation_sync_history WHERE conversation_id=?",
            [id],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
}

fn begin_bootstrap(store: &mut SqliteStore, operation: u64) {
    let action = change(&current(store), ConversationSyncMutation::BeginBootstrap);
    assert!(run(store, operation, action).is_ok());
}

#[test]
fn message_pages_follow_published_and_archived_history() {
    let d = Directory::new();
    let mut store = d.open();
    let unrelated = Message {
        conversation_id: "other".into(),
        ..msg(8)
    };
    let zero = Message {
        conversation_id: "other".into(),
        server_id: "other_zero".into(),
        client_id: "other_zero".into(),
        ..msg(0)
    };
    assert!(
        run(
            &mut store,
            1,
            batch(
                0,
                vec![
                    msg(0),
                    msg(1),
                    msg(3),
                    msg(4),
                    msg(6),
                    zero.clone(),
                    unrelated.clone()
                ]
            )
        )
        .is_ok()
    );
    assert_eq!(
        message_page(&mut store, "room", None, 1).items,
        vec![msg(0)]
    );
    from_server(
        &mut store,
        2,
        false,
        vec![upsert("other", 1), cleared("room", 2, 3)],
    );
    from_server(&mut store, 3, false, vec![remove("other", 3)]);
    assert_eq!(
        message_page(&mut store, "other", None, 64).items,
        vec![zero, unrelated]
    );
    assert_eq!(
        message_page(&mut store, "room", Some(1), 1),
        MessagePage {
            items: vec![msg(4)],
            next: Some(4)
        }
    );
    assert_eq!(
        message_page(&mut store, "room", Some(4), 64).items,
        vec![msg(6)]
    );

    let late_revision = current(&mut store).revision;
    let late = req(&store, 4, batch(late_revision, vec![msg(2)]));
    assert!(store.execute(late.clone()).result.is_ok());
    assert_eq!(found(&mut store, 2), Some(msg(2)));
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(4), msg(6)]
    );
    assert!(matches!(
        store.execute(late).result,
        Ok(Response::Committed(Receipt { replayed: true, .. }))
    ));
    assert!(run(&mut store, 5, Action::Enqueue(pending(99))).is_ok());

    begin_bootstrap(&mut store, 6);
    from_server(&mut store, 7, true, vec![cleared("room", 4, 4)]);
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(4), msg(6)]
    );
    from_server(&mut store, 8, false, vec![]);
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(6)]
    );

    begin_bootstrap(&mut store, 9);
    from_server(&mut store, 10, false, vec![]);
    assert_eq!(archived_floor(&d, "room"), Some(4));
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(6)]
    );
    from_server(&mut store, 11, false, vec![upsert("room", 5)]);
    assert_eq!(archived_floor(&d, "room"), None);
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(6)]
    );

    begin_bootstrap(&mut store, 12);
    from_server(&mut store, 13, false, vec![]);
    assert_eq!(archived_floor(&d, "room"), Some(4));
    begin_bootstrap(&mut store, 14);
    from_server(&mut store, 15, true, vec![cleared("room", 6, 6)]);
    assert_eq!(archived_floor(&d, "room"), Some(4));
    drop(store);
    let mut store = d.open();
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(6)]
    );

    let staged = current(&mut store);
    let db = d.db();
    db.execute_batch(
        "CREATE TRIGGER reject_publish BEFORE UPDATE OF published_round ON conversation_sync_state
         BEGIN SELECT RAISE(ABORT,'fixture publish failure'); END;",
    )
    .unwrap();
    let result = run(&mut store, 16, page(&staged, 16, false, vec![]));
    db.execute_batch("DROP TRIGGER reject_publish;").unwrap();
    assert_eq!(result, Err(StoreError::IdentityConflict));
    assert_eq!(current(&mut store), staged);
    assert_eq!(archived_floor(&d, "room"), Some(4));
    assert_eq!(
        message_page(&mut store, "room", None, 64).items,
        vec![msg(6)]
    );
    from_server(&mut store, 16, false, vec![]);
    assert_eq!(archived_floor(&d, "room"), None);
    assert!(message_page(&mut store, "room", None, 64).items.is_empty());

    for n in [0, 1, 2, 3, 4, 6] {
        assert_eq!(found(&mut store, n), Some(msg(n)));
    }
    assert_eq!(
        run(
            &mut store,
            1,
            Action::Pending {
                after: None,
                limit: 64
            }
        ),
        Ok(Response::Pending(PendingPage {
            items: vec![pending(99)],
            next: None
        }))
    );
}

#[test]
fn history_floor_survives_removal_reopen_and_absent_bootstrap() {
    let d = Directory::new();
    let mut store = d.open();
    from_server(
        &mut store,
        1,
        false,
        vec![cleared("active", 1, 11), cleared("removed", 2, 7)],
    );
    from_server(&mut store, 2, false, vec![remove("removed", 3)]);
    drop(store);

    let mut store = d.open();
    from_server(&mut store, 3, false, vec![upsert("removed", 4)]);
    expect_legacy_history(&published(&mut store, Some("active".into()), 1).items[0], 7);
    from_server(&mut store, 4, false, vec![cleared("removed", 5, 7)]);
    from_server(
        &mut store,
        5,
        false,
        vec![upsert("removed", 7), remove("removed", 6)],
    );
    expect_legacy_history(&published(&mut store, Some("active".into()), 1).items[0], 7);
    from_server(&mut store, 6, false, vec![remove("removed", 8)]);

    let before_bootstrap = current(&mut store);
    assert!(
        run(
            &mut store,
            7,
            change(&before_bootstrap, ConversationSyncMutation::BeginBootstrap)
        )
        .is_ok()
    );
    from_server(&mut store, 8, true, vec![cleared("active", 9, 19)]);
    assert!(published(&mut store, None, 1).items[0].allows_message(12));
    let staged = current(&mut store);
    assert!(
        run(
            &mut store,
            9,
            change(&staged, ConversationSyncMutation::BeginBootstrap)
        )
        .is_ok()
    );
    from_server(&mut store, 10, false, vec![]);
    assert!(published(&mut store, None, 2).items.is_empty());
    drop(store);

    let mut store = d.open();
    let resumed = read(
        &mut store,
        ConversationSyncQuery::Merge {
            ids: vec!["active".into(), "removed".into()],
        },
    );
    assert!(resumed.items.is_empty());
    assert_eq!(
        resumed.history_floors,
        [("active".into(), 11), ("removed".into(), 7)].into()
    );
    assert_eq!(
        run(
            &mut store,
            11,
            page(&resumed, 11, false, vec![upsert("active", 10)])
        ),
        Err(StoreError::StaleRevision)
    );
    from_server(
        &mut store,
        11,
        false,
        vec![upsert("active", 10), upsert("removed", 11)],
    );
    let restored = published(&mut store, None, 2);
    assert_eq!(restored.items.len(), 2);
    expect_legacy_history(&restored.items[0], 11);
    expect_legacy_history(&restored.items[1], 7);
}

#[test]
fn round_publish_reopen_and_generation_guards() {
    let d = Directory::new();
    let mut s = d.open();
    let mut old = upsert("old", 3);
    let ConversationSyncChange::Upsert(summary) = &mut old.change else {
        unreachable!()
    };
    summary.last_read_seq = Some(27);
    summary.settings = Some(ConversationSyncSettings {
        version: 5,
        pinned: true,
        muted: false,
        draft: format!("{}\0x", "界".repeat(1364)),
        clear_through_seq: 27,
    });
    old.known_clear_through_seq = 27;
    let initial = current(&mut s);
    assert!(run(&mut s, 1, page(&initial, 1, true, vec![old.clone()])).is_ok());
    assert_eq!(published(&mut s, None, 1).items, vec![]);
    let stage = current(&mut s);
    assert!(run(&mut s, 2, page(&stage, 2, false, vec![])).is_ok());
    assert_eq!(published(&mut s, None, 1).items, vec![old.clone()]);
    let removal = remove("hidden", 7);
    let checkpoint = current(&mut s);
    assert!(
        run(
            &mut s,
            3,
            page(&checkpoint, 3, false, vec![removal.clone()])
        )
        .is_ok()
    );
    let checkpoint = current(&mut s);
    assert!(
        run(
            &mut s,
            4,
            change(&checkpoint, ConversationSyncMutation::BeginBootstrap)
        )
        .is_ok()
    );
    let bootstrap = current(&mut s);
    let old_cursor = req(&s, 5, page(&checkpoint, 4, false, vec![]));
    assert_eq!(s.execute(old_cursor).result, Err(StoreError::StaleRevision));
    assert_eq!(
        read(
            &mut s,
            ConversationSyncQuery::Merge {
                ids: vec!["hidden".into(), "old".into()]
            }
        )
        .items,
        vec![removal.clone(), old.clone()]
    );

    let new = upsert("new", 8);
    let stage_request = req(&s, 5, page(&bootstrap, 5, true, vec![new.clone()]));
    assert!(s.execute(stage_request.clone()).result.is_ok());
    drop(s);
    let mut s = d.open();
    assert!(matches!(
        s.execute(stage_request).result,
        Ok(Response::Committed(Receipt { replayed: true, .. }))
    ));
    let old_round = published(&mut s, None, 1);
    assert_eq!(old_round.published_round, Some(1));
    assert_eq!(old_round.items, vec![old]);

    let stage = current(&mut s);
    assert!(run(&mut s, 6, page(&stage, 6, false, vec![])).is_ok());
    let new_round = published(&mut s, None, 1);
    assert_eq!(new_round.published_round, Some(2));
    assert_eq!(
        new_round.position.phase,
        ConversationSyncPhase::DeltaCheckpoint(Blob(vec![6]))
    );
    assert_eq!(new_round.items, vec![new]);
    assert_eq!(
        read(
            &mut s,
            ConversationSyncQuery::Merge {
                ids: vec!["hidden".into(), "old".into()]
            }
        )
        .items,
        vec![]
    );

    let at = current(&mut s);
    let request = req(&s, 7, page(&at, 7, false, vec![]));
    assert!(run(&mut s, 7, Action::AdvanceGeneration).is_ok());
    assert_eq!(s.execute(request).result, Err(StoreError::StaleGeneration));
}

#[test]
fn page_commit_is_atomic_for_stale_revision_and_conflicts() {
    let d = Directory::new();
    let mut s = d.open();
    let stable = upsert("stable", 4);
    let initial = current(&mut s);
    assert!(run(&mut s, 1, page(&initial, 1, false, vec![stable.clone()])).is_ok());

    let before = current(&mut s);
    let delta = page(&before, 2, false, vec![upsert("new", 5)]);
    assert!(run(&mut s, 2, batch(before.revision, vec![msg(3)])).is_ok());
    assert_eq!(run(&mut s, 3, delta), Err(StoreError::StaleRevision));
    let replay = current(&mut s);
    assert_eq!(replay.position, before.position);

    let conflict = ConversationSyncItem {
        change: ConversationSyncChange::Remove,
        ..stable.clone()
    };
    assert_eq!(
        run(
            &mut s,
            3,
            page(&replay, 2, false, vec![upsert("new", 5), conflict])
        ),
        Err(StoreError::IdentityConflict)
    );
    assert_eq!(current(&mut s), replay);
    assert_eq!(published(&mut s, None, 2).items, vec![stable]);
    assert_eq!(
        run(
            &mut s,
            3,
            page(&replay, 2, false, vec![upsert("stable", 3)])
        ),
        Err(StoreError::StaleRevision)
    );
    assert!(run(&mut s, 3, page(&replay, 2, false, vec![upsert("new", 5)])).is_ok());
    assert!(found(&mut s, 3).is_some());
}

#[test]
fn full_disjoint_directories_swap_and_capacity_failure_keeps_checkpoint() {
    let d = Directory::new();
    let mut s = d.open();
    let fill = |s: &mut SqliteStore, prefix: &str, first_operation: u64, next_is_terminal: bool| {
        let ids: Vec<_> = (0..MAX_SYNC_DIRECTORY_ITEMS)
            .map(|i| upsert(&format!("{prefix}{i:04}"), 1))
            .collect();
        let pages = ids.chunks(MAX_ITEMS);
        let total = pages.len();
        for (i, items) in pages.enumerate() {
            let at = current(s);
            assert!(
                run(
                    s,
                    first_operation + i as u64,
                    page(
                        &at,
                        i as u8 + 1,
                        i + 1 != total || !next_is_terminal,
                        items.to_vec()
                    ),
                )
                .is_ok()
            );
        }
        first_operation + total as u64
    };
    let operation = fill(&mut s, "a", 1, true);
    let old = published(&mut s, None, MAX_ITEMS);
    assert_eq!(old.items.len(), MAX_ITEMS);
    assert_eq!(old.next.as_deref(), Some("a0063"));
    assert_eq!(
        published(&mut s, old.next.clone(), 1).items[0].conversation_id,
        "a0064"
    );
    let at = current(&mut s);
    assert!(
        run(
            &mut s,
            operation,
            change(&at, ConversationSyncMutation::BeginBootstrap)
        )
        .is_ok()
    );
    let operation = fill(&mut s, "b", operation + 1, false);
    let stage = current(&mut s);
    assert_eq!(published(&mut s, None, MAX_ITEMS).items, old.items);
    assert_eq!(
        run(
            &mut s,
            operation,
            page(&stage, 99, false, vec![upsert("overflow", 2)])
        ),
        Err(StoreError::CapacityExceeded)
    );
    assert_eq!(current(&mut s), stage);
    assert_eq!(published(&mut s, None, MAX_ITEMS).items, old.items);
    assert!(run(&mut s, operation, page(&stage, 99, false, vec![])).is_ok());
    assert_eq!(published(&mut s, None, 1).items[0].conversation_id, "b0000");
    let checkpoint = current(&mut s);
    assert_eq!(
        run(
            &mut s,
            operation + 1,
            page(&checkpoint, 100, false, vec![upsert("overflow", 2)])
        ),
        Err(StoreError::CapacityExceeded)
    );
    assert_eq!(current(&mut s), checkpoint);

    drop(s);
    let conn = d.db();
    let count: i64 = conn
        .query_row("SELECT count(*) FROM conversation_sync_items", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, MAX_SYNC_DIRECTORY_ITEMS as i64);
}

#[test]
fn terminal_directory_page_replaces_optimistic_unread_and_survives_reopen() {
    let directory = Directory::new();
    let mut store = directory.open();
    let optimistic = Action::Apply(Batch {
        expected_revision: current(&mut store).revision,
        messages: Vec::new(),
        conversations: vec![Conversation {
            id: "room".into(),
            unread: 7,
            summary: Blob(vec![42]),
        }],
        cursor: None,
        resolve_pending: Vec::new(),
    });
    assert!(matches!(
        run(&mut store, 2, optimistic),
        Ok(Response::Committed(_))
    ));
    let snapshot_unread = |store: &mut SqliteStore| {
        let response = run(
            store,
            1,
            Action::Snapshot {
                conversation: Some("room".into()),
            },
        )
        .unwrap();
        let Response::Snapshot(snapshot) = response else {
            panic!("missing conversation snapshot")
        };
        snapshot.conversation.unwrap().unread
    };
    let mut item = upsert("room", 2);
    let ConversationSyncChange::Upsert(summary) = &mut item.change else {
        unreachable!()
    };
    summary.latest_conversation_seq = 5;
    summary.latest_server_msg_id = Some("last".into());
    summary.last_read_seq = Some(2);
    summary.unread = Some(ConversationUnread {
        count: 2,
        visibility_revision: 1,
    });
    from_server(&mut store, 3, true, vec![item.clone()]);
    assert_eq!(snapshot_unread(&mut store), 7);
    from_server(&mut store, 4, false, Vec::new());
    assert_eq!(snapshot_unread(&mut store), 2);
    drop(store);

    let mut store = directory.open();
    assert_eq!(snapshot_unread(&mut store), 2);
    assert_eq!(published(&mut store, None, 1).items, vec![item.clone()]);
    let ConversationSyncChange::Upsert(summary) = &mut item.change else {
        unreachable!()
    };
    item.revision += 1;
    summary.last_read_seq = Some(5);
    summary.unread = Some(ConversationUnread {
        count: 0,
        visibility_revision: 2,
    });
    from_server(&mut store, 5, false, vec![item.clone()]);
    assert_eq!(snapshot_unread(&mut store), 0);
    assert_eq!(published(&mut store, None, 1).items, vec![item]);
}
