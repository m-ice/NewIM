use newim_sdk_core::conversation_sync::{InFlightPage, PAGE_LIMIT, Page, PageRequest};
use newim_sdk_core::store::*;

fn fence() -> Fence {
    Fence {
        account: "alice".into(),
        instance: "local_store".into(),
        generation: 1,
    }
}

fn cursor(value: &str) -> Blob {
    Blob(value.as_bytes().to_vec())
}

fn snapshot(
    phase: ConversationSyncPhase,
    items: Vec<ConversationSyncItem>,
) -> ConversationSyncSnapshot {
    ConversationSyncSnapshot {
        revision: 4,
        position: ConversationSyncPosition { round: 2, phase },
        published_round: Some(1),
        items,
        history_floors: Default::default(),
        next: None,
    }
}

fn upsert(id: &str, revision: u64, sequence: u64, read: Option<u64>) -> ConversationSyncItem {
    ConversationSyncItem {
        conversation_id: id.into(),
        revision,
        known_clear_through_seq: 0,
        change: ConversationSyncChange::Upsert(ConversationSyncSummary {
            latest_conversation_seq: sequence,
            latest_server_msg_id: (sequence > 0).then(|| format!("message_{sequence}")),
            last_read_seq: read,
            settings: None,
        }),
    }
}

fn remove(id: &str, revision: u64) -> ConversationSyncItem {
    ConversationSyncItem {
        conversation_id: id.into(),
        revision,
        known_clear_through_seq: 0,
        change: ConversationSyncChange::Remove,
    }
}

fn expect_items(action: Action) -> Vec<ConversationSyncItem> {
    let Action::ApplyConversationSync(batch) = action else {
        panic!("not a conversation sync batch");
    };
    let ConversationSyncMutation::CommitPage { items, .. } = batch.mutation else {
        panic!("not a page");
    };
    items
}

#[test]
fn out_of_order_items_retain_known_fields_and_tombstones() {
    let current = fence();
    let state = snapshot(
        ConversationSyncPhase::DeltaCheckpoint(cursor("checkpoint")),
        vec![upsert("retained", 1, 3, Some(2)), remove("removed", 6)],
    );
    let flight = InFlightPage::new(&current, &state);
    assert_eq!(
        flight.request(),
        PageRequest::Delta {
            checkpoint: cursor("checkpoint"),
            limit: PAGE_LIMIT,
        }
    );
    let page = Page {
        items: vec![
            upsert("retained", 5, 6, None),
            upsert("removed", 4, 6, Some(3)),
            upsert("retained", 3, 4, Some(3)),
            upsert("retained", 3, 4, Some(3)),
            remove("ended", 7),
            upsert("ended", 2, 2, Some(1)),
        ],
        next_cursor: cursor("next_checkpoint"),
        has_more: false,
    };
    assert_eq!(
        flight.snapshot_query(&page),
        Ok(ConversationSyncQuery::Merge {
            ids: vec!["ended".into(), "removed".into(), "retained".into()],
        })
    );
    assert_eq!(
        expect_items(flight.apply(&current, &state, &page).unwrap()),
        vec![remove("ended", 7), upsert("retained", 5, 6, Some(3))]
    );
    let mut conflicting = page;
    conflicting.items.push(upsert("retained", 3, 4, Some(2)));
    assert_eq!(
        flight.apply(&current, &state, &conflicting),
        Err(StoreError::IdentityConflict)
    );
}

#[test]
fn bootstrap_copies_known_rows_and_restarts_only_the_bound_request() {
    let current = fence();
    let known = upsert("room", 4, 6, Some(3));
    let mut state = snapshot(ConversationSyncPhase::BootstrapStart, vec![known.clone()]);
    let flight = InFlightPage::new(&current, &state);
    assert_eq!(
        flight.request(),
        PageRequest::Bootstrap { limit: PAGE_LIMIT }
    );
    let page = Page {
        items: vec![upsert("room", 4, 6, None)],
        next_cursor: cursor("bootstrap_page"),
        has_more: true,
    };
    assert_eq!(
        expect_items(flight.apply(&current, &state, &page).unwrap()),
        vec![known]
    );
    assert_eq!(
        flight.restart(&current, &state, 409, "SYNC_ACCOUNT_STATE_CONFLICT"),
        Err(StoreError::InvalidInput)
    );
    let mut new_fence = current.clone();
    new_fence.generation += 1;
    assert_eq!(
        flight.apply(&new_fence, &state, &page),
        Err(StoreError::StaleGeneration)
    );
    assert_eq!(
        flight.restart(&new_fence, &state, 409, "SYNC_CURSOR_EXPIRED"),
        Err(StoreError::StaleGeneration)
    );
    state.revision += 1;
    assert!(matches!(
        flight.restart(&current, &state, 409, "SYNC_CURSOR_EXPIRED"),
        Ok(Action::ApplyConversationSync(ConversationSyncBatch {
            expected_revision: 5,
            mutation: ConversationSyncMutation::BeginBootstrap,
            ..
        }))
    ));
    state.position = state.position.begin_bootstrap().unwrap();
    assert_eq!(
        flight.apply(&current, &state, &page),
        Err(StoreError::StaleRevision)
    );
    let next = state.position.after_page(page.next_cursor, true);
    assert_eq!(
        next.phase,
        ConversationSyncPhase::BootstrapPage(cursor("bootstrap_page"))
    );
}

#[test]
fn largest_known_drafts_fit_one_normalized_page() {
    let current = fence();
    let mut stored = Vec::new();
    let mut updates = Vec::new();
    for index in 0..PAGE_LIMIT {
        let id = format!("room_{index:02}");
        let mut item = upsert(&id, 1, 1, Some(0));
        let ConversationSyncChange::Upsert(ref mut summary) = item.change else {
            unreachable!()
        };
        summary.settings = Some(ConversationSyncSettings {
            version: 1,
            pinned: true,
            muted: false,
            draft: "x".repeat(MAX_CONVERSATION_DRAFT_BYTES),
            clear_through_seq: 0,
        });
        updates.push(upsert(&id, index as u64 + 2, 2, None));
        stored.push(item);
    }
    let state = snapshot(
        ConversationSyncPhase::DeltaCheckpoint(cursor("checkpoint")),
        stored,
    );
    let page = Page {
        items: updates,
        next_cursor: cursor("next"),
        has_more: false,
    };
    let request = Request {
        fence: current.clone(),
        operation_id: 5,
        action: InFlightPage::new(&current, &state)
            .apply(&current, &state, &page)
            .unwrap(),
    };
    assert!(request_bytes(&request).unwrap().len() <= MAX_BATCH_BYTES);
}

#[test]
fn legacy_apply_request_bytes_remain_the_receipt_identity() {
    let request = Request {
        fence: fence(),
        operation_id: 7,
        action: Action::Apply(Batch {
            expected_revision: 4,
            messages: vec![],
            conversations: vec![],
            cursor: None,
            resolve_pending: vec![],
        }),
    };
    let mut expected = Vec::new();
    for key in ["alice", "local_store"] {
        expected.extend((key.len() as u64).to_le_bytes());
        expected.extend(key.as_bytes());
    }
    for field in [1_u64, 7, 5, 4, 0, 0, 0, 0] {
        expected.extend(field.to_le_bytes());
    }
    assert_eq!(request_bytes(&request).unwrap(), expected);
}

#[test]
fn late_message_respects_known_personal_history_and_directory_removal() {
    let mut room = upsert("room", 6, 9, Some(7));
    let ConversationSyncChange::Upsert(ref mut summary) = room.change else {
        unreachable!()
    };
    summary.settings = Some(ConversationSyncSettings {
        version: 2,
        pinned: false,
        muted: true,
        draft: String::new(),
        clear_through_seq: 7,
    });
    assert!(!room.allows_message(7));
    assert!(room.allows_message(8));
    assert!(!remove("room", 7).allows_message(8));
}

#[test]
fn same_page_removal_discards_settings_and_keeps_the_history_floor() {
    let current = fence();
    let mut state = snapshot(
        ConversationSyncPhase::DeltaCheckpoint(cursor("checkpoint")),
        vec![],
    );
    state.history_floors.insert("archived".into(), 5);
    let mut cleared = upsert("room", 2, 8, Some(5));
    let ConversationSyncChange::Upsert(ref mut summary) = cleared.change else {
        unreachable!()
    };
    summary.settings = Some(ConversationSyncSettings {
        version: 2,
        pinned: true,
        muted: true,
        draft: "will be removed".into(),
        clear_through_seq: 7,
    });
    let page = Page {
        items: vec![
            upsert("room", 4, 9, None),
            upsert("archived", 1, 8, None),
            remove("room", 3),
            cleared.clone(),
        ],
        next_cursor: cursor("next"),
        has_more: false,
    };
    let flight = InFlightPage::new(&current, &state);
    let items = expect_items(flight.apply(&current, &state, &page).unwrap());
    for (item, floor) in items.iter().zip([5, 7]) {
        assert_eq!(item.known_clear_through_seq, floor);
        assert!(!item.allows_message(floor));
        assert!(item.allows_message(floor + 1));
        let ConversationSyncChange::Upsert(summary) = &item.change else {
            panic!("expected a visible conversation");
        };
        assert_eq!(summary.settings, None);
    }
    assert_eq!(items.len(), 2);

    // An older admitted revision can fill in the history boundary without restoring settings.
    let legacy = upsert("room", 4, 9, None);
    let mut expected = legacy.clone();
    expected.known_clear_through_seq = 7;
    assert_eq!(
        newim_sdk_core::conversation_sync::merge_revision(Some(&legacy), &cleared),
        Ok(expected)
    );
}
