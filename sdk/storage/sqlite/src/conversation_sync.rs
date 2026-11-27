use crate::{
    db, message_sync,
    operations::{bounded, row_read_error, unsigned},
};
use newim_sdk_core::{conversation_sync::merge_revision, store::*};
use rusqlite::{OptionalExtension, Row, Statement, Transaction, params};
use std::collections::BTreeMap;

const ITEM_COLUMNS: &str = "conversation_id,revision,latest_seq,latest_server_id,last_read_seq,settings_version,pinned,muted,draft,clear_through_seq,known_clear_through_seq";

struct State {
    position: ConversationSyncPosition,
    published_round: Option<u64>,
}

fn state(tx: &Transaction<'_>) -> Result<State, StoreError> {
    tx.query_row(
        "SELECT round,phase,cursor,published_round FROM conversation_sync_state WHERE id=1",
        [],
        |row| {
            bounded(row, 2, MAX_SYNC_CURSOR_BYTES)?;
            let cursor = row.get::<_, Option<Vec<u8>>>(2)?.map(Blob);
            let phase = match (row.get::<_, i64>(1)?, cursor) {
                (0, None) => ConversationSyncPhase::BootstrapStart,
                (1, Some(cursor)) => ConversationSyncPhase::BootstrapPage(cursor),
                (2, Some(cursor)) => ConversationSyncPhase::DeltaCheckpoint(cursor),
                (3, Some(cursor)) => ConversationSyncPhase::DeltaPage(cursor),
                _ => return Err(rusqlite::Error::InvalidQuery),
            };
            Ok(State {
                position: ConversationSyncPosition {
                    round: unsigned(row, 0)?,
                    phase,
                },
                published_round: optional_unsigned(row, 3)?,
            })
        },
    )
    .optional()
    .map_err(row_read_error)?
    .ok_or(StoreError::Corrupt)
}

fn optional_unsigned(row: &Row<'_>, index: usize) -> rusqlite::Result<Option<u64>> {
    row.get::<_, Option<i64>>(index)?
        .map(|value| {
            u64::try_from(value).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, value))
        })
        .transpose()
}

fn item(row: &Row<'_>) -> rusqlite::Result<ConversationSyncItem> {
    bounded(row, 0, 128)?;
    bounded(row, 3, 128)?;
    bounded(row, 8, MAX_CONVERSATION_DRAFT_BYTES)?;
    let change = match optional_unsigned(row, 2)? {
        None => ConversationSyncChange::Remove,
        Some(latest_conversation_seq) => {
            let settings = optional_unsigned(row, 5)?
                .map(|version| {
                    Ok::<_, rusqlite::Error>(ConversationSyncSettings {
                        version,
                        pinned: row.get(6)?,
                        muted: row.get(7)?,
                        draft: row.get(8)?,
                        clear_through_seq: unsigned(row, 9)?,
                    })
                })
                .transpose()?;
            ConversationSyncChange::Upsert(ConversationSyncSummary {
                latest_conversation_seq,
                latest_server_msg_id: row.get(3)?,
                last_read_seq: optional_unsigned(row, 4)?,
                settings,
            })
        }
    };
    let item = ConversationSyncItem {
        conversation_id: row.get(0)?,
        revision: unsigned(row, 1)?,
        change,
        known_clear_through_seq: unsigned(row, 10)?,
    };
    item.validate().map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(item)
}

fn prepare_merge<'tx>(tx: &'tx Transaction<'_>) -> Result<Statement<'tx>, StoreError> {
    // Prefer an item already staged in this round, including a staged tombstone.
    tx.prepare(&format!(
        "SELECT {ITEM_COLUMNS} FROM conversation_sync_items
         WHERE round IN (?1,?2) AND conversation_id=?3 ORDER BY round DESC LIMIT 1"
    ))
    .map_err(db)
}

fn lookup(
    statement: &mut Statement<'_>,
    state: &State,
    id: &str,
) -> Result<Option<ConversationSyncItem>, StoreError> {
    statement
        .query_row(
            params![
                state.position.round as i64,
                state.published_round.map(|round| round as i64),
                id
            ],
            item,
        )
        .optional()
        .map_err(row_read_error)
}

fn prepare_history_lookup<'tx>(tx: &'tx Transaction<'_>) -> Result<Statement<'tx>, StoreError> {
    tx.prepare("SELECT clear_through_seq FROM conversation_sync_history WHERE conversation_id=?")
        .map_err(db)
}

fn history_floor(statement: &mut Statement<'_>, id: &str) -> Result<Option<u64>, StoreError> {
    statement
        .query_row([id], |row| unsigned(row, 0))
        .optional()
        .map_err(row_read_error)
}

pub(crate) fn published_history_floor(
    tx: &Transaction<'_>,
    id: &str,
) -> Result<Option<i64>, StoreError> {
    let (published, archived) = tx
        .query_row(
            "SELECT
             (SELECT known_clear_through_seq FROM conversation_sync_items
              WHERE round=(SELECT published_round FROM conversation_sync_state WHERE id=1)
                AND conversation_id=?1),
             (SELECT clear_through_seq FROM conversation_sync_history WHERE conversation_id=?1)",
            [id],
            |row| Ok((optional_unsigned(row, 0)?, optional_unsigned(row, 1)?)),
        )
        .map_err(row_read_error)?;
    // Zero denotes no known clear operation and must still admit legacy sequence zero.
    Ok(published
        .max(archived)
        .filter(|&floor| floor > 0)
        .map(|floor| floor as i64))
}

pub(crate) fn snapshot(
    tx: &Transaction<'_>,
    query: &ConversationSyncQuery,
) -> Result<ConversationSyncSnapshot, StoreError> {
    let revision = tx
        .query_row(
            "SELECT revision FROM store_metadata WHERE id=1",
            [],
            |row| unsigned(row, 0),
        )
        .map_err(row_read_error)?;
    let state = state(tx)?;
    let mut items = Vec::new();
    let mut history_floors = BTreeMap::new();
    let mut next = None;
    match query {
        ConversationSyncQuery::State => {}
        ConversationSyncQuery::Merge { ids } => {
            let mut statement = prepare_merge(tx)?;
            let mut history = prepare_history_lookup(tx)?;
            for id in ids {
                if let Some(item) = lookup(&mut statement, &state, id)? {
                    items.push(item);
                } else if let Some(floor) = history_floor(&mut history, id)? {
                    history_floors.insert(id.clone(), floor);
                }
            }
        }
        ConversationSyncQuery::Published { after, limit } => {
            if let Some(round) = state.published_round {
                let mut statement = tx
                    .prepare(&format!(
                        "SELECT {ITEM_COLUMNS} FROM conversation_sync_items
                         WHERE round=?1 AND conversation_id>?2 AND latest_seq IS NOT NULL
                         ORDER BY conversation_id LIMIT ?3"
                    ))
                    .map_err(db)?;
                items = statement
                    .query_map(
                        params![
                            round as i64,
                            after.as_deref().unwrap_or(""),
                            *limit as i64 + 1
                        ],
                        item,
                    )
                    .map_err(db)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(row_read_error)?;
                if items.len() > *limit {
                    items.pop();
                    next = items.last().map(|item| item.conversation_id.clone());
                }
            }
        }
    }
    Ok(ConversationSyncSnapshot {
        revision,
        position: state.position,
        published_round: state.published_round,
        items,
        history_floors,
        next,
    })
}

fn write_page(
    tx: &Transaction<'_>,
    state: &State,
    items: &[ConversationSyncItem],
) -> Result<(), StoreError> {
    let mut previous = prepare_merge(tx)?;
    let mut history = prepare_history_lookup(tx)?;
    let mut statement = tx
        .prepare(
            "INSERT INTO conversation_sync_items VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
             ON CONFLICT(round,conversation_id) DO UPDATE SET
             revision=excluded.revision,latest_seq=excluded.latest_seq,
             latest_server_id=excluded.latest_server_id,last_read_seq=excluded.last_read_seq,
             settings_version=excluded.settings_version,pinned=excluded.pinned,muted=excluded.muted,
             draft=excluded.draft,clear_through_seq=excluded.clear_through_seq,
             known_clear_through_seq=excluded.known_clear_through_seq",
        )
        .map_err(db)?;
    for item in items {
        let current = lookup(&mut previous, state, &item.conversation_id)?;
        let floor = match &current {
            Some(row) => row.known_clear_through_seq,
            None => history_floor(&mut history, &item.conversation_id)?.unwrap_or(0),
        };
        // The coordinator may have folded a remove and a newer upsert together.
        // Preserve that reset of optional fields while checking replays and the history floor.
        let same_or_newer = current.as_ref().filter(|row| row.revision >= item.revision);
        if item.known_clear_through_seq < floor || merge_revision(same_or_newer, item)? != *item {
            return Err(StoreError::StaleRevision);
        }
        let summary = match &item.change {
            ConversationSyncChange::Upsert(summary) => Some(summary),
            ConversationSyncChange::Remove => None,
        };
        let settings = summary.and_then(|summary| summary.settings.as_ref());
        statement
            .execute(params![
                state.position.round as i64,
                item.conversation_id,
                item.revision as i64,
                summary.map(|summary| summary.latest_conversation_seq as i64),
                summary.and_then(|summary| summary.latest_server_msg_id.as_deref()),
                summary.and_then(|summary| summary.last_read_seq.map(|seq| seq as i64)),
                settings.map(|settings| settings.version as i64),
                settings.map(|settings| settings.pinned),
                settings.map(|settings| settings.muted),
                settings.map(|settings| settings.draft.as_str()),
                settings.map(|settings| settings.clear_through_seq as i64),
                item.known_clear_through_seq as i64,
            ])
            .map_err(db)?;
    }
    let count: i64 = tx
        .query_row(
            "SELECT count(*) FROM conversation_sync_items WHERE round=?",
            [state.position.round as i64],
            |row| row.get(0),
        )
        .map_err(db)?;
    if count > MAX_SYNC_DIRECTORY_ITEMS as i64 {
        return Err(StoreError::CapacityExceeded);
    }
    Ok(())
}

fn delete_round(tx: &Transaction<'_>, round: u64) -> Result<(), StoreError> {
    tx.execute(
        "DELETE FROM conversation_sync_items WHERE round=?",
        [round as i64],
    )
    .map_err(db)?;
    Ok(())
}

fn archive_missing_history(
    tx: &Transaction<'_>,
    previous_round: u64,
    replacement_round: u64,
) -> Result<(), StoreError> {
    // Rows carried to the replacement round already inherited their history boundary.
    tx.execute(
        "INSERT INTO conversation_sync_history
         SELECT previous.conversation_id,previous.known_clear_through_seq
         FROM conversation_sync_items previous
         WHERE previous.round=?1 AND previous.known_clear_through_seq>0
           AND NOT EXISTS (
             SELECT 1 FROM conversation_sync_items replacement
             WHERE replacement.round=?2 AND replacement.conversation_id=previous.conversation_id
           )
         ON CONFLICT(conversation_id) DO UPDATE SET
         clear_through_seq=max(clear_through_seq,excluded.clear_through_seq)",
        [previous_round as i64, replacement_round as i64],
    )
    .map_err(db)?;
    Ok(())
}

fn prune_delta_history(
    tx: &Transaction<'_>,
    items: &[ConversationSyncItem],
) -> Result<(), StoreError> {
    let mut delete = tx
        .prepare(
            "DELETE FROM conversation_sync_history
             WHERE conversation_id=? AND clear_through_seq<=?",
        )
        .map_err(db)?;
    for item in items.iter().filter(|item| item.known_clear_through_seq > 0) {
        delete
            .execute(params![
                item.conversation_id,
                item.known_clear_through_seq as i64
            ])
            .map_err(db)?;
    }
    Ok(())
}

fn prune_round_history(tx: &Transaction<'_>, round: u64) -> Result<(), StoreError> {
    // Drive the deletion from the bounded directory, using archive primary-key lookups.
    tx.execute(
        "DELETE FROM conversation_sync_history AS history
         WHERE conversation_id IN (
             SELECT conversation_id FROM conversation_sync_items
             WHERE round=?1 AND known_clear_through_seq>0
         ) AND clear_through_seq <= (
             SELECT known_clear_through_seq FROM conversation_sync_items
             WHERE round=?1 AND conversation_id=history.conversation_id
         )",
        [round as i64],
    )
    .map_err(db)?;
    Ok(())
}

pub(crate) fn apply(
    tx: &Transaction<'_>,
    batch: &ConversationSyncBatch,
) -> Result<u32, StoreError> {
    let state = state(tx)?;
    if state.position != batch.expected_position {
        return Err(StoreError::StaleRevision);
    }
    let mut published_round = state.published_round;
    let (position, affected) = match &batch.mutation {
        ConversationSyncMutation::BeginBootstrap => {
            let next = state.position.begin_bootstrap()?;
            if state.position.is_bootstrap() {
                delete_round(tx, state.position.round)?;
            }
            (next, 0)
        }
        ConversationSyncMutation::CommitPage {
            next_cursor,
            has_more,
            items,
        } => {
            write_page(tx, &state, items)?;
            if !state.position.is_bootstrap() {
                let removed = items
                    .iter()
                    .filter(|item| item.change == ConversationSyncChange::Remove)
                    .map(|item| item.conversation_id.as_str());
                message_sync::invalidate(tx, removed)?;
                prune_delta_history(tx, items)?;
            } else if !has_more {
                if let Some(previous) = published_round {
                    archive_missing_history(tx, previous, state.position.round)?;
                    message_sync::directory_replaced(tx, previous, state.position.round)?;
                    delete_round(tx, previous)?;
                }
                prune_round_history(tx, state.position.round)?;
                published_round = Some(state.position.round);
            }
            (
                state.position.after_page(next_cursor.clone(), *has_more),
                items.len() as u32,
            )
        }
    };
    let (phase, cursor) = match &position.phase {
        ConversationSyncPhase::BootstrapStart => (0, None),
        ConversationSyncPhase::BootstrapPage(cursor) => (1, Some(cursor.0.as_slice())),
        ConversationSyncPhase::DeltaCheckpoint(cursor) => (2, Some(cursor.0.as_slice())),
        ConversationSyncPhase::DeltaPage(cursor) => (3, Some(cursor.0.as_slice())),
    };
    tx.execute(
        "UPDATE conversation_sync_state SET round=?,phase=?,cursor=?,published_round=? WHERE id=1",
        params![
            position.round as i64,
            phase,
            cursor,
            published_round.map(|round| round as i64)
        ],
    )
    .map_err(db)?;
    Ok(affected)
}
