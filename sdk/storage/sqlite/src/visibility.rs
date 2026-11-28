use crate::{db, operations::row_read_error};
use newim_sdk_core::store::{Message, StoreError, VisibilityAction, VisibilityEvent};
use rusqlite::{Connection, params};

fn locate(
    conn: &Connection,
    conversation: &str,
    sequence: i64,
    server_id: &str,
) -> Result<Option<VisibilityAction>, StoreError> {
    let mut query = conn
        .prepare(
            "SELECT conversation_id,sequence,server_id,action FROM message_visibility
             WHERE server_id=?1 OR (conversation_id=?2 AND sequence=?3) LIMIT 2",
        )
        .map_err(db)?;
    let mut rows = query
        .query(params![server_id, conversation, sequence])
        .map_err(db)?;
    let Some(row) = rows.next().map_err(row_read_error)? else {
        return Ok(None);
    };
    if row.get::<_, String>(0).map_err(row_read_error)? != conversation
        || row.get::<_, i64>(1).map_err(row_read_error)? != sequence
        || row.get::<_, String>(2).map_err(row_read_error)? != server_id
    {
        return Err(StoreError::IdentityConflict);
    }
    let action = row.get::<_, u8>(3).map_err(row_read_error)?;
    if rows.next().map_err(row_read_error)?.is_some() {
        return Err(StoreError::IdentityConflict);
    }
    VisibilityAction::from_code(action)
        .map(Some)
        .ok_or(StoreError::Corrupt)
}

fn prefer(
    current: Option<VisibilityAction>,
    candidate: VisibilityAction,
) -> Result<VisibilityAction, StoreError> {
    match (current, candidate) {
        (None | Some(VisibilityAction::DeleteForMe), _) => Ok(candidate),
        (Some(action), VisibilityAction::DeleteForMe) => Ok(action),
        (Some(action), next) if action == next => Ok(action),
        _ => Err(StoreError::IdentityConflict),
    }
}

pub(crate) fn sanitize_insert(
    conn: &Connection,
    item: &Message,
) -> Result<Option<Message>, StoreError> {
    let Some(action) = locate(conn, &item.conversation_id, item.sequence, &item.server_id)? else {
        return Ok(None);
    };
    let prior = item
        .payload
        .as_ref()
        .filter(|_| item.is_hidden())
        .and_then(VisibilityAction::from_payload);
    let mut hidden = item.clone();
    prefer(prior, action)?.hide(&mut hidden);
    Ok(Some(hidden))
}

pub(crate) fn hide_message(
    conn: &Connection,
    old: &Message,
    action: VisibilityAction,
) -> Result<u32, StoreError> {
    let prior = old
        .payload
        .as_ref()
        .filter(|_| old.is_hidden())
        .and_then(VisibilityAction::from_payload);
    let mut hidden = old.clone();
    prefer(prior, action)?.hide(&mut hidden);
    if hidden == *old {
        return Ok(0);
    }
    conn.execute(
        "UPDATE messages SET message_type=?,schema_version=1,payload=? WHERE server_id=?",
        params![
            hidden.message_type,
            hidden.payload.as_ref().map(|blob| &blob.0),
            old.server_id
        ],
    )
    .map_err(db)
    .map(|changed| changed as u32)
}

pub(crate) fn apply_event(
    conn: &Connection,
    conversation: &str,
    event: &VisibilityEvent,
    max_records: usize,
) -> Result<u32, StoreError> {
    let previous = locate(conn, conversation, event.sequence, &event.server_id)?;
    let action = prefer(previous, event.action)?;
    if previous.is_none() {
        let count: i64 = conn
            .query_row("SELECT count(*) FROM message_visibility", [], |row| {
                row.get(0)
            })
            .map_err(db)?;
        if count >= max_records as i64 {
            return Err(StoreError::CapacityExceeded);
        }
    }
    if previous != Some(action) {
        conn.execute(
            "INSERT INTO message_visibility VALUES(?,?,?,?)
             ON CONFLICT(conversation_id,sequence) DO UPDATE SET action=excluded.action",
            params![conversation, event.sequence, event.server_id, action.code()],
        )
        .map_err(db)?;
    }

    let mut query = conn
        .prepare(&format!(
            "SELECT {} FROM messages
             WHERE server_id=?1 OR (conversation_id=?2 AND sequence=?3) LIMIT 2",
            crate::operations::COLUMNS
        ))
        .map_err(db)?;
    let matches = query
        .query_map(
            params![event.server_id, conversation, event.sequence],
            crate::operations::message,
        )
        .map_err(db)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(row_read_error)?;
    let item = match matches.as_slice() {
        [] => return Ok(0),
        [item]
            if item.server_id == event.server_id
                && item.conversation_id == conversation
                && item.sequence == event.sequence =>
        {
            item
        }
        _ => return Err(StoreError::IdentityConflict),
    };
    crate::operations::validate_message_read(item)?;
    hide_message(conn, item, action)
}
