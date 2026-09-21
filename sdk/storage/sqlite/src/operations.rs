use crate::{SqliteStore, db};
use newim_sdk_core::store::*;
use rusqlite::{Connection, OptionalExtension, Row, TransactionBehavior, params};
use std::{sync::mpsc, time::Duration};

const COLUMNS: &str = "server_id,sender_id,client_id,conversation_id,sequence,server_time,schema_version,message_type,payload";
fn bounded(row: &Row<'_>, index: usize, max: usize) -> rusqlite::Result<()> {
    let value = row.get_ref(index)?;
    let len = match value {
        rusqlite::types::ValueRef::Text(v) | rusqlite::types::ValueRef::Blob(v) => v.len(),
        rusqlite::types::ValueRef::Null => 0,
        _ => return Err(rusqlite::Error::InvalidQuery),
    };
    if len > max {
        return Err(rusqlite::Error::InvalidQuery);
    }
    Ok(())
}
fn message(row: &Row<'_>) -> rusqlite::Result<Message> {
    for index in 0..4 {
        bounded(row, index, 128)?;
    }
    bounded(row, 7, 64)?;
    bounded(row, 8, MAX_VALUE_BYTES)?;
    Ok(Message {
        server_id: row.get(0)?,
        sender_id: row.get(1)?,
        client_id: row.get(2)?,
        conversation_id: row.get(3)?,
        sequence: row.get(4)?,
        server_time: row.get(5)?,
        schema_version: row.get(6)?,
        message_type: row.get(7)?,
        payload: row.get::<_, Option<Vec<u8>>>(8)?.map(Blob),
    })
}
fn lookup(conn: &Connection, id: &str) -> Result<Option<Message>, StoreError> {
    conn.query_row(
        &format!("SELECT {COLUMNS} FROM messages WHERE server_id=?"),
        [id],
        message,
    )
    .optional()
    .map_err(db)
}
fn identity(a: &Message, b: &Message) -> bool {
    a.server_id == b.server_id
        && a.sender_id == b.sender_id
        && a.client_id == b.client_id
        && a.conversation_id == b.conversation_id
        && a.sequence == b.sequence
}
fn unsigned(row: &Row<'_>, index: usize) -> rusqlite::Result<u64> {
    let n: i64 = row.get(index)?;
    u64::try_from(n).map_err(|_| rusqlite::Error::IntegralValueOutOfRange(index, n))
}
fn revision(conn: &Connection) -> Result<u64, StoreError> {
    conn.query_row("SELECT revision FROM store_metadata WHERE id=1", [], |r| {
        unsigned(r, 0)
    })
    .map_err(db)
}
fn pending(row: &Row<'_>) -> rusqlite::Result<Pending> {
    for index in 0..3 {
        bounded(row, index, 128)?;
    }
    bounded(row, 3, MAX_VALUE_BYTES)?;
    Ok(Pending {
        sender_id: row.get(0)?,
        client_id: row.get(1)?,
        conversation_id: row.get(2)?,
        payload: Blob(row.get(3)?),
    })
}
pub(crate) fn pending_page(
    conn: &Connection,
    after: Option<&str>,
    limit: usize,
) -> Result<PendingPage, StoreError> {
    let mut statement=conn.prepare("SELECT sender_id,client_id,conversation_id,payload FROM pending_outbox WHERE (sender_id || ':' || client_id)>? ORDER BY (sender_id || ':' || client_id) LIMIT ?").map_err(db)?;
    let rows = statement
        .query_map(params![after.unwrap_or(""), limit as i64 + 1], pending)
        .map_err(db)?;
    let mut items = Vec::new();
    let mut bytes = 0;
    let mut more = false;
    for row in rows {
        let p = row.map_err(db)?;
        if p.payload.0.len() > MAX_VALUE_BYTES
            || p.sender_id.len() > 128
            || p.client_id.len() > 128
            || p.conversation_id.len() > 128
        {
            return Err(StoreError::Corrupt);
        }
        let size =
            p.payload.0.len() + p.sender_id.len() + p.client_id.len() + p.conversation_id.len();
        if items.len() == limit || bytes + size > MAX_BATCH_BYTES {
            more = true;
            break;
        }
        bytes += size;
        items.push(p);
    }
    let next = if more {
        items
            .last()
            .map(|p| format!("{}:{}", p.sender_id, p.client_id))
    } else {
        None
    };
    Ok(PendingPage { items, next })
}
impl SqliteStore {
    pub(crate) fn perform(&mut self, r: &Request) -> Result<Response, StoreError> {
        match &r.action {
            Action::Lookup { server_id } => Ok(Response::Found(lookup(&self.conn, server_id)?)),
            Action::Snapshot { conversation } => {
                let tx = self.conn.transaction().map_err(db)?;
                let (revision, recovery_required) = tx
                    .query_row(
                        "SELECT revision,recovery_required FROM store_metadata WHERE id=1",
                        [],
                        |r| Ok((unsigned(r, 0)?, r.get(1)?)),
                    )
                    .map_err(db)?;
                let cursor = Blob(
                    tx.query_row("SELECT cursor FROM sync_state WHERE id=1", [], |r| r.get(0))
                        .map_err(db)?,
                );
                let conversation = if let Some(id) = conversation {
                    tx.query_row(
                        "SELECT id,unread,summary FROM conversations WHERE id=?",
                        [id],
                        |r| {
                            Ok(Conversation {
                                id: r.get(0)?,
                                unread: unsigned(r, 1)?,
                                summary: Blob(r.get(2)?),
                            })
                        },
                    )
                    .optional()
                    .map_err(db)?
                } else {
                    None
                };
                Ok(Response::Snapshot(Snapshot {
                    revision,
                    cursor,
                    conversation,
                    recovery_required,
                }))
            }
            Action::Messages {
                conversation,
                after,
                limit,
            } => {
                let mut statement=self.conn.prepare(&format!("SELECT {COLUMNS} FROM messages WHERE conversation_id=? AND sequence>? ORDER BY sequence LIMIT ?")).map_err(db)?;
                let rows = statement
                    .query_map(
                        params![conversation, after.unwrap_or(-1), *limit as i64 + 1],
                        message,
                    )
                    .map_err(db)?;
                let mut items = Vec::new();
                let mut bytes = 0;
                let mut more = false;
                for row in rows {
                    let m = row.map_err(db)?;
                    if m.payload
                        .as_ref()
                        .is_some_and(|p| p.0.len() > MAX_VALUE_BYTES)
                    {
                        return Err(StoreError::Corrupt);
                    }
                    let size = m.payload.as_ref().map_or(0, |p| p.0.len())
                        + m.server_id.len()
                        + m.sender_id.len()
                        + m.client_id.len()
                        + m.conversation_id.len()
                        + m.message_type.len()
                        + 32;
                    if items.len() == *limit || bytes + size > MAX_BATCH_BYTES {
                        more = true;
                        break;
                    }
                    bytes += size;
                    items.push(m);
                }
                let next = if more {
                    items.last().map(|m| m.sequence)
                } else {
                    None
                };
                Ok(Response::Messages(MessagePage { items, next }))
            }
            Action::Pending { after, limit } => Ok(Response::Pending(pending_page(
                &self.conn,
                after.as_deref(),
                *limit,
            )?)),
            Action::Metrics => {
                let number = |p: &str| {
                    self.conn
                        .pragma_query_value(None, p, |r| unsigned(r, 0))
                        .map_err(db)
                };
                let (pending_count,recovery_required)=self.conn.query_row("SELECT (SELECT count(*) FROM pending_outbox),recovery_required FROM store_metadata WHERE id=1",[],|r|Ok((unsigned(r,0)?,r.get(1)?))).map_err(db)?;
                let file_size =
                    |name: &str| match std::fs::metadata(self.root.join("active").join(name)) {
                        Ok(m) => Ok(m.len()),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
                        Err(_) => Err(StoreError::Io),
                    };
                Ok(Response::Metrics(Metrics {
                    revision: revision(&self.conn)?,
                    page_count: number("page_count")?,
                    free_pages: number("freelist_count")?,
                    page_size: number("page_size")?,
                    database_bytes: file_size("store.db")?,
                    wal_bytes: file_size("store.db-wal")?,
                    pending_count,
                    recovery_required,
                }))
            }
            Action::Compact { budget_ms } => {
                let handle = self.conn.get_interrupt_handle();
                let (done, wait) = mpsc::channel();
                let worker = std::thread::spawn({
                    let ms = *budget_ms;
                    move || {
                        if wait.recv_timeout(Duration::from_millis(ms)).is_err() {
                            handle.interrupt();
                        }
                    }
                });
                let result = (|| {
                    let busy: i64 = self
                        .conn
                        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
                        .map_err(db)?;
                    if busy != 0 {
                        return Err(StoreError::Busy);
                    }
                    // VACUUM attaches one private transient database; no public SQL is accepted.
                    self.conn
                        .set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_ATTACHED, 1)
                        .map_err(db)?;
                    let vacuum = self.conn.execute_batch("VACUUM").map_err(db);
                    self.conn
                        .set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_ATTACHED, 0)
                        .map_err(db)?;
                    vacuum?;
                    Ok(Response::Compacted)
                })();
                let _ = done.send(());
                worker.join().map_err(|_| StoreError::Io)?;
                result
            }
            _ => self.mutate(r),
        }
    }
    fn mutate(&mut self, r: &Request) -> Result<Response, StoreError> {
        let bytes = request_bytes(r)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db)?;
        let (rev,last,previous,last_rev,last_affected):(u64,u64,Vec<u8>,u64,u32)=tx.query_row("SELECT revision,last_operation,last_request,last_revision,last_affected FROM store_metadata WHERE id=1",[],|r|Ok((unsigned(r,0)?,unsigned(r,1)?,r.get(2)?,unsigned(r,3)?,r.get(4)?))).map_err(db)?;
        if r.operation_id < last {
            return Err(StoreError::OperationExpired);
        }
        if r.operation_id == last {
            return if bytes == previous {
                Ok(Response::Committed(Receipt {
                    revision: last_rev,
                    affected: last_affected,
                    replayed: true,
                }))
            } else {
                Err(StoreError::OperationConflict)
            };
        }
        if rev == i64::MAX as u64 {
            return Err(StoreError::CapacityExceeded);
        }
        let mut affected = 0;
        match &r.action {
            Action::Enqueue(p) => {
                let old=tx.query_row("SELECT sender_id,client_id,conversation_id,payload FROM pending_outbox WHERE sender_id=? AND client_id=?",params![p.sender_id,p.client_id],pending).optional().map_err(db)?;
                if let Some(old) = old {
                    return Ok(Response::ExistingPending(old));
                }
                let persisted: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM messages WHERE sender_id=? AND client_id=?)",
                        params![p.sender_id, p.client_id],
                        |r| r.get(0),
                    )
                    .map_err(db)?;
                if persisted {
                    return Err(StoreError::IdentityConflict);
                }
                let count: i64 = tx
                    .query_row("SELECT count(*) FROM pending_outbox", [], |r| r.get(0))
                    .map_err(db)?;
                if count >= MAX_PENDING as i64 {
                    return Err(StoreError::CapacityExceeded);
                }
                tx.execute(
                    "INSERT INTO pending_outbox VALUES(?,?,?,?)",
                    params![p.sender_id, p.client_id, p.conversation_id, p.payload.0],
                )
                .map_err(db)?;
                affected = 1;
            }
            Action::Apply(batch) => {
                if batch.expected_revision != rev {
                    return Err(StoreError::StaleRevision);
                }
                for write in &batch.messages {
                    let m = match write {
                        MessageWrite::Insert(m) | MessageWrite::PreserveExisting(m) => m,
                    };
                    let mut st=tx.prepare(&format!("SELECT {COLUMNS} FROM messages WHERE server_id=? OR (sender_id=? AND client_id=?) OR (conversation_id=? AND sequence=?)")).map_err(db)?;
                    let existing = st
                        .query_map(
                            params![
                                m.server_id,
                                m.sender_id,
                                m.client_id,
                                m.conversation_id,
                                m.sequence
                            ],
                            message,
                        )
                        .map_err(db)?
                        .collect::<rusqlite::Result<Vec<_>>>()
                        .map_err(db)?;
                    if existing.len() > 1 || existing.first().is_some_and(|old| !identity(old, m)) {
                        return Err(StoreError::IdentityConflict);
                    }
                    match (write, existing.first()) {
                        (MessageWrite::Insert(_), Some(old)) => {
                            return Ok(Response::Existing(old.clone()));
                        }
                        (MessageWrite::PreserveExisting(_), Some(old)) if old == m => {}
                        (MessageWrite::PreserveExisting(_), _) => {
                            return Err(StoreError::StaleRevision);
                        }
                        (MessageWrite::Insert(_), None) => {
                            let count: i64 = tx
                                .query_row("SELECT count(*) FROM messages", [], |r| r.get(0))
                                .map_err(db)?;
                            if count >= self.limits.records as i64 {
                                return Err(StoreError::CapacityExceeded);
                            }
                            tx.execute(
                                "INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?)",
                                params![
                                    m.server_id,
                                    m.sender_id,
                                    m.client_id,
                                    m.conversation_id,
                                    m.sequence,
                                    m.server_time,
                                    m.schema_version,
                                    m.message_type,
                                    m.payload.as_ref().map(|p| &p.0)
                                ],
                            )
                            .map_err(db)?;
                            affected += 1;
                        }
                    }
                }
                for c in &batch.conversations {
                    tx.execute("INSERT INTO conversations VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET unread=excluded.unread,summary=excluded.summary",params![c.id,c.unread as i64,c.summary.0]).map_err(db)?;
                }
                let count: i64 = tx
                    .query_row("SELECT count(*) FROM conversations", [], |r| r.get(0))
                    .map_err(db)?;
                if count > 1000 {
                    return Err(StoreError::CapacityExceeded);
                }
                if let Some(cursor) = &batch.cursor {
                    tx.execute("UPDATE sync_state SET cursor=? WHERE id=1", [&cursor.0])
                        .map_err(db)?;
                }
                for p in &batch.resolve_pending {
                    let matched:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM pending_outbox p JOIN messages m ON p.sender_id=m.sender_id AND p.client_id=m.client_id AND p.conversation_id=m.conversation_id WHERE p.sender_id=? AND p.client_id=? AND m.server_id=?)",params![p.sender_id,p.client_id,p.server_id],|r|r.get(0)).map_err(db)?;
                    if !matched {
                        return Err(StoreError::IdentityConflict);
                    }
                    tx.execute(
                        "DELETE FROM pending_outbox WHERE sender_id=? AND client_id=?",
                        params![p.sender_id, p.client_id],
                    )
                    .map_err(db)?;
                }
            }
            Action::Trim {
                conversation,
                through,
                limit,
            } => {
                affected=tx.execute("UPDATE messages SET payload=NULL WHERE server_id IN (SELECT server_id FROM messages WHERE conversation_id=? AND sequence<=? AND payload IS NOT NULL ORDER BY sequence LIMIT ?)",params![conversation,through,*limit as i64]).map_err(db)? as u32;
            }
            Action::AdvanceGeneration => {
                if self.fence.generation == i64::MAX as u64 {
                    return Err(StoreError::CapacityExceeded);
                }
                tx.execute("UPDATE store_metadata SET generation=generation+1,revision=revision+1,last_operation=0,last_request=X'',last_revision=0,last_affected=0 WHERE id=1",[]).map_err(db)?;
                tx.commit().map_err(|_| StoreError::CommitOutcomeUnknown)?;
                self.fence.generation += 1;
                return Ok(Response::Generation(self.fence.clone()));
            }
            _ => return Err(StoreError::InvalidInput),
        }
        tx.execute("UPDATE store_metadata SET revision=?,last_operation=?,last_request=?,last_revision=?,last_affected=? WHERE id=1",params![(rev+1) as i64,r.operation_id as i64,bytes,(rev+1) as i64,affected]).map_err(db)?;
        tx.commit().map_err(|_| StoreError::CommitOutcomeUnknown)?;
        Ok(Response::Committed(Receipt {
            revision: rev + 1,
            affected,
            replayed: false,
        }))
    }
}
