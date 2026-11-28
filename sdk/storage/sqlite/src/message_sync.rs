use crate::{
    conversation_sync, db,
    operations::{
        COLUMNS, existing_message, insert_message, message, pending, revision, row_read_error,
        unsigned, validate_message_read,
    },
};
use newim_sdk_core::{
    outbox::{BASE_RETRY_DELAY_MS, MAX_ATTEMPTS, MAX_RETRY_AGE_MS, MAX_RETRY_DELAY_MS},
    store::*,
};
use rusqlite::{OptionalExtension, Row, Transaction, params};
use std::collections::BTreeSet;

const ATTEMPT_MS: u64 = 30_000;
const CLEAR_JOB: &str = "phase=0,started_at_ms=NULL,failures=NULL,page_limit=NULL,wake_at_ms=NULL,
attempt_id=NULL,generation=NULL,attempt_after_seq=NULL,expires_at_ms=NULL";

fn state_row(row: &Row<'_>) -> rusqlite::Result<MessageSyncState> {
    let phase: i64 = row.get(3)?;
    let job = if phase == 0 {
        None
    } else {
        let limit = usize::from(row.get::<_, u8>(6)?);
        let status = match phase {
            1 => GapStatus::Waiting {
                until_ms: unsigned(row, 7)?,
            },
            2 => GapStatus::InFlight(GapAttempt {
                operation_id: unsigned(row, 8)?,
                generation: unsigned(row, 9)?,
                after_seq: row.get(10)?,
                limit,
                expires_at_ms: unsigned(row, 11)?,
            }),
            3 => GapStatus::AuthRecovery,
            4 => GapStatus::Stopped,
            _ => return Err(rusqlite::Error::InvalidQuery),
        };
        Some(GapJob {
            started_at_ms: unsigned(row, 4)?,
            failures: row.get(5)?,
            limit,
            status,
        })
    };
    Ok(MessageSyncState {
        continuous_seq: row.get(0)?,
        history_after_seq: row.get(1)?,
        highest_observed_seq: row.get(2)?,
        job,
    })
}

struct PublishedItem {
    round: u64,
    latest_seq: i64,
}

fn state(
    tx: &Transaction<'_>,
    id: &str,
) -> Result<(MessageSyncState, Option<PublishedItem>), StoreError> {
    let mut state = tx
        .query_row(
            "SELECT continuous_seq,history_after_seq,highest_observed_seq,phase,started_at_ms,
             failures,page_limit,wake_at_ms,attempt_id,generation,attempt_after_seq,expires_at_ms
             FROM message_sync WHERE conversation_id=?",
            [id],
            state_row,
        )
        .optional()
        .map_err(row_read_error)?
        .unwrap_or_default();
    let published = tx
        .query_row(
            "SELECT round,latest_seq FROM conversation_sync_items
             WHERE round=(SELECT published_round FROM conversation_sync_state WHERE id=1)
               AND conversation_id=? AND latest_seq IS NOT NULL",
            [id],
            |row| {
                Ok(PublishedItem {
                    round: unsigned(row, 0)?,
                    latest_seq: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(row_read_error)?;
    let prior_after = state.after_seq();
    state.history_after_seq = state
        .history_after_seq
        .max(conversation_sync::published_history_floor(tx, id)?.unwrap_or(0));
    if state.after_seq() > prior_after
        && let Some(job) = &mut state.job
    {
        job.failures = 0;
        if let GapStatus::Waiting { until_ms } = &mut job.status {
            *until_ms = job.started_at_ms;
        }
    }
    if published.is_some() {
        observe_cached_high(tx, id, &mut state)?;
    }
    Ok((state, published))
}

fn observe_cached_high(
    tx: &Transaction<'_>,
    id: &str,
    state: &mut MessageSyncState,
) -> Result<(), StoreError> {
    let highest: Option<i64> = tx
        .query_row(
            "SELECT max(sequence) FROM messages WHERE conversation_id=?",
            [id],
            |row| row.get(0),
        )
        .map_err(row_read_error)?;
    state.highest_observed_seq = state.highest_observed_seq.max(highest.unwrap_or(0));
    Ok(())
}

// Cached trimmed rows still prove positional continuity.
fn advance_cached(
    tx: &Transaction<'_>,
    id: &str,
    state: &mut MessageSyncState,
) -> Result<(), StoreError> {
    let mut after = state.after_seq();
    if state.highest_observed_seq <= after {
        return Ok(());
    }
    let mut stmt = tx
        .prepare("SELECT sequence FROM messages WHERE conversation_id=? AND sequence>? ORDER BY sequence LIMIT ?")
        .map_err(db)?;
    let mut rows = stmt
        .query(params![id, after, MAX_ITEMS as i64])
        .map_err(db)?;
    while let Some(row) = rows.next().map_err(db)? {
        let next: i64 = row.get(0).map_err(row_read_error)?;
        if after.checked_add(1) != Some(next) {
            break;
        }
        after = next;
        state.continuous_seq = next;
    }
    Ok(())
}

pub(crate) fn snapshot(
    tx: &Transaction<'_>,
    query: &MessageSyncQuery,
) -> Result<MessageSyncSnapshot, StoreError> {
    let (state, published) = state(tx, &query.conversation_id)?;
    let visible = published.is_some();
    let mut result = MessageSyncSnapshot {
        conversation_id: query.conversation_id.clone(),
        revision: revision(tx)?,
        visible,
        state,
        messages: Vec::new(),
        pending: Vec::new(),
    };
    if !visible || query.lookups.is_empty() {
        return Ok(result);
    }
    let mut seen_messages = BTreeSet::new();
    let mut seen_pending = BTreeSet::new();
    let mut messages = tx
        .prepare(&format!(
            "SELECT {COLUMNS} FROM messages WHERE server_id=? OR (sender_id=? AND client_id=?)"
        ))
        .map_err(db)?;
    let mut pending_items = tx.prepare(
        "SELECT sender_id,client_id,conversation_id,payload FROM pending_outbox WHERE sender_id=? AND client_id=?",
    ).map_err(db)?;
    let mut bytes = 0;
    for key in &query.lookups {
        let rows = messages
            .query_map(
                params![key.server_id, key.sender_id, key.client_id],
                message,
            )
            .map_err(db)?;
        for row in rows {
            let item = row.map_err(row_read_error)?;
            validate_message_read(&item)?;
            if seen_messages.insert(item.server_id.clone()) {
                bytes += item.server_id.len()
                    + item.sender_id.len()
                    + item.client_id.len()
                    + item.conversation_id.len()
                    + item.message_type.len()
                    + item.payload.as_ref().map_or(0, |payload| payload.0.len());
                result.messages.push(item);
            }
        }
        if seen_pending.insert((&key.sender_id, &key.client_id))
            && let Some(item) = pending_items
                .query_row(params![key.sender_id, key.client_id], pending)
                .optional()
                .map_err(row_read_error)?
        {
            bytes += item.sender_id.len()
                + item.client_id.len()
                + item.conversation_id.len()
                + item.payload.0.len();
            result.pending.push(item);
        }
        if bytes > MAX_BATCH_BYTES {
            return Err(StoreError::CapacityExceeded);
        }
    }
    Ok(result)
}

pub(crate) fn history_floor(tx: &Transaction<'_>, id: &str) -> Result<Option<i64>, StoreError> {
    tx.query_row(
        "SELECT history_after_seq FROM message_sync WHERE conversation_id=? AND history_after_seq>0",
        [id],
        |row| row.get(0),
    )
    .optional()
    .map_err(row_read_error)
}

pub(crate) fn apply(
    tx: &Transaction<'_>,
    request: &Request,
    batch: &MessageSyncBatch,
    max_records: usize,
) -> Result<u32, StoreError> {
    let id = &batch.conversation_id;
    let now = batch.now_ms;
    let generation = request.fence.generation;
    let (mut state, published) = state(tx, id)?;
    let Some(published) = published else {
        return Err(StoreError::StaleRevision);
    };
    let mut affected = 0;
    match &batch.mutation {
        MessageSyncMutation::Observe(write) => {
            let item = message_from_write(write);
            if item.sequence > state.history_after_seq {
                affected += write_message(tx, write, max_records)?;
                state.highest_observed_seq = state.highest_observed_seq.max(item.sequence);
            }
            let before = state.after_seq();
            advance_cached(tx, id, &mut state)?;
            observe_position(&mut state, before, now);
        }
        MessageSyncMutation::ObserveTarget {
            published_round,
            through_seq,
        } => {
            if *published_round != published.round || *through_seq > published.latest_seq {
                return Err(StoreError::StaleRevision);
            }
            state.highest_observed_seq = state.highest_observed_seq.max(*through_seq);
            let after = state.after_seq();
            observe_position(&mut state, after, now);
        }
        MessageSyncMutation::StartAttempt => {
            let before = state.after_seq();
            advance_cached(tx, id, &mut state)?;
            observe_position(&mut state, before, now);
            let after = state.after_seq();
            if let Some(job) = &mut state.job {
                start_attempt(job, now, generation, request.operation_id, after)?;
            }
        }
        MessageSyncMutation::CommitPage { attempt, page } => {
            let job = current_job(&mut state, attempt, generation)?;
            if now >= attempt.expires_at_ms || now < job.started_at_ms {
                return Err(StoreError::StaleRevision);
            }
            page.validate(id, attempt)?;
            let before = state.after_seq();
            state.history_after_seq = state.history_after_seq.max(page.history_after_seq);
            for write in &page.messages {
                if message_from_write(write).sequence > state.history_after_seq {
                    affected += write_message(tx, write, max_records)?;
                }
            }
            if let Some(last) = page
                .next_after_seq
                .filter(|seq| *seq > state.history_after_seq)
            {
                state.continuous_seq = state.continuous_seq.max(last);
            }
            state.highest_observed_seq = state.highest_observed_seq.max(page.latest_seq);
            observe_cached_high(tx, id, &mut state)?;
            advance_cached(tx, id, &mut state)?;
            let progressed = state.after_seq() > before || state.after_seq() > attempt.after_seq;
            if state.after_seq() >= state.highest_observed_seq {
                state.job = None;
            } else if let Some(job) = &mut state.job {
                if progressed {
                    job.failures = 0;
                    job.status = GapStatus::Waiting { until_ms: now };
                } else {
                    fail(job, GapFailure::Temporary, now);
                }
            }
        }
        MessageSyncMutation::FailAttempt { attempt, failure } => {
            advance_cached(tx, id, &mut state)?;
            let after = state.after_seq();
            let complete = after >= state.highest_observed_seq;
            let job = current_job(&mut state, attempt, generation)?;
            if complete {
                state.job = None;
            } else {
                let failure = if now >= attempt.expires_at_ms {
                    GapFailure::Temporary
                } else {
                    *failure
                };
                fail_at_position(job, failure, now, after > attempt.after_seq);
            }
        }
        MessageSyncMutation::Resume => {
            let before = state.after_seq();
            advance_cached(tx, id, &mut state)?;
            observe_position(&mut state, before, now);
            if let Some(job) = &mut state.job {
                match job.status {
                    GapStatus::Stopped => *job = initial_job(now),
                    GapStatus::AuthRecovery => resume_job(job, now),
                    GapStatus::InFlight(ref attempt) if attempt.generation != generation => {
                        resume_job(job, now);
                    }
                    _ => {}
                }
            }
        }
    }
    save(tx, id, &state)?;
    Ok(affected)
}

fn message_from_write(write: &MessageWrite) -> &Message {
    match write {
        MessageWrite::Insert(message) | MessageWrite::PreserveExisting(message) => message,
    }
}

fn write_message(
    tx: &Transaction<'_>,
    write: &MessageWrite,
    max_records: usize,
) -> Result<u32, StoreError> {
    let incoming = message_from_write(write);
    let existing = existing_message(tx, incoming)?;
    match (write, existing) {
        (MessageWrite::Insert(_), None) => {
            insert_message(tx, incoming, max_records)?;
            Ok(1)
        }
        (MessageWrite::Insert(_), Some(old))
            if old == *incoming
                || (old.payload.is_none()
                    && old.server_time == incoming.server_time
                    && old.schema_version == incoming.schema_version
                    && old.message_type == incoming.message_type) =>
        {
            Ok(0)
        }
        (MessageWrite::Insert(_), Some(_)) => Err(StoreError::IdentityConflict),
        (MessageWrite::PreserveExisting(_), Some(old)) if old == *incoming => Ok(0),
        (MessageWrite::PreserveExisting(_), _) => Err(StoreError::StaleRevision),
    }
}

fn initial_job(now: u64) -> GapJob {
    GapJob {
        started_at_ms: now,
        failures: 0,
        limit: MAX_ITEMS,
        status: GapStatus::Waiting { until_ms: now },
    }
}

fn observe_position(state: &mut MessageSyncState, before: i64, now: u64) {
    let after = state.after_seq();
    if after >= state.highest_observed_seq {
        state.job = None;
        return;
    }
    let job = state.job.get_or_insert_with(|| initial_job(now));
    if after > before {
        if let GapStatus::Waiting { until_ms } = &mut job.status {
            *until_ms = now;
        }
        job.failures = 0;
    }
}

fn deadline(now: u64, delay: u64) -> u64 {
    now.saturating_add(delay).min(i64::MAX as u64)
}

fn start_attempt(
    job: &mut GapJob,
    now: u64,
    generation: u64,
    operation_id: u64,
    after_seq: i64,
) -> Result<(), StoreError> {
    let horizon = deadline(job.started_at_ms, MAX_RETRY_AGE_MS);
    if now >= horizon || now < job.started_at_ms {
        job.status = GapStatus::Stopped;
        return Ok(());
    }
    match &job.status {
        GapStatus::InFlight(attempt) if attempt.generation != generation => {
            job.status = GapStatus::AuthRecovery;
        }
        GapStatus::InFlight(attempt) if now >= attempt.expires_at_ms => {
            fail_at_position(
                job,
                GapFailure::Temporary,
                now,
                after_seq > attempt.after_seq,
            );
        }
        GapStatus::Waiting { until_ms } if now >= *until_ms => {
            job.status = GapStatus::InFlight(GapAttempt {
                operation_id,
                generation,
                after_seq,
                limit: job.limit,
                expires_at_ms: deadline(now, ATTEMPT_MS).min(horizon),
            });
        }
        _ => return Err(StoreError::StaleRevision),
    }
    Ok(())
}

fn current_job<'a>(
    state: &'a mut MessageSyncState,
    attempt: &GapAttempt,
    generation: u64,
) -> Result<&'a mut GapJob, StoreError> {
    if attempt.generation != generation {
        return Err(StoreError::StaleGeneration);
    }
    state
        .job
        .as_mut()
        .filter(|job| matches!(&job.status, GapStatus::InFlight(current) if current == attempt))
        .ok_or(StoreError::StaleRevision)
}

fn resume_job(job: &mut GapJob, now: u64) {
    job.status = if now < job.started_at_ms || now >= deadline(job.started_at_ms, MAX_RETRY_AGE_MS)
    {
        GapStatus::Stopped
    } else {
        GapStatus::Waiting { until_ms: now }
    };
}

fn fail_at_position(job: &mut GapJob, failure: GapFailure, now: u64, progressed: bool) {
    if progressed {
        job.failures = 0;
    }
    if progressed && failure != GapFailure::Authentication {
        resume_job(job, now);
    } else {
        fail(job, failure, now);
    }
}

fn fail(job: &mut GapJob, failure: GapFailure, now: u64) {
    let horizon = deadline(job.started_at_ms, MAX_RETRY_AGE_MS);
    if now >= horizon || now < job.started_at_ms {
        job.status = GapStatus::Stopped;
        return;
    }
    job.status = match failure {
        GapFailure::InvalidCursor => GapStatus::Stopped,
        GapFailure::Authentication => GapStatus::AuthRecovery,
        GapFailure::PageCapacity if job.limit > 1 => {
            job.limit = (job.limit / 2).max(1);
            GapStatus::Waiting { until_ms: now }
        }
        GapFailure::PageCapacity => GapStatus::Stopped,
        GapFailure::Temporary => {
            job.failures = job.failures.saturating_add(1);
            if job.failures >= MAX_ATTEMPTS {
                GapStatus::Stopped
            } else {
                let delay = (BASE_RETRY_DELAY_MS << (job.failures - 1)).min(MAX_RETRY_DELAY_MS);
                GapStatus::Waiting {
                    until_ms: deadline(now, delay).min(horizon),
                }
            }
        }
    };
}

fn save(tx: &Transaction<'_>, id: &str, state: &MessageSyncState) -> Result<(), StoreError> {
    let job = state.job.as_ref();
    let mut wake = None;
    let mut attempt = None;
    let phase = match job.map(|job| &job.status) {
        None => 0,
        Some(GapStatus::Waiting { until_ms }) => {
            wake = Some(*until_ms as i64);
            1
        }
        Some(GapStatus::InFlight(in_flight)) => {
            attempt = Some(in_flight);
            2
        }
        Some(GapStatus::AuthRecovery) => 3,
        Some(GapStatus::Stopped) => 4,
    };
    tx.execute(
        "INSERT INTO message_sync VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
         ON CONFLICT(conversation_id) DO UPDATE SET continuous_seq=excluded.continuous_seq,
         history_after_seq=excluded.history_after_seq,highest_observed_seq=excluded.highest_observed_seq,
         phase=excluded.phase,started_at_ms=excluded.started_at_ms,failures=excluded.failures,
         page_limit=excluded.page_limit,wake_at_ms=excluded.wake_at_ms,attempt_id=excluded.attempt_id,
         generation=excluded.generation,attempt_after_seq=excluded.attempt_after_seq,expires_at_ms=excluded.expires_at_ms",
        params![id, state.continuous_seq, state.history_after_seq, state.highest_observed_seq,
            phase, job.map(|job| job.started_at_ms as i64), job.map(|job| job.failures),
            job.map(|job| job.limit as i64), wake, attempt.map(|attempt| attempt.operation_id as i64),
            attempt.map(|attempt| attempt.generation as i64), attempt.map(|attempt| attempt.after_seq),
            attempt.map(|attempt| attempt.expires_at_ms as i64)],
    ).map_err(db)?;
    Ok(())
}

pub(crate) fn invalidate<'a>(
    tx: &Transaction<'_>,
    ids: impl IntoIterator<Item = &'a str>,
) -> Result<(), StoreError> {
    let mut clear = tx
        .prepare(&format!(
            "UPDATE message_sync SET {CLEAR_JOB} WHERE conversation_id=? AND phase<>0"
        ))
        .map_err(db)?;
    for id in ids {
        clear.execute([id]).map_err(db)?;
    }
    Ok(())
}

pub(crate) fn directory_replaced(
    tx: &Transaction<'_>,
    previous: u64,
    replacement: u64,
) -> Result<(), StoreError> {
    tx.execute(
        &format!(
            "UPDATE message_sync SET {CLEAR_JOB} WHERE phase<>0 AND conversation_id IN (
           SELECT previous.conversation_id FROM conversation_sync_items previous
           WHERE previous.round=?1 AND previous.latest_seq IS NOT NULL AND NOT EXISTS (
             SELECT 1 FROM conversation_sync_items replacement
             WHERE replacement.round=?2 AND replacement.conversation_id=previous.conversation_id
               AND replacement.latest_seq IS NOT NULL
           )
         )"
        ),
        [previous as i64, replacement as i64],
    )
    .map_err(db)?;
    // A rebootstrap may contain the same ID after an unseen membership change.
    // 重新引导可能跨过未看到的成员变更，即使同一会话仍在目录。
    tx.execute(
        "UPDATE message_sync SET phase=1,wake_at_ms=started_at_ms,attempt_id=NULL,generation=NULL,
          attempt_after_seq=NULL,expires_at_ms=NULL
         WHERE phase=2 AND conversation_id IN (
           SELECT conversation_id FROM conversation_sync_items WHERE round=? AND latest_seq IS NOT NULL
         )",
        [replacement as i64],
    )
    .map_err(db)?;
    Ok(())
}
