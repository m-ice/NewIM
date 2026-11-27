//! Transport-independent conversation pagination and revision merge.

use std::collections::{BTreeMap, BTreeSet};

use crate::store::{
    Action, Blob, ConversationSyncBatch, ConversationSyncChange, ConversationSyncItem,
    ConversationSyncMutation, ConversationSyncPhase, ConversationSyncPosition,
    ConversationSyncQuery, ConversationSyncSnapshot, ConversationSyncSummary, Fence,
    MAX_SYNC_CURSOR_BYTES, StoreError,
};

// Preserve headroom for known drafts carried forward from earlier revisions.
pub const PAGE_LIMIT: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PageRequest {
    Bootstrap { limit: usize },
    ContinueBootstrap { cursor: Blob },
    Delta { checkpoint: Blob, limit: usize },
    ContinueDelta { cursor: Blob },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page {
    pub items: Vec<ConversationSyncItem>,
    pub next_cursor: Blob,
    pub has_more: bool,
}

/// Bind a server request to both the authenticated local store and its sync round.
#[derive(Clone, Debug)]
pub struct InFlightPage {
    fence: Fence,
    position: ConversationSyncPosition,
}

impl InFlightPage {
    pub fn new(fence: &Fence, snapshot: &ConversationSyncSnapshot) -> Self {
        Self {
            fence: fence.clone(),
            position: snapshot.position.clone(),
        }
    }

    pub fn request(&self) -> PageRequest {
        match &self.position.phase {
            ConversationSyncPhase::BootstrapStart => PageRequest::Bootstrap { limit: PAGE_LIMIT },
            ConversationSyncPhase::BootstrapPage(cursor) => PageRequest::ContinueBootstrap {
                cursor: cursor.clone(),
            },
            ConversationSyncPhase::DeltaCheckpoint(checkpoint) => PageRequest::Delta {
                checkpoint: checkpoint.clone(),
                limit: PAGE_LIMIT,
            },
            ConversationSyncPhase::DeltaPage(cursor) => PageRequest::ContinueDelta {
                cursor: cursor.clone(),
            },
        }
    }

    /// Request a fresh same-transaction snapshot of the affected directory keys.
    pub fn snapshot_query(&self, page: &Page) -> Result<ConversationSyncQuery, StoreError> {
        Ok(ConversationSyncQuery::Merge {
            ids: self.page_ids(page)?,
        })
    }

    fn page_ids(&self, page: &Page) -> Result<Vec<String>, StoreError> {
        if page.items.len() > PAGE_LIMIT
            || !(1..=MAX_SYNC_CURSOR_BYTES).contains(&page.next_cursor.0.len())
        {
            return Err(StoreError::InvalidInput);
        }
        let input_cursor = match &self.position.phase {
            ConversationSyncPhase::BootstrapStart => None,
            ConversationSyncPhase::BootstrapPage(cursor)
            | ConversationSyncPhase::DeltaCheckpoint(cursor)
            | ConversationSyncPhase::DeltaPage(cursor) => Some(cursor),
        };
        if page.has_more && input_cursor == Some(&page.next_cursor) {
            return Err(StoreError::InvalidInput);
        }
        let mut ids = BTreeSet::new();
        for item in &page.items {
            item.validate()?;
            if self.position.is_bootstrap() && item.change == ConversationSyncChange::Remove {
                return Err(StoreError::InvalidInput);
            }
            ids.insert(item.conversation_id.clone());
        }
        Ok(ids.into_iter().collect())
    }

    /// Recompute against a fresh snapshot after a local CAS conflict.
    ///
    /// The host must discard an old response if its phase, input cursor or fence changed.
    pub fn apply(
        &self,
        current: &Fence,
        snapshot: &ConversationSyncSnapshot,
        page: &Page,
    ) -> Result<Action, StoreError> {
        self.check_current(current, snapshot)?;
        let ids = self.page_ids(page)?;
        let mut previous = BTreeMap::new();
        for item in &snapshot.items {
            if ids.binary_search(&item.conversation_id).is_err()
                || previous
                    .insert(item.conversation_id.as_str(), item)
                    .is_some()
            {
                return Err(StoreError::Corrupt);
            }
            item.validate().map_err(|_| StoreError::Corrupt)?;
        }
        for (id, floor) in &snapshot.history_floors {
            if ids.binary_search(id).is_err()
                || previous.contains_key(id.as_str())
                || !(1..=i64::MAX as u64).contains(floor)
            {
                return Err(StoreError::Corrupt);
            }
        }

        let mut ordered: Vec<_> = page.items.iter().collect();
        ordered.sort_by_key(|item| item.revision);
        let mut folded = BTreeMap::new();
        for incoming in ordered {
            let id = incoming.conversation_id.as_str();
            let current = folded.get(id).or_else(|| previous.get(id).copied());
            let mut item = merge_revision(current, incoming)?;
            if let Some(&floor) = snapshot.history_floors.get(id) {
                item.known_clear_through_seq = item.known_clear_through_seq.max(floor);
            }
            folded.insert(id, item);
        }
        let mut items = Vec::with_capacity(folded.len());
        for (id, item) in folded {
            // An unchanged published row still has to be recorded in the new directory.
            if self.position.is_bootstrap() || previous.get(id).copied() != Some(&item) {
                items.push(item);
            }
        }
        Ok(Action::ApplyConversationSync(ConversationSyncBatch {
            expected_revision: snapshot.revision,
            expected_position: self.position.clone(),
            mutation: ConversationSyncMutation::CommitPage {
                next_cursor: page.next_cursor.clone(),
                has_more: page.has_more,
                items,
            },
        }))
    }

    /// Rebootstrap only for the current request's explicit cursor-expiry response.
    pub fn restart(
        &self,
        current: &Fence,
        snapshot: &ConversationSyncSnapshot,
        status: u16,
        error_code: &str,
    ) -> Result<Action, StoreError> {
        self.check_current(current, snapshot)?;
        if status != 409 || error_code != "SYNC_CURSOR_EXPIRED" {
            return Err(StoreError::InvalidInput);
        }
        self.position.begin_bootstrap()?;
        Ok(Action::ApplyConversationSync(ConversationSyncBatch {
            expected_revision: snapshot.revision,
            expected_position: self.position.clone(),
            mutation: ConversationSyncMutation::BeginBootstrap,
        }))
    }

    fn check_current(
        &self,
        current: &Fence,
        snapshot: &ConversationSyncSnapshot,
    ) -> Result<(), StoreError> {
        if &self.fence != current {
            return Err(StoreError::StaleGeneration);
        }
        if self.position != snapshot.position {
            return Err(StoreError::StaleRevision);
        }
        Ok(())
    }
}

/// Merge one admitted change with the current row. A store may use this to
/// verify the normalized page it receives before writing it.
pub fn merge_revision(
    current: Option<&ConversationSyncItem>,
    incoming: &ConversationSyncItem,
) -> Result<ConversationSyncItem, StoreError> {
    incoming.validate()?;
    let mut item = incoming.clone();
    item.known_clear_through_seq = incoming.history_floor();
    let Some(current) = current else {
        return Ok(item);
    };
    current.validate().map_err(|_| StoreError::Corrupt)?;
    if current.conversation_id != incoming.conversation_id {
        return Err(StoreError::IdentityConflict);
    }
    let floor = item.known_clear_through_seq.max(current.history_floor());
    if item.revision < current.revision {
        item = current.clone();
        item.known_clear_through_seq = floor;
        return Ok(item);
    }
    let same_revision = item.revision == current.revision;
    match (&current.change, &mut item.change) {
        (ConversationSyncChange::Upsert(before), ConversationSyncChange::Upsert(after)) => {
            merge_summary(before, after, same_revision)?;
        }
        (ConversationSyncChange::Remove, ConversationSyncChange::Remove) => {}
        _ if same_revision => return Err(StoreError::IdentityConflict),
        _ => {}
    }
    item.known_clear_through_seq = floor;
    item.validate()?;
    Ok(item)
}

fn merge_summary(
    before: &ConversationSyncSummary,
    after: &mut ConversationSyncSummary,
    same_revision: bool,
) -> Result<(), StoreError> {
    if same_revision
        && (before.latest_conversation_seq != after.latest_conversation_seq
            || before.latest_server_msg_id != after.latest_server_msg_id)
    {
        return Err(StoreError::IdentityConflict);
    }
    retain_known(
        &before.last_read_seq,
        &mut after.last_read_seq,
        same_revision,
    )?;
    retain_known(&before.settings, &mut after.settings, same_revision)?;
    Ok(())
}

fn retain_known<T: PartialEq + Clone>(
    previous: &Option<T>,
    incoming: &mut Option<T>,
    same_revision: bool,
) -> Result<(), StoreError> {
    match (previous, incoming.as_ref()) {
        (Some(before), Some(after)) if same_revision && before != after => {
            return Err(StoreError::IdentityConflict);
        }
        (Some(before), None) => *incoming = Some(before.clone()),
        _ => {}
    }
    Ok(())
}
