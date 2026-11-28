//! Store types for the durable account conversation directory.

use super::{Blob, StoreError, validation};
use std::collections::BTreeMap;
use std::fmt;

pub const MAX_SYNC_CURSOR_BYTES: usize = 2048;
pub const MAX_CONVERSATION_DRAFT_BYTES: usize = 4096;
pub const MAX_SYNC_DIRECTORY_ITEMS: usize = 1000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationSyncPhase {
    BootstrapStart,
    BootstrapPage(Blob),
    DeltaCheckpoint(Blob),
    DeltaPage(Blob),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSyncPosition {
    pub round: u64,
    pub phase: ConversationSyncPhase,
}

impl ConversationSyncPosition {
    pub const fn initial() -> Self {
        Self {
            round: 1,
            phase: ConversationSyncPhase::BootstrapStart,
        }
    }

    pub fn begin_bootstrap(&self) -> Result<Self, StoreError> {
        let round = self
            .round
            .checked_add(1)
            .filter(|&round| round <= i64::MAX as u64)
            .ok_or(StoreError::CapacityExceeded)?;
        Ok(Self {
            round,
            phase: ConversationSyncPhase::BootstrapStart,
        })
    }

    pub fn after_page(&self, next: Blob, has_more: bool) -> Self {
        let phase = match (&self.phase, has_more) {
            (_, false) => ConversationSyncPhase::DeltaCheckpoint(next),
            (
                ConversationSyncPhase::BootstrapStart | ConversationSyncPhase::BootstrapPage(_),
                true,
            ) => ConversationSyncPhase::BootstrapPage(next),
            (_, true) => ConversationSyncPhase::DeltaPage(next),
        };
        Self {
            round: self.round,
            phase,
        }
    }

    pub fn is_bootstrap(&self) -> bool {
        matches!(
            self.phase,
            ConversationSyncPhase::BootstrapStart | ConversationSyncPhase::BootstrapPage(_)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationSyncQuery {
    State,
    /// Include tombstones for these IDs in the current synchronization round.
    Merge {
        ids: Vec<String>,
    },
    /// Read the active items in the published directory, in conversation ID order.
    Published {
        after: Option<String>,
        limit: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSyncSnapshot {
    pub revision: u64,
    pub position: ConversationSyncPosition,
    pub published_round: Option<u64>,
    pub items: Vec<ConversationSyncItem>,
    /// Known history boundaries for requested merge keys that have no directory row.
    pub history_floors: BTreeMap<String, u64>,
    pub next: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSyncBatch {
    pub expected_revision: u64,
    pub expected_position: ConversationSyncPosition,
    pub mutation: ConversationSyncMutation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationSyncMutation {
    BeginBootstrap,
    /// Items folded by the coordinator. A newer upsert can follow a removal in this page.
    CommitPage {
        next_cursor: Blob,
        has_more: bool,
        items: Vec<ConversationSyncItem>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSyncItem {
    pub conversation_id: String,
    pub revision: u64,
    /// Local history boundary, independent of settings. Decoded wire items start at zero.
    pub known_clear_through_seq: u64,
    pub change: ConversationSyncChange,
}

impl ConversationSyncItem {
    /// Filter a late message for this directory item using its known personal history.
    pub fn allows_message(&self, sequence: u64) -> bool {
        matches!(&self.change, ConversationSyncChange::Upsert(_)) && sequence > self.history_floor()
    }

    pub(crate) fn history_floor(&self) -> u64 {
        match &self.change {
            ConversationSyncChange::Upsert(summary) => self.known_clear_through_seq.max(
                summary
                    .settings
                    .as_ref()
                    .map_or(0, |settings| settings.clear_through_seq),
            ),
            ConversationSyncChange::Remove => self.known_clear_through_seq,
        }
    }

    pub fn validate(&self) -> Result<(), StoreError> {
        if validation::sync_item(self) {
            Ok(())
        } else {
            Err(StoreError::InvalidInput)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConversationSyncChange {
    Upsert(ConversationSyncSummary),
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversationSyncSummary {
    pub latest_conversation_seq: u64,
    pub latest_server_msg_id: Option<String>,
    pub last_read_seq: Option<u64>,
    pub unread: Option<ConversationUnread>,
    pub settings: Option<ConversationSyncSettings>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConversationUnread {
    pub count: u64,
    pub visibility_revision: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ConversationSyncSettings {
    pub version: u64,
    pub pinned: bool,
    pub muted: bool,
    pub draft: String,
    pub clear_through_seq: u64,
}

impl fmt::Debug for ConversationSyncSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConversationSyncSettings")
            .field("version", &self.version)
            .field("pinned", &self.pinned)
            .field("muted", &self.muted)
            .field("draft_bytes", &self.draft.len())
            .field("clear_through_seq", &self.clear_through_seq)
            .finish()
    }
}
