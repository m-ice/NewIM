use crate::{Result, Status};
use newim_protocol::conversation_sync as wire;
use newim_sdk_core::{conversation_sync as sdk, store::*};

fn cursor(value: Blob) -> Result<String> {
    String::from_utf8(value.0).map_err(|_| Status::StoreUnavailable)
}

pub fn directory_request(flight: &sdk::InFlightPage) -> Result<wire::PageRequest> {
    Ok(match flight.request() {
        sdk::PageRequest::Bootstrap { limit } => wire::PageRequest::Bootstrap { limit },
        sdk::PageRequest::ContinueBootstrap { cursor: value } => {
            wire::PageRequest::ContinueBootstrap {
                cursor: cursor(value)?,
            }
        }
        sdk::PageRequest::Delta { checkpoint, limit } => wire::PageRequest::Delta {
            checkpoint: cursor(checkpoint)?,
            limit,
        },
        sdk::PageRequest::ContinueDelta { cursor: value } => wire::PageRequest::ContinueDelta {
            cursor: cursor(value)?,
        },
    })
}

pub fn directory_page(body: &str) -> Result<sdk::Page> {
    let page = wire::decode_page(body.as_bytes()).map_err(|_| Status::InvalidInput)?;
    let items = page
        .items
        .into_iter()
        .map(|item| ConversationSyncItem {
            conversation_id: item.conversation_id,
            revision: item.revision,
            known_clear_through_seq: 0,
            change: match item.change {
                wire::Change::Remove => ConversationSyncChange::Remove,
                wire::Change::Upsert(summary) => {
                    ConversationSyncChange::Upsert(ConversationSyncSummary {
                        latest_conversation_seq: summary.latest_conversation_seq,
                        latest_server_msg_id: summary.latest_server_msg_id,
                        last_read_seq: summary.last_read_seq,
                        unread: summary.unread.map(|unread| ConversationUnread {
                            count: unread.count,
                            visibility_revision: unread.visibility_revision,
                        }),
                        settings: summary.settings.map(|settings| ConversationSyncSettings {
                            version: settings.version,
                            pinned: settings.pinned,
                            muted: settings.muted,
                            draft: settings.draft,
                            clear_through_seq: settings.clear_through_seq,
                        }),
                    })
                }
            },
        })
        .collect();
    Ok(sdk::Page {
        items,
        next_cursor: Blob(page.next_cursor.into_bytes()),
        has_more: page.has_more,
    })
}

pub fn utf8(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|_| Status::InternalFailure)
}
