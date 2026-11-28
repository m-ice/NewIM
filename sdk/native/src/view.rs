use crate::{Result, Status, client::Client};
use newim_sdk_core::store::*;
use serde_json::json;

fn sequence(value: &str) -> Result<i64> {
    let parsed = value.parse::<i64>().map_err(|_| Status::InvalidInput)?;
    if parsed < 0 || parsed.to_string() != value {
        return Err(Status::InvalidInput);
    }
    Ok(parsed)
}

impl Client {
    pub(crate) fn authorize_local_read(&self) -> Result<()> {
        if self
            .last_context
            .as_ref()
            .is_some_and(|context| self.connection.accepts_ack(context, self.now_ms))
        {
            Ok(())
        } else {
            Err(Status::Busy)
        }
    }

    pub(crate) fn list_messages(
        &mut self,
        conversation: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<()> {
        self.authorize_local_read()?;
        if !self.message_snapshot(conversation, &[])?.visible {
            return Err(Status::InvalidInput);
        }
        let after = after.map(sequence).transpose()?;
        let Response::Messages(page) = self.read(Action::Messages {
            conversation: conversation.to_owned(),
            after,
            limit,
        })?
        else {
            return Err(Status::InternalFailure);
        };
        let mut items = Vec::with_capacity(page.items.len());
        for message in page.items {
            let payload = message
                .payload
                .map(|blob| String::from_utf8(blob.0))
                .transpose()
                .map_err(|_| Status::StoreUnavailable)?;
            items.push(json!({
                "serverMsgId":message.server_id,"clientMsgId":message.client_id,"senderId":message.sender_id,
                "conversationSeq":message.sequence.to_string(),"serverTime":message.server_time.to_string(),
                "messageType":message.message_type,"schemaVersion":message.schema_version,"payload":payload
            }));
        }
        self.emit(json!({"type":"messages","conversationId":conversation,
                        "items":items,"next":page.next.map(|seq| seq.to_string())}))
    }

    pub(crate) fn list_conversations(&mut self, after: Option<String>, limit: usize) -> Result<()> {
        self.authorize_local_read()?;
        let page = self.directory_snapshot(ConversationSyncQuery::Published { after, limit })?;
        let mut items = Vec::with_capacity(page.items.len());
        for item in page.items {
            let ConversationSyncChange::Upsert(summary) = item.change else {
                return Err(Status::InternalFailure);
            };
            let settings = summary.settings.map(|value| {
                json!({
                    "version":value.version.to_string(),"pinned":value.pinned,"muted":value.muted,
                    "draft":value.draft,"clearThroughSeq":value.clear_through_seq.to_string(),
                })
            });
            items.push(json!({
                "conversationId":item.conversation_id,
                "latestSeq":summary.latest_conversation_seq.to_string(),
                "latestServerMsgId":summary.latest_server_msg_id,
                "lastReadSeq":summary.last_read_seq.map(|seq| seq.to_string()),
                "unreadCount":summary.unread.map(|unread| unread.count.to_string()),
                "settings":settings,
            }));
        }
        self.emit(json!({"type":"conversations","items":items,"next":page.next}))
    }
}
