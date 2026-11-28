//! Visibility metadata in persisted message history.

use serde_json::{json, value::RawValue};

use crate::{Error, Object, decimal, field, id, object, scan, string};

pub const HIDDEN_MESSAGE_TYPE: &str = "message_hidden";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Recall,
    DeleteForMe,
    AdminRemove,
}

impl Action {
    fn decode(value: &str) -> Result<Self, Error> {
        match value {
            "recall" => Ok(Self::Recall),
            "deleteForMe" => Ok(Self::DeleteForMe),
            "adminRemove" => Ok(Self::AdminRemove),
            _ => Err(Error::InvalidMessage),
        }
    }
}

/// Decode a body-free placeholder. A visible message can never be supplied in
/// this payload, even as an extra field.
pub fn decode_hidden_payload(payload: &str) -> Result<Action, Error> {
    let fields = object(payload)?;
    if fields.len() != 2 || field(&fields, "countAsUnread")?.get() != "false" {
        return Err(Error::InvalidMessage);
    }
    Action::decode(&string(&fields, "action")?)
}

pub const VISIBILITY_PATH: &str = "/api/v1/sync/message-visibility";
const MAX_PAGE_ITEMS: usize = 100;

#[derive(Clone, PartialEq, Eq)]
pub struct PageRequest {
    pub conversation_id: String,
    pub after_revision: i64,
    pub through_revision: i64,
    pub limit: usize,
}

impl PageRequest {
    fn page_limit(&self) -> Result<usize, Error> {
        if !id(&self.conversation_id)
            || self.after_revision < 0
            || self.through_revision < self.after_revision
            || self.limit > MAX_PAGE_ITEMS
        {
            return Err(Error::InvalidMessage);
        }
        Ok(if self.limit == 0 {
            MAX_PAGE_ITEMS
        } else {
            self.limit
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.page_limit()?;
        let mut body = json!({
            "conversationId": self.conversation_id,
            "afterRevision": self.after_revision.to_string(),
            "throughRevision": self.through_revision.to_string(),
        });
        if self.limit != 0 {
            body["limit"] = json!(self.limit);
        }
        serde_json::to_vec(&body).map_err(|_| Error::InvalidJson)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Event {
    pub server_msg_id: String,
    pub conversation_seq: i64,
    pub revision: i64,
    pub action: Action,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Page {
    pub items: Vec<Event>,
    pub next_after_revision: i64,
    pub has_next: bool,
}

fn sequence(fields: &Object, name: &str) -> Result<i64, Error> {
    let raw = string(fields, name)?;
    if !decimal(&raw) {
        return Err(Error::InvalidMessage);
    }
    raw.parse().map_err(|_| Error::InvalidMessage)
}

pub fn decode_page(request: &PageRequest, body: &[u8]) -> Result<Page, Error> {
    let limit = request.page_limit()?;
    scan::check(body)?;
    let fields = object(std::str::from_utf8(body).map_err(|_| Error::InvalidJson)?)?;
    if fields.len() != 3 {
        return Err(Error::InvalidMessage);
    }
    let has_next: bool = serde_json::from_str(field(&fields, "hasNext")?.get())
        .map_err(|_| Error::InvalidMessage)?;
    let next_after_revision = sequence(&fields, "nextAfterRevision")?;
    let entries: Vec<Box<RawValue>> =
        serde_json::from_str(field(&fields, "items")?.get()).map_err(|_| Error::InvalidMessage)?;
    if entries.len() > limit {
        return Err(Error::InvalidMessage);
    }
    let mut items = Vec::with_capacity(entries.len());
    let mut previous = request.after_revision;
    for raw in entries {
        let entry = object(raw.get())?;
        if entry.len() != 6
            || string(&entry, "conversationId")? != request.conversation_id
            || field(&entry, "countAsUnread")?.get() != "false"
        {
            return Err(Error::InvalidMessage);
        }
        let server_msg_id = string(&entry, "serverMsgId")?;
        let conversation_seq = sequence(&entry, "conversationSeq")?;
        let revision = sequence(&entry, "visibilityRevision")?;
        let action = Action::decode(&string(&entry, "action")?)?;
        if !id(&server_msg_id)
            || conversation_seq == 0
            || revision <= previous
            || revision > request.through_revision
        {
            return Err(Error::InvalidMessage);
        }
        items.push(Event {
            server_msg_id,
            conversation_seq,
            revision,
            action,
        });
        previous = revision;
    }
    if if has_next {
        items.len() != limit
            || next_after_revision != previous
            || previous >= request.through_revision
    } else {
        next_after_revision != request.through_revision
    } {
        return Err(Error::InvalidMessage);
    }
    Ok(Page {
        items,
        next_after_revision,
        has_next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_pages_allow_filtered_revisions_and_require_progress() {
        let mut request = PageRequest {
            conversation_id: "room".into(),
            after_revision: 0,
            through_revision: 8,
            limit: 1,
        };
        let first = br#"{"items":[{"conversationId":"room","serverMsgId":"message","conversationSeq":"2","visibilityRevision":"6","action":"recall","countAsUnread":false}],"nextAfterRevision":"6","hasNext":true}"#;
        let page = decode_page(&request, first).unwrap();
        assert_eq!(page.next_after_revision, 6);
        assert_eq!(page.items[0].action, Action::Recall);
        request.after_revision = 6;
        assert!(decode_page(&request, first).is_err());
        let terminal = br#"{"items":[],"nextAfterRevision":"8","hasNext":false}"#;
        assert!(decode_page(&request, terminal).unwrap().items.is_empty());
        let invalid = br#"{"items":[],"nextAfterRevision":"7","hasNext":false}"#;
        assert!(decode_page(&request, invalid).is_err());
    }

    #[test]
    fn hidden_payload_is_body_free() {
        assert_eq!(
            decode_hidden_payload(r#"{"action":"recall","countAsUnread":false}"#),
            Ok(Action::Recall)
        );
        for bad in [
            r#"{"action":"recall","countAsUnread":true}"#,
            r#"{"action":"deleteForMe"}"#,
            r#"{"action":"adminRemove","countAsUnread":false,"text":"old body"}"#,
            r#"{"action":"clear","countAsUnread":false}"#,
        ] {
            assert_eq!(decode_hidden_payload(bad), Err(Error::InvalidMessage));
        }
    }
}
