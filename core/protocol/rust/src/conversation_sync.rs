//! Bounded JSON codec for TLS conversation bootstrap and delta.

use std::fmt;

use serde_json::{json, value::RawValue};

use crate::{Error, Object, decimal, field, id, object, scan, string};

pub const BOOTSTRAP_PATH: &str = "/api/v1/sync/bootstrap";
pub const DELTA_PATH: &str = "/api/v1/sync/delta";
pub const MAX_PAGE_ITEMS: usize = 100;
pub const MAX_CURSOR_BYTES: usize = 2048;
pub const MAX_DRAFT_BYTES: usize = 4096;
const MAX_REQUEST_BYTES: usize = 4096;

#[derive(Clone, PartialEq, Eq)]
pub enum PageRequest {
    Bootstrap { limit: usize },
    ContinueBootstrap { cursor: String },
    Delta { checkpoint: String, limit: usize },
    ContinueDelta { cursor: String },
}

impl PageRequest {
    pub fn path(&self) -> &'static str {
        match self {
            Self::Bootstrap { .. } | Self::ContinueBootstrap { .. } => BOOTSTRAP_PATH,
            Self::Delta { .. } | Self::ContinueDelta { .. } => DELTA_PATH,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let body = match self {
            Self::Bootstrap { limit } if valid_limit(*limit) => json!({ "limit": limit }),
            Self::Delta { checkpoint, limit }
                if valid_cursor(checkpoint) && valid_limit(*limit) =>
            {
                json!({ "checkpoint": checkpoint, "limit": limit })
            }
            Self::ContinueBootstrap { cursor } | Self::ContinueDelta { cursor }
                if valid_cursor(cursor) =>
            {
                json!({ "cursor": cursor })
            }
            _ => return Err(Error::InvalidMessage),
        };
        let encoded = serde_json::to_vec(&body).map_err(|_| Error::InvalidJson)?;
        if encoded.len() > MAX_REQUEST_BYTES {
            return Err(Error::MessageTooLarge);
        }
        Ok(encoded)
    }
}

impl fmt::Debug for PageRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Bootstrap { .. } => "Bootstrap",
            Self::ContinueBootstrap { .. } => "ContinueBootstrap",
            Self::Delta { .. } => "Delta",
            Self::ContinueDelta { .. } => "ContinueDelta",
        };
        formatter.write_str(kind)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Page {
    pub items: Vec<Item>,
    pub next_cursor: String,
    pub has_more: bool,
}

impl fmt::Debug for Page {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Page")
            .field("items", &self.items.len())
            .field("has_more", &self.has_more)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item {
    pub conversation_id: String,
    pub revision: u64,
    pub change: Change,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Upsert(Summary),
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    pub latest_conversation_seq: u64,
    pub latest_server_msg_id: Option<String>,
    pub last_read_seq: Option<u64>,
    pub settings: Option<Settings>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct Settings {
    pub version: u64,
    pub pinned: bool,
    pub muted: bool,
    pub draft: String,
    pub clear_through_seq: u64,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("version", &self.version)
            .field("pinned", &self.pinned)
            .field("muted", &self.muted)
            .field("draft_bytes", &self.draft.len())
            .field("clear_through_seq", &self.clear_through_seq)
            .finish()
    }
}

pub fn decode_page(input: &[u8]) -> Result<Page, Error> {
    scan::check(input)?;
    let text = std::str::from_utf8(input).map_err(|_| Error::InvalidJson)?;
    let fields = object(text)?;
    let next_cursor = string(&fields, "nextCursor")?;
    if !valid_cursor(&next_cursor) {
        return Err(Error::InvalidMessage);
    }
    let has_more = boolean(&fields, "hasMore")?;
    let raw_items: Vec<Box<RawValue>> =
        serde_json::from_str(field(&fields, "items")?.get()).map_err(|_| Error::InvalidMessage)?;
    if raw_items.len() > MAX_PAGE_ITEMS {
        return Err(Error::InvalidMessage);
    }
    let items = raw_items
        .iter()
        .map(|raw| decode_item(raw.get()))
        .collect::<Result<_, _>>()?;
    Ok(Page {
        items,
        next_cursor,
        has_more,
    })
}

/// A verified transport may use this to distinguish cursor expiry from other 409 responses.
pub fn decode_error_code(input: &[u8]) -> Result<String, Error> {
    scan::check_with_limits(input, MAX_REQUEST_BYTES, crate::MAX_DEPTH)?;
    let text = std::str::from_utf8(input).map_err(|_| Error::InvalidJson)?;
    let fields = object(text)?;
    let error = object(field(&fields, "error")?.get())?;
    let code = string(&error, "code")?;
    if code.len() > 64
        || !code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        || !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(Error::InvalidMessage);
    }
    Ok(code)
}

fn valid_cursor(cursor: &str) -> bool {
    (1..=MAX_CURSOR_BYTES).contains(&cursor.len())
        && cursor
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn valid_limit(limit: usize) -> bool {
    limit <= MAX_PAGE_ITEMS
}

fn boolean(fields: &Object, name: &str) -> Result<bool, Error> {
    serde_json::from_str(field(fields, name)?.get()).map_err(|_| Error::InvalidMessage)
}

fn sequence(fields: &Object, name: &str) -> Result<u64, Error> {
    let value = string(fields, name)?;
    if !decimal(&value) {
        return Err(Error::InvalidMessage);
    }
    value.parse().map_err(|_| Error::InvalidMessage)
}

fn decode_item(raw: &str) -> Result<Item, Error> {
    let fields = object(raw)?;
    let conversation_id = string(&fields, "conversationId")?;
    let revision = sequence(&fields, "revision")?;
    if !id(&conversation_id) || revision == 0 {
        return Err(Error::InvalidMessage);
    }
    let change = match string(&fields, "kind")?.as_str() {
        "upsert" => Change::Upsert(decode_summary(&fields)?),
        "remove"
            if ![
                "latestConversationSeq",
                "latestServerMsgId",
                "lastReadSeq",
                "settings",
            ]
            .iter()
            .any(|&field| fields.contains_key(field)) =>
        {
            Change::Remove
        }
        _ => return Err(Error::InvalidMessage),
    };
    Ok(Item {
        conversation_id,
        revision,
        change,
    })
}

fn decode_summary(fields: &Object) -> Result<Summary, Error> {
    let latest_conversation_seq = sequence(fields, "latestConversationSeq")?;
    let latest_server_msg_id: Option<String> = fields
        .get("latestServerMsgId")
        .map(|raw| serde_json::from_str(raw.get()).map_err(|_| Error::InvalidMessage))
        .transpose()?
        .flatten();
    if (latest_conversation_seq == 0) != latest_server_msg_id.is_none()
        || latest_server_msg_id.as_deref().is_some_and(|key| !id(key))
    {
        return Err(Error::InvalidMessage);
    }
    let last_read_seq = fields
        .contains_key("lastReadSeq")
        .then(|| sequence(fields, "lastReadSeq"))
        .transpose()?;
    if latest_conversation_seq != 0
        && last_read_seq.is_some_and(|seq| seq > latest_conversation_seq)
    {
        return Err(Error::InvalidMessage);
    }
    let settings = fields
        .get("settings")
        .map(|raw| decode_settings(raw.get()))
        .transpose()?;
    Ok(Summary {
        latest_conversation_seq,
        latest_server_msg_id,
        last_read_seq,
        settings,
    })
}

fn decode_settings(raw: &str) -> Result<Settings, Error> {
    let fields = object(raw)?;
    let draft = string(&fields, "draft")?;
    if draft.len() > MAX_DRAFT_BYTES || boolean(&fields, "deleted")? {
        return Err(Error::InvalidMessage);
    }
    Ok(Settings {
        version: sequence(&fields, "settingsVersion")?,
        pinned: boolean(&fields, "pinned")?,
        muted: boolean(&fields, "muted")?,
        draft,
        clear_through_seq: sequence(&fields, "clearThroughSeq")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn continuation_sends_the_exact_opaque_cursor() {
        let request = PageRequest::ContinueDelta {
            cursor: "f97_a-B".into(),
        };
        assert_eq!(request.path(), DELTA_PATH);
        assert_eq!(request.encode().unwrap(), br#"{"cursor":"f97_a-B"}"#);
        assert_eq!(
            decode_error_code(br#"{"error":{"code":"SYNC_CURSOR_EXPIRED"}}"#),
            Ok("SYNC_CURSOR_EXPIRED".into())
        );
    }

    #[test]
    fn preserves_empty_summary_absolute_zero_and_optional_fields() {
        let page = decode_page(
            br#"{"items":[
            {"conversationId":"old","revision":"1","kind":"upsert","latestConversationSeq":"0"},
            {"conversationId":"new","revision":"2","kind":"upsert","latestConversationSeq":"0",
             "lastReadSeq":"12","settings":{"settingsVersion":"0","pinned":false,"muted":false,
             "draft":"","clearThroughSeq":"12","deleted":false}},
            {"conversationId":"gone","revision":"3","kind":"remove"}],
            "nextCursor":"checkpoint","hasMore":false}"#,
        )
        .unwrap();
        assert_eq!(
            page.items[0].change,
            Change::Upsert(Summary {
                latest_conversation_seq: 0,
                latest_server_msg_id: None,
                last_read_seq: None,
                settings: None,
            })
        );
        assert!(matches!(
            page.items[1].change,
            Change::Upsert(Summary {
                last_read_seq: Some(12),
                settings: Some(Settings { version: 0, .. }),
                ..
            })
        ));
        assert_eq!(page.items[2].change, Change::Remove);
    }

    #[test]
    fn rejects_duplicate_fields_invalid_numbers_and_summary_on_remove() {
        for item in [
            r#"{"conversationId":"c","revision":"1","kind":"upsert","latestConversationSeq":"00"}"#,
            r#"{"conversationId":"c","revision":"1","kind":"remove","lastReadSeq":null}"#,
            r#"{"conversationId":"c","revision":"1","kind":"upsert","latestConversationSeq":"0","lastReadSeq":0}"#,
            r#"{"conversationId":"c","revision":"1","kind":"remove","revision":"2"}"#,
        ] {
            let input =
                format!(r#"{{"items":[{item}],"nextCursor":"checkpoint","hasMore":false}}"#);
            assert!(decode_page(input.as_bytes()).is_err());
        }
    }
}
