use std::collections::BTreeMap;

use serde_json::value::{RawValue, to_raw_value};

use crate::{Error, Object, string};

/// Server-assigned metadata on a persisted result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SenderType {
    Human,
    Bot,
    System,
    AiAgent,
}

impl SenderType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Human => "HUMAN",
            Self::Bot => "BOT",
            Self::System => "SYSTEM",
            Self::AiAgent => "AI_AGENT",
        }
    }
}

pub(super) fn decode(fields: &Object) -> Result<Option<SenderType>, Error> {
    if !fields.contains_key("senderType") {
        return Ok(None);
    }
    let sender_type = match string(fields, "senderType")?.as_str() {
        "HUMAN" => SenderType::Human,
        "BOT" => SenderType::Bot,
        "SYSTEM" => SenderType::System,
        "AI_AGENT" => SenderType::AiAgent,
        _ => return Err(Error::InvalidMessage),
    };
    Ok(Some(sender_type))
}

pub(super) fn insert(
    fields: &mut BTreeMap<&str, Box<RawValue>>,
    sender_type: Option<SenderType>,
) -> Result<(), Error> {
    if let Some(sender_type) = sender_type.filter(|value| *value != SenderType::Human) {
        fields.insert(
            "senderType",
            to_raw_value(sender_type.as_str()).map_err(|_| Error::InvalidMessage)?,
        );
    }
    Ok(())
}
