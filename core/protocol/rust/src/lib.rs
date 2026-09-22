//! 有界持久消息 JSON 编解码 / Bounded persisted-message JSON codec.
//!
//! Parsing validates a wire contract; it never confirms server persistence or
//! authorizes a send. Unknown payload schemas remain opaque and unsupported.

mod intent;
pub mod media;
mod scan;
pub mod send;

use std::collections::BTreeMap;
use std::fmt;

use serde_json::value::RawValue;

/// 最大编码字节数（含边界）/ Inclusive encoded-message byte limit.
pub const MAX_WIRE_BYTES: usize = 65_536;
/// 容器最大嵌套深度（含外层对象）/ Maximum depth including the envelope.
pub const MAX_DEPTH: usize = 32;
/// 文本最大 UTF-8 字节数 / Maximum UTF-8 bytes in a text payload.
pub const MAX_TEXT_BYTES: usize = 16_384;

/// 稳定且不泄露输入内容的错误 / Stable errors without user input in diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidJson,
    InvalidMessage,
    UnsupportedVersion,
    MessageTooLarge,
    NestingExceeded,
}

impl Error {
    /// 返回稳定协议错误码 / Return the stable protocol error code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidJson => "PROTOCOL_INVALID_JSON",
            Self::InvalidMessage => "PROTOCOL_INVALID_MESSAGE",
            Self::UnsupportedVersion => "PROTOCOL_UNSUPPORTED_VERSION",
            Self::MessageTooLarge => "MESSAGE_TOO_LARGE",
            Self::NestingExceeded => "PROTOCOL_NESTING_EXCEEDED",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for Error {}

/// 已解码的消息；编码时重新校验 / Decoded message, revalidated on encoding.
///
/// Payload stays raw so future schemas never round numbers or reinterpret
/// parser-internal-looking object keys. Public fields do not imply validity.
#[derive(Clone, Debug)]
pub struct Message {
    pub protocol_version: u32,
    pub version: u32,
    pub client_msg_id: String,
    pub server_msg_id: String,
    pub conversation_id: String,
    pub conversation_seq: String,
    pub sender_id: String,
    pub message_type: String,
    pub server_time: String,
    pub payload: Box<RawValue>,
}

impl Message {
    /// 是否识别此消息 schema（非发送授权）/ Whether this schema is understood.
    pub fn supported(&self) -> bool {
        self.protocol_version == 1
            && self.version == 1
            && matches!(self.message_type.as_str(), "text" | "media")
    }
}

type Object = BTreeMap<String, Box<RawValue>>;

fn object(raw: &str) -> Result<Object, Error> {
    if !raw.trim_start().starts_with('{') {
        return Err(Error::InvalidMessage);
    }
    serde_json::from_str(raw).map_err(|_| Error::InvalidMessage)
}

fn field<'a>(fields: &'a Object, name: &str) -> Result<&'a RawValue, Error> {
    fields
        .get(name)
        .map(Box::as_ref)
        .ok_or(Error::InvalidMessage)
}

fn string(fields: &Object, name: &str) -> Result<String, Error> {
    serde_json::from_str(field(fields, name)?.get()).map_err(|_| Error::InvalidMessage)
}

fn version(fields: &Object, name: &str) -> Result<u32, Error> {
    let token = field(fields, name)?.get();
    if token.is_empty() || !token.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidMessage);
    }
    let value: u32 = token.parse().map_err(|_| Error::InvalidMessage)?;
    if value == 0 || value > i32::MAX as u32 {
        return Err(Error::InvalidMessage);
    }
    Ok(value)
}

fn id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn decimal(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 19
        && (value.len() == 1 || !value.starts_with('0'))
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value.parse::<i64>().is_ok()
}

fn message_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// 严格解析且保留未知 payload / Strictly decode while retaining opaque payloads.
///
/// Error precedence follows ADR 0003: bytes, JSON/depth, full envelope shape,
/// supported protocol version, then the known payload schema.
pub fn decode(input: &[u8]) -> Result<Message, Error> {
    scan::check(input)?;
    let text = std::str::from_utf8(input).map_err(|_| Error::InvalidJson)?;
    let message = decode_shape(text)?;
    validate_semantics(&message)?;
    Ok(message)
}

fn decode_shape(text: &str) -> Result<Message, Error> {
    let mut fields = object(text)?;
    let protocol_version = version(&fields, "protocolVersion")?;
    let version = version(&fields, "version")?;
    let client_msg_id = string(&fields, "clientMsgId")?;
    let server_msg_id = string(&fields, "serverMsgId")?;
    let conversation_id = string(&fields, "conversationId")?;
    let conversation_seq = string(&fields, "conversationSeq")?;
    let sender_id = string(&fields, "senderId")?;
    let message_type = string(&fields, "type")?;
    let server_time = string(&fields, "serverTime")?;
    let payload = fields.remove("payload").ok_or(Error::InvalidMessage)?;
    object(payload.get())?;
    let message = Message {
        protocol_version,
        version,
        client_msg_id,
        server_msg_id,
        conversation_id,
        conversation_seq,
        sender_id,
        message_type,
        server_time,
        payload,
    };
    if !id(&message.client_msg_id)
        || !id(&message.server_msg_id)
        || !id(&message.conversation_id)
        || !id(&message.sender_id)
        || !decimal(&message.conversation_seq)
        || !decimal(&message.server_time)
        || !self::message_type(&message.message_type)
    {
        return Err(Error::InvalidMessage);
    }
    Ok(message)
}

fn validate_semantics(message: &Message) -> Result<(), Error> {
    if message.protocol_version != 1 {
        return Err(Error::UnsupportedVersion);
    }
    if message.version != 1 {
        return Ok(());
    }
    match message.message_type.as_str() {
        "text" => {
            let text = string(&object(message.payload.get())?, "text")?;
            if text.is_empty() || text.len() > MAX_TEXT_BYTES {
                return Err(Error::InvalidMessage);
            }
        }
        "media" => media::validate(message.payload.get())?,
        _ => {}
    }
    Ok(())
}

/// 校验实际可编码消息 / Validate the complete encoded representation.
pub fn validate(message: &Message) -> Result<(), Error> {
    encode(message).map(|_| ())
}

/// 校验并编码，不舍入未知数值 / Validate and encode without rounding opaque numbers.
pub fn encode(message: &Message) -> Result<Vec<u8>, Error> {
    // Reject obviously oversized public values before cloning/escaping them.
    // JSON output cannot be shorter than these unescaped field bytes.
    let minimum_bytes = [
        message.client_msg_id.len(),
        message.server_msg_id.len(),
        message.conversation_id.len(),
        message.conversation_seq.len(),
        message.sender_id.len(),
        message.message_type.len(),
        message.server_time.len(),
        message.payload.get().len(),
    ]
    .into_iter()
    .fold(0usize, usize::saturating_add);
    if minimum_bytes > MAX_WIRE_BYTES {
        return Err(Error::MessageTooLarge);
    }
    let mut fields: BTreeMap<&str, Box<RawValue>> = BTreeMap::new();
    for (name, value) in [
        ("clientMsgId", message.client_msg_id.as_str()),
        ("serverMsgId", message.server_msg_id.as_str()),
        ("conversationId", message.conversation_id.as_str()),
        ("conversationSeq", message.conversation_seq.as_str()),
        ("senderId", message.sender_id.as_str()),
        ("type", message.message_type.as_str()),
        ("serverTime", message.server_time.as_str()),
    ] {
        fields.insert(
            name,
            serde_json::value::to_raw_value(&value).map_err(|_| Error::InvalidMessage)?,
        );
    }
    for (name, value) in [
        ("protocolVersion", message.protocol_version),
        ("version", message.version),
    ] {
        fields.insert(
            name,
            serde_json::value::to_raw_value(&value).map_err(|_| Error::InvalidMessage)?,
        );
    }
    fields.insert("payload", message.payload.clone());
    let encoded = serde_json::to_vec(&fields).map_err(|_| Error::InvalidJson)?;
    decode(&encoded)?;
    Ok(encoded)
}
