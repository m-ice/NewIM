//! 发送与持久化回执帧；解析不证明提交 / Send and result frames; parsing does not prove commit.
use std::{collections::BTreeMap, fmt};

use serde_json::value::{RawValue, to_raw_value};

pub use crate::intent::{compare_results, correlate, same_intent};
use crate::{Message, Object, decimal, field, id, object, scan, string, version};

pub const MAX_FRAME_BYTES: usize = 66_048;
pub const MAX_SEND_BYTES: usize = 61_440;
pub const MAX_RESULT_BYTES: usize = 4_096;
pub const MAX_METADATA_BYTES: usize = 512;

/// 保留旧错误枚举兼容性 / Preserve the existing codec error enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    Protocol(crate::Error),
    UnsupportedFrame,
    CorrelationMismatch,
    ResultConflict,
}
impl Error {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Protocol(error) => error.code(),
            Self::UnsupportedFrame => "PROTOCOL_UNSUPPORTED_FRAME",
            Self::CorrelationMismatch => "SEND_CORRELATION_MISMATCH",
            Self::ResultConflict => "SEND_RESULT_CONFLICT",
        }
    }
}
impl From<crate::Error> for Error {
    fn from(error: crate::Error) -> Self {
        Self::Protocol(error)
    }
}
impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}
impl std::error::Error for Error {}

/// 发送意图，不包含服务器权威字段 / Send intent without server-authoritative fields.
#[derive(Clone, Debug)]
pub struct Send {
    pub protocol_version: u32,
    pub client_msg_id: String,
    pub conversation_id: String,
    pub version: u32,
    pub message_type: String,
    pub payload: Box<RawValue>,
}

impl Send {
    /// 是否识别发送 schema（非授权）/ Whether the send schema is understood, not authorized.
    pub fn supported(&self) -> bool {
        self.protocol_version == 1
            && self.version == 1
            && matches!(self.message_type.as_str(), "text" | "media")
    }
}

/// 仅代表 SERVER_PERSISTED；生产者须保证事务已提交 / Producer must ensure transaction commit.
#[derive(Clone, Debug)]
pub struct Ack {
    pub client_msg_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub server_msg_id: String,
    pub conversation_seq: String,
    pub server_time: String,
}
/// 仅可关联可信解析的请求；未知错误码保留 / Correlate only parsed requests; retain unknown codes.
#[derive(Clone, Debug)]
pub struct SendError {
    pub client_msg_id: String,
    pub conversation_id: String,
    pub code: String,
}
/// 明确区分持久结果与失败；错误不能撤销持久化 / Errors cannot revoke a persisted result.
#[derive(Clone, Debug)]
pub enum ServerFrame {
    Ack(Ack),
    Error(SendError),
    Message(Message),
}

/// 本地保守重试分类；调度和有限退避由 SDK 负责 / SDK owns scheduling and bounded backoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Disposition {
    RetrySameIntent,
    AuthRecovery,
    StopAutomaticRetry,
}
pub fn disposition(code: &str) -> Disposition {
    match code {
        "SERVER_TEMPORARY_UNAVAILABLE" => Disposition::RetrySameIntent,
        "AUTH_REQUIRED" | "AUTH_TOKEN_EXPIRED" => Disposition::AuthRecovery,
        _ => Disposition::StopAutomaticRetry,
    }
}

fn invalid<T>() -> Result<T, Error> {
    Err(crate::Error::InvalidMessage.into())
}
fn identifiers(values: &[&str]) -> Result<(), Error> {
    if values.iter().all(|value| id(value)) {
        Ok(())
    } else {
        invalid()
    }
}
fn forbidden(fields: &Object) -> bool {
    [
        "senderId",
        "serverMsgId",
        "conversationSeq",
        "serverTime",
        "status",
    ]
    .iter()
    .any(|key| fields.contains_key(*key))
}
fn code_valid(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code.as_bytes()[0].is_ascii_uppercase()
        && code
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
}
fn send_shape(mut fields: Object, protocol_version: u32) -> Result<Send, Error> {
    let send = Send {
        protocol_version,
        client_msg_id: string(&fields, "clientMsgId")?,
        conversation_id: string(&fields, "conversationId")?,
        version: version(&fields, "version")?,
        message_type: string(&fields, "type")?,
        payload: fields
            .remove("payload")
            .ok_or(crate::Error::InvalidMessage)?,
    };
    identifiers(&[&send.client_msg_id, &send.conversation_id])?;
    if !crate::message_type(&send.message_type) {
        return invalid();
    }
    object(send.payload.get())?;
    Ok(send)
}
fn send_semantics(send: &Send) -> Result<(), Error> {
    if send.version != 1 {
        return Ok(());
    }
    match send.message_type.as_str() {
        "text" => {
            let text = string(&object(send.payload.get())?, "text")?;
            if text.is_empty() || text.len() > crate::MAX_TEXT_BYTES {
                return invalid();
            }
        }
        "media" => crate::media::validate(send.payload.get())?,
        _ => {}
    }
    Ok(())
}
fn ack_shape(fields: &Object) -> Result<Ack, Error> {
    if string(fields, "status")? != "SERVER_PERSISTED" {
        return invalid();
    }
    let ack = Ack {
        client_msg_id: string(fields, "clientMsgId")?,
        conversation_id: string(fields, "conversationId")?,
        sender_id: string(fields, "senderId")?,
        server_msg_id: string(fields, "serverMsgId")?,
        conversation_seq: string(fields, "conversationSeq")?,
        server_time: string(fields, "serverTime")?,
    };
    identifiers(&[
        &ack.client_msg_id,
        &ack.conversation_id,
        &ack.sender_id,
        &ack.server_msg_id,
    ])?;
    if !decimal(&ack.conversation_seq) || !decimal(&ack.server_time) {
        return invalid();
    }
    Ok(ack)
}
fn error_shape(fields: &Object) -> Result<SendError, Error> {
    let error = SendError {
        client_msg_id: string(fields, "clientMsgId")?,
        conversation_id: string(fields, "conversationId")?,
        code: string(fields, "code")?,
    };
    identifiers(&[&error.client_msg_id, &error.conversation_id])?;
    if !code_valid(&error.code) {
        return invalid();
    }
    Ok(error)
}

enum Frame {
    Send(Send),
    Server(ServerFrame),
    Unknown,
}
fn decode_frame(input: &[u8], client_direction: bool) -> Result<Frame, Error> {
    scan::check_with_limits(input, MAX_FRAME_BYTES, crate::MAX_DEPTH + 1)?;
    let text = std::str::from_utf8(input).map_err(|_| crate::Error::InvalidJson)?;
    let outer = object(text)?;
    let outer_version = version(&outer, "protocolVersion")?;
    let kind = string(&outer, "kind")?;
    let raw_body = field(&outer, "body")?.get();
    let body = object(raw_body)?;
    if kind == "send" && (forbidden(&outer) || forbidden(&body)) {
        return invalid();
    }
    let body_budget = match kind.as_str() {
        "send" => MAX_SEND_BYTES,
        "send_ack" | "send_error" => MAX_RESULT_BYTES,
        _ => crate::MAX_WIRE_BYTES,
    };
    if input.len() - raw_body.len() > MAX_METADATA_BYTES || raw_body.len() > body_budget {
        return Err(crate::Error::MessageTooLarge.into());
    }
    scan::check_with_limits(raw_body.as_bytes(), body_budget, crate::MAX_DEPTH)?;
    let frame = match kind.as_str() {
        "send" => Frame::Send(send_shape(body, outer_version)?),
        "send_ack" => Frame::Server(ServerFrame::Ack(ack_shape(&body)?)),
        "send_error" => Frame::Server(ServerFrame::Error(error_shape(&body)?)),
        "message" => Frame::Server(ServerFrame::Message(crate::decode_shape(raw_body)?)),
        _ => Frame::Unknown,
    };
    if outer_version != 1 {
        return Err(crate::Error::UnsupportedVersion.into());
    }
    match &frame {
        Frame::Send(send) if client_direction => send_semantics(send)?,
        Frame::Server(ServerFrame::Message(message)) if !client_direction => {
            crate::validate_semantics(message)?
        }
        Frame::Server(_) if !client_direction => (),
        _ => return Err(Error::UnsupportedFrame),
    }
    Ok(frame)
}

/// 完整形状先于版本和方向校验 / Complete shape precedes version and direction checks.
pub fn decode_send(input: &[u8]) -> Result<Send, Error> {
    match decode_frame(input, true)? {
        Frame::Send(send) => Ok(send),
        _ => Err(Error::UnsupportedFrame),
    }
}
/// 仅解析服务器方向帧，不猜测裸消息 / Decode server frames without guessing bare messages.
pub fn decode_server_frame(input: &[u8]) -> Result<ServerFrame, Error> {
    match decode_frame(input, false)? {
        Frame::Server(frame) => Ok(frame),
        _ => Err(Error::UnsupportedFrame),
    }
}

type Output = BTreeMap<&'static str, Box<RawValue>>;
fn strings(fields: &[(&'static str, &str)], budget: usize) -> Result<Output, Error> {
    if fields
        .iter()
        .map(|(_, value)| value.len())
        .fold(0usize, usize::saturating_add)
        > budget
    {
        return Err(crate::Error::MessageTooLarge.into());
    }
    fields
        .iter()
        .map(|(name, value)| {
            Ok((
                *name,
                to_raw_value(value).map_err(|_| crate::Error::InvalidMessage)?,
            ))
        })
        .collect()
}
fn wrap(protocol_version: u32, kind: &str, body: Box<RawValue>) -> Result<Vec<u8>, Error> {
    let mut outer = Output::new();
    outer.insert(
        "protocolVersion",
        to_raw_value(&protocol_version).map_err(|_| crate::Error::InvalidMessage)?,
    );
    outer.insert(
        "kind",
        to_raw_value(kind).map_err(|_| crate::Error::InvalidMessage)?,
    );
    outer.insert("body", body);
    serde_json::to_vec(&outer).map_err(|_| crate::Error::InvalidJson.into())
}

/// 重验公共字段且保留原始 payload 数值 / Revalidate public fields without changing raw numbers.
pub fn encode_send(send: &Send) -> Result<Vec<u8>, Error> {
    let minimum = [
        send.client_msg_id.len(),
        send.conversation_id.len(),
        send.message_type.len(),
        send.payload.get().len(),
    ]
    .into_iter()
    .fold(0usize, usize::saturating_add);
    if minimum > MAX_SEND_BYTES {
        return Err(crate::Error::MessageTooLarge.into());
    }
    // RawValue guarantees an independent JSON fragment; full-frame scanning below
    // preserves size-before-duplicate/depth precedence even after string escaping.
    let mut body = strings(
        &[
            ("clientMsgId", &send.client_msg_id),
            ("conversationId", &send.conversation_id),
            ("type", &send.message_type),
        ],
        MAX_SEND_BYTES,
    )?;
    body.insert(
        "version",
        to_raw_value(&send.version).map_err(|_| crate::Error::InvalidMessage)?,
    );
    body.insert("payload", send.payload.clone());
    let encoded = wrap(
        send.protocol_version,
        "send",
        to_raw_value(&body).map_err(|_| crate::Error::InvalidMessage)?,
    )?;
    decode_send(&encoded)?;
    Ok(encoded)
}
/// 重验结果字段；编码成功不证明服务端提交 / Revalidate fields; encoding never proves commit.
pub fn encode_server_frame(frame: &ServerFrame) -> Result<Vec<u8>, Error> {
    let (kind, body) = match frame {
        ServerFrame::Ack(ack) => (
            "send_ack",
            strings(
                &[
                    ("status", "SERVER_PERSISTED"),
                    ("clientMsgId", &ack.client_msg_id),
                    ("conversationId", &ack.conversation_id),
                    ("senderId", &ack.sender_id),
                    ("serverMsgId", &ack.server_msg_id),
                    ("conversationSeq", &ack.conversation_seq),
                    ("serverTime", &ack.server_time),
                ],
                MAX_RESULT_BYTES,
            )?,
        ),
        ServerFrame::Error(error) => (
            "send_error",
            strings(
                &[
                    ("clientMsgId", &error.client_msg_id),
                    ("conversationId", &error.conversation_id),
                    ("code", &error.code),
                ],
                MAX_RESULT_BYTES,
            )?,
        ),
        ServerFrame::Message(message) => {
            let raw = String::from_utf8(crate::encode(message)?)
                .map_err(|_| crate::Error::InvalidJson)?;
            let encoded = wrap(
                1,
                "message",
                RawValue::from_string(raw).map_err(|_| crate::Error::InvalidJson)?,
            )?;
            decode_server_frame(&encoded)?;
            return Ok(encoded);
        }
    };
    let encoded = wrap(
        1,
        kind,
        to_raw_value(&body).map_err(|_| crate::Error::InvalidMessage)?,
    )?;
    decode_server_frame(&encoded)?;
    Ok(encoded)
}
