//! Wire-neutral send domain events and trusted host context.
//! 平台适配器负责把已校验的 wire 值映射到这些类型；本模块不解析 JSON。

use crate::store::{Blob, Fence, MAX_VALUE_BYTES, StoreError};
use std::fmt;

pub const MAX_ID_BYTES: usize = 128;
pub const MAX_MESSAGE_TYPE_BYTES: usize = 64;
pub const MAX_ERROR_CODE_BYTES: usize = 64;

/// 主机提供的连接代次；它与 LocalStore 的 generation 分离。
/// Opaque host connection generation, separate from the local-store generation.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectionGeneration(pub u64);
impl ConnectionGeneration {
    pub fn validate(self) -> Result<(), MessageError> {
        if self.0 == 0 || self.0 > i64::MAX as u64 {
            Err(MessageError::InvalidInput)
        } else {
            Ok(())
        }
    }
}
impl fmt::Debug for ConnectionGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ConnectionGeneration")
            .field(&"redacted")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageError {
    InvalidInput,
    InvalidErrorCode,
}
impl MessageError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "SEND_DOMAIN_INVALID_INPUT",
            Self::InvalidErrorCode => "SEND_FAILURE_CODE_INVALID",
        }
    }
}
impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}
impl std::error::Error for MessageError {}
impl From<MessageError> for StoreError {
    fn from(_: MessageError) -> Self {
        StoreError::InvalidInput
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ID_BYTES
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn valid_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MESSAGE_TYPE_BYTES
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ERROR_CODE_BYTES
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
}

/// 不可变发送意图的 wire-neutral 子集。
/// A wire-neutral immutable send intent; the platform adapter owns wire decoding.
#[derive(Clone, PartialEq, Eq)]
pub struct SendIntent {
    pub protocol_version: u32,
    pub client_id: String,
    pub conversation_id: String,
    pub schema_version: u32,
    pub message_type: String,
    pub payload: Blob,
}
impl SendIntent {
    pub fn validate(&self) -> Result<(), MessageError> {
        if !(1..=i32::MAX as u32).contains(&self.protocol_version)
            || !valid_id(&self.client_id)
            || !valid_id(&self.conversation_id)
            || !(1..=i32::MAX as u32).contains(&self.schema_version)
            || !valid_type(&self.message_type)
            || self.payload.0.len() > MAX_VALUE_BYTES
        {
            return Err(MessageError::InvalidInput);
        }
        Ok(())
    }
}
impl fmt::Debug for SendIntent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendIntent")
            .field("protocol_version", &self.protocol_version)
            .field("client_id", &"redacted")
            .field("conversation_id", &"redacted")
            .field("schema_version", &self.schema_version)
            .field("message_type_len", &self.message_type.len())
            .field("payload_len", &self.payload.0.len())
            .finish()
    }
}

/// 已由协议适配器校验的持久化确认。
/// A persisted ACK after the adapter has validated the wire codec and authority.
#[derive(Clone, PartialEq, Eq)]
pub struct PersistedAck {
    pub sender_id: String,
    pub client_id: String,
    pub conversation_id: String,
    pub server_id: String,
    pub sequence: i64,
    pub server_time: i64,
    pub protocol_version: u32,
    pub schema_version: u32,
    pub message_type: String,
    pub payload: Blob,
}
impl PersistedAck {
    pub fn validate(&self) -> Result<(), MessageError> {
        if !valid_id(&self.sender_id)
            || !valid_id(&self.client_id)
            || !valid_id(&self.conversation_id)
            || !valid_id(&self.server_id)
            || self.sequence < 0
            || self.server_time < 0
            || !(1..=i32::MAX as u32).contains(&self.protocol_version)
            || !(1..=i32::MAX as u32).contains(&self.schema_version)
            || !valid_type(&self.message_type)
            || self.payload.0.len() > MAX_VALUE_BYTES
        {
            return Err(MessageError::InvalidInput);
        }
        Ok(())
    }
}
impl fmt::Debug for PersistedAck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistedAck")
            .field("sender_id", &"redacted")
            .field("client_id", &"redacted")
            .field("conversation_id", &"redacted")
            .field("server_id", &"redacted")
            .field("sequence", &self.sequence)
            .field("server_time", &self.server_time)
            .field("schema_version", &self.schema_version)
            .field("message_type_len", &self.message_type.len())
            .field("payload_len", &self.payload.0.len())
            .finish()
    }
}

/// Core-owned conservative classification of a validated failure code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendDisposition {
    RetrySameIntent,
    AuthRecovery,
    PermanentFailure,
}
impl SendDisposition {
    pub fn from_code(code: &str) -> Result<Self, MessageError> {
        if !valid_error_code(code) {
            return Err(MessageError::InvalidErrorCode);
        }
        Ok(match code {
            "SERVER_TEMPORARY_UNAVAILABLE" => Self::RetrySameIntent,
            "AUTH_REQUIRED" | "AUTH_TOKEN_EXPIRED" => Self::AuthRecovery,
            _ => Self::PermanentFailure,
        })
    }
}

/// 已校验的发送失败事件；不携带服务端诊断或正文。
/// A validated send failure event without server diagnostics or body text.
#[derive(Clone, PartialEq, Eq)]
pub struct SendFailure {
    pub client_id: String,
    pub conversation_id: String,
    pub code: String,
    pub disposition: SendDisposition,
}
impl SendFailure {
    pub fn from_validated_code(
        client_id: impl Into<String>,
        conversation_id: impl Into<String>,
        code: impl Into<String>,
    ) -> Result<Self, MessageError> {
        let client_id = client_id.into();
        let conversation_id = conversation_id.into();
        let code = code.into();
        if !valid_id(&client_id) || !valid_id(&conversation_id) {
            return Err(MessageError::InvalidInput);
        }
        Ok(Self {
            client_id,
            conversation_id,
            disposition: SendDisposition::from_code(&code)?,
            code,
        })
    }
    pub fn validate(&self) -> Result<(), MessageError> {
        if !valid_id(&self.client_id) || !valid_id(&self.conversation_id) {
            return Err(MessageError::InvalidInput);
        }
        if SendDisposition::from_code(&self.code)? != self.disposition {
            return Err(MessageError::InvalidInput);
        }
        Ok(())
    }
}
impl fmt::Debug for SendFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendFailure")
            .field("client_id", &"redacted")
            .field("conversation_id", &"redacted")
            .field("code", &self.code)
            .field("disposition", &self.disposition)
            .finish()
    }
}

/// 主机可信发送者、LocalStore fence 与独立连接代次。
/// Trusted sender, LocalStore fence and independent connection generation.
#[derive(Clone, PartialEq, Eq)]
pub struct SendContext {
    pub sender_id: String,
    pub fence: Fence,
    pub connection_generation: ConnectionGeneration,
}
impl SendContext {
    pub fn validate(&self) -> Result<(), MessageError> {
        if !valid_id(&self.sender_id)
            || !valid_id(&self.fence.account)
            || !valid_id(&self.fence.instance)
            || self.fence.generation == 0
            || self.fence.generation > i64::MAX as u64
        {
            return Err(MessageError::InvalidInput);
        }
        self.connection_generation.validate()
    }
}
impl fmt::Debug for SendContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendContext")
            .field("sender_id", &"redacted")
            .field("fence_account", &"redacted")
            .field("fence_instance", &"redacted")
            .field("store_generation", &"redacted")
            .field("connection_generation", &"redacted")
            .finish()
    }
}
