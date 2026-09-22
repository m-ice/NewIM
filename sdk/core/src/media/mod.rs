//! 协议无关的媒体元数据边界 / Protocol-neutral media metadata boundary.
//!
//! This module carries validated intent and ready-asset metadata. It never
//! carries bytes, object URLs or upload/download grants.

use std::fmt;

/// 媒体元数据对象的最大编码字节数 / Maximum encoded metadata object bytes.
pub const MAX_MEDIA_PAYLOAD_BYTES: usize = 4_096;
/// 最大声明或实际对象字节数 / Maximum declared or actual object bytes.
pub const MAX_MEDIA_SIZE: u64 = 104_857_600;

/// 媒体类型 / Media kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaKind {
    Image,
    Video,
    Audio,
    File,
}

impl MediaKind {
    /// 解析冻结的 v1 枚举 / Parse the frozen v1 enum.
    pub fn parse(value: &str) -> Result<Self, MediaError> {
        match value {
            "image" => Ok(Self::Image),
            "video" => Ok(Self::Video),
            "audio" => Ok(Self::Audio),
            "file" => Ok(Self::File),
            _ => Err(MediaError::InvalidInput),
        }
    }

    /// 返回线上名称 / Return the wire name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Video => "video",
            Self::Audio => "audio",
            Self::File => "file",
        }
    }
}

/// 无内容诊断的稳定错误 / Stable errors without content-bearing diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MediaError {
    InvalidInput,
}

impl MediaError {
    /// 返回稳定错误码 / Return the stable code.
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "MEDIA_INVALID_INPUT",
        }
    }
}

impl fmt::Display for MediaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for MediaError {}

/// 已校验的媒体元数据 / Validated media metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaMetadata {
    pub media_key: String,
    pub kind: MediaKind,
    pub content_type: String,
    pub size: u64,
    pub sha256: [u8; 32],
}

impl MediaMetadata {
    /// 先做全部边界校验 / Validate every boundary before use.
    pub fn validate(&self) -> Result<(), MediaError> {
        if !valid_media_key(&self.media_key)
            || !valid_content_type(&self.content_type)
            || !(1..=MAX_MEDIA_SIZE).contains(&self.size)
        {
            return Err(MediaError::InvalidInput);
        }
        Ok(())
    }

    /// 编码后的元数据不得携带二进制正文 / No binary body exists in this type.
    pub fn has_binary_field(&self) -> bool {
        false
    }
}

/// 服务端生成媒体标识 / Server-generated media identifier.
pub fn valid_media_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// 精确 media v1 MIME 语法 / Exact media v1 MIME grammar.
pub fn valid_content_type(value: &str) -> bool {
    if value.is_empty() || value.len() > 128 {
        return false;
    }
    let bytes = value.as_bytes();
    let mut slash = None;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'/' {
            if slash.is_some() {
                return false;
            }
            slash = Some(index);
        }
    }
    let Some(slash) = slash else {
        return false;
    };
    if !(1..=63).contains(&slash) || !(1..=64).contains(&(bytes.len() - slash - 1)) {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| {
        if index == 0 || index == slash + 1 {
            byte.is_ascii_lowercase() || byte.is_ascii_digit()
        } else if index == slash {
            true
        } else {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(
                    byte,
                    b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
                )
        }
    })
}
