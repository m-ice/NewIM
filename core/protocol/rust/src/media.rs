//! 封闭的 media v1 元数据契约 / Closed media v1 metadata contract.
//!
//! This module validates metadata only. It never accepts bytes or expiring URLs.

use std::collections::BTreeMap;

use serde_json::value::RawValue;

use crate::{Error, MAX_DEPTH};

/// 媒体元数据对象的最大编码字节数 / Maximum encoded metadata object bytes.
pub const MAX_MEDIA_PAYLOAD_BYTES: usize = 4_096;
/// 媒体对象的最大声明或实际字节数 / Maximum declared or actual object bytes.
pub const MAX_MEDIA_SIZE: i64 = 104_857_600;

/// 媒体 v1 元数据 / Media v1 metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaMetadata {
    pub media_key: String,
    pub kind: String,
    pub content_type: String,
    pub size: String,
    pub sha256: String,
}

/// 判断 kind 是否属于冻结的 v1 枚举 / Check the frozen v1 kind enum.
pub fn supported_kind(kind: &str) -> bool {
    matches!(kind, "image" | "video" | "audio" | "file")
}

/// 判断服务端生成的媒体标识 / Check a server-generated media identifier.
pub fn valid_media_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// 判断精确且有界的 media v1 MIME 语法 / Check the exact bounded media v1 MIME grammar.
pub fn valid_content_type(value: &str) -> bool {
    if value.is_empty() || value.len() > 128 {
        return false;
    }
    let mut slash = None;
    for (index, byte) in value.bytes().enumerate() {
        if byte == b'/' {
            if slash.is_some() {
                return false;
            }
            slash = Some(index);
        }
    }
    let Some(slash) = slash else {
        return false;
    };
    if !(1..=63).contains(&slash) || !(1..=64).contains(&(value.len() - slash - 1)) {
        return false;
    }
    value.bytes().enumerate().all(|(index, byte)| {
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

/// 判断精确十进制 size 且位于 1..=100MiB / Check exact decimal range.
pub fn valid_size(value: &str) -> bool {
    if value.is_empty() || value.len() > 9 || !value.as_bytes()[0].is_ascii_digit() {
        return false;
    }
    if value.as_bytes()[0] == b'0' || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    value
        .parse::<i64>()
        .is_ok_and(|number| (1..=MAX_MEDIA_SIZE).contains(&number))
}

/// 判断 64 个小写十六进制 SHA-256 / Check a lowercase SHA-256 hex string.
pub fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// 校验全部媒体 v1 字段 / Validate every media v1 field.
pub fn valid(metadata: &MediaMetadata) -> bool {
    valid_media_key(&metadata.media_key)
        && supported_kind(&metadata.kind)
        && valid_content_type(&metadata.content_type)
        && valid_size(&metadata.size)
        && valid_sha256(&metadata.sha256)
}

type Object = BTreeMap<String, Box<RawValue>>;

fn decode_object(raw: &str) -> Result<Object, Error> {
    if !raw.trim_start().starts_with('{') {
        return Err(Error::InvalidMessage);
    }
    serde_json::from_str(raw).map_err(|_| Error::InvalidMessage)
}

fn string(fields: &Object, name: &str) -> Result<String, Error> {
    let raw = fields.get(name).ok_or(Error::InvalidMessage)?.get();
    if raw == "null" {
        return Err(Error::InvalidMessage);
    }
    serde_json::from_str(raw).map_err(|_| Error::InvalidMessage)
}

/// 严格解码一个封闭的 media v1 对象 / Decode one closed media v1 object.
pub fn decode(raw: &str) -> Result<MediaMetadata, Error> {
    if raw.len() > MAX_MEDIA_PAYLOAD_BYTES {
        return Err(Error::MessageTooLarge);
    }
    crate::scan::check_with_limits(raw.as_bytes(), MAX_MEDIA_PAYLOAD_BYTES, MAX_DEPTH)?;
    let fields = decode_object(raw)?;
    if fields.len() != 5 {
        return Err(Error::InvalidMessage);
    }
    let metadata = MediaMetadata {
        media_key: string(&fields, "mediaKey")?,
        kind: string(&fields, "kind")?,
        content_type: string(&fields, "contentType")?,
        size: string(&fields, "size")?,
        sha256: string(&fields, "sha256")?,
    };
    if !valid(&metadata) {
        return Err(Error::InvalidMessage);
    }
    Ok(metadata)
}

/// 校验一个封闭的 media v1 对象 / Validate one closed media v1 object.
pub fn validate(raw: &str) -> Result<(), Error> {
    decode(raw).map(|_| ())
}

/// 校验并编码规范对象 / Validate and encode the canonical object.
pub fn encode(metadata: &MediaMetadata) -> Result<String, Error> {
    if !valid(metadata) {
        return Err(Error::InvalidMessage);
    }
    let mut fields = BTreeMap::new();
    for (name, value) in [
        ("mediaKey", metadata.media_key.as_str()),
        ("kind", metadata.kind.as_str()),
        ("contentType", metadata.content_type.as_str()),
        ("size", metadata.size.as_str()),
        ("sha256", metadata.sha256.as_str()),
    ] {
        fields.insert(
            name,
            serde_json::value::to_raw_value(value).map_err(|_| Error::InvalidMessage)?,
        );
    }
    let encoded = serde_json::to_string(&fields).map_err(|_| Error::InvalidJson)?;
    if encoded.len() > MAX_MEDIA_PAYLOAD_BYTES {
        return Err(Error::MessageTooLarge);
    }
    validate(&encoded)?;
    Ok(encoded)
}
