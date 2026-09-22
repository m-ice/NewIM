use super::{
    AUTH_RECOVERY_RESUMED, MAX_ATTEMPTS, MAX_RETRY_AGE_MS, OutboxError, OutboxRecord, OutboxState,
    STORE_GENERATION_CHANGED,
};
use crate::message::{ConnectionGeneration, SendIntent};
use crate::store::{Blob, Fence, MAX_VALUE_BYTES};

const MAGIC: &[u8; 4] = b"NIOS";
const VERSION: u8 = 2;
pub const MAX_ENVELOPE_BYTES: usize = MAX_VALUE_BYTES - 1;

struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8], OutboxError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or(OutboxError::RecordInvalid)?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(OutboxError::RecordInvalid)?;
        self.offset = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, OutboxError> {
        self.take(1).map(|value| value[0])
    }
    fn u16(&mut self) -> Result<u16, OutboxError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }
    fn u32(&mut self) -> Result<u32, OutboxError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
    fn u64(&mut self) -> Result<u64, OutboxError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
    fn text(&mut self, max: usize) -> Result<String, OutboxError> {
        let length = self.u16()? as usize;
        if length > max {
            return Err(OutboxError::RecordInvalid);
        }
        let bytes = self.take(length)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| OutboxError::RecordInvalid)
    }
    fn bytes(&mut self, max: usize) -> Result<Vec<u8>, OutboxError> {
        let length = self.u32()? as usize;
        if length > max {
            return Err(OutboxError::RecordInvalid);
        }
        Ok(self.take(length)?.to_vec())
    }
    fn finish(&self) -> Result<(), OutboxError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(OutboxError::RecordInvalid)
        }
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}
fn valid_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
fn valid_fence(fence: &Fence) -> bool {
    valid_identity(&fence.account)
        && valid_identity(&fence.instance)
        && fence.generation > 0
        && fence.generation <= i64::MAX as u64
}
fn valid_connection(generation: ConnectionGeneration) -> bool {
    generation.0 > 0 && generation.0 <= i64::MAX as u64
}

fn put_u16(out: &mut Vec<u8>, value: usize) -> Result<(), OutboxError> {
    let value = u16::try_from(value).map_err(|_| OutboxError::RecordTooLarge)?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}
fn put_u32(out: &mut Vec<u8>, value: usize) -> Result<(), OutboxError> {
    let value = u32::try_from(value).map_err(|_| OutboxError::RecordTooLarge)?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}
fn put_text(out: &mut Vec<u8>, value: &str) -> Result<(), OutboxError> {
    put_u16(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Ok(())
}
fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), OutboxError> {
    put_u32(out, value.len())?;
    out.extend_from_slice(value);
    Ok(())
}
fn put_fence(out: &mut Vec<u8>, fence: &Fence) -> Result<(), OutboxError> {
    put_text(out, &fence.account)?;
    put_text(out, &fence.instance)?;
    out.extend_from_slice(&fence.generation.to_be_bytes());
    Ok(())
}

fn retry_code_valid(state: OutboxState, code: &str) -> bool {
    match state {
        OutboxState::Ready | OutboxState::InFlight => code.is_empty(),
        OutboxState::RetryWait => {
            code == "SERVER_TEMPORARY_UNAVAILABLE" || code == AUTH_RECOVERY_RESUMED
        }
        OutboxState::AuthRecovery => matches!(
            code,
            "AUTH_REQUIRED" | "AUTH_TOKEN_EXPIRED" | STORE_GENERATION_CHANGED
        ),
        OutboxState::PermanentFailure => valid_code(code),
    }
}

fn validate_record(record: &OutboxRecord) -> Result<(), OutboxError> {
    record
        .intent
        .validate()
        .map_err(|_| OutboxError::InvalidInput)?;
    if !valid_fence(&record.fence)
        || !valid_fence(&record.active_fence)
        || record.active_fence.account != record.fence.account
        || record.active_fence.instance != record.fence.instance
        || !valid_connection(record.connection_generation)
        || !valid_connection(record.active_connection_generation)
        || record.attempts > MAX_ATTEMPTS
        || (!record.sender_id.is_empty() && record.sender_id != record.fence.account)
    {
        return Err(OutboxError::InvalidInput);
    }
    if !record.last_code.is_empty() && !valid_code(&record.last_code) {
        return Err(OutboxError::InvalidInput);
    }
    let age_deadline = record.created_at_ms.checked_add(MAX_RETRY_AGE_MS);
    let state_valid = match record.state {
        OutboxState::Ready => {
            record.attempts == 0
                && record.deadline_ms == record.created_at_ms
                && record.last_code.is_empty()
        }
        OutboxState::InFlight => {
            record.attempts > 0
                && record.deadline_ms > record.created_at_ms
                && age_deadline.is_some_and(|deadline| record.deadline_ms <= deadline)
                && record.last_code.is_empty()
        }
        OutboxState::RetryWait => {
            let attempts_valid = if record.last_code == AUTH_RECOVERY_RESUMED {
                record.attempts <= MAX_ATTEMPTS
            } else {
                record.attempts > 0
            };
            attempts_valid
                && record.deadline_ms > record.created_at_ms
                && age_deadline.is_some_and(|deadline| record.deadline_ms < deadline)
        }
        OutboxState::AuthRecovery => record.deadline_ms == 0,
        OutboxState::PermanentFailure => record.deadline_ms == 0,
    };
    if !state_valid || !retry_code_valid(record.state, &record.last_code) {
        return Err(OutboxError::RecordInvalid);
    }
    Ok(())
}

pub fn encode_envelope(record: &OutboxRecord) -> Result<Vec<u8>, OutboxError> {
    validate_record(record)?;
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&record.intent.protocol_version.to_be_bytes());
    put_text(&mut out, &record.intent.client_id)?;
    put_text(&mut out, &record.intent.conversation_id)?;
    out.extend_from_slice(&record.intent.schema_version.to_be_bytes());
    put_text(&mut out, &record.intent.message_type)?;
    put_bytes(&mut out, &record.intent.payload.0)?;
    put_fence(&mut out, &record.fence)?;
    out.extend_from_slice(&record.connection_generation.0.to_be_bytes());
    put_fence(&mut out, &record.active_fence)?;
    out.extend_from_slice(&record.active_connection_generation.0.to_be_bytes());
    out.push(record.state.as_byte());
    out.push(record.attempts);
    out.extend_from_slice(&record.created_at_ms.to_be_bytes());
    out.extend_from_slice(&record.deadline_ms.to_be_bytes());
    put_text(&mut out, &record.last_code)?;
    if out.len() > MAX_ENVELOPE_BYTES {
        return Err(OutboxError::RecordTooLarge);
    }
    let checksum = checksum(&out);
    out.extend_from_slice(&checksum.to_be_bytes());
    if out.len() > MAX_ENVELOPE_BYTES {
        Err(OutboxError::RecordTooLarge)
    } else {
        Ok(out)
    }
}

pub fn decode_envelope(input: &[u8]) -> Result<OutboxRecord, OutboxError> {
    if input.len() > MAX_ENVELOPE_BYTES || input.len() < 5 + 8 {
        return Err(OutboxError::RecordInvalid);
    }
    let checksum_offset = input.len() - 8;
    let expected = u64::from_be_bytes(
        input[checksum_offset..]
            .try_into()
            .map_err(|_| OutboxError::RecordInvalid)?,
    );
    if checksum(&input[..checksum_offset]) != expected {
        return Err(OutboxError::RecordInvalid);
    }
    let mut reader = Reader::new(&input[..checksum_offset]);
    if reader.take(4)? != MAGIC || reader.byte()? != VERSION {
        return Err(OutboxError::RecordInvalid);
    }
    let protocol_version = reader.u32()?;
    let client_id = reader.text(128)?;
    let conversation_id = reader.text(128)?;
    let schema_version = reader.u32()?;
    let message_type = reader.text(64)?;
    let payload = Blob(reader.bytes(MAX_VALUE_BYTES)?);
    let fence = Fence {
        account: reader.text(128)?,
        instance: reader.text(128)?,
        generation: reader.u64()?,
    };
    let connection_generation = ConnectionGeneration(reader.u64()?);
    let active_fence = Fence {
        account: reader.text(128)?,
        instance: reader.text(128)?,
        generation: reader.u64()?,
    };
    let active_connection_generation = ConnectionGeneration(reader.u64()?);
    let state = OutboxState::from_byte(reader.byte()?).ok_or(OutboxError::RecordInvalid)?;
    let attempts = reader.byte()?;
    let created_at_ms = reader.u64()?;
    let deadline_ms = reader.u64()?;
    let last_code = reader.text(64)?;
    reader.finish()?;
    let record = OutboxRecord {
        sender_id: String::new(),
        intent: SendIntent {
            protocol_version,
            client_id,
            conversation_id,
            schema_version,
            message_type,
            payload,
        },
        fence,
        connection_generation,
        active_fence,
        active_connection_generation,
        state,
        attempts,
        created_at_ms,
        deadline_ms,
        last_code,
    };
    validate_record(&record)?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ConnectionGeneration, SendContext, SendIntent};
    use crate::store::Fence;

    fn context() -> SendContext {
        SendContext {
            sender_id: "alice".into(),
            fence: Fence {
                account: "alice".into(),
                instance: "instance".into(),
                generation: 1,
            },
            connection_generation: ConnectionGeneration(1),
        }
    }

    fn intent() -> SendIntent {
        SendIntent {
            protocol_version: 1,
            client_id: "client".into(),
            conversation_id: "room".into(),
            schema_version: 1,
            message_type: "text".into(),
            payload: Blob(vec![1, 2, 3]),
        }
    }

    fn skip_text(bytes: &[u8], offset: &mut usize) {
        let length = u16::from_be_bytes([bytes[*offset], bytes[*offset + 1]]) as usize;
        *offset += 2 + length;
    }

    fn skip_bytes(bytes: &[u8], offset: &mut usize) {
        let length = u32::from_be_bytes([
            bytes[*offset],
            bytes[*offset + 1],
            bytes[*offset + 2],
            bytes[*offset + 3],
        ]) as usize;
        *offset += 4 + length;
    }

    fn set_deadline(bytes: &mut [u8], value: u64) {
        let mut offset = 5;
        offset += 4; // protocol version
        skip_text(bytes, &mut offset);
        skip_text(bytes, &mut offset);
        offset += 4; // schema version
        skip_text(bytes, &mut offset);
        skip_bytes(bytes, &mut offset);
        skip_text(bytes, &mut offset);
        skip_text(bytes, &mut offset);
        offset += 8; // origin generation
        offset += 8; // origin connection generation
        skip_text(bytes, &mut offset);
        skip_text(bytes, &mut offset);
        offset += 8; // active generation
        offset += 8; // active connection generation
        offset += 1; // state
        offset += 1; // attempts
        offset += 8; // created_at
        bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        let checksum_offset = bytes.len() - 8;
        let checksum_value = checksum(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum_value.to_be_bytes());
    }

    #[test]
    fn impossible_retry_wait_is_rejected_on_validate_and_decode() {
        let mut record = OutboxRecord::new(intent(), &context(), 1_000).unwrap();
        record.state = OutboxState::RetryWait;
        record.attempts = 1;
        record.deadline_ms = 1_001;
        record.last_code = "SERVER_TEMPORARY_UNAVAILABLE".into();
        assert!(validate_record(&record).is_ok());

        let mut invalid = record.clone();
        invalid.deadline_ms = 0;
        assert_eq!(validate_record(&invalid), Err(OutboxError::RecordInvalid));

        let mut bytes = encode_envelope(&record).unwrap();
        set_deadline(&mut bytes, 0);
        assert_eq!(decode_envelope(&bytes), Err(OutboxError::RecordInvalid));
    }
}
