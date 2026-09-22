use super::{MAX_ATTEMPTS, OutboxError, OutboxRecord, OutboxState};
use crate::message::{ConnectionGeneration, SendIntent};
use crate::store::{Blob, Fence, MAX_VALUE_BYTES};

const MAGIC: &[u8; 4] = b"NIOS";
const VERSION: u8 = 1;
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

fn validate_record(record: &OutboxRecord) -> Result<(), OutboxError> {
    record
        .intent
        .validate()
        .map_err(|_| OutboxError::InvalidInput)?;
    if record.fence.account.is_empty()
        || record.fence.account.len() > 128
        || record.fence.instance.is_empty()
        || record.fence.instance.len() > 128
        || record.fence.generation == 0
        || record.fence.generation > i64::MAX as u64
        || record.connection_generation.0 == 0
        || record.connection_generation.0 > i64::MAX as u64
        || record.attempts > MAX_ATTEMPTS
    {
        return Err(OutboxError::InvalidInput);
    }
    if !record.last_code.is_empty()
        && (record.last_code.len() > 64
            || !record.last_code.as_bytes()[0].is_ascii_uppercase()
            || !record
                .last_code
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
    {
        return Err(OutboxError::InvalidInput);
    }
    let valid = match record.state {
        OutboxState::Ready => record.attempts == 0 && record.deadline_ms == record.created_at_ms,
        OutboxState::InFlight | OutboxState::RetryWait => record.attempts > 0,
        OutboxState::AuthRecovery | OutboxState::PermanentFailure => {
            record.deadline_ms == 0 && !record.last_code.is_empty()
        }
    };
    if valid {
        Ok(())
    } else {
        Err(OutboxError::RecordInvalid)
    }
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
    put_text(&mut out, &record.fence.account)?;
    put_text(&mut out, &record.fence.instance)?;
    out.extend_from_slice(&record.fence.generation.to_be_bytes());
    out.extend_from_slice(&record.connection_generation.0.to_be_bytes());
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
    let account = reader.text(128)?;
    let instance = reader.text(128)?;
    let store_generation = reader.u64()?;
    let connection_generation = ConnectionGeneration(reader.u64()?);
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
        fence: Fence {
            account,
            instance,
            generation: store_generation,
        },
        connection_generation,
        state,
        attempts,
        created_at_ms,
        deadline_ms,
        last_code,
    };
    validate_record(&record)?;
    Ok(record)
}
