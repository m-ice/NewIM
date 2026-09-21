//! 仅做关联与无损意图比较，不修改存储 / Pure correlation and lossless intent comparison.
use serde_json::value::RawValue;

use crate::send::{Error, Send, ServerFrame, encode_send, encode_server_frame};

/// 不比较幂等键 clientMsgId；调用者另行验证可信身份和键 / Caller separately checks trusted identity and key.
pub fn same_intent(left: &Send, right: &Send) -> Result<bool, Error> {
    encode_send(left)?;
    encode_send(right)?;
    intent_equal(left, right)
}
fn intent_equal(left: &Send, right: &Send) -> Result<bool, Error> {
    Ok(left.protocol_version == right.protocol_version
        && left.conversation_id == right.conversation_id
        && left.version == right.version
        && left.message_type == right.message_type
        && payload_equal(&left.payload, &right.payload)?)
}

/// sender 必须来自可信账户上下文 / Sender must come from trusted account context.
pub fn correlate(send: &Send, trusted_sender: &str, frame: &ServerFrame) -> Result<(), Error> {
    encode_send(send)?;
    if !crate::id(trusted_sender) {
        return Err(crate::Error::InvalidMessage.into());
    }
    encode_server_frame(frame)?;
    let (client, conversation) = match frame {
        ServerFrame::Ack(ack) => {
            if ack.sender_id != trusted_sender {
                return Err(Error::CorrelationMismatch);
            }
            (&ack.client_msg_id, &ack.conversation_id)
        }
        ServerFrame::Error(error) => (&error.client_msg_id, &error.conversation_id),
        ServerFrame::Message(message) => {
            if message.sender_id != trusted_sender
                || send.protocol_version != message.protocol_version
                || send.version != message.version
                || send.message_type != message.message_type
                || !payload_equal(&send.payload, &message.payload)?
            {
                return Err(Error::CorrelationMismatch);
            }
            (&message.client_msg_id, &message.conversation_id)
        }
    };
    if *client != send.client_msg_id || *conversation != send.conversation_id {
        return Err(Error::CorrelationMismatch);
    }
    Ok(())
}

// Identity tuple and immutable persisted result; error frames never revoke persistence.
fn persisted(frame: &ServerFrame) -> Result<([&str; 3], [&str; 3]), Error> {
    match frame {
        ServerFrame::Ack(ack) => Ok((
            [&ack.client_msg_id, &ack.conversation_id, &ack.sender_id],
            [&ack.server_msg_id, &ack.conversation_seq, &ack.server_time],
        )),
        ServerFrame::Message(message) => Ok((
            [
                &message.client_msg_id,
                &message.conversation_id,
                &message.sender_id,
            ],
            [
                &message.server_msg_id,
                &message.conversation_seq,
                &message.server_time,
            ],
        )),
        ServerFrame::Error(_) => Err(crate::Error::InvalidMessage.into()),
    }
}
/// 两种到达顺序均检查同一持久结果 / Both arrival orders must identify the same persisted result.
pub fn compare_results(left: &ServerFrame, right: &ServerFrame) -> Result<(), Error> {
    encode_server_frame(left)?;
    encode_server_frame(right)?;
    let (left_key, left_result) = persisted(left)?;
    let (right_key, right_result) = persisted(right)?;
    if left_key != right_key {
        return Err(Error::CorrelationMismatch);
    }
    if left_result != right_result {
        return Err(Error::ResultConflict);
    }
    Ok(())
}

// Operands have already passed the bounded strict scan. RawValue never parses numbers as floats.
fn payload_equal(left: &RawValue, right: &RawValue) -> Result<bool, Error> {
    let left = left.get().trim();
    let right = right.get().trim();
    let a = left.as_bytes()[0];
    let b = right.as_bytes()[0];
    if matches!(a, b'-' | b'0'..=b'9') && matches!(b, b'-' | b'0'..=b'9') {
        return Ok(number(left) == number(right));
    }
    if a != b {
        return Ok(false);
    }
    match a {
        b'{' => {
            let left = crate::object(left)?;
            let right = crate::object(right)?;
            if left.len() != right.len() {
                return Ok(false);
            }
            for (key, value) in left {
                let Some(other) = right.get(&key) else {
                    return Ok(false);
                };
                if !payload_equal(&value, other)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        b'[' => {
            let left: Vec<Box<RawValue>> =
                serde_json::from_str(left).map_err(|_| crate::Error::InvalidJson)?;
            let right: Vec<Box<RawValue>> =
                serde_json::from_str(right).map_err(|_| crate::Error::InvalidJson)?;
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(&right) {
                if !payload_equal(left, right)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        b'"' => {
            let left: String = serde_json::from_str(left).map_err(|_| crate::Error::InvalidJson)?;
            let right: String =
                serde_json::from_str(right).map_err(|_| crate::Error::InvalidJson)?;
            Ok(left == right)
        }
        _ => Ok(left == right),
    }
}

// Bounded decimal-string exponent arithmetic. Adjustment never exceeds token length (128).
// No exponent-sized allocation and no machine integer conversion of the exponent.
fn add_magnitude(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(left.len().max(right.len()) + 1);
    let mut carry = 0;
    for offset in 0..left.len().max(right.len()) {
        let a = left
            .len()
            .checked_sub(offset + 1)
            .map_or(0, |index| left[index] - b'0');
        let b = right
            .len()
            .checked_sub(offset + 1)
            .map_or(0, |index| right[index] - b'0');
        let sum = a + b + carry;
        result.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    if carry != 0 {
        result.push(b'0' + carry);
    }
    result.reverse();
    result
}
// Caller guarantees left >= right; each digit is visited once.
fn subtract_magnitude(left: &[u8], right: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(left.len());
    let mut borrow = 0i16;
    for offset in 0..left.len() {
        let a = i16::from(left[left.len() - offset - 1] - b'0');
        let b = right
            .len()
            .checked_sub(offset + 1)
            .map_or(0, |index| i16::from(right[index] - b'0'));
        let difference = a - b - borrow;
        borrow = i16::from(difference < 0);
        result.push(b'0' + (difference + borrow * 10) as u8);
    }
    while result.len() > 1 && result.last() == Some(&b'0') {
        result.pop();
    }
    result.reverse();
    result
}
fn shifted_exponent(raw: &str, adjustment: i32) -> (bool, Vec<u8>) {
    let digits = raw.trim_start_matches(['-', '+']).trim_start_matches('0');
    let magnitude = if digits.is_empty() {
        b"0".as_slice()
    } else {
        digits.as_bytes()
    };
    let negative = raw.starts_with('-') && magnitude != b"0";
    let delta = adjustment.unsigned_abs().to_string();
    let delta = delta.as_bytes();
    let subtract = adjustment < 0;
    let (negative, magnitude) = if negative == subtract {
        (negative, add_magnitude(magnitude, delta))
    } else {
        let order = magnitude
            .len()
            .cmp(&delta.len())
            .then_with(|| magnitude.cmp(delta));
        if order.is_lt() {
            (subtract, subtract_magnitude(delta, magnitude))
        } else {
            (negative, subtract_magnitude(magnitude, delta))
        }
    };
    (negative && magnitude != b"0", magnitude)
}
fn number(raw: &str) -> (bool, String, (bool, Vec<u8>)) {
    let negative = raw.starts_with('-');
    let raw = raw.trim_start_matches('-');
    let (mantissa, exponent) = match raw.find(['e', 'E']) {
        Some(position) => (&raw[..position], &raw[position + 1..]),
        None => (raw, "0"),
    };
    let fraction = mantissa
        .find('.')
        .map_or(0, |position| mantissa.len() - position - 1);
    let coefficient: String = mantissa
        .chars()
        .filter(|character| *character != '.')
        .collect();
    let coefficient = coefficient.trim_start_matches('0');
    if coefficient.is_empty() {
        return (false, "0".into(), (false, vec![b'0']));
    }
    let trimmed = coefficient.trim_end_matches('0');
    let adjustment = (coefficient.len() - trimmed.len()) as i32 - fraction as i32;
    (
        negative,
        trimmed.to_owned(),
        shifted_exponent(exponent, adjustment),
    )
}
