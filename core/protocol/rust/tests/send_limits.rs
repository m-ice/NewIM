mod send_common;
use newim_protocol::send::*;
use send_common::*;
use serde_json::value::RawValue;

fn padded_object(base: &str, target: usize) -> String {
    assert!(base.ends_with('}'));
    let separator = if base == "{}" { "" } else { "," };
    let prefix = format!("{}{separator}\"padding\":\"", &base[..base.len() - 1]);
    assert!(target >= prefix.len() + 2);
    format!("{}{}\"}}", prefix, "x".repeat(target - prefix.len() - 2))
}
fn metadata_size(kind: &str, body: &str, bytes: usize) -> String {
    let frame = wrap(kind, body);
    let current = frame.len() - body.len();
    assert!(bytes >= current);
    format!("{}{}", frame, " ".repeat(bytes - current))
}

#[test]
fn exact_body_and_metadata_bounds_include_every_kind() {
    let ack_wire =
        String::from_utf8(encode_server_frame(&ServerFrame::Ack(ack())).unwrap()).unwrap();
    let outer: std::collections::BTreeMap<String, Box<RawValue>> =
        serde_json::from_str(&ack_wire).unwrap();
    let ack_body = outer["body"].get();
    let error_body = r#"{"clientMsgId":"c1","conversationId":"room1","code":"UNKNOWN_FUTURE"}"#;
    let message_wire = newim_protocol::encode(match &message() {
        ServerFrame::Message(m) => m,
        _ => unreachable!(),
    })
    .unwrap();
    let message_body = std::str::from_utf8(&message_wire).unwrap();
    for (kind, base, limit) in [
        ("send", body("{}").as_str(), MAX_SEND_BYTES),
        ("send_ack", ack_body, MAX_RESULT_BYTES),
        ("send_error", error_body, MAX_RESULT_BYTES),
        ("message", message_body, newim_protocol::MAX_WIRE_BYTES),
    ] {
        for size in [limit, limit + 1] {
            let body = padded_object(base, size);
            assert_eq!(body.len(), size);
            let frame = wrap(kind, &body);
            let result = if kind == "send" {
                decode_send(frame.as_bytes()).map(|_| ())
            } else {
                decode_server_frame(frame.as_bytes()).map(|_| ())
            };
            if size == limit {
                result.unwrap();
            } else {
                check_error(result, "MESSAGE_TOO_LARGE");
            }
        }
        for size in [MAX_METADATA_BYTES, MAX_METADATA_BYTES + 1] {
            let frame = metadata_size(kind, base, size);
            assert_eq!(frame.len() - base.len(), size);
            let result = if kind == "send" {
                decode_send(frame.as_bytes()).map(|_| ())
            } else {
                decode_server_frame(frame.as_bytes()).map(|_| ())
            };
            if size == MAX_METADATA_BYTES {
                result.unwrap();
            } else {
                check_error(result, "MESSAGE_TOO_LARGE");
            }
        }
    }
}

#[test]
fn exact_total_frame_limit_and_unknown_kind_common_budget() {
    let ServerFrame::Message(message) = message() else {
        unreachable!()
    };
    let base = String::from_utf8(newim_protocol::encode(&message).unwrap()).unwrap();
    let body = padded_object(&base, newim_protocol::MAX_WIRE_BYTES);
    let mut maximum = metadata_size("message", &body, MAX_METADATA_BYTES).into_bytes();
    assert_eq!(maximum.len(), MAX_FRAME_BYTES);
    decode_server_frame(&maximum).unwrap();
    maximum.push(b' ');
    check_error(decode_server_frame(&maximum), "MESSAGE_TOO_LARGE");
    // Size rejection precedes malformed JSON.
    maximum[0] = 0xff;
    check_error(decode_server_frame(&maximum), "MESSAGE_TOO_LARGE");
    let unknown = padded_object("{}", newim_protocol::MAX_WIRE_BYTES);
    check_error(
        decode_server_frame(wrap("unknown", &unknown).as_bytes()),
        "PROTOCOL_UNSUPPORTED_FRAME",
    );
    let unknown = padded_object("{}", newim_protocol::MAX_WIRE_BYTES + 1);
    check_error(
        decode_server_frame(wrap("unknown", &unknown).as_bytes()),
        "MESSAGE_TOO_LARGE",
    );
}

#[test]
fn maximum_legacy_message_remains_wrappable_at_depth_32() {
    let ServerFrame::Message(mut message) = message() else {
        unreachable!()
    };
    let deep = format!("{}0{}", "[".repeat(30), "]".repeat(30));
    message.payload = RawValue::from_string(format!(r#"{{"deep":{deep},"padding":""}}"#)).unwrap();
    let minimum = newim_protocol::encode(&message).unwrap().len();
    message.payload = RawValue::from_string(format!(
        r#"{{"deep":{deep},"padding":"{}"}}"#,
        "x".repeat(newim_protocol::MAX_WIRE_BYTES - minimum)
    ))
    .unwrap();
    let bare = newim_protocol::encode(&message).unwrap();
    assert_eq!(bare.len(), 65_536);
    newim_protocol::decode(&bare).unwrap();
    let payload = message.payload.get().to_owned();
    let frame = ServerFrame::Message(message);
    let encoded = encode_server_frame(&frame).unwrap();
    assert_eq!(
        encoded.len(),
        65_582,
        "canonical wrapper adds exactly 46 bytes"
    );
    let ServerFrame::Message(again) = decode_server_frame(&encoded).unwrap() else {
        unreachable!()
    };
    assert_eq!(again.payload.get(), payload);
    assert_eq!(newim_protocol::encode(&again).unwrap(), bare);
}

#[test]
fn body_and_outer_metadata_depth_limits_are_separate() {
    for arrays in [30, 31] {
        let payload = format!(
            r#"{{"nested":{}0{}}}"#,
            "[".repeat(arrays),
            "]".repeat(arrays)
        );
        let result = request(&payload);
        if arrays == 30 {
            encode_send(&result.unwrap()).unwrap();
        } else {
            check_error(result, "PROTOCOL_NESTING_EXCEEDED");
        }
    }
    // Unknown outer metadata may use depth 33; body independently remains capped at 32.
    for arrays in [32, 33] {
        let frame = format!(
            r#"{{"protocolVersion":1,"kind":"send","body":{},"extra":{}0{}}}"#,
            body("{}"),
            "[".repeat(arrays),
            "]".repeat(arrays)
        );
        let result = decode_send(frame.as_bytes());
        if arrays == 32 {
            result.unwrap();
        } else {
            check_error(result, "PROTOCOL_NESTING_EXCEEDED");
        }
    }
    let body = format!(r#"{{"deep":{}0{}}}"#, "[".repeat(32), "]".repeat(32));
    check_error(
        decode_server_frame(wrap("unknown", &body).as_bytes()),
        "PROTOCOL_NESTING_EXCEEDED",
    );
}

#[test]
fn exact_number_token_limits_and_extreme_exponents_are_lossless() {
    for token in ["9".repeat(128), format!("1e{}", "9".repeat(126))] {
        assert_eq!(token.len(), 128);
        let payload = format!(r#"{{"n":{token}}}"#);
        let send = request(&payload).unwrap();
        assert_eq!(
            decode_send(&encode_send(&send).unwrap())
                .unwrap()
                .payload
                .get(),
            payload
        );
        assert!(same_intent(&send, &send).unwrap());
        check_error(
            request(&format!(r#"{{"n":{token}9}}"#)),
            "PROTOCOL_INVALID_JSON",
        );
    }
    let nines = "9".repeat(123);
    let power = format!("1{}", "0".repeat(123));
    for (left, right) in [
        (format!("10e{nines}"), format!("1e{power}")),
        (format!("0.1e-{nines}"), format!("1e-{power}")),
        (format!("10e-{power}"), format!("1e-{nines}")),
        (format!("0.1e{power}"), format!("1e{nines}")),
        ("10e-1".into(), "1".into()),
        ("0.1e1".into(), "1".into()),
        (
            "-0e999999999999999999".into(),
            "0e-999999999999999999".into(),
        ),
    ] {
        let a = request(&format!(r#"{{"n":{left}}}"#)).unwrap();
        let b = request(&format!(r#"{{"n":{right}}}"#)).unwrap();
        assert!(same_intent(&a, &b).unwrap(), "{left} vs {right}");
        assert!(same_intent(&b, &a).unwrap());
    }
    let left = request(r#"{"n":9007199254740992}"#).unwrap();
    let right = request(r#"{"n":9007199254740993}"#).unwrap();
    assert!(!same_intent(&left, &right).unwrap());
}

#[test]
fn public_encoders_reject_invalid_fragments_and_bound_allocation() {
    assert!(RawValue::from_string(r#"{},"kind":"send_ack"}"#.into()).is_err());
    let mut send = request("{}").unwrap();
    for (payload, code) in [
        (r#"{"a":1,"\u0061":2}"#, "PROTOCOL_INVALID_JSON"),
        ("[]", "PROTOCOL_INVALID_MESSAGE"),
        ("null", "PROTOCOL_INVALID_MESSAGE"),
    ] {
        send.payload = RawValue::from_string(payload.into()).unwrap();
        check_error(encode_send(&send), code);
        check_error(same_intent(&send, &send), code);
    }
    send.payload = RawValue::from_string(format!(r#"{{"n":{}}}"#, "9".repeat(129))).unwrap();
    check_error(encode_send(&send), "PROTOCOL_INVALID_JSON");
    send.payload =
        RawValue::from_string(format!(r#"{{"pad":"{}"}}"#, "x".repeat(MAX_SEND_BYTES))).unwrap();
    check_error(encode_send(&send), "MESSAGE_TOO_LARGE");
    send = request("{}").unwrap();
    send.client_msg_id = "x".repeat(MAX_SEND_BYTES + 1);
    check_error(encode_send(&send), "MESSAGE_TOO_LARGE");
    let mut oversized = ack();
    oversized.sender_id = "x".repeat(MAX_RESULT_BYTES + 1);
    check_error(
        encode_server_frame(&ServerFrame::Ack(oversized)),
        "MESSAGE_TOO_LARGE",
    );
    let oversized = SendError {
        client_msg_id: "c1".into(),
        conversation_id: "room1".into(),
        code: "X".repeat(MAX_RESULT_BYTES + 1),
    };
    check_error(
        encode_server_frame(&ServerFrame::Error(oversized)),
        "MESSAGE_TOO_LARGE",
    );
}

#[test]
fn decoded_nul_unicode_and_text_byte_bounds_are_preserved() {
    for payload in [
        r#"{"text":"a\u0000b"}"#,
        r#"{"\u0000":["\u0000"],"$serde_json::private::Number":"ordinary"}"#,
    ] {
        let send = request(payload).unwrap();
        let saved = send.payload.get().to_owned();
        assert_eq!(
            decode_send(&encode_send(&send).unwrap())
                .unwrap()
                .payload
                .get(),
            saved
        );
        assert!(same_intent(&send, &send).unwrap());
        assert_eq!(send.payload.get(), saved);
    }
    for size in [
        newim_protocol::MAX_TEXT_BYTES,
        newim_protocol::MAX_TEXT_BYTES + 1,
    ] {
        let wire = wrap(
            "send",
            &body(&format!(r#"{{"text":"{}"}}"#, "x".repeat(size)))
                .replace("\"future\"", "\"text\""),
        );
        let result = decode_send(wire.as_bytes());
        if size == newim_protocol::MAX_TEXT_BYTES {
            encode_send(&result.unwrap()).unwrap();
        } else {
            check_error(result, "PROTOCOL_INVALID_MESSAGE");
        }
    }
    let wire = wrap(
        "send",
        &body(&format!(r#"{{"text":"{}"}}"#, "界".repeat(5462))).replace("\"future\"", "\"text\""),
    );
    check_error(decode_send(wire.as_bytes()), "PROTOCOL_INVALID_MESSAGE");
}

#[test]
fn maximum_send_reserves_space_for_authoritative_result_fields() {
    let mut send = request(r#"{"pad":""}"#).unwrap();
    send.client_msg_id = "c".repeat(128);
    send.conversation_id = "r".repeat(128);
    send.message_type = "f".repeat(64);
    send.version = i32::MAX as u32;
    let encoded = encode_send(&send).unwrap();
    let outer: std::collections::BTreeMap<String, Box<RawValue>> =
        serde_json::from_slice(&encoded).unwrap();
    let padding = MAX_SEND_BYTES - outer["body"].get().len();
    send.payload =
        RawValue::from_string(format!(r#"{{"pad":"{}"}}"#, "x".repeat(padding))).unwrap();
    let encoded = encode_send(&send).unwrap();
    let outer: std::collections::BTreeMap<String, Box<RawValue>> =
        serde_json::from_slice(&encoded).unwrap();
    assert_eq!(outer["body"].get().len(), MAX_SEND_BYTES);
    let sender = "u".repeat(128);
    let message = newim_protocol::Message {
        protocol_version: send.protocol_version,
        version: send.version,
        client_msg_id: send.client_msg_id.clone(),
        server_msg_id: "m".repeat(128),
        conversation_id: send.conversation_id.clone(),
        conversation_seq: i64::MAX.to_string(),
        sender_id: sender.clone(),
        message_type: send.message_type.clone(),
        server_time: i64::MAX.to_string(),
        payload: send.payload.clone(),
    };
    let bare = newim_protocol::encode(&message).unwrap();
    assert!(bare.len() > MAX_SEND_BYTES && bare.len() <= newim_protocol::MAX_WIRE_BYTES);
    let frame = ServerFrame::Message(message);
    encode_server_frame(&frame).unwrap();
    correlate(&send, &sender, &frame).unwrap();
    let original = send.payload.get().to_owned();
    send.payload =
        RawValue::from_string(format!(r#"{{"pad":"{}"}}"#, "x".repeat(padding + 1))).unwrap();
    check_error(encode_send(&send), "MESSAGE_TOO_LARGE");
    let ServerFrame::Message(message) = frame else {
        unreachable!()
    };
    assert_eq!(message.payload.get(), original);
}

#[test]
fn larger_persisted_payload_is_valid_but_does_not_match_small_send() {
    let send = request("{}").unwrap();
    let ServerFrame::Message(mut message) = message() else {
        unreachable!()
    };
    message.payload =
        RawValue::from_string(format!(r#"{{"pad":"{}"}}"#, "x".repeat(MAX_SEND_BYTES))).unwrap();
    assert!(message.payload.get().len() > MAX_SEND_BYTES);
    let encoded = newim_protocol::encode(&message).unwrap();
    assert!(encoded.len() < newim_protocol::MAX_WIRE_BYTES);
    let frame = ServerFrame::Message(message);
    encode_server_frame(&frame).unwrap();
    check_error(correlate(&send, "u1", &frame), "SEND_CORRELATION_MISMATCH");
}

#[test]
fn encoder_full_size_precedes_duplicate_fragment_after_escaping() {
    let mut send = request("{}").unwrap();
    send.client_msg_id = "\0".repeat(20_000);
    send.payload = RawValue::from_string(r#"{"a":1,"a":2}"#.into()).unwrap();
    check_error(encode_send(&send), "MESSAGE_TOO_LARGE");
    // Under the frame limit, the duplicate is encountered before invalid ID shape.
    send.client_msg_id = "\0".repeat(100);
    check_error(encode_send(&send), "PROTOCOL_INVALID_JSON");
}

#[test]
fn wire_string_escaping_keeps_size_and_shape_priority() {
    for character in ["<", ">", "&", "\u{2028}", "\u{2029}"] {
        let mut send = request("{}").unwrap();
        send.client_msg_id = character.repeat(11_500);
        check_error(encode_send(&send), "PROTOCOL_INVALID_MESSAGE");
        let mut result = ack();
        result.client_msg_id = character.repeat(800);
        check_error(
            encode_server_frame(&ServerFrame::Ack(result)),
            "PROTOCOL_INVALID_MESSAGE",
        );
    }
    for value in [
        "a\"", "a\\", "a\n", "a\r", "a\t", "a\u{8}", "a\u{c}", "a\0", "a\u{1f}",
    ] {
        let mut send = request("{}").unwrap();
        send.client_msg_id = value.into();
        check_error(encode_send(&send), "PROTOCOL_INVALID_MESSAGE");
    }
}
