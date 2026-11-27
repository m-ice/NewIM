mod send_common;

use newim_protocol::SenderType;
use newim_protocol::send::*;
use send_common::*;

#[test]
fn shared_frame_golden_roundtrips_without_payload_mutation() {
    for case in cases("frames") {
        let name = case["id"].as_str().unwrap();
        let bytes = bytes(&case);
        let expected = case.get("error").unwrap();
        match case["direction"].as_str().expect("direction") {
            "send" => {
                let result = decode_send(&bytes);
                if let Some(expected) = expected.as_str() {
                    assert_eq!(result.unwrap_err().code(), expected, "{name}");
                    continue;
                }
                let first = result.unwrap_or_else(|error| panic!("{name}: {error}"));
                let payload = first.payload.get().to_owned();
                let encoded = encode_send(&first).unwrap();
                let second = decode_send(&encoded).unwrap();
                assert_eq!(payload, first.payload.get(), "{name}: input mutation");
                assert_eq!(payload, second.payload.get(), "{name}: opaque payload");
                assert_eq!(
                    encode_send(&second).unwrap(),
                    encoded,
                    "{name}: canonical fields"
                );
                assert!(same_intent(&first, &second).unwrap(), "{name}");
            }
            "server" => {
                let result = decode_server_frame(&bytes);
                if let Some(expected) = expected.as_str() {
                    assert_eq!(result.unwrap_err().code(), expected, "{name}");
                    continue;
                }
                let first = result.unwrap_or_else(|error| panic!("{name}: {error}"));
                let payload = match &first {
                    ServerFrame::Message(message) => Some(message.payload.get().to_owned()),
                    _ => None,
                };
                let encoded = encode_server_frame(&first).unwrap();
                let second = decode_server_frame(&encoded).unwrap();
                if let Some(expected) = case["senderType"].as_str() {
                    for frame in [&first, &second] {
                        let sender_type = match frame {
                            ServerFrame::Ack(ack) => ack.sender_type,
                            ServerFrame::Message(message) => message.sender_type,
                            ServerFrame::Error(_) => None,
                        };
                        assert_eq!(
                            sender_type.map(SenderType::as_str),
                            Some(expected),
                            "{name}"
                        );
                    }
                }
                if let (Some(payload), ServerFrame::Message(a), ServerFrame::Message(b)) =
                    (payload, &first, &second)
                {
                    assert_eq!(payload, a.payload.get(), "{name}: input mutation");
                    assert_eq!(payload, b.payload.get(), "{name}: opaque payload");
                }
                assert_eq!(
                    encode_server_frame(&second).unwrap(),
                    encoded,
                    "{name}: canonical fields"
                );
            }
            other => panic!("unknown direction: {other}"),
        }
    }
}

#[test]
fn shared_exact_intent_equality_is_symmetric_and_preserves_tokens() {
    for case in cases("intents") {
        let name = case["id"].as_str().unwrap();
        let equal = case["equal"].as_bool().expect("explicit equality required");
        let left = case["left"].as_str().expect("left raw payload");
        let right = case["right"].as_str().expect("right raw payload");
        let result = request(left).and_then(|a| request(right).and_then(|b| same_intent(&a, &b)));
        if let Some(error) = case["error"].as_str() {
            assert_eq!(result.unwrap_err().code(), error, "{name}");
        } else {
            assert_eq!(result.unwrap(), equal, "{name}");
            let a = request(left).unwrap();
            let b = request(right).unwrap();
            let a_raw = a.payload.get().to_owned();
            let b_raw = b.payload.get().to_owned();
            assert_eq!(same_intent(&b, &a).unwrap(), equal, "{name}: reversed");
            assert_eq!(a.payload.get(), a_raw, "{name}");
            assert_eq!(b.payload.get(), b_raw, "{name}");
            assert_eq!(
                decode_send(&encode_send(&a).unwrap())
                    .unwrap()
                    .payload
                    .get(),
                a_raw,
                "{name}"
            );
        }
    }
}

#[test]
fn human_sender_type_preserves_legacy_frame_bytes() {
    for case in cases("frames") {
        let id = case["id"].as_str().unwrap();
        if id != "ack" && id != "message" {
            continue;
        }
        let wire = String::from_utf8(bytes(&case)).unwrap();
        let legacy = decode_server_frame(wire.as_bytes()).unwrap();
        let human = wire.replacen(r#""senderId":"#, r#""senderType":"HUMAN","senderId":"#, 1);
        let frame = decode_server_frame(human.as_bytes()).unwrap();
        let sender_type = match &frame {
            ServerFrame::Ack(ack) => ack.sender_type,
            ServerFrame::Message(message) => message.sender_type,
            ServerFrame::Error(_) => panic!("unexpected frame kind"),
        };
        assert_eq!(sender_type, Some(SenderType::Human));
        assert_eq!(
            encode_server_frame(&frame).unwrap(),
            encode_server_frame(&legacy).unwrap(),
            "{id}"
        );
    }
}

#[test]
fn sender_type_counts_against_message_byte_limit() {
    use newim_protocol::{Error, MAX_WIRE_BYTES, encode};
    use serde_json::value::RawValue;

    let ServerFrame::Message(mut message) = message() else {
        panic!("expected message frame");
    };
    message.sender_type = Some(SenderType::AiAgent);
    message.payload = RawValue::from_string(r#"{"n":""}"#.into()).unwrap();
    let count = MAX_WIRE_BYTES - encode(&message).unwrap().len();
    message.payload = RawValue::from_string(format!(r#"{{"n":"{}"}}"#, "a".repeat(count))).unwrap();
    assert_eq!(encode(&message).unwrap().len(), MAX_WIRE_BYTES);
    message.payload =
        RawValue::from_string(format!(r#"{{"n":"{}"}}"#, "a".repeat(count + 1))).unwrap();
    assert_eq!(encode(&message), Err(Error::MessageTooLarge));
}
