mod send_common;

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
