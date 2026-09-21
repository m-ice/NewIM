use newim_protocol::{Error, MAX_TEXT_BYTES, MAX_WIRE_BYTES, Message, decode, encode, validate};
use serde_json::value::RawValue;

fn wire(payload: &str) -> String {
    format!(
        r#"{{"protocolVersion":1,"version":1,"clientMsgId":"client_1","serverMsgId":"server_1","conversationId":"c_1","conversationSeq":"1088","senderId":"u_1","type":"text","serverTime":"0","payload":{payload}}}"#
    )
}

fn sample() -> Message {
    decode(wire(r#"{"text":"你好"}"#).as_bytes()).unwrap()
}

#[test]
fn raw_payload_numbers_and_private_looking_keys_survive_without_interpretation() {
    let payload = r#"{"text":"hi","$serde_json::private::Number":"ordinary","nested":{"$serde_json::private::Number":"still ordinary"},"big":9007199254740993,"decimal":1.2300,"exponent":1e99999999999999999999999999999999999999999999,"negativeZero":-0}"#;
    let message = decode(wire(payload).as_bytes()).unwrap();
    let encoded = encode(&message).unwrap();
    let again = decode(&encoded).unwrap();
    assert_eq!(again.payload.get(), payload);
    assert!(std::str::from_utf8(&encoded).unwrap().contains("1.2300"));
    assert!(
        std::str::from_utf8(&encoded)
            .unwrap()
            .contains("9007199254740993")
    );
}

#[test]
fn numeric_token_length_limit_does_not_limit_finite_float_range() {
    for digits in [128, 129] {
        let payload = format!(r#"{{"text":"x","n":{}}}"#, "9".repeat(digits));
        let result = decode(wire(&payload).as_bytes());
        if digits == 128 {
            let message = result.unwrap();
            assert_eq!(
                decode(&encode(&message).unwrap()).unwrap().payload.get(),
                payload
            );
        } else {
            assert_eq!(result.unwrap_err(), Error::InvalidJson);
        }
    }
}

#[test]
fn sequence_and_time_use_canonical_bigint_strings() {
    for value in [
        "0",
        "9",
        "10",
        "99",
        "100",
        "9007199254740991",
        "9007199254740992",
        "9007199254740993",
        "9223372036854775807",
    ] {
        let mut message = sample();
        message.conversation_seq = value.into();
        message.server_time = value.into();
        let decoded = decode(&encode(&message).unwrap()).unwrap();
        assert_eq!(decoded.conversation_seq, value);
        assert_eq!(decoded.server_time, value);
    }
    for value in [
        "",
        "00",
        "01",
        "-0",
        "-1",
        "+1",
        " 1",
        "1.0",
        "1e3",
        "9223372036854775808",
    ] {
        let mut message = sample();
        message.conversation_seq = value.into();
        assert_eq!(validate(&message), Err(Error::InvalidMessage), "{value}");
        message.conversation_seq = "1".into();
        message.server_time = value.into();
        assert_eq!(validate(&message), Err(Error::InvalidMessage), "{value}");
    }
}

#[test]
fn exact_byte_and_container_depth_boundaries_are_enforced() {
    let valid = wire(r#"{"text":"x"}"#);
    let mut boundary = valid.into_bytes();
    boundary.resize(MAX_WIRE_BYTES, b' ');
    assert!(decode(&boundary).is_ok());
    boundary.push(b' ');
    assert_eq!(decode(&boundary).unwrap_err(), Error::MessageTooLarge);
    // Envelope + payload = 2 containers; arrays add 30 or 31 more.
    for depth in [30, 31] {
        let payload = format!(
            r#"{{"text":"x","extra":{}0{}}}"#,
            "[".repeat(depth),
            "]".repeat(depth)
        );
        let result = decode(wire(&payload).as_bytes());
        if depth == 30 {
            assert!(result.is_ok());
        } else {
            assert_eq!(result.unwrap_err(), Error::NestingExceeded);
        }
    }
}

#[test]
fn encoded_size_and_public_field_validation_cannot_be_bypassed() {
    let mut message = sample();
    message.version = 0;
    assert_eq!(encode(&message).unwrap_err(), Error::InvalidMessage);
    message.version = i32::MAX as u32 + 1;
    assert_eq!(encode(&message).unwrap_err(), Error::InvalidMessage);
    message.version = 1;
    message.protocol_version = 2;
    assert_eq!(validate(&message), Err(Error::UnsupportedVersion));
    message.client_msg_id.clear();
    assert_eq!(validate(&message), Err(Error::InvalidMessage));
    message = sample();
    message.payload = RawValue::from_string(format!(
        r#"{{"text":"x","extra":"{}"}}"#,
        "a".repeat(MAX_WIRE_BYTES)
    ))
    .unwrap();
    assert_eq!(encode(&message).unwrap_err(), Error::MessageTooLarge);
    message.payload = RawValue::from_string(r#"{"text":"x","text":"y"}"#.into()).unwrap();
    assert_eq!(encode(&message).unwrap_err(), Error::InvalidJson);
}

#[test]
fn text_bytes_are_bounded_without_normalization_or_trimming() {
    for size in [MAX_TEXT_BYTES, MAX_TEXT_BYTES + 1] {
        let text = "x".repeat(size);
        let result = decode(wire(&format!(r#"{{"text":"{text}"}}"#)).as_bytes());
        if size == MAX_TEXT_BYTES {
            assert!(result.is_ok());
        } else {
            assert_eq!(result.unwrap_err(), Error::InvalidMessage);
        }
    }
    assert!(decode(wire(r#"{"text":"   "}"#).as_bytes()).is_ok());
    assert!(decode(wire(r#"{"text":"e\u0301"}"#).as_bytes()).is_ok());
    assert_eq!(
        decode(wire(r#"{"text":""}"#).as_bytes()).unwrap_err(),
        Error::InvalidMessage
    );
    // Each character is 3 UTF-8 bytes; code-point counting must not accept it.
    assert_eq!(
        decode(wire(&format!(r#"{{"text":"{}"}}"#, "界".repeat(5462))).as_bytes()).unwrap_err(),
        Error::InvalidMessage
    );
}

#[test]
fn unknown_fields_still_receive_strict_json_validation() {
    for payload in [
        r#"{"text":"x","extra":{"a":1,"\u0061":2}}"#,
        r#"{"text":"x","extra":"\uD800"}"#,
        r#"{"text":"x","extra":"\uDC00"}"#,
        r#"{"text":"x","extra":01}"#,
        r#"{"text":"x","extra":1.}"#,
        r#"{"text":"x","extra":1e+}"#,
        r#"{"text":"x","extra":NaN}"#,
        r#"{"text":"x","extra":[0,]}"#,
    ] {
        assert_eq!(
            decode(wire(payload).as_bytes()).unwrap_err(),
            Error::InvalidJson,
            "{payload}"
        );
    }
    assert!(decode(wire(r#"{"text":"\uD83D\uDE00"}"#).as_bytes()).is_ok());
    let invalid_utf8 = wire(r#"{"text":"x"}"#).replace('x', "PLACEHOLDER");
    let mut bytes = invalid_utf8.into_bytes();
    let position = bytes
        .windows(11)
        .position(|part| part == b"PLACEHOLDER")
        .unwrap();
    bytes[position] = 0xff;
    assert_eq!(decode(&bytes).unwrap_err(), Error::InvalidJson);
}

#[test]
fn envelope_shape_precedes_version_and_version_precedes_known_payload() {
    let base = wire(r#"{}"#).replace("\"protocolVersion\":1", "\"protocolVersion\":2");
    assert_eq!(
        decode(base.as_bytes()).unwrap_err(),
        Error::UnsupportedVersion
    );
    for malformed in [
        base.replace("\"clientMsgId\":\"client_1\",", ""),
        base.replace("\"payload\":{}", "\"payload\":null"),
    ] {
        assert_eq!(
            decode(malformed.as_bytes()).unwrap_err(),
            Error::InvalidMessage
        );
    }
    let future = wire(r#"{"future":1e400}"#).replace("\"version\":1", "\"version\":2");
    let message = decode(future.as_bytes()).unwrap();
    assert!(!message.supported());
    assert_eq!(
        decode(&encode(&message).unwrap()).unwrap().payload.get(),
        r#"{"future":1e400}"#
    );
}

#[test]
fn deterministic_mutation_corpus_never_panics_and_roundtrips_every_accepted_value() {
    let original = wire(r#"{"text":"safe","extra":9007199254740993}"#).into_bytes();
    for length in 0..original.len() {
        assert!(decode(&original[..length]).is_err());
    }
    for position in 0..original.len() {
        for byte in [0, b'"', b'\\', b'{', b']', b'0', 0x7f, 0xff] {
            let mut changed = original.clone();
            changed[position] = byte;
            if let Ok(message) = decode(&changed) {
                let again = decode(&encode(&message).unwrap()).unwrap();
                assert_eq!(message.payload.get(), again.payload.get());
                assert_eq!(message.conversation_seq, again.conversation_seq);
            }
        }
    }
}

#[test]
fn outbound_combined_error_precedence_matches_wire_validation() {
    let wire = include_str!("../../fixtures/cases.json");
    let cases: serde_json::Value = serde_json::from_str(wire).unwrap();
    let mut message =
        newim_protocol::decode(cases[0]["wire"].as_str().unwrap().as_bytes()).unwrap();
    message.client_msg_id.clear();
    message.payload =
        serde_json::value::RawValue::from_string(r#"{"text":"a","text":"b"}"#.to_owned()).unwrap();
    assert_eq!(
        newim_protocol::encode(&message).unwrap_err(),
        newim_protocol::Error::InvalidJson
    );
    message.client_msg_id = "x".repeat(newim_protocol::MAX_WIRE_BYTES + 1);
    assert_eq!(
        newim_protocol::encode(&message).unwrap_err(),
        newim_protocol::Error::MessageTooLarge
    );
}
