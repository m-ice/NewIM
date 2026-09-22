use newim_protocol::media::{self, MAX_MEDIA_PAYLOAD_BYTES, MAX_MEDIA_SIZE, MediaMetadata};
use newim_protocol::{Message, decode, encode};
use serde_json::value::RawValue;

const FIXTURES: &str = include_str!("../../../../tests/compatibility/media/fixtures/cases.json");

fn metadata_from(value: &serde_json::Value) -> MediaMetadata {
    MediaMetadata {
        media_key: value["mediaKey"].as_str().unwrap().to_owned(),
        kind: value["kind"].as_str().unwrap().to_owned(),
        content_type: value["contentType"].as_str().unwrap().to_owned(),
        size: value["size"].as_str().unwrap().to_owned(),
        sha256: value["sha256"].as_str().unwrap().to_owned(),
    }
}

fn message(payload: &str) -> Message {
    Message {
        protocol_version: 1,
        version: 1,
        client_msg_id: "client_media".into(),
        server_msg_id: "server_media".into(),
        conversation_id: "conversation_media".into(),
        conversation_seq: "1".into(),
        sender_id: "user_media".into(),
        message_type: "media".into(),
        server_time: "1800000000000".into(),
        payload: RawValue::from_string(payload.to_owned()).unwrap(),
    }
}

#[test]
fn media_golden_round_trip_and_send() {
    let fixtures: serde_json::Value = serde_json::from_str(FIXTURES).unwrap();
    let expected = metadata_from(&fixtures["golden"]);
    let payload = media::encode(&expected).unwrap();
    assert_eq!(media::decode(&payload).unwrap(), expected);
    let wire = encode(&message(&payload)).unwrap();
    let decoded = decode(&wire).unwrap();
    assert!(decoded.supported());
    assert_eq!(decoded.payload.get(), payload);

    let send = newim_protocol::send::Send {
        protocol_version: 1,
        client_msg_id: "client_media".into(),
        conversation_id: "conversation_media".into(),
        version: 1,
        message_type: "media".into(),
        payload: RawValue::from_string(payload).unwrap(),
    };
    let send_wire = newim_protocol::send::encode_send(&send).unwrap();
    assert!(
        newim_protocol::send::decode_send(&send_wire)
            .unwrap()
            .supported()
    );
}

#[test]
fn media_rejects_closed_fields_and_unknown_values() {
    let fixtures: serde_json::Value = serde_json::from_str(FIXTURES).unwrap();
    for test_case in fixtures["invalid"].as_array().unwrap() {
        let payload = test_case["payload"].as_str().unwrap();
        let expected = test_case["error"].as_str().unwrap();
        let message = message(payload);
        assert_eq!(encode(&message).unwrap_err().code(), expected);
        let send = newim_protocol::send::Send {
            protocol_version: 1,
            client_msg_id: "client_media".into(),
            conversation_id: "conversation_media".into(),
            version: 1,
            message_type: "media".into(),
            payload: RawValue::from_string(payload.to_owned()).unwrap(),
        };
        assert_eq!(
            newim_protocol::send::encode_send(&send).unwrap_err().code(),
            expected
        );
    }
}

#[test]
fn media_exact_boundaries_and_text_ack_compatibility() {
    let fixtures: serde_json::Value = serde_json::from_str(FIXTURES).unwrap();
    let mut base = metadata_from(&fixtures["golden"]);
    let mime128 = format!("{}/{}", "a".repeat(63), "b".repeat(64));
    let mime129 = format!("{}/{}", "a".repeat(64), "b".repeat(64));
    base.content_type = mime128;
    base.size = "1".into();
    assert!(media::validate(&media::encode(&base).unwrap()).is_ok());
    base.content_type = mime129;
    assert!(media::encode(&base).is_err());
    base.content_type = "image/jpeg".into();
    base.size = MAX_MEDIA_SIZE.to_string();
    assert!(media::validate(&media::encode(&base).unwrap()).is_ok());
    base.size = (MAX_MEDIA_SIZE + 1).to_string();
    assert!(media::encode(&base).is_err());

    base.size = "1".into();
    let raw = media::encode(&base).unwrap();
    let exact = raw.replacen(
        '{',
        &format!(
            "{{{}{}",
            "",
            " ".repeat(MAX_MEDIA_PAYLOAD_BYTES - raw.len())
        ),
        1,
    );
    assert_eq!(exact.len(), MAX_MEDIA_PAYLOAD_BYTES);
    assert!(media::validate(&exact).is_ok());
    let excessive = raw.replacen(
        '{',
        &format!("{{{}", " ".repeat(MAX_MEDIA_PAYLOAD_BYTES + 1 - raw.len())),
        1,
    );
    assert_eq!(excessive.len(), MAX_MEDIA_PAYLOAD_BYTES + 1);
    assert_eq!(
        media::validate(&excessive).unwrap_err().code(),
        "MESSAGE_TOO_LARGE"
    );

    let text = newim_protocol::send::Send {
        protocol_version: 1,
        client_msg_id: "client_text".into(),
        conversation_id: "conversation_text".into(),
        version: 1,
        message_type: "text".into(),
        payload: RawValue::from_string(r#"{"text":"hello","future":1}"#.to_owned()).unwrap(),
    };
    let text_wire = newim_protocol::send::encode_send(&text).unwrap();
    assert!(
        newim_protocol::send::decode_send(&text_wire)
            .unwrap()
            .supported()
    );
    let ack = newim_protocol::send::ServerFrame::Ack(newim_protocol::send::Ack {
        client_msg_id: "client_text".into(),
        conversation_id: "conversation_text".into(),
        sender_id: "user_text".into(),
        server_msg_id: "server_text".into(),
        conversation_seq: "1".into(),
        server_time: "1800000000000".into(),
    });
    let frame = newim_protocol::send::encode_server_frame(&ack).unwrap();
    assert!(matches!(
        newim_protocol::send::decode_server_frame(&frame).unwrap(),
        newim_protocol::send::ServerFrame::Ack(_)
    ));
}
