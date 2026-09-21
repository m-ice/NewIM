#![allow(dead_code)]
use std::collections::BTreeSet;

use newim_protocol::send::{self, Ack, Send, ServerFrame};
use serde_json::Value;

pub fn cases(name: &str) -> Vec<Value> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../tests/compatibility/send-ack/fixtures")
        .join(format!("{name}.json"));
    // Fixture metadata contains wire strings; opaque numbers are never deserialized into Value.
    let value: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let cases = value.as_array().expect("fixture array").clone();
    assert!(!cases.is_empty(), "empty fixture group {name}");
    let mut ids = BTreeSet::new();
    for case in &cases {
        let id = case["id"].as_str().expect("case id");
        assert!(
            !id.is_empty() && ids.insert(id),
            "missing or duplicate id: {id}"
        );
        let expected = case.get("error").expect("explicit error or null required");
        assert!(expected.is_null() || expected.as_str().is_some_and(|s| !s.is_empty()));
    }
    println!("{name}: {} shared cases", cases.len());
    cases
}
pub fn bytes(case: &Value) -> Vec<u8> {
    if let Some(wire) = case.get("wire") {
        assert!(case.get("wireHex").is_none(), "ambiguous fixture wire");
        return wire.as_str().expect("wire string").as_bytes().to_vec();
    }
    let hex = case["wireHex"].as_str().expect("wire or wireHex required");
    assert!(hex.is_ascii() && hex.len().is_multiple_of(2));
    (0..hex.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
        .collect()
}
pub fn wrap(kind: &str, body: &str) -> String {
    format!(r#"{{"protocolVersion":1,"kind":"{kind}","body":{body}}}"#)
}
pub fn body(payload: &str) -> String {
    format!(
        r#"{{"clientMsgId":"c1","conversationId":"room1","version":1,"type":"future","payload":{payload}}}"#
    )
}
pub fn request(payload: &str) -> Result<Send, send::Error> {
    send::decode_send(wrap("send", &body(payload)).as_bytes())
}
pub fn ack() -> Ack {
    Ack {
        client_msg_id: "c1".into(),
        conversation_id: "room1".into(),
        sender_id: "u1".into(),
        server_msg_id: "m1".into(),
        conversation_seq: "8".into(),
        server_time: "99".into(),
    }
}
pub fn message() -> ServerFrame {
    ServerFrame::Message(newim_protocol::decode(br#"{"protocolVersion":1,"clientMsgId":"c1","conversationId":"room1","senderId":"u1","serverMsgId":"m1","conversationSeq":"8","serverTime":"99","version":1,"type":"future","payload":{"n":1}}"#).unwrap())
}
pub fn check_error<T: std::fmt::Debug>(result: Result<T, send::Error>, expected: &str) {
    assert_eq!(result.unwrap_err().code(), expected);
}
