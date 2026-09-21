mod send_common;
use newim_protocol::send::*;
use send_common::*;

#[test]
fn shared_error_precedence_and_stable_codes() {
    for case in cases("errors") {
        let name = case["id"].as_str().unwrap();
        let expected = case["error"].as_str().expect("error fixture must fail");
        let wire = bytes(&case);
        let actual = match case["direction"].as_str().unwrap() {
            "send" => decode_send(&wire).unwrap_err(),
            "server" => decode_server_frame(&wire).unwrap_err(),
            other => panic!("unknown direction {other}"),
        };
        assert_eq!(actual.code(), expected, "{name}");
        assert_eq!(actual.to_string(), expected, "diagnostics expose only code");
    }
}

#[test]
fn trusted_identity_and_intent_are_required_for_correlation() {
    let send = request(r#"{"n":1.0}"#).unwrap();
    let result = ServerFrame::Ack(ack());
    correlate(&send, "u1", &result).unwrap();
    correlate(&send, "u1", &message()).unwrap();
    check_error(correlate(&send, "u2", &result), "SEND_CORRELATION_MISMATCH");
    check_error(
        correlate(&send, "u2", &message()),
        "SEND_CORRELATION_MISMATCH",
    );
    check_error(
        correlate(&send, "bad sender", &result),
        "PROTOCOL_INVALID_MESSAGE",
    );
    for index in 0..3 {
        let mut altered = ack();
        match index {
            0 => altered.client_msg_id = "other".into(),
            1 => altered.conversation_id = "other".into(),
            _ => altered.sender_id = "other".into(),
        }
        check_error(
            correlate(&send, "u1", &ServerFrame::Ack(altered)),
            "SEND_CORRELATION_MISMATCH",
        );
    }
    for index in 0..6 {
        let ServerFrame::Message(mut altered) = message() else {
            unreachable!()
        };
        match index {
            0 => altered.client_msg_id = "other".into(),
            1 => altered.conversation_id = "other".into(),
            2 => altered.sender_id = "other".into(),
            3 => altered.message_type = "other".into(),
            4 => altered.version = 2,
            _ => {
                altered.payload =
                    serde_json::value::RawValue::from_string(r#"{"n":2}"#.into()).unwrap()
            }
        }
        check_error(
            correlate(&send, "u1", &ServerFrame::Message(altered)),
            "SEND_CORRELATION_MISMATCH",
        );
    }
    let mut error = SendError {
        client_msg_id: "c1".into(),
        conversation_id: "room1".into(),
        code: "UNKNOWN_FUTURE".into(),
    };
    correlate(&send, "u1", &ServerFrame::Error(error.clone())).unwrap();
    error.client_msg_id = "other".into();
    check_error(
        correlate(&send, "u1", &ServerFrame::Error(error.clone())),
        "SEND_CORRELATION_MISMATCH",
    );
    error.client_msg_id = "c1".into();
    error.conversation_id = "other".into();
    check_error(
        correlate(&send, "u1", &ServerFrame::Error(error)),
        "SEND_CORRELATION_MISMATCH",
    );
}

#[test]
fn ack_and_message_arrival_orders_preserve_the_same_result() {
    let ack_frame = ServerFrame::Ack(ack());
    let message_frame = message();
    for (left, right) in [
        (&ack_frame, &message_frame),
        (&message_frame, &ack_frame),
        (&ack_frame, &ack_frame),
        (&message_frame, &message_frame),
    ] {
        compare_results(left, right).unwrap();
    }
    for index in 0..6 {
        let mut changed = ack();
        match index {
            0 => changed.client_msg_id = "other".into(),
            1 => changed.conversation_id = "other".into(),
            2 => changed.sender_id = "other".into(),
            3 => changed.server_msg_id = "other".into(),
            4 => changed.conversation_seq = "9".into(),
            _ => changed.server_time = "100".into(),
        }
        let changed = ServerFrame::Ack(changed);
        let code = if index < 3 {
            "SEND_CORRELATION_MISMATCH"
        } else {
            "SEND_RESULT_CONFLICT"
        };
        for base in [&ack_frame, &message_frame] {
            check_error(compare_results(base, &changed), code);
            check_error(compare_results(&changed, base), code);
        }
    }
    let late_error = ServerFrame::Error(SendError {
        client_msg_id: "c1".into(),
        conversation_id: "room1".into(),
        code: "SERVER_TEMPORARY_UNAVAILABLE".into(),
    });
    check_error(
        compare_results(&ack_frame, &late_error),
        "PROTOCOL_INVALID_MESSAGE",
    );
    check_error(
        compare_results(&late_error, &message_frame),
        "PROTOCOL_INVALID_MESSAGE",
    );
}

#[test]
fn retries_are_conservatively_derived_from_local_codes() {
    assert_eq!(
        disposition("SERVER_TEMPORARY_UNAVAILABLE"),
        Disposition::RetrySameIntent
    );
    for code in ["AUTH_REQUIRED", "AUTH_TOKEN_EXPIRED"] {
        assert_eq!(disposition(code), Disposition::AuthRecovery);
    }
    for code in [
        "SEND_ID_CONFLICT",
        "MESSAGE_PERMISSION_DENIED",
        "MESSAGE_UNSUPPORTED_SCHEMA",
        "MESSAGE_TOO_LARGE",
        "PROTOCOL_INVALID_MESSAGE",
        "PROTOCOL_UNSUPPORTED_VERSION",
        "UNKNOWN_FUTURE",
        "",
        "server_temporary_unavailable",
    ] {
        assert_eq!(disposition(code), Disposition::StopAutomaticRetry, "{code}");
    }
}

#[test]
fn public_structs_cannot_bypass_validation_and_key_is_not_intent() {
    let send = request(r#"{"n":1}"#).unwrap();
    let mut changed = send.clone();
    changed.client_msg_id = "different".into();
    assert!(same_intent(&send, &changed).unwrap());
    changed.conversation_id = "different".into();
    assert!(!same_intent(&send, &changed).unwrap());
    changed = send.clone();
    changed.version = 2;
    assert!(!same_intent(&send, &changed).unwrap());
    changed = send.clone();
    changed.message_type = "different".into();
    assert!(!same_intent(&send, &changed).unwrap());
    changed = send.clone();
    changed.client_msg_id.clear();
    check_error(encode_send(&changed), "PROTOCOL_INVALID_MESSAGE");
    check_error(same_intent(&send, &changed), "PROTOCOL_INVALID_MESSAGE");
    check_error(
        correlate(&changed, "u1", &ServerFrame::Ack(ack())),
        "PROTOCOL_INVALID_MESSAGE",
    );
    changed = send.clone();
    changed.protocol_version = 2;
    check_error(encode_send(&changed), "PROTOCOL_UNSUPPORTED_VERSION");
    changed = send.clone();
    changed.version = 0;
    check_error(encode_send(&changed), "PROTOCOL_INVALID_MESSAGE");
    let mut bad_ack = ack();
    bad_ack.conversation_seq = "01".into();
    check_error(
        encode_server_frame(&ServerFrame::Ack(bad_ack.clone())),
        "PROTOCOL_INVALID_MESSAGE",
    );
    check_error(
        compare_results(&ServerFrame::Ack(bad_ack), &message()),
        "PROTOCOL_INVALID_MESSAGE",
    );
    let bad_error = ServerFrame::Error(SendError {
        client_msg_id: "c1".into(),
        conversation_id: "room1".into(),
        code: "lowercase".into(),
    });
    check_error(encode_server_frame(&bad_error), "PROTOCOL_INVALID_MESSAGE");
}
