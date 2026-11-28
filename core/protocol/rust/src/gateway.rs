//! Subscription and close frames for the v1 WSS gateway.

use serde_json::json;

use crate::{Error, Message, Object, id, send, string};

pub const GATEWAY_PATH: &str = "/api/v1/gateway";
pub const GATEWAY_SUBPROTOCOL: &str = "newim.v1";

#[derive(Clone, PartialEq, Eq)]
pub struct GatewaySubscription {
    pub conversation_id: String,
    pub after_seq: i64,
    /// Published visibility revision, when the directory supports visibility.
    pub after_visibility_revision: Option<i64>,
}

/// Encode the first text frame. The host owns TLS, authentication, and subprotocol checks.
pub fn encode_gateway_subscription(request: &GatewaySubscription) -> Result<Vec<u8>, Error> {
    if !id(&request.conversation_id)
        || request.after_seq < 0
        || request
            .after_visibility_revision
            .is_some_and(|revision| revision < 0)
    {
        return Err(Error::InvalidMessage);
    }
    let mut body = json!({
        "conversationId": request.conversation_id,
        "afterSeq": request.after_seq.to_string(),
    });
    if let Some(revision) = request.after_visibility_revision {
        body["afterVisibilityRevision"] = json!(revision.to_string());
    }
    serde_json::to_vec(&body).map_err(|_| Error::InvalidJson)
}

#[derive(Clone, PartialEq, Eq)]
pub struct VisibilityNotice {
    pub conversation_id: String,
    pub revision: i64,
}

#[derive(Clone)]
pub enum GatewayFrame {
    Message(Message),
    VisibilityChanged(VisibilityNotice),
}

pub(crate) fn visibility_notice(fields: &Object) -> Result<VisibilityNotice, Error> {
    let conversation_id = string(fields, "conversationId")?;
    let value = string(fields, "visibilityRevision")?;
    let revision: i64 = value.parse().map_err(|_| Error::InvalidMessage)?;
    if fields.len() != 2 || !id(&conversation_id) || revision <= 0 || value != revision.to_string()
    {
        return Err(Error::InvalidMessage);
    }
    Ok(VisibilityNotice {
        conversation_id,
        revision,
    })
}

/// Decode the frames carried by the subscribed socket.
pub fn decode_gateway_frame(input: &[u8]) -> Result<GatewayFrame, send::Error> {
    match send::decode_frame(input, false)? {
        send::Frame::Server(send::ServerFrame::Message(message)) => {
            Ok(GatewayFrame::Message(message))
        }
        send::Frame::VisibilityChanged(notice) => Ok(GatewayFrame::VisibilityChanged(notice)),
        _ => Err(send::Error::UnsupportedFrame),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GatewayClose {
    ServerDraining,
    SessionRevoked,
    DeviceLimit,
    ConversationForbidden,
    InvalidSubscription,
    SubscriptionTimeout,
    TemporarilyUnavailable,
    Unknown,
}

/// Interpret a close received on a host-verified gateway connection.
pub fn classify_gateway_close(code: u16, reason: &str) -> GatewayClose {
    match (code, reason) {
        (1001, "server_draining") => GatewayClose::ServerDraining,
        (1008, "session_revoked") => GatewayClose::SessionRevoked,
        (1008, "device_limit") => GatewayClose::DeviceLimit,
        (1008, "conversation_forbidden") => GatewayClose::ConversationForbidden,
        (1008, "invalid_subscription") => GatewayClose::InvalidSubscription,
        (1008, "subscription_timeout") => GatewayClose::SubscriptionTimeout,
        (1011, "temporarily_unavailable") => GatewayClose::TemporarilyUnavailable,
        _ => GatewayClose::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_matches_gateway_shape_and_range() {
        let mut request = GatewaySubscription {
            conversation_id: "conv_1".into(),
            after_seq: 0,
            after_visibility_revision: None,
        };
        let wire = encode_gateway_subscription(&request).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&wire).unwrap(),
            json!({"conversationId": "conv_1", "afterSeq": "0"}),
        );

        request.conversation_id = "a".repeat(128);
        request.after_seq = i64::MAX;
        let wire = encode_gateway_subscription(&request).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&wire).unwrap(),
            json!({"conversationId": request.conversation_id, "afterSeq": "9223372036854775807"}),
        );
        assert!(wire.len() <= 1024);

        request.after_seq = -1;
        assert_eq!(
            encode_gateway_subscription(&request),
            Err(Error::InvalidMessage)
        );
        request.after_seq = 0;
        for id in [String::new(), "a".repeat(129), "conv/1".into()] {
            request.conversation_id = id;
            assert_eq!(
                encode_gateway_subscription(&request),
                Err(Error::InvalidMessage)
            );
        }
    }

    #[test]
    fn visibility_updates_are_explicit_and_do_not_change_message_decoding() {
        let mut subscription = GatewaySubscription {
            conversation_id: "room".into(),
            after_seq: 8,
            after_visibility_revision: Some(0),
        };
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(
                &encode_gateway_subscription(&subscription).unwrap()
            )
            .unwrap(),
            json!({"conversationId":"room","afterSeq":"8","afterVisibilityRevision":"0"})
        );
        subscription.after_visibility_revision = Some(-1);
        assert!(encode_gateway_subscription(&subscription).is_err());

        let notice = br#"{"protocolVersion":1,"kind":"visibility_changed","body":{"conversationId":"room","visibilityRevision":"64"}}"#;
        let Ok(GatewayFrame::VisibilityChanged(notice_value)) = decode_gateway_frame(notice) else {
            panic!("expected visibility notice");
        };
        assert_eq!(notice_value.conversation_id, "room");
        assert_eq!(notice_value.revision, 64);
        assert!(send::decode_server_frame(notice).is_err());
        for invalid in [
            br#"{"protocolVersion":1,"kind":"visibility_changed","body":{"conversationId":"room","visibilityRevision":"0"}}"#.as_slice(),
            br#"{"protocolVersion":1,"kind":"visibility_changed","body":{"conversationId":"room","visibilityRevision":"064"}}"#,
            br#"{"protocolVersion":1,"kind":"visibility_changed","body":{"conversationId":"room","visibilityRevision":"64","text":"body"}}"#,
        ] {
            assert!(decode_gateway_frame(invalid).is_err());
        }

        let message = r#"{"protocolVersion":1,"version":1,"conversationId":"room","serverMsgId":"msg","clientMsgId":"client","senderId":"peer","conversationSeq":"8","serverTime":"10","type":"message_hidden","payload":{"countAsUnread": false,"action":"recall"}}"#;
        let canonical = r#"{"action":"recall","countAsUnread":false}"#;
        assert_eq!(
            crate::decode(message.as_bytes()).unwrap().payload.get(),
            canonical
        );
        let frame = format!(r#"{{"protocolVersion":1,"kind":"message","body":{message}}}"#);
        let GatewayFrame::Message(decoded) = decode_gateway_frame(frame.as_bytes()).unwrap() else {
            panic!("expected redacted message frame");
        };
        assert_eq!(decoded.payload.get(), canonical);

        let future = message.replace(r#""version":1"#, r#""version":2"#);
        let opaque = crate::decode(future.as_bytes()).unwrap();
        assert_eq!(
            opaque.payload.get(),
            r#"{"countAsUnread": false,"action":"recall"}"#
        );
    }

    #[test]
    fn only_exact_gateway_close_pairs_are_classified() {
        use GatewayClose::*;

        for (code, reason, expected) in [
            (1001, "server_draining", ServerDraining),
            (1008, "session_revoked", SessionRevoked),
            (1008, "device_limit", DeviceLimit),
            (1008, "conversation_forbidden", ConversationForbidden),
            (1008, "invalid_subscription", InvalidSubscription),
            (1008, "subscription_timeout", SubscriptionTimeout),
            (1011, "temporarily_unavailable", TemporarilyUnavailable),
            (1008, "server_draining", Unknown),
            (1001, "session_revoked", Unknown),
            (1011, "device_limit", Unknown),
            (1008, "session_revoked ", Unknown),
            (1008, "unrecognized", Unknown),
            (1009, "", Unknown),
        ] {
            assert_eq!(classify_gateway_close(code, reason), expected);
        }
    }
}
