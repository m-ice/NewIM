//! Subscription and close frames for the v1 WSS gateway.

use serde_json::json;

use crate::{Error, id};

pub const GATEWAY_PATH: &str = "/api/v1/gateway";
pub const GATEWAY_SUBPROTOCOL: &str = "newim.v1";

#[derive(Clone, PartialEq, Eq)]
pub struct GatewaySubscription {
    pub conversation_id: String,
    pub after_seq: i64,
}

/// Encode the first text frame. The host owns TLS, authentication, and subprotocol checks.
pub fn encode_gateway_subscription(request: &GatewaySubscription) -> Result<Vec<u8>, Error> {
    if !id(&request.conversation_id) || request.after_seq < 0 {
        return Err(Error::InvalidMessage);
    }
    let body = json!({
        "conversationId": request.conversation_id,
        "afterSeq": request.after_seq.to_string(),
    });
    serde_json::to_vec(&body).map_err(|_| Error::InvalidJson)
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
