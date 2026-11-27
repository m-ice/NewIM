//! JSON codec for authenticated TLS message pages.

use serde_json::{json, value::RawValue};

use crate::{
    Error, MAX_DEPTH, Message, Object, decimal, field, id, object, scan,
    send::{self, ServerFrame, decode_server_frame},
};

pub const MESSAGE_DELTA_PATH: &str = "/api/v1/sync/messages";
pub const MAX_MESSAGE_DELTA_PAGE_BYTES: usize = 262_144;
pub const MAX_MESSAGE_DELTA_PAGE_ITEMS: usize = 100;

#[derive(Clone, PartialEq, Eq)]
pub struct MessageDeltaRequest {
    pub conversation_id: String,
    pub after_seq: i64,
    /// Zero omits the limit and uses the server default of 100.
    pub limit: usize,
}

impl MessageDeltaRequest {
    fn page_limit(&self) -> Result<usize, Error> {
        if !id(&self.conversation_id)
            || self.after_seq < 0
            || self.limit > MAX_MESSAGE_DELTA_PAGE_ITEMS
        {
            return Err(Error::InvalidMessage);
        }
        Ok(if self.limit == 0 {
            MAX_MESSAGE_DELTA_PAGE_ITEMS
        } else {
            self.limit
        })
    }
}

#[derive(Clone)]
pub struct MessageDeltaPage {
    pub history_after_seq: i64,
    pub latest_seq: i64,
    pub has_next: bool,
    pub next_after_seq: Option<i64>,
    pub items: Vec<Message>,
}

pub fn encode_message_delta_request(request: &MessageDeltaRequest) -> Result<Vec<u8>, Error> {
    request.page_limit()?;
    let mut body = json!({
        "conversationId": request.conversation_id,
        "afterSeq": request.after_seq.to_string(),
    });
    if request.limit != 0 {
        body["limit"] = json!(request.limit);
    }
    serde_json::to_vec(&body).map_err(|_| Error::InvalidJson)
}

pub fn decode_message_delta_page(
    request: &MessageDeltaRequest,
    body: &[u8],
) -> Result<MessageDeltaPage, send::Error> {
    let limit = request.page_limit()?;
    // The page object and items array wrap the existing server-frame depth.
    scan::check_with_limits(body, MAX_MESSAGE_DELTA_PAGE_BYTES, MAX_DEPTH + 3)?;
    let text = std::str::from_utf8(body).map_err(|_| Error::InvalidJson)?;
    let fields = object(text)?;
    if fields.len() != 5 {
        return Err(Error::InvalidMessage.into());
    }
    let history_after_seq = sequence(&fields, "historyAfterSeq")?.ok_or(Error::InvalidMessage)?;
    let latest_seq = sequence(&fields, "latestSeq")?.ok_or(Error::InvalidMessage)?;
    let next_after_seq = sequence(&fields, "nextAfterSeq")?;
    let has_next: bool = serde_json::from_str(field(&fields, "hasNext")?.get())
        .map_err(|_| Error::InvalidMessage)?;
    let frames: Vec<Box<RawValue>> =
        serde_json::from_str(field(&fields, "items")?.get()).map_err(|_| Error::InvalidMessage)?;

    let mut last_seq = request.after_seq.max(history_after_seq);
    if last_seq > latest_seq || frames.len() > limit {
        return Err(Error::InvalidMessage.into());
    }

    let mut items = Vec::with_capacity(frames.len());
    for frame in frames {
        let ServerFrame::Message(message) = decode_server_frame(frame.get().as_bytes())? else {
            return Err(Error::InvalidMessage.into());
        };
        let sequence: i64 = message
            .conversation_seq
            .parse()
            .map_err(|_| Error::InvalidMessage)?;
        if message.conversation_id != request.conversation_id
            || last_seq.checked_add(1) != Some(sequence)
            || sequence > latest_seq
        {
            return Err(Error::InvalidMessage.into());
        }
        last_seq = sequence;
        items.push(message);
    }

    if next_after_seq != (!items.is_empty()).then_some(last_seq)
        || has_next != (last_seq < latest_seq)
        || (has_next && items.is_empty())
    {
        return Err(Error::InvalidMessage.into());
    }
    Ok(MessageDeltaPage {
        history_after_seq,
        latest_seq,
        has_next,
        next_after_seq,
        items,
    })
}

fn sequence(fields: &Object, name: &str) -> Result<Option<i64>, Error> {
    let value: Option<String> =
        serde_json::from_str(field(fields, name)?.get()).map_err(|_| Error::InvalidMessage)?;
    value
        .map(|value| {
            if !decimal(&value) {
                return Err(Error::InvalidMessage);
            }
            value.parse().map_err(|_| Error::InvalidMessage)
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(after_seq: i64, limit: usize) -> MessageDeltaRequest {
        MessageDeltaRequest {
            conversation_id: "room".into(),
            after_seq,
            limit,
        }
    }

    fn frame(conversation: &str, sequence: i64, payload: &str) -> String {
        format!(
            r#"{{"protocolVersion":1,"kind":"message","body":{{"protocolVersion":1,"version":1,"clientMsgId":"client","serverMsgId":"server","conversationId":"{conversation}","conversationSeq":"{sequence}","senderId":"sender","type":"text","serverTime":"1","payload":{payload}}}}}"#,
        )
    }

    fn page(
        floor: i64,
        latest: i64,
        has_next: bool,
        next: Option<i64>,
        frames: &[String],
    ) -> String {
        let next = next.map_or_else(|| "null".into(), |seq| format!("\"{seq}\""));
        format!(
            r#"{{"historyAfterSeq":"{floor}","latestSeq":"{latest}","items":[{}],"hasNext":{has_next},"nextAfterSeq":{next}}}"#,
            frames.join(","),
        )
    }

    #[test]
    fn request_uses_decimal_strings_and_omits_the_default_limit() {
        let encoded = encode_message_delta_request(&request(0, 0)).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            json!({"conversationId": "room", "afterSeq": "0"})
        );
        let encoded = encode_message_delta_request(&request(i64::MAX, 100)).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),
            json!({"conversationId": "room", "afterSeq": "9223372036854775807", "limit": 100})
        );

        for invalid in [
            request(-1, 1),
            request(0, 101),
            MessageDeltaRequest {
                conversation_id: "room/path".into(),
                ..request(0, 1)
            },
        ] {
            assert_eq!(
                encode_message_delta_request(&invalid),
                Err(Error::InvalidMessage)
            );
        }
    }

    #[test]
    fn pages_preserve_message_bytes_and_authorized_bounds() {
        let raw = r#"{"text":"one", "number":9007199254740993}"#;
        let first = page(5, 7, true, Some(6), &[frame("room", 6, raw)]);
        let decoded = decode_message_delta_page(&request(2, 1), first.as_bytes()).unwrap();
        assert_eq!((decoded.history_after_seq, decoded.latest_seq), (5, 7));
        assert!(decoded.has_next);
        assert_eq!(decoded.next_after_seq, Some(6));
        assert_eq!(decoded.items[0].payload.get(), raw);

        let last = page(5, 7, false, Some(7), &[frame("room", 7, raw)]);
        let decoded = decode_message_delta_page(&request(6, 1), last.as_bytes()).unwrap();
        assert!(!decoded.has_next);
        assert_eq!(decoded.next_after_seq, Some(7));

        for (after, floor, latest) in [(2, 5, 5), (7, 5, 7), (0, i64::MAX, i64::MAX)] {
            let empty = page(floor, latest, false, None, &[]);
            let decoded = decode_message_delta_page(&request(after, 0), empty.as_bytes()).unwrap();
            assert_eq!(
                (decoded.history_after_seq, decoded.latest_seq),
                (floor, latest)
            );
            assert!(decoded.items.is_empty());
            assert_eq!(decoded.next_after_seq, None);
        }

        let last = page(
            i64::MAX - 1,
            i64::MAX,
            false,
            Some(i64::MAX),
            &[frame("room", i64::MAX, raw)],
        );
        assert!(decode_message_delta_page(&request(0, 1), last.as_bytes()).is_ok());
    }

    #[test]
    fn rejects_inconsistent_pages_and_non_message_frames() {
        let text = r#"{"text":"one"}"#;
        let next = frame("room", 6, text);
        let last = frame("room", 7, text);
        let cases = [
            page(6, 5, false, None, &[]),
            page(5, 7, false, None, &[]),
            page(5, 7, true, None, &[]),
            page(5, 7, false, Some(6), std::slice::from_ref(&next)),
            page(5, 7, true, None, std::slice::from_ref(&next)),
            page(5, 7, true, Some(7), std::slice::from_ref(&next)),
            page(5, 7, true, Some(7), &[next.clone(), last.clone()]),
            page(5, 7, false, Some(7), std::slice::from_ref(&last)),
            page(5, 7, false, Some(7), &[next.clone(), next.clone()]),
            page(
                5,
                7,
                false,
                Some(7),
                &[next.clone(), frame("other", 7, text)],
            ),
            page(5, 6, false, Some(7), &[next.clone(), last.clone()]),
            page(
                5,
                7,
                false,
                Some(7),
                &[next.clone(), last.clone(), frame("room", 8, text)],
            ),
            page(
                5,
                6,
                false,
                Some(6),
                &[String::from(
                    r#"{"protocolVersion":1,"kind":"send_error","body":{"clientMsgId":"client","conversationId":"room","code":"AUTH_REQUIRED"}}"#,
                )],
            ),
        ];
        for body in cases {
            assert_eq!(
                decode_message_delta_page(&request(2, 2), body.as_bytes()).err(),
                Some(Error::InvalidMessage.into()),
                "{body}"
            );
        }
        let valid = page(5, 7, false, Some(7), &[next, last]);
        assert_eq!(
            decode_message_delta_page(&request(2, 1), valid.as_bytes()).err(),
            Some(Error::InvalidMessage.into())
        );
        assert_eq!(
            decode_message_delta_page(&request(8, 2), valid.as_bytes()).err(),
            Some(Error::InvalidMessage.into())
        );
    }

    #[test]
    fn page_limits_match_http_without_relaxing_frame_limits() {
        let empty = page(0, 0, false, None, &[]);
        let mut bounded = empty.as_bytes().to_vec();
        bounded.resize(MAX_MESSAGE_DELTA_PAGE_BYTES, b' ');
        assert!(decode_message_delta_page(&request(0, 0), &bounded).is_ok());
        bounded.push(b' ');
        assert_eq!(
            decode_message_delta_page(&request(0, 0), &bounded).err(),
            Some(Error::MessageTooLarge.into())
        );
        for malformed in [
            empty.replace(r#""latestSeq":"0""#, r#""latestSeq":0"#),
            empty.replace(r#""latestSeq":"0""#, r#""latestSeq":"00""#),
            empty.replace(r#""latestSeq":"0""#, r#""latestSeq":"9223372036854775808""#),
            empty.replace(r#""latestSeq":"0""#, r#""latestSeq":null"#),
            empty.replace(r#""nextAfterSeq":null"#, r#""nextAfterSeq":"0""#),
            empty.replace(r#""nextAfterSeq":null"#, r#""future":null"#),
            empty.replace(r#""latestSeq":"0""#, r#""latestSeq":"0","future":null"#),
            empty.replace(
                r#""latestSeq":"0""#,
                r#""latestSeq":"0","latestSe\u0071":"0""#,
            ),
        ] {
            assert!(decode_message_delta_page(&request(0, 0), malformed.as_bytes()).is_err());
        }

        let payload = format!(
            r#"{{"text":"one","nested":{}0{}}}"#,
            "[".repeat(MAX_DEPTH - 2),
            "]".repeat(MAX_DEPTH - 2)
        );
        let with_deep_frame = page(0, 1, false, Some(1), &[frame("room", 1, &payload)]);
        assert!(decode_message_delta_page(&request(0, 0), with_deep_frame.as_bytes()).is_ok());
        let too_deep = with_deep_frame.replace("[0]", "[[0]]");
        assert_eq!(
            decode_message_delta_page(&request(0, 0), too_deep.as_bytes()).err(),
            Some(Error::NestingExceeded.into())
        );
    }
}
