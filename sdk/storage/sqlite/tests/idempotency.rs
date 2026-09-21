mod common;
use common::*;
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore};

#[test]
fn atomic_receipt_reopen_and_reconciliation() {
    let d = Directory::new();
    let mut s = d.open();
    let request = req(&s, 1, Action::Enqueue(pending(1)));
    assert!(matches!(
        s.execute(request.clone()).result,
        Ok(Response::Committed(_))
    ));
    let action = Action::Apply(Batch {
        expected_revision: 1,
        messages: vec![MessageWrite::Insert(msg(1))],
        conversations: vec![Conversation {
            id: "room".into(),
            unread: 1,
            summary: Blob(vec![1]),
        }],
        cursor: Some(Blob(vec![2])),
        resolve_pending: vec![PendingResolution {
            sender_id: "alice".into(),
            client_id: "c1".into(),
            server_id: "s1".into(),
        }],
    });
    let apply = req(&s, 2, action);
    assert!(matches!(
        s.execute(apply.clone()).result,
        Ok(Response::Committed(_))
    ));
    drop(s);
    let mut s = d.open();
    assert!(matches!(
        s.execute(apply.clone()).result,
        Ok(Response::Committed(Receipt { replayed: true, .. }))
    ));
    assert_eq!(s.execute(request).result, Err(StoreError::OperationExpired));
    assert_eq!(snapshot(&mut s).conversation.unwrap().unread, 1);
    assert_eq!(snapshot(&mut s).cursor, Blob(vec![2]));
    assert_eq!(
        run(
            &mut s,
            1,
            Action::Pending {
                after: None,
                limit: 64
            }
        )
        .unwrap(),
        Response::Pending(PendingPage {
            items: vec![],
            next: None
        })
    );
    let mut duplicate = msg(1);
    duplicate.payload = Some(Blob(br#"{ "text": "\u0068ello" }"#.to_vec()));
    assert!(matches!(
        run(&mut s, 3, batch(2, vec![msg(2), duplicate])),
        Ok(Response::Existing(_))
    ));
    assert!(found(&mut s, 2).is_none());
    assert_eq!(snapshot(&mut s).revision, 2);
    let preserved = found(&mut s, 1).unwrap();
    let b = Batch {
        expected_revision: 2,
        messages: vec![MessageWrite::PreserveExisting(preserved)],
        conversations: vec![],
        cursor: Some(Blob(vec![3])),
        resolve_pending: vec![],
    };
    assert!(run(&mut s, 3, Action::Apply(b)).is_ok());
    assert_eq!(snapshot(&mut s).revision, 3);
}
#[test]
fn indexes_numeric_pages_identity_and_fences() {
    let d = Directory::new();
    let mut s = d.open();
    assert!(SqliteStore::open(&d.0, "alice", Limits::default()).is_err());
    assert!(
        run(
            &mut s,
            1,
            batch(0, vec![msg(10), msg(2), msg(0), msg(i64::MAX)])
        )
        .is_ok()
    );
    let page = run(
        &mut s,
        1,
        Action::Messages {
            conversation: "room".into(),
            after: None,
            limit: 2,
        },
    )
    .unwrap();
    assert!(
        matches!(page,Response::Messages(MessagePage{ref items,next:Some(2)}) if items.iter().map(|m|m.sequence).collect::<Vec<_>>()==vec![0,2])
    );
    let page = run(
        &mut s,
        1,
        Action::Messages {
            conversation: "room".into(),
            after: Some(2),
            limit: 2,
        },
    )
    .unwrap();
    assert!(
        matches!(page,Response::Messages(MessagePage{ref items,next:None}) if items.iter().map(|m|m.sequence).collect::<Vec<_>>()==vec![10,i64::MAX])
    );
    for mut m in [msg(3), msg(4), msg(5)] {
        match m.sequence {
            3 => m.server_id = "s2".into(),
            4 => m.client_id = "c2".into(),
            _ => m.sequence = 2,
        };
        assert_eq!(
            run(&mut s, 2, batch(1, vec![msg(6), m])),
            Err(StoreError::IdentityConflict)
        );
        assert!(found(&mut s, 6).is_none());
    }
    assert_eq!(
        run(&mut s, 2, batch(0, vec![])),
        Err(StoreError::StaleRevision)
    );
    let old = req(&s, 2, Action::Metrics);
    let completion = s.execute(old.clone());
    assert!(run(&mut s, 2, Action::AdvanceGeneration).is_ok());
    assert!(!completion.belongs_to(s.fence()));
    assert_eq!(s.execute(old).result, Err(StoreError::StaleGeneration));
    drop(s);
    assert!(matches!(
        SqliteStore::open(&d.0, "bob", Limits::default()),
        Err(StoreError::StaleGeneration)
    ));
    let conn = d.db();
    let plan:String=conn.query_row("EXPLAIN QUERY PLAN SELECT * FROM messages WHERE conversation_id='room' AND sequence>2 ORDER BY sequence LIMIT 64",[],|r|r.get(3)).unwrap();
    assert!(plan.contains("INDEX"));
}
#[test]
fn codec_opaque_roundtrip_and_byte_bounded_pages() {
    let d = Directory::new();
    let mut s = d.open();
    for (n, payload) in [
        r#"{"number":1e9999,"integer":9007199254740993,"text":"\u0000"}"#.to_owned(),
        format!("{{\"data\":\"{}\"}}", "x".repeat(64_000)),
    ]
    .iter()
    .enumerate()
    {
        let wire = format!(
            r#"{{"protocolVersion":1,"version":2147483647,"clientMsgId":"c{n}","serverMsgId":"s{n}","conversationId":"room","conversationSeq":"{n}","senderId":"alice","type":"future","serverTime":"9223372036854775807","payload":{payload}}}"#
        );
        let mut decoded = newim_protocol::decode(wire.as_bytes()).unwrap();
        let mut m = msg(n as i64);
        m.payload = Some(Blob(decoded.payload.get().as_bytes().to_vec()));
        m.message_type = decoded.message_type.clone();
        m.schema_version = decoded.version;
        m.server_time = i64::MAX;
        assert!(run(&mut s, n as u64 + 1, batch(n as u64, vec![m])).is_ok());
        let got = found(&mut s, n as i64).unwrap();
        assert_eq!(got.payload.as_ref().unwrap().0, payload.as_bytes());
        let replacement = String::from_utf8(got.payload.unwrap().0).unwrap();
        let encoded = newim_protocol::encode(&decoded).unwrap();
        decoded = newim_protocol::decode(&encoded).unwrap();
        assert_eq!(decoded.payload.get(), replacement);
    }
    let mut messages = Vec::new();
    for n in 2..7 {
        let mut m = msg(n);
        m.payload = Some(Blob(vec![42; 64_000]));
        messages.push(m);
    }
    for (index, m) in messages.into_iter().enumerate() {
        assert!(run(&mut s, index as u64 + 3, batch(index as u64 + 2, vec![m])).is_ok());
    }
    match run(
        &mut s,
        1,
        Action::Messages {
            conversation: "room".into(),
            after: Some(0),
            limit: 64,
        },
    )
    .unwrap()
    {
        Response::Messages(p) => {
            assert_eq!(p.items.len(), 4);
            assert_eq!(p.next, Some(4));
        }
        _ => panic!(),
    }
}
#[test]
fn completion_queue_and_validation() {
    let d = Directory::new();
    let mut s = d.open();
    let r = req(&s, 1, Action::Metrics);
    s.submit(r.clone()).unwrap();
    assert_eq!(s.submit(r), Err(StoreError::Busy));
    assert!(s.take_completion().is_some());
    assert!(s.take_completion().is_none());
    assert_eq!(
        run(&mut s, 0, Action::Metrics),
        Err(StoreError::InvalidInput)
    );
}

#[test]
fn accepted_shared_protocol_fixtures_survive_sqlite_and_codec() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../../core/protocol/fixtures/cases.json");
    let cases: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let mut count = 0;
    for case in cases.as_array().unwrap() {
        if !case["error"].is_null() {
            continue;
        }
        let Some(wire) = case["wire"].as_str() else {
            continue;
        };
        let mut decoded = newim_protocol::decode(wire.as_bytes()).unwrap();
        let d = Directory::new();
        let mut s = d.open();
        let stored = Message {
            server_id: decoded.server_msg_id.clone(),
            sender_id: decoded.sender_id.clone(),
            client_id: decoded.client_msg_id.clone(),
            conversation_id: decoded.conversation_id.clone(),
            sequence: decoded.conversation_seq.parse().unwrap(),
            server_time: decoded.server_time.parse().unwrap(),
            schema_version: decoded.version,
            message_type: decoded.message_type.clone(),
            payload: Some(Blob(decoded.payload.get().as_bytes().to_vec())),
        };
        run(&mut s, 1, batch(0, vec![stored])).unwrap();
        drop(s);
        let mut s = d.open();
        let Response::Found(Some(read)) = run(
            &mut s,
            1,
            Action::Lookup {
                server_id: decoded.server_msg_id.clone(),
            },
        )
        .unwrap() else {
            panic!()
        };
        decoded.payload = serde_json::value::RawValue::from_string(
            String::from_utf8(read.payload.unwrap().0).unwrap(),
        )
        .unwrap();
        let again = newim_protocol::decode(&newim_protocol::encode(&decoded).unwrap()).unwrap();
        assert_eq!(decoded.payload.get(), again.payload.get());
        count += 1;
    }
    assert!(count >= 10);
}

#[test]
fn duplicate_keys_within_batch_never_return_phantom_existing() {
    let d = Directory::new();
    let mut s = d.open();
    assert_eq!(
        run(&mut s, 1, batch(0, vec![msg(1), msg(1)])),
        Err(StoreError::InvalidInput)
    );
    assert!(found(&mut s, 1).is_none());
    assert_eq!(snapshot(&mut s).revision, 0);
}

#[path = "../../../core/tests/store/scenarios.rs"]
mod scenarios;
#[test]
fn portable_retry_scenario_executes_against_disk() {
    let d = Directory::new();
    let mut s = d.open();
    let [first, retry] = scenarios::enqueue_retry(s.fence().clone());
    assert!(matches!(
        s.execute(first).result,
        Ok(Response::Committed(Receipt {
            replayed: false,
            revision: 1,
            ..
        }))
    ));
    drop(s);
    let mut s = d.open();
    assert!(matches!(
        s.execute(retry).result,
        Ok(Response::Committed(Receipt {
            replayed: true,
            revision: 1,
            ..
        }))
    ));
}
