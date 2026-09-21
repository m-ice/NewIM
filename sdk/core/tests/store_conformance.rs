use newim_sdk_core::store::*;
#[test]
fn portable_bounds_identity_and_content_free_diagnostics() {
    let mut r = Request {
        fence: Fence {
            account: "a".into(),
            instance: "i".into(),
            generation: 1,
        },
        operation_id: 1,
        action: Action::Enqueue(Pending {
            sender_id: "a".into(),
            client_id: "c".into(),
            conversation_id: "r".into(),
            payload: Blob(vec![42; MAX_VALUE_BYTES]),
        }),
    };
    assert!(validate_request(&r).is_ok());
    let first = request_bytes(&r).unwrap();
    r.operation_id = 2;
    assert_ne!(first, request_bytes(&r).unwrap());
    if let Action::Enqueue(p) = &mut r.action {
        p.payload.0.push(42);
    }
    assert_eq!(validate_request(&r), Err(StoreError::InvalidInput));
    assert!(!format!("{:?}", Blob(b"private-text".to_vec())).contains("private-text"));
    r.action = Action::Messages {
        conversation: "r".into(),
        after: None,
        limit: 65,
    };
    assert_eq!(validate_request(&r), Err(StoreError::InvalidInput));
}
#[test]
fn stale_completion_is_not_publishable() {
    let f = Fence {
        account: "a".into(),
        instance: "i".into(),
        generation: 1,
    };
    let c = Completion {
        fence: f.clone(),
        operation_id: 1,
        result: Err(StoreError::Busy),
    };
    assert!(c.belongs_to(&f));
    let mut changed = f;
    changed.generation = 2;
    assert!(!c.belongs_to(&changed));
}

#[path = "store/scenarios.rs"]
mod scenarios;
#[test]
fn retry_scenario_has_portable_stable_identity() {
    let requests = scenarios::enqueue_retry(Fence {
        account: "a".into(),
        instance: "i".into(),
        generation: 1,
    });
    for r in &requests {
        validate_request(r).unwrap();
    }
    assert_eq!(request_bytes(&requests[0]), request_bytes(&requests[1]));
}
