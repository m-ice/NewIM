use newim_sdk_core::message::{
    ConnectionGeneration, PersistedAck, SendContext, SendFailure, SendIntent,
};
use newim_sdk_core::outbox::{
    self, AckResolution, MAX_ENVELOPE_BYTES, OutboxError, OutboxOperation, OutboxRecord,
    OutboxState, OutboxTransition, RETRY_EXHAUSTED,
};
use newim_sdk_core::store::*;
use std::collections::BTreeMap;

const SENTINEL_CLIENT: &str = "client-sentinel-9f4c";
const SENTINEL_CONVERSATION: &str = "conversation-sentinel-7b2e";
const SENTINEL_ACCOUNT: &str = "account-sentinel-1a0d";
const SENTINEL_INSTANCE: &str = "instance-sentinel-3c7f";
const SENTINEL_SERVER: &str = "server-sentinel-5d8a";
const SENTINEL_PAYLOAD: &[u8] = b"payload-sentinel-6e1b";

struct HarnessStore {
    fence: Fence,
    revision: u64,
    pending: BTreeMap<(String, String), Pending>,
    messages: Vec<Message>,
    completion: Option<Completion>,
}

impl HarnessStore {
    fn new(fence: Fence) -> Self {
        Self {
            fence,
            revision: 0,
            pending: BTreeMap::new(),
            messages: Vec::new(),
            completion: None,
        }
    }

    fn pending(&self, identity: &PendingIdentity) -> Option<&Pending> {
        self.pending
            .get(&(identity.sender_id.clone(), identity.client_id.clone()))
    }

    fn pending_item(&self, identity: &PendingIdentity) -> Option<Pending> {
        self.pending(identity).cloned()
    }

    fn execute(&mut self, request: &Request) -> Result<Response, StoreError> {
        if request.fence != self.fence {
            return Err(StoreError::StaleGeneration);
        }
        match &request.action {
            Action::Enqueue(item) => {
                let key = (item.sender_id.clone(), item.client_id.clone());
                if let Some(existing) = self.pending.get(&key) {
                    return Ok(Response::ExistingPending(existing.clone()));
                }
                if self
                    .messages
                    .iter()
                    .any(|m| m.sender_id == item.sender_id && m.client_id == item.client_id)
                {
                    return Err(StoreError::IdentityConflict);
                }
                self.pending.insert(key, item.clone());
                self.revision += 1;
                Ok(Response::Committed(Receipt {
                    revision: self.revision,
                    affected: 1,
                    replayed: false,
                }))
            }
            Action::Apply(batch) => {
                if batch.expected_revision != self.revision {
                    return Err(StoreError::StaleRevision);
                }
                let mut next_messages = self.messages.clone();
                let mut next_pending = self.pending.clone();
                for write in &batch.messages {
                    let message = match write {
                        MessageWrite::Insert(message) | MessageWrite::PreserveExisting(message) => {
                            message
                        }
                    };
                    let existing = next_messages.iter().position(|old| {
                        old.server_id == message.server_id
                            || (old.sender_id == message.sender_id
                                && old.client_id == message.client_id)
                            || (old.conversation_id == message.conversation_id
                                && old.sequence == message.sequence)
                    });
                    match (write, existing) {
                        (MessageWrite::Insert(_), Some(index)) => {
                            return Ok(Response::Existing(next_messages[index].clone()));
                        }
                        (MessageWrite::PreserveExisting(_), Some(index))
                            if next_messages[index] == *message => {}
                        (MessageWrite::PreserveExisting(_), _) => {
                            return Err(StoreError::StaleRevision);
                        }
                        (MessageWrite::Insert(_), None) => next_messages.push(message.clone()),
                    }
                }
                for resolution in &batch.resolve_pending {
                    let matched = next_messages.iter().any(|message| {
                        message.sender_id == resolution.sender_id
                            && message.client_id == resolution.client_id
                            && message.server_id == resolution.server_id
                    });
                    if !matched {
                        return Err(StoreError::IdentityConflict);
                    }
                    let key = (resolution.sender_id.clone(), resolution.client_id.clone());
                    if next_pending.remove(&key).is_none() {
                        return Err(StoreError::IdentityConflict);
                    }
                }
                self.messages = next_messages;
                self.pending = next_pending;
                self.revision += 1;
                Ok(Response::Committed(Receipt {
                    revision: self.revision,
                    affected: 1,
                    replayed: false,
                }))
            }
            Action::Pending { after, limit } => {
                let mut items = self
                    .pending
                    .values()
                    .filter(|item| {
                        after.as_ref().is_none_or(|after| {
                            format!("{}:{}", item.sender_id, item.client_id) > *after
                        })
                    })
                    .take(*limit)
                    .cloned()
                    .collect::<Vec<_>>();
                items.sort_by(|a, b| {
                    (a.sender_id.as_str(), a.client_id.as_str())
                        .cmp(&(b.sender_id.as_str(), b.client_id.as_str()))
                });
                Ok(Response::Pending(PendingPage { items, next: None }))
            }
            Action::Snapshot { conversation } => Ok(Response::Snapshot(Snapshot {
                revision: self.revision,
                cursor: Blob::default(),
                conversation: conversation.as_ref().map(|id| Conversation {
                    id: id.clone(),
                    unread: 0,
                    summary: Blob::default(),
                }),
                recovery_required: false,
            })),
            _ => Err(StoreError::InvalidInput),
        }
    }
}

impl LocalStore for HarnessStore {
    fn submit(&mut self, request: Request) -> Result<(), StoreError> {
        if self.completion.is_some() {
            return Err(StoreError::Busy);
        }
        let result = self.execute(&request);
        self.completion = Some(Completion {
            fence: request.fence,
            operation_id: request.operation_id,
            result,
        });
        Ok(())
    }

    fn take_completion(&mut self) -> Option<Completion> {
        self.completion.take()
    }
}

impl PendingMutationStore for HarnessStore {
    fn update_pending(&mut self, mutation: PendingMutation) -> Result<PendingReceipt, StoreError> {
        mutation.validate()?;
        if mutation.expected_revision != self.revision {
            return Err(StoreError::StaleRevision);
        }
        let current = self
            .pending(&mutation.identity)
            .cloned()
            .ok_or(StoreError::IdentityConflict)?;
        if current.conversation_id != mutation.identity.conversation_id {
            return Err(StoreError::IdentityConflict);
        }
        self.pending.insert(
            (
                mutation.identity.sender_id.clone(),
                mutation.identity.client_id.clone(),
            ),
            Pending {
                sender_id: mutation.identity.sender_id,
                client_id: mutation.identity.client_id,
                conversation_id: current.conversation_id,
                payload: mutation.payload,
            },
        );
        self.revision += 1;
        Ok(PendingReceipt {
            revision: self.revision,
        })
    }

    fn remove_pending(&mut self, removal: PendingRemoval) -> Result<PendingReceipt, StoreError> {
        removal.validate()?;
        if removal.expected_revision != self.revision {
            return Err(StoreError::StaleRevision);
        }
        let key = (
            removal.identity.sender_id.clone(),
            removal.identity.client_id.clone(),
        );
        let current = self.pending.get(&key).ok_or(StoreError::IdentityConflict)?;
        if current.conversation_id != removal.identity.conversation_id {
            return Err(StoreError::IdentityConflict);
        }
        self.pending.remove(&key);
        self.revision += 1;
        Ok(PendingReceipt {
            revision: self.revision,
        })
    }
}

fn fence() -> Fence {
    Fence {
        account: SENTINEL_ACCOUNT.into(),
        instance: SENTINEL_INSTANCE.into(),
        generation: 1,
    }
}

fn context() -> SendContext {
    SendContext {
        sender_id: SENTINEL_ACCOUNT.into(),
        fence: fence(),
        connection_generation: ConnectionGeneration(7),
    }
}

fn intent() -> SendIntent {
    SendIntent {
        protocol_version: 1,
        client_id: SENTINEL_CLIENT.into(),
        conversation_id: SENTINEL_CONVERSATION.into(),
        schema_version: 1,
        message_type: "text".into(),
        payload: Blob(SENTINEL_PAYLOAD.to_vec()),
    }
}

fn identity(record: &OutboxRecord) -> PendingIdentity {
    record.identity()
}

#[test]
fn envelope_bounds_and_redaction() {
    let context = context();
    let record = OutboxRecord::new(intent(), &context, 1_000).unwrap();
    let encoded = record.encode().unwrap();
    assert!(encoded.0.len() <= MAX_ENVELOPE_BYTES);
    let pending = Pending {
        sender_id: record.sender_id.clone(),
        client_id: record.intent.client_id.clone(),
        conversation_id: record.intent.conversation_id.clone(),
        payload: encoded.clone(),
    };
    let decoded = OutboxRecord::decode(&pending).unwrap();
    assert_eq!(decoded.intent, record.intent);
    assert_eq!(decoded.fence, record.fence);
    assert_eq!(decoded.connection_generation, record.connection_generation);

    for diagnostic in [
        format!("{context:?}"),
        format!("{:?}", record.intent),
        format!("{record:?}"),
        format!("{:?}", record.observation(OutboxOperation::Load, 2_000)),
    ] {
        for sentinel in [
            SENTINEL_CLIENT,
            SENTINEL_CONVERSATION,
            SENTINEL_ACCOUNT,
            SENTINEL_INSTANCE,
            "payload-sentinel",
        ] {
            assert!(
                !diagnostic.contains(sentinel),
                "{diagnostic} leaked {sentinel}"
            );
        }
    }

    let binary = OutboxRecord::new(
        SendIntent {
            payload: Blob(vec![0, 255, 1, 128, 42]),
            ..intent()
        },
        &context,
        1_000,
    )
    .unwrap();
    let binary_pending = Pending {
        sender_id: binary.sender_id.clone(),
        client_id: binary.intent.client_id.clone(),
        conversation_id: binary.intent.conversation_id.clone(),
        payload: binary.encode().unwrap(),
    };
    assert_eq!(
        OutboxRecord::decode(&binary_pending)
            .unwrap()
            .intent
            .payload,
        Blob(vec![0, 255, 1, 128, 42])
    );
    assert_eq!(
        OutboxRecord::decode(&Pending {
            payload: Blob(vec![1, 2, 3]),
            ..binary_pending
        })
        .unwrap_err(),
        OutboxError::RecordInvalid
    );

    let mut truncated = encoded.0.clone();
    truncated.pop();
    assert_eq!(
        OutboxRecord::decode(&Pending {
            payload: Blob(truncated),
            ..pending.clone()
        })
        .unwrap_err(),
        OutboxError::RecordInvalid
    );
    let mut unknown = encoded.0.clone();
    unknown[4] = 2;
    assert_eq!(
        OutboxRecord::decode(&Pending {
            payload: Blob(unknown),
            ..pending
        })
        .unwrap_err(),
        OutboxError::RecordInvalid
    );
    let oversized = OutboxRecord::new(
        SendIntent {
            payload: Blob(vec![7; MAX_VALUE_BYTES]),
            ..intent()
        },
        &context,
        1_000,
    )
    .unwrap();
    assert_eq!(oversized.encode().unwrap_err(), OutboxError::RecordTooLarge);
}

#[test]
fn restart_resumes_same_intent() {
    let context = context();
    let mut store = HarnessStore::new(fence());
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 1_000).unwrap();
    assert_eq!(created.intent, intent());
    let pending = store.pending_item(&identity(&created)).unwrap();
    let decoded = OutboxRecord::decode(&pending).unwrap();
    assert_eq!(decoded.intent.client_id, SENTINEL_CLIENT);

    let revision = store.revision;
    let transition =
        outbox::plan_dispatch(&mut store, &pending, revision, &context, 1_000).unwrap();
    match transition {
        OutboxTransition::Send { intent: sent } => assert_eq!(sent, intent()),
        _ => panic!("expected dispatch"),
    }
    let persisted = store.pending_item(&identity(&created)).unwrap();
    let resumed = OutboxRecord::decode(&persisted).unwrap();
    assert_eq!(resumed.intent.client_id, SENTINEL_CLIENT);
    assert_eq!(resumed.state, OutboxState::InFlight);
    assert_eq!(resumed.attempts, 1);
}

#[test]
fn retry_deadline_and_exhaustion() {
    let context = context();
    let mut store = HarnessStore::new(fence());
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let mut pending = store.pending_item(&identity(&created)).unwrap();
    let mut revision = 1;
    let mut now = 0;
    for attempt in 1..=8 {
        let transition =
            outbox::plan_dispatch(&mut store, &pending, revision, &context, now).unwrap();
        assert!(matches!(transition, OutboxTransition::Send { .. }));
        pending = store.pending_item(&identity(&created)).unwrap();
        let failure = SendFailure::from_validated_code(
            SENTINEL_CLIENT,
            SENTINEL_CONVERSATION,
            "SERVER_TEMPORARY_UNAVAILABLE",
        )
        .unwrap();
        let transition =
            outbox::apply_failure(&mut store, &pending, revision + 1, &context, &failure, now)
                .unwrap();
        pending = store.pending_item(&identity(&created)).unwrap();
        let record = OutboxRecord::decode(&pending).unwrap();
        if attempt == 8 {
            assert_eq!(record.state, OutboxState::PermanentFailure);
            assert_eq!(record.last_code, RETRY_EXHAUSTED);
            assert!(matches!(transition, OutboxTransition::Terminal { .. }));
            break;
        }
        let OutboxTransition::Wait { until_ms } = transition else {
            panic!("expected wait");
        };
        assert!(until_ms > now);
        assert!(matches!(
            outbox::plan_dispatch(&mut store, &pending, revision + 2, &context, until_ms - 1)
                .unwrap(),
            OutboxTransition::Wait { .. }
        ));
        now = until_ms;
        revision += 2;
    }
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &pending, revision, &context, now).unwrap(),
        OutboxTransition::Terminal { ref code } if code == RETRY_EXHAUSTED
    ));
}

#[test]
fn clock_rollback_and_deadline_overflow_are_terminal_safe() {
    let context = context();
    let mut store = HarnessStore::new(fence());
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 1_000).unwrap();
    let pending = store.pending_item(&identity(&created)).unwrap();
    let _ = outbox::plan_dispatch(&mut store, &pending, 1, &context, 1_000).unwrap();
    let in_flight = store.pending_item(&identity(&created)).unwrap();
    let failure = SendFailure::from_validated_code(
        SENTINEL_CLIENT,
        SENTINEL_CONVERSATION,
        "SERVER_TEMPORARY_UNAVAILABLE",
    )
    .unwrap();
    let transition =
        outbox::apply_failure(&mut store, &in_flight, 2, &context, &failure, 1_000).unwrap();
    let OutboxTransition::Wait { until_ms } = transition else {
        panic!("retry wait expected");
    };
    let retry = store.pending_item(&identity(&created)).unwrap();
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &retry, 3, &context, 999).unwrap(),
        OutboxTransition::Wait { .. }
    ));

    let overflow_context = context.clone();
    let mut overflow_store = HarnessStore::new(fence());
    let overflow = outbox::enqueue(
        &mut overflow_store,
        4,
        &overflow_context,
        intent(),
        u64::MAX - 1_000,
    )
    .unwrap();
    let overflow_pending = overflow_store.pending_item(&identity(&overflow)).unwrap();
    let transition = outbox::plan_dispatch(
        &mut overflow_store,
        &overflow_pending,
        1,
        &overflow_context,
        u64::MAX,
    )
    .unwrap();
    assert!(
        matches!(transition, OutboxTransition::Terminal { ref code } if code == RETRY_EXHAUSTED)
    );
    let persisted = overflow_store.pending_item(&identity(&overflow)).unwrap();
    assert_eq!(
        OutboxRecord::decode(&persisted).unwrap().last_code,
        RETRY_EXHAUSTED
    );
    assert!(until_ms > 1_000);
}

#[test]
fn generation_terminal_and_explicit_removal() {
    let context = context();
    let mut store = HarnessStore::new(fence());
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let pending = store.pending_item(&identity(&created)).unwrap();
    let _ = outbox::plan_dispatch(&mut store, &pending, 1, &context, 0).unwrap();
    let in_flight = store.pending_item(&identity(&created)).unwrap();

    let mut stale = context.clone();
    stale.connection_generation = ConnectionGeneration(8);
    assert_eq!(
        outbox::plan_dispatch(&mut store, &in_flight, 2, &stale, 0).unwrap_err(),
        OutboxError::GenerationMismatch
    );
    assert!(store.pending_item(&identity(&created)).is_some());

    let auth =
        SendFailure::from_validated_code(SENTINEL_CLIENT, SENTINEL_CONVERSATION, "AUTH_REQUIRED")
            .unwrap();
    let transition = outbox::apply_failure(&mut store, &in_flight, 2, &context, &auth, 0).unwrap();
    assert!(matches!(transition, OutboxTransition::AuthRecovery { .. }));
    let auth_record = store.pending_item(&identity(&created)).unwrap();
    let auth_record = OutboxRecord::decode(&auth_record).unwrap();
    assert_eq!(auth_record.state, OutboxState::AuthRecovery);
    let auth_pending = store.pending_item(&identity(&created)).unwrap();
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &auth_pending, 3, &context, 0).unwrap(),
        OutboxTransition::AuthRecovery { .. }
    ));
    let resumed = outbox::resume_auth(&mut store, &auth_pending, 3, &context, 0).unwrap();
    assert!(matches!(resumed, OutboxTransition::Wait { .. }));
    assert!(store.pending_item(&identity(&created)).is_some());
    let resumed_pending = store.pending_item(&identity(&created)).unwrap();
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &resumed_pending, 4, &context, 0).unwrap(),
        OutboxTransition::Send { .. }
    ));
    let resumed_pending = store.pending_item(&identity(&created)).unwrap();
    let terminal = outbox::apply_failure(
        &mut store,
        &resumed_pending,
        5,
        &context,
        &SendFailure::from_validated_code(
            SENTINEL_CLIENT,
            SENTINEL_CONVERSATION,
            "SERVER_REJECTED",
        )
        .unwrap(),
        1,
    )
    .unwrap();
    assert!(matches!(terminal, OutboxTransition::Terminal { .. }));
    let terminal_pending = store.pending_item(&identity(&created)).unwrap();
    assert!(matches!(
        outbox::plan_dispatch(&mut store, &terminal_pending, 6, &context, 1).unwrap(),
        OutboxTransition::Terminal { .. }
    ));
    assert_eq!(
        outbox::remove_terminal(&mut store, &terminal_pending, 5, &context).unwrap_err(),
        OutboxError::Store(StoreError::StaleRevision)
    );
    assert!(store.pending_item(&identity(&created)).is_some());
    outbox::remove_terminal(&mut store, &terminal_pending, 6, &context).unwrap();
    assert!(store.pending_item(&identity(&created)).is_none());
}

#[test]
fn ack_batch_is_exact_and_recoverable() {
    let context = context();
    let mut store = HarnessStore::new(fence());
    let created = outbox::enqueue(&mut store, 1, &context, intent(), 0).unwrap();
    let pending = store.pending_item(&identity(&created)).unwrap();
    let ack = PersistedAck {
        sender_id: SENTINEL_ACCOUNT.into(),
        client_id: SENTINEL_CLIENT.into(),
        conversation_id: SENTINEL_CONVERSATION.into(),
        server_id: SENTINEL_SERVER.into(),
        sequence: 1,
        server_time: 1,
        protocol_version: 1,
        schema_version: 1,
        message_type: "text".into(),
        payload: Blob(SENTINEL_PAYLOAD.to_vec()),
    };
    let result = outbox::resolve_ack(&mut store, 2, &pending, 1, &context, &ack).unwrap();
    assert!(matches!(result, AckResolution::Resolved(_)));
    assert!(store.pending_item(&identity(&created)).is_none());
    assert_eq!(store.messages.len(), 1);

    let mut wrong = ack.clone();
    wrong.payload = Blob(b"different".to_vec());
    assert_eq!(
        outbox::resolve_ack(
            &mut store,
            3,
            &Pending {
                sender_id: SENTINEL_ACCOUNT.into(),
                client_id: SENTINEL_CLIENT.into(),
                conversation_id: SENTINEL_CONVERSATION.into(),
                payload: created.encode().unwrap(),
            },
            2,
            &context,
            &wrong
        )
        .unwrap_err(),
        OutboxError::AckCorrelationMismatch
    );

    for sentinel in [
        SENTINEL_CLIENT,
        SENTINEL_ACCOUNT,
        SENTINEL_SERVER,
        "different",
    ] {
        assert!(!format!("{wrong:?}").contains(sentinel));
    }
}
