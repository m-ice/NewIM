#![allow(dead_code)]
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static ID: AtomicU64 = AtomicU64::new(0);
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let base = std::env::temp_dir().canonicalize().unwrap();
        Self(base.join(format!(
            "newim-store-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::SeqCst)
        )))
    }
    pub fn open(&self) -> SqliteStore {
        SqliteStore::open(&self.0, "alice", Limits::default()).unwrap()
    }
    pub fn db(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.0.join("active/store.db")).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
pub fn req(s: &SqliteStore, id: u64, action: Action) -> Request {
    Request {
        fence: s.fence().clone(),
        operation_id: id,
        action,
    }
}
pub fn run(s: &mut SqliteStore, id: u64, action: Action) -> Result<Response, StoreError> {
    let r = req(s, id, action);
    s.execute(r).result
}
pub fn msg(n: i64) -> Message {
    Message {
        server_id: format!("s{n}"),
        sender_id: "alice".into(),
        client_id: format!("c{n}"),
        conversation_id: "room".into(),
        sequence: n,
        server_time: 0,
        schema_version: 1,
        message_type: "text".into(),
        payload: Some(Blob(br#"{"text":"hello"}"#.to_vec())),
    }
}
pub fn pending(n: i64) -> Pending {
    Pending {
        sender_id: "alice".into(),
        client_id: format!("c{n}"),
        conversation_id: "room".into(),
        payload: Blob(br#"{"text":"hello"}"#.to_vec()),
    }
}
pub fn batch(rev: u64, messages: Vec<Message>) -> Action {
    Action::Apply(Batch {
        expected_revision: rev,
        messages: messages.into_iter().map(MessageWrite::Insert).collect(),
        conversations: vec![],
        cursor: None,
        resolve_pending: vec![],
    })
}
pub fn snapshot(s: &mut SqliteStore) -> Snapshot {
    match run(
        s,
        1,
        Action::Snapshot {
            conversation: Some("room".into()),
        },
    )
    .unwrap()
    {
        Response::Snapshot(x) => x,
        _ => panic!(),
    }
}
pub fn found(s: &mut SqliteStore, n: i64) -> Option<Message> {
    match run(
        s,
        1,
        Action::Lookup {
            server_id: format!("s{n}"),
        },
    )
    .unwrap()
    {
        Response::Found(x) => x,
        _ => panic!(),
    }
}
