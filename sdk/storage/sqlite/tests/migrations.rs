mod common;
use common::*;
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore};
use rusqlite::params;
#[allow(dead_code)]
#[path = "../src/migrations.rs"]
mod migrations_source;
// Reuse the actual supported migration SQL, not a fabricated historical schema.
#[allow(dead_code)]
fn db(e: rusqlite::Error) -> StoreError {
    let _ = e;
    StoreError::Io
}
fn legacy(d: &Directory) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(d.0.join("active")).unwrap();
    std::fs::set_permissions(&d.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    let conn = d.db();
    conn.execute_batch(migrations_source::SQL1).unwrap();
    conn.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, script BLOB NOT NULL); PRAGMA application_id=1313426737; PRAGMA user_version=1;").unwrap();
    conn.execute(
        "INSERT INTO schema_migrations VALUES(1,?,?)",
        params![migrations_source::HASH1, migrations_source::SQL1.as_bytes()],
    )
    .unwrap();
    conn.execute_batch("INSERT INTO store_metadata(id,account,instance,generation,revision) VALUES(1,'alice','legacy',1,0); INSERT INTO pending_outbox VALUES('alice','stable','room',X'010203'); UPDATE sync_state SET cursor=X'1234';").unwrap();
}
#[test]
fn populated_version_one_upgrade_and_repeat() {
    let d = Directory::new();
    legacy(&d);
    let mut s = d.open();
    assert_eq!(snapshot(&mut s).cursor, Blob(vec![0x12, 0x34]));
    assert!(
        matches!(run(&mut s,1,Action::Pending{after:None,limit:64}),Ok(Response::Pending(PendingPage{ref items,..})) if items[0].client_id=="stable")
    );
    drop(s);
    drop(d.open());
    assert_eq!(
        d.db()
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        migrations_source::VERSION
    );
}
#[test]
fn populated_version_two_keeps_legacy_apply_receipt() {
    let d = Directory::new();
    legacy(&d);
    let conn = d.db();
    conn.execute_batch(migrations_source::SQL2).unwrap();
    conn.execute(
        "INSERT INTO schema_migrations VALUES(2,?,?)",
        params![migrations_source::HASH2, migrations_source::SQL2.as_bytes()],
    )
    .unwrap();
    conn.execute_batch(
        "PRAGMA user_version=2;
         INSERT INTO conversations VALUES('room',4,X'F0');
         INSERT INTO messages VALUES('s3','alice','c3','room',3,0,1,'text',X'FA');
         UPDATE sync_state SET cursor=X'1234';",
    )
    .unwrap();
    // Exact version-2 request bytes: fence, operation 9, old Apply tag 5,
    // expected revision 5, no message, one conversation, cursor, no pending resolution.
    let mut receipt = Vec::new();
    receipt.extend_from_slice(b"\x05\0\0\0\0\0\0\0alice\x06\0\0\0\0\0\0\0legacy");
    for value in [1u64, 9, 5, 5, 0, 1, 4] {
        receipt.extend_from_slice(&value.to_le_bytes());
    }
    receipt.extend_from_slice(b"room");
    for value in [4u64, 1] {
        receipt.extend_from_slice(&value.to_le_bytes());
    }
    receipt.push(0xf0);
    for value in [1u64, 2] {
        receipt.extend_from_slice(&value.to_le_bytes());
    }
    receipt.extend_from_slice(&[0x12, 0x34]);
    receipt.extend_from_slice(&0u64.to_le_bytes());
    conn.execute(
        "UPDATE store_metadata SET revision=6,last_operation=9,last_request=?,last_revision=6",
        [receipt.as_slice()],
    )
    .unwrap();
    drop(conn);

    let mut store = d.open();
    let action = Action::Apply(Batch {
        expected_revision: 5,
        conversations: vec![Conversation {
            id: "room".into(),
            unread: 4,
            summary: Blob(vec![0xf0]),
        }],
        cursor: Some(Blob(vec![0x12, 0x34])),
        messages: vec![],
        resolve_pending: vec![],
    });
    assert_eq!(
        request_bytes(&req(&store, 9, action.clone())).unwrap(),
        receipt
    );
    assert_eq!(
        run(&mut store, 9, action),
        Ok(Response::Committed(Receipt {
            revision: 6,
            affected: 0,
            replayed: true,
        }))
    );
    assert_eq!(
        run(&mut store, 9, batch(5, vec![])),
        Err(StoreError::OperationConflict)
    );
    let old = snapshot(&mut store);
    assert_eq!(old.conversation.unwrap().summary, Blob(vec![0xf0]));
    assert_eq!(old.cursor, Blob(vec![0x12, 0x34]));
    assert_eq!(
        found(&mut store, 3).unwrap().payload,
        Some(Blob(vec![0xfa]))
    );
    assert!(
        matches!(run(&mut store,1,Action::Pending{after:None,limit:1}),Ok(Response::Pending(PendingPage{ref items,..})) if items[0].client_id=="stable")
    );
    assert_eq!(
        run(
            &mut store,
            1,
            Action::ConversationSyncSnapshot(ConversationSyncQuery::State)
        ),
        Ok(Response::ConversationSyncSnapshot(
            ConversationSyncSnapshot {
                revision: 6,
                position: ConversationSyncPosition::initial(),
                published_round: None,
                items: vec![],
                history_floors: Default::default(),
                next: None,
            }
        ))
    );
    drop(store);
    let conn = d.db();
    let (hash, script): (String, Vec<u8>) = conn
        .query_row(
            "SELECT checksum,script FROM schema_migrations WHERE version=3",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(hash, migrations_source::HASH3);
    assert_eq!(script, migrations_source::SQL3.as_bytes());
}

#[test]
fn future_tampered_and_interrupted_migrations() {
    let d = Directory::new();
    legacy(&d);
    let conn = d.db();
    conn.execute_batch("CREATE INDEX messages_payload_trim ON messages(server_id)")
        .unwrap();
    assert!(SqliteStore::open(&d.0, "alice", Limits::default()).is_err());
    assert_eq!(
        conn.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row::<i64, _, _>("SELECT count(*) FROM schema_migrations", [], |r| r.get(0))
            .unwrap(),
        1
    );
    conn.execute_batch("DROP INDEX messages_payload_trim")
        .unwrap();
    drop(d.open());
    conn.execute_batch("PRAGMA user_version=99").unwrap();
    assert!(matches!(
        SqliteStore::open(&d.0, "alice", Limits::default()),
        Err(StoreError::UnsupportedSchema)
    ));
    conn.execute_batch(
        "PRAGMA user_version=3; UPDATE schema_migrations SET checksum='changed' WHERE version=3",
    )
    .unwrap();
    assert!(matches!(
        SqliteStore::open(&d.0, "alice", Limits::default()),
        Err(StoreError::UnsupportedSchema)
    ));
}

#[test]
fn missing_or_extra_schema_objects_are_refused() {
    let d = Directory::new();
    drop(d.open());
    d.db().execute_batch("DROP INDEX pending_page").unwrap();
    assert!(matches!(
        SqliteStore::open(&d.0, "alice", Limits::default()),
        Err(StoreError::UnsupportedSchema)
    ));
}

#[test]
fn migration_crash_child() {
    let Some(path) = std::env::var_os("NEWIM_MIGRATION_CRASH") else {
        return;
    };
    let root = std::path::PathBuf::from(path);
    let conn = rusqlite::Connection::open(root.join("active/store.db")).unwrap();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    conn.execute_batch(migrations_source::SQL2).unwrap();
    conn.execute_batch("PRAGMA user_version=2").unwrap();
    std::fs::write(root.join("ready"), b"uncommitted-migration").unwrap();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
#[test]
fn killed_migration_transaction_keeps_prior_schema() {
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    let d = Directory::new();
    legacy(&d);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "migration_crash_child"])
        .env("NEWIM_MIGRATION_CRASH", &d.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let started = Instant::now();
    while !d.0.join("ready").exists() {
        if started.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("migration child timed out");
        }
        assert!(child.try_wait().unwrap().is_none());
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        d.db()
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        1
    );
    let mut s = d.open();
    assert_eq!(snapshot(&mut s).cursor, Blob(vec![0x12, 0x34]));
}
