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
        2
    );
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
        "PRAGMA user_version=2; UPDATE schema_migrations SET checksum='changed' WHERE version=1",
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
