mod common;
use common::*;
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore, quarantine, rebuild, salvage_pending};
use std::{
    fs,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("NEWIM_TEST_CRASH_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(path);
    let mut s = SqliteStore::open(&root, "alice", Limits::default()).unwrap();
    run(&mut s, 1, Action::Enqueue(pending(1))).unwrap();
    // Keep the actual adapter alive so committed state remains in WAL.
    let conn = rusqlite::Connection::open(root.join("active/store.db")).unwrap();
    conn.execute_batch(
        "BEGIN IMMEDIATE; INSERT INTO pending_outbox VALUES('alice','uncommitted','room',X'01');",
    )
    .unwrap();
    fs::write(root.join("ready"), b"committed-and-uncommitted").unwrap();
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
#[test]
fn process_kill_recovers_wal_and_receipt() {
    let d = Directory::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("NEWIM_TEST_CRASH_ROOT", &d.0)
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !d.0.join("ready").exists() {
        if start.elapsed() > Duration::from_secs(10) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child handshake timed out");
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before handshake"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let mut s = d.open();
    assert!(matches!(
        run(&mut s, 1, Action::Enqueue(pending(1))),
        Ok(Response::Committed(Receipt { replayed: true, .. }))
    ));
    assert!(
        matches!(run(&mut s,2,Action::Pending{after:None,limit:64}),Ok(Response::Pending(PendingPage{ref items,..})) if items.len()==1 && items[0].client_id=="c1")
    );
}
#[test]
fn quarantine_export_and_rebuild_preserve_evidence() {
    let d = Directory::new();
    let mut s = d.open();
    let old = s.fence().clone();
    run(&mut s, 1, Action::Enqueue(pending(1))).unwrap();
    assert_eq!(quarantine(&d.0, "first").unwrap_err(), StoreError::Busy);
    drop(s);
    quarantine(&d.0, "first").unwrap();
    assert!(matches!(
        SqliteStore::open(&d.0, "alice", Limits::default()),
        Err(StoreError::RecoveryRequired)
    ));
    let bytes = fs::read(d.0.join("quarantine-first/store.db")).unwrap();
    let page = salvage_pending(&d.0, "first", "alice", None, 64).unwrap();
    assert_eq!(page.items, vec![pending(1)]);
    assert_eq!(
        salvage_pending(&d.0, "first", "bob", None, 64),
        Err(StoreError::StaleGeneration)
    );
    let mut fresh = rebuild(&d.0, "alice", Limits::default()).unwrap();
    assert_ne!(&old, fresh.fence());
    assert!(snapshot(&mut fresh).recovery_required);
    assert_eq!(snapshot(&mut fresh).cursor, Blob(vec![]));
    drop(fresh);
    assert_eq!(
        fs::read(d.0.join("quarantine-first/store.db")).unwrap(),
        bytes
    );
    assert!(snapshot(&mut d.open()).recovery_required);
}
#[test]
fn real_corruption_never_reports_pending_recovered() {
    let d = Directory::new();
    let mut s = d.open();
    run(&mut s, 1, Action::Enqueue(pending(1))).unwrap();
    drop(s);
    let db = d.0.join("active/store.db");
    let mut data = fs::read(&db).unwrap();
    data[..16].fill(0x55);
    fs::write(&db, &data).unwrap();
    assert!(matches!(
        SqliteStore::open(&d.0, "alice", Limits::default()),
        Err(StoreError::Corrupt)
    ));
    assert_eq!(fs::read(&db).unwrap(), data);
    quarantine(&d.0, "corrupt").unwrap();
    assert_eq!(
        salvage_pending(&d.0, "corrupt", "alice", None, 64),
        Err(StoreError::RecoveryRequired)
    );
    let mut s = rebuild(&d.0, "alice", Limits::default()).unwrap();
    assert!(snapshot(&mut s).recovery_required);
    assert_eq!(
        fs::read(d.0.join("quarantine-corrupt/store.db")).unwrap(),
        data
    );
}
#[test]
fn recovery_marker_interruption_and_symlinks() {
    let d = Directory::new();
    drop(d.open());
    fs::write(d.0.join("RECOVERY"), b"interrupted").unwrap();
    assert!(matches!(
        SqliteStore::open(&d.0, "alice", Limits::default()),
        Err(StoreError::RecoveryRequired)
    ));
    quarantine(&d.0, "interrupted").unwrap();
    quarantine(&d.0, "interrupted").unwrap();
    let mut s = rebuild(&d.0, "alice", Limits::default()).unwrap();
    assert!(snapshot(&mut s).recovery_required);
    drop(s);
    fs::write(d.0.join("RECOVERY"), b"interrupted").unwrap();
    drop(rebuild(&d.0, "alice", Limits::default()).unwrap());
    let other = Directory::new();
    std::os::unix::fs::symlink(&d.0, &other.0).unwrap();
    assert!(matches!(
        SqliteStore::open(&other.0, "alice", Limits::default()),
        Err(StoreError::InvalidInput)
    ));
    fs::remove_file(&other.0).unwrap();
}
#[test]
fn oversized_quarantine_row_is_error_not_empty_page() {
    let d = Directory::new();
    drop(d.open());
    let conn = d.db();
    conn.execute_batch("PRAGMA ignore_check_constraints=ON; INSERT INTO pending_outbox VALUES('alice','oversized','room',zeroblob(300000));").unwrap();
    drop(conn);
    quarantine(&d.0, "oversized").unwrap();
    assert_eq!(
        salvage_pending(&d.0, "oversized", "alice", None, 64),
        Err(StoreError::RecoveryRequired)
    );
}
