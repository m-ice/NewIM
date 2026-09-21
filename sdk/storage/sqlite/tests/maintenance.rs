mod common;
use common::*;
use newim_sdk_core::store::*;
use newim_store_sqlite::{Limits, SqliteStore};
#[test]
fn trim_retains_pending_identity_cursor_and_metrics() {
    let d = Directory::new();
    let mut s = d.open();
    run(&mut s, 1, Action::Enqueue(pending(99))).unwrap();
    run(&mut s, 2, batch(1, vec![msg(1), msg(2), msg(3)])).unwrap();
    let before = snapshot(&mut s);
    assert!(matches!(
        run(
            &mut s,
            3,
            Action::Trim {
                conversation: "room".into(),
                through: 3,
                limit: 2
            }
        ),
        Ok(Response::Committed(Receipt { affected: 2, .. }))
    ));
    assert!(found(&mut s, 1).unwrap().payload.is_none());
    assert!(found(&mut s, 2).unwrap().payload.is_none());
    assert!(found(&mut s, 3).unwrap().payload.is_some());
    assert!(matches!(
        run(&mut s, 4, batch(3, vec![msg(1)])),
        Ok(Response::Existing(_))
    ));
    assert_eq!(snapshot(&mut s).cursor, before.cursor);
    assert!(
        matches!(run(&mut s,1,Action::Pending{after:None,limit:64}),Ok(Response::Pending(PendingPage{ref items,..})) if items.len()==1)
    );
    let metrics = run(&mut s, 1, Action::Metrics).unwrap();
    assert!(
        matches!(metrics,Response::Metrics(Metrics{page_count,pending_count:1,database_bytes,..}) if page_count>0&&database_bytes>0)
    );
    run(&mut s, 4, Action::Compact { budget_ms: 30_000 }).unwrap();
}
#[test]
fn real_busy_full_and_record_capacity() {
    let d = Directory::new();
    let mut s = SqliteStore::open(
        &d.0,
        "alice",
        Limits {
            records: 2,
            pages: 32,
        },
    )
    .unwrap();
    let conn = d.db();
    conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        run(&mut s, 1, Action::Enqueue(pending(1))),
        Err(StoreError::Busy)
    );
    conn.execute_batch("ROLLBACK").unwrap();
    let mut large = pending(1);
    large.payload = Blob(vec![1; 65_536]);
    assert_eq!(
        run(&mut s, 1, Action::Enqueue(large)),
        Err(StoreError::StorageFull)
    );
    assert_eq!(snapshot(&mut s).revision, 0);
    drop(s);
    let mut s = SqliteStore::open(
        &d.0,
        "alice",
        Limits {
            records: 2,
            ..Limits::default()
        },
    )
    .unwrap();
    run(&mut s, 1, batch(0, vec![msg(1), msg(2)])).unwrap();
    assert_eq!(
        run(&mut s, 2, batch(1, vec![msg(3)])),
        Err(StoreError::CapacityExceeded)
    );
    assert_eq!(snapshot(&mut s).revision, 1);
    conn.execute_batch("BEGIN; SELECT * FROM messages;")
        .unwrap();
    run(
        &mut s,
        2,
        Action::Trim {
            conversation: "room".into(),
            through: 2,
            limit: 2,
        },
    )
    .unwrap();
    assert_eq!(
        run(&mut s, 3, Action::Compact { budget_ms: 30_000 }),
        Err(StoreError::Busy)
    );
    conn.execute_batch("ROLLBACK").unwrap();
}
#[test]
fn pending_capacity_pagination_and_no_silent_trim() {
    let d = Directory::new();
    let mut s = d.open();
    for n in 0..256 {
        run(&mut s, n as u64 + 1, Action::Enqueue(pending(n))).unwrap();
    }
    assert_eq!(
        run(&mut s, 257, Action::Enqueue(pending(256))),
        Err(StoreError::CapacityExceeded)
    );
    let mut after = None;
    let mut seen = std::collections::BTreeSet::new();
    loop {
        match run(&mut s, 1, Action::Pending { after, limit: 31 }).unwrap() {
            Response::Pending(p) => {
                for item in p.items {
                    assert!(seen.insert(item.client_id));
                }
                after = p.next;
                if after.is_none() {
                    break;
                }
            }
            _ => panic!(),
        }
    }
    assert_eq!(seen.len(), 256);
}
