use crate::{Limits, SqliteStore, db, files, operations::pending_page};
use newim_sdk_core::store::*;
use rusqlite::{Connection, OpenFlags};
use std::{fs, path::Path};

/// 保留隔离证据；不声称已恢复待发数据 / Evidence is retained; pending recovery is unresolved.
#[derive(Debug)]
pub struct RecoveryReport {
    pub quarantine: String,
    pub pending_recovery_required: bool,
}
fn name_valid(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn marker_name(root: &Path) -> Result<String, StoreError> {
    files::no_symlinks(&root.join("RECOVERY"))?;
    let meta = fs::metadata(root.join("RECOVERY")).map_err(|_| StoreError::RecoveryRequired)?;
    if meta.len() > 128 {
        return Err(StoreError::RecoveryRequired);
    }
    let name =
        fs::read_to_string(root.join("RECOVERY")).map_err(|_| StoreError::RecoveryRequired)?;
    if !name_valid(&name) {
        return Err(StoreError::RecoveryRequired);
    }
    Ok(name)
}
/// 必须先关闭 adapter；隔离目录名不可复用 / Close the adapter first; never reuse a quarantine name.
pub fn quarantine(root: impl AsRef<Path>, name: &str) -> Result<RecoveryReport, StoreError> {
    let root = root.as_ref();
    let _lock = files::lock_root(root)?;
    if !name_valid(name) {
        return Err(StoreError::InvalidInput);
    }
    let active = files::safe_active(root)?;
    let dest = root.join(format!("quarantine-{name}"));
    files::no_symlinks(&dest)?;
    if !root.join("RECOVERY").exists() {
        if dest.exists() || !active.is_dir() {
            return Err(StoreError::RecoveryRequired);
        }
        files::marker(&root.join("RECOVERY"), name.as_bytes())?;
    }
    let previous = marker_name(root)?;
    if previous != name {
        // A failed rebuild may have a partial active directory. Preserve it under a
        // fresh name without ever replacing the original incident's quarantine.
        let prior = root.join(format!("quarantine-{previous}"));
        files::no_symlinks(&prior)?;
        if !prior.is_dir() || !active.is_dir() || dest.exists() {
            return Err(StoreError::RecoveryRequired);
        }
        files::replace_marker(
            &root.join("RECOVERY"),
            &active.join("recovery.next"),
            name.as_bytes(),
        )?;
    }
    if !dest.exists() {
        if !active.is_dir() {
            return Err(StoreError::RecoveryRequired);
        }
        fs::rename(active, &dest).map_err(|_| StoreError::Io)?;
        files::sync_dir(root)?;
    }
    Ok(RecoveryReport {
        quarantine: name.into(),
        pending_recovery_required: true,
    })
}
/// 新实例保留 recovery_required；上层必须另行核对未发数据 / New instance remains recovery-required.
pub fn rebuild(
    root: impl AsRef<Path>,
    account: &str,
    limits: Limits,
) -> Result<SqliteStore, StoreError> {
    let root = root.as_ref();
    let lock = files::lock_root(root)?;
    let name = marker_name(root)?;
    let quarantine = root.join(format!("quarantine-{name}"));
    files::no_symlinks(&quarantine)?;
    if !quarantine.is_dir() {
        return Err(StoreError::RecoveryRequired);
    }
    let store = SqliteStore::open_locked(root, account, limits, lock, true)?;
    let required: bool = store
        .conn
        .query_row(
            "SELECT recovery_required FROM store_metadata WHERE id=1",
            [],
            |r| r.get(0),
        )
        .map_err(db)?;
    if !required {
        return Err(StoreError::RecoveryRequired);
    }
    files::sync_dir(&root.join("active"))?;
    files::sync_dir(root)?;
    fs::remove_file(root.join("RECOVERY")).map_err(|_| StoreError::Io)?;
    files::sync_dir(root)?;
    Ok(store)
}
/// 只读分页导出；无法完整读取时显式失败 / Read-only bounded export; unreadable pending is an error.
pub fn salvage_pending(
    root: impl AsRef<Path>,
    name: &str,
    account: &str,
    after: Option<&str>,
    limit: usize,
) -> Result<PendingPage, StoreError> {
    crate::verify_engine()?;
    let root = root.as_ref();
    let _lock = files::lock_root(root)?;
    if !name_valid(name)
        || !(1..=MAX_ITEMS).contains(&limit)
        || after.is_some_and(|s| s.len() > 257)
    {
        return Err(StoreError::InvalidInput);
    }
    let dir = root.join(format!("quarantine-{name}"));
    for path in [
        &dir,
        &dir.join("store.db"),
        &dir.join("store.db-wal"),
        &dir.join("store.db-shm"),
    ] {
        files::no_symlinks(path)?;
    }
    let conn = Connection::open_with_flags(
        dir.join("store.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|_| StoreError::RecoveryRequired)?;
    conn.busy_timeout(std::time::Duration::from_millis(100))
        .map_err(|_| StoreError::RecoveryRequired)?;
    conn.set_limit(
        rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
        MAX_BATCH_BYTES as i32 + 4096,
    )
    .map_err(db)?;
    conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_SQL_LENGTH, 16_384)
        .map_err(db)?;
    conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_ATTACHED, 0)
        .map_err(db)?;
    let _deadline = crate::deadline::Deadline::new(&conn, 5_000);
    conn.execute_batch("PRAGMA trusted_schema=OFF; PRAGMA query_only=ON;")
        .map_err(|_| StoreError::RecoveryRequired)?;
    let check: String = conn
        .query_row("PRAGMA quick_check(1)", [], |r| r.get(0))
        .map_err(|_| StoreError::RecoveryRequired)?;
    if check != "ok" {
        return Err(StoreError::RecoveryRequired);
    }
    let owner: String = conn
        .query_row("SELECT account FROM store_metadata WHERE id=1", [], |r| {
            r.get(0)
        })
        .map_err(|_| StoreError::RecoveryRequired)?;
    if owner.len() > 128 {
        return Err(StoreError::RecoveryRequired);
    }
    if owner != account {
        return Err(StoreError::StaleGeneration);
    }
    pending_page(&conn, after, limit).map_err(|_| StoreError::RecoveryRequired)
}
