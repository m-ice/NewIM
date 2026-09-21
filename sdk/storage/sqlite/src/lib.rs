//! 原生 SQLite 本地存储 / Native SQLite store with explicit durability boundaries.
#![forbid(unsafe_code)]
mod deadline;
mod files;
mod migrations;
mod operations;
mod recovery;
pub use recovery::{RecoveryReport, quarantine, rebuild, salvage_pending};

use newim_sdk_core::store::*;
use rusqlite::{Connection, ErrorCode, OpenFlags};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    time::Duration,
};

const SOURCE_ID: &str =
    "2026-07-24 19:02:57 bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc";
/// 首版工程容量，可向下配置 / Initial engineering limits, configurable downward.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub records: usize,
    pub pages: u32,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            records: 10_000,
            pages: 32_768,
        }
    }
}

pub struct SqliteStore {
    conn: Connection,
    root: PathBuf,
    fence: Fence,
    limits: Limits,
    completion: Option<Completion>,
    requires_reopen: bool,
    _lock: File,
}

pub(crate) fn db(error: rusqlite::Error) -> StoreError {
    match error.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => StoreError::Busy,
        Some(ErrorCode::DiskFull) => StoreError::StorageFull,
        Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => StoreError::Corrupt,
        Some(ErrorCode::ConstraintViolation) => StoreError::IdentityConflict,
        Some(ErrorCode::OperationInterrupted) => StoreError::Cancelled,
        _ => StoreError::Io,
    }
}
pub fn verify_engine() -> Result<(), StoreError> {
    let conn = Connection::open_in_memory().map_err(db)?;
    let (version, source): (String, String) = conn
        .query_row("SELECT sqlite_version(),sqlite_source_id()", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .map_err(db)?;
    if version != "3.53.4" || source != SOURCE_ID {
        return Err(StoreError::UnsupportedEngine);
    }
    for option in [
        "OMIT_LOAD_EXTENSION",
        "THREADSAFE=1",
        "DQS=0",
        "ENABLE_API_ARMOR",
    ] {
        let enabled: bool = conn
            .query_row("SELECT sqlite_compileoption_used(?)", [option], |r| {
                r.get(0)
            })
            .map_err(db)?;
        if !enabled {
            return Err(StoreError::UnsupportedEngine);
        }
    }
    Ok(())
}
impl SqliteStore {
    /// 独占账户目录；不自动删除损坏数据 / Own an account directory; never silently rebuild.
    pub fn open(root: impl AsRef<Path>, account: &str, limits: Limits) -> Result<Self, StoreError> {
        let root = root.as_ref();
        let lock = files::lock_root(root)?;
        if root.join("RECOVERY").exists() {
            return Err(StoreError::RecoveryRequired);
        }
        Self::open_locked(root, account, limits, lock, false)
    }
    pub(crate) fn open_locked(
        root: &Path,
        account: &str,
        limits: Limits,
        lock: File,
        recovery: bool,
    ) -> Result<Self, StoreError> {
        if !(1..=10_000).contains(&limits.records) || !(32..=32_768).contains(&limits.pages) {
            return Err(StoreError::InvalidInput);
        }
        let valid = Request {
            fence: Fence {
                account: account.to_owned(),
                instance: "check".into(),
                generation: 1,
            },
            operation_id: 1,
            action: Action::Metrics,
        };
        validate_request(&valid)?;
        verify_engine()?;
        let active = files::safe_active(root)?;
        if !recovery && root.join("initialized").exists() && !active.join("store.db").is_file() {
            return Err(StoreError::RecoveryRequired);
        }
        fs::create_dir_all(&active).map_err(|_| StoreError::Io)?;
        let mut conn = Connection::open_with_flags(
            active.join("store.db"),
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(db)?;
        conn.busy_timeout(Duration::from_millis(100)).map_err(db)?;
        conn.set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            MAX_BATCH_BYTES as i32 + 4096,
        )
        .map_err(db)?;
        conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_SQL_LENGTH, 16_384)
            .map_err(db)?;
        conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_ATTACHED, 0)
            .map_err(db)?;
        let _deadline = deadline::Deadline::new(&conn, 30_000);
        conn.execute_batch(
            "PRAGMA trusted_schema=OFF; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;",
        )
        .map_err(db)?;
        let check: String = conn
            .query_row("PRAGMA quick_check(1)", [], |r| r.get(0))
            .map_err(db)?;
        if check != "ok" {
            return Err(StoreError::Corrupt);
        }
        migrations::apply(&mut conn, account, recovery)?;
        let mode: String = conn
            .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
            .map_err(db)?;
        if mode != "wal" {
            return Err(StoreError::Io);
        }
        conn.pragma_update(None, "max_page_count", limits.pages)
            .map_err(db)?;
        let pages: i64 = conn
            .pragma_query_value(None, "max_page_count", |r| r.get(0))
            .map_err(db)?;
        if pages > i64::from(limits.pages) {
            return Err(StoreError::StorageFull);
        }
        conn.pragma_update(None, "journal_size_limit", 4_194_304i64)
            .map_err(db)?;
        let fence = conn
            .query_row(
                "SELECT account,instance,generation FROM store_metadata WHERE id=1",
                [],
                |r| {
                    Ok(Fence {
                        account: r.get(0)?,
                        instance: r.get(1)?,
                        generation: r.get::<_, i64>(2)? as u64,
                    })
                },
            )
            .map_err(db)?;
        if !root.join("initialized").exists() {
            files::marker(&root.join("initialized"), b"NewIM LocalStore v1\n")?;
        }
        Ok(Self {
            conn,
            root: root.to_path_buf(),
            fence,
            limits,
            completion: None,
            requires_reopen: false,
            _lock: lock,
        })
    }
    pub fn fence(&self) -> &Fence {
        &self.fence
    }
    /// 同步执行器供宿主工作线程使用 / Blocking executor for a host storage worker.
    pub fn execute(&mut self, request: Request) -> Completion {
        let result = validate_request(&request).and_then(|_| {
            if self.requires_reopen {
                return Err(StoreError::RecoveryRequired);
            }
            if request.fence != self.fence {
                return Err(StoreError::StaleGeneration);
            }
            self.perform(&request)
        });
        if matches!(
            result,
            Err(StoreError::Corrupt | StoreError::CommitOutcomeUnknown)
        ) {
            self.requires_reopen = true;
        }
        Completion {
            fence: request.fence,
            operation_id: request.operation_id,
            result,
        }
    }
}
impl LocalStore for SqliteStore {
    fn submit(&mut self, request: Request) -> Result<(), StoreError> {
        if self.completion.is_some() {
            return Err(StoreError::Busy);
        }
        self.completion = Some(self.execute(request));
        Ok(())
    }
    fn take_completion(&mut self) -> Option<Completion> {
        self.completion.take()
    }
}
