pub const SQL1: &str = include_str!("../migrations/001-initial.sql");
pub const HASH1: &str = "d51daa1770eed169456c75d73faaf8d357e0017b131291a83b97622018854581";
pub const SQL2: &str = include_str!("../migrations/002-cache-indexes.sql");
pub const HASH2: &str = "352174f3eea924f97ccb19795f0b365f28e600fdb3e8e67f2482f139de43555b";
use crate::{StoreError, db};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
pub const VERSION: i64 = 2;
const APPLICATION_ID: i64 = 0x4e494d31;

pub fn apply(
    conn: &mut Connection,
    account: &str,
    recovery: bool,
    allow_create: bool,
) -> Result<(), StoreError> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(db)?;
    let version: i64 = tx
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .map_err(db)?;
    let application: i64 = tx
        .pragma_query_value(None, "application_id", |r| r.get(0))
        .map_err(db)?;
    if !(0..=VERSION).contains(&version) || (version > 0 && application != APPLICATION_ID) {
        return Err(StoreError::UnsupportedSchema);
    }
    if version == 0 {
        if !allow_create {
            return Err(StoreError::RecoveryRequired);
        }
        let tables: i64 = tx
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                [],
                |r| r.get(0),
            )
            .map_err(db)?;
        if tables != 0 {
            return Err(StoreError::UnsupportedSchema);
        }
        tx.execute_batch("CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, checksum TEXT NOT NULL, script BLOB NOT NULL)").map_err(db)?;
    }
    for (v, sql, hash) in [(1, SQL1, HASH1), (2, SQL2, HASH2)] {
        if v <= version {
            let existing: Option<(String, Vec<u8>)> = tx
                .query_row(
                    "SELECT checksum,script FROM schema_migrations WHERE version=?",
                    [v],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(db)?;
            if existing != Some((hash.to_owned(), sql.as_bytes().to_vec())) {
                return Err(StoreError::UnsupportedSchema);
            }
        } else {
            tx.execute_batch(sql).map_err(db)?;
            tx.execute(
                "INSERT INTO schema_migrations VALUES(?,?,?)",
                params![v, hash, sql.as_bytes()],
            )
            .map_err(db)?;
        }
    }
    let n: i64 = tx
        .query_row("SELECT count(*) FROM schema_migrations", [], |r| r.get(0))
        .map_err(db)?;
    if n != VERSION {
        return Err(StoreError::UnsupportedSchema);
    }
    if version == 0 {
        tx.execute("INSERT INTO store_metadata(id,account,instance,generation,revision,recovery_required) VALUES(1,?,lower(hex(randomblob(16))),1,0,?)",params![account,recovery]).map_err(db)?;
    }
    let owner: String = tx
        .query_row("SELECT account FROM store_metadata WHERE id=1", [], |r| {
            r.get(0)
        })
        .map_err(db)?;
    if owner != account {
        return Err(StoreError::StaleGeneration);
    }
    tx.pragma_update(None, "application_id", APPLICATION_ID)
        .map_err(db)?;
    tx.pragma_update(None, "user_version", VERSION)
        .map_err(db)?;
    validate_schema(&tx)?;
    tx.commit().map_err(db)
}

fn schema(conn: &Connection) -> Result<Vec<(String, String, String)>, StoreError> {
    let mut statement=conn.prepare("SELECT name,type,coalesce(sql,'') FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' AND name!='schema_migrations' ORDER BY name LIMIT 33").map_err(db)?;
    statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .map_err(db)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(db)
}
fn validate_schema(conn: &Connection) -> Result<(), StoreError> {
    let expected = Connection::open_in_memory().map_err(db)?;
    expected.execute_batch(SQL1).map_err(db)?;
    expected.execute_batch(SQL2).map_err(db)?;
    if schema(conn)? != schema(&expected)? {
        return Err(StoreError::UnsupportedSchema);
    }
    Ok(())
}
