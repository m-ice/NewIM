"""Generate one atomic migration session from immutable SQL bytes. 不读取远端脚本。"""
import hashlib
from pathlib import Path

from runtime import DB_DIR, Failure


def migration_sql(target=None, *, directory: Path | None = None):
    sources = sorted((directory or DB_DIR / 'migrations').glob('[0-9][0-9][0-9]_*.sql'))
    if not sources:
        raise Failure('no migrations found')
    versions = [int(p.name[:3]) for p in sources]
    if versions != list(range(1, len(sources)+1)):
        raise Failure('migration filenames are not a unique contiguous sequence')
    target = len(sources) if target is None else target
    if not 1 <= target <= len(sources):
        raise Failure('unsupported migration target')
    contents = [p.read_bytes() for p in sources[:target]]
    hashes = [hashlib.sha256(raw).hexdigest() for raw in contents]
    array = 'ARRAY[' + ','.join("'"+h+"'" for h in hashes) + ']::text[]'
    statements = ["BEGIN; SELECT pg_advisory_xact_lock(1947620131);",
                  "CREATE SCHEMA IF NOT EXISTS newim_meta; REVOKE ALL ON SCHEMA newim_meta FROM PUBLIC;",
                  "CREATE TABLE IF NOT EXISTS newim_meta.migrations (version integer PRIMARY KEY CHECK (version > 0), sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'), applied_at timestamptz NOT NULL DEFAULT clock_timestamp());",
                  f"""DO $migration_guard$
DECLARE actual record; expected text[] := {array};
BEGIN
 IF EXISTS (SELECT 1 FROM newim_meta.migrations WHERE version > {target}) THEN
  RAISE EXCEPTION USING ERRCODE='NM001', MESSAGE='MIGRATION_FUTURE_VERSION';
 END IF;
 IF (SELECT count(*) <> COALESCE(max(version),0) FROM newim_meta.migrations) THEN
  RAISE EXCEPTION USING ERRCODE='NM003', MESSAGE='MIGRATION_LEDGER_GAP';
 END IF;
 FOR actual IN SELECT version, sha256 FROM newim_meta.migrations LOOP
  IF actual.sha256 <> expected[actual.version] THEN
   RAISE EXCEPTION USING ERRCODE='NM002', MESSAGE='MIGRATION_CHECKSUM_MISMATCH';
  END IF;
 END LOOP;
END;
$migration_guard$;"""]
    for version, raw in enumerate(contents, 1):
        statements.extend([
            f'SELECT NOT EXISTS (SELECT 1 FROM newim_meta.migrations WHERE version={version}) AS apply_{version} \\gset',
            f'\\if :apply_{version}', raw.decode('utf-8'),
            f"INSERT INTO newim_meta.migrations(version,sha256) VALUES ({version},'{hashes[version-1]}');",
            '\\endif'])
    statements.append('COMMIT;')
    return '\n'.join(statements)


if __name__ == '__main__':
    # Explicit reviewed local bytes only; psql owns connection credentials.
    # 仅输出本地已审迁移，凭证与连接由运维显式提供给 psql。
    print(migration_sql())
