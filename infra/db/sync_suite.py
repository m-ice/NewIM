"""Run tagged sync integration tests in an owned, durable PostgreSQL container."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import uuid

from migrate import migration_sql
from runtime import Commands, Database, DB_DIR, Failure, ROOT, image_identity

TESTS = {'bootstrap': 'TestBootstrap', 'delta': 'TestDeltaTombstones',
         'cursor': 'TestCursorExpiry', 'query-plan': 'TestQueryPlan',
         'recovery': 'TestIsolationRecovery', 'migrations': 'TestSyncMigrations'}


def run_test(db, name, phase='single'):
    argv = ['docker', 'exec', '-e', 'NEWIM_SYNC_PHASE='+phase, db.name,
            '/tmp/conversationsync.test']
    listing = db.commands.run(argv+['-test.list', '^'+name+'$'], label='list-'+phase)
    if listing.stdout.decode().splitlines() != [name]:
        raise Failure('integration test discovery must identify exactly one test')
    result = db.commands.run(argv+['-test.v', '-test.count=1', '-test.timeout=180s',
                                  '-test.run', '^'+name+'$'], timeout=195,
                             label='integration-'+phase, check=False)
    (db.commands.directory / ('test-'+phase+'.log')).write_bytes(result.stdout+result.stderr)
    print(result.stdout.decode(), end='', flush=True)
    combined = result.stdout + result.stderr
    if (result.returncode or b'--- SKIP:' in combined or
            not re.search(rb'^--- PASS: '+name.encode()+rb'\b', result.stdout, re.M) or
            not re.search(rb'^PASS$', result.stdout, re.M)):
        raise Failure('integration execution failed or skipped: '+phase)


def migration_faults(db):
    db.sql(migration_sql(2))
    db.sql("INSERT INTO newim.im_users(user_id) VALUES ('migration_existing'); "
           "INSERT INTO newim.im_conversations(conversation_id) VALUES ('migration_conv'); "
           "INSERT INTO newim.im_conversation_members VALUES ('migration_conv','migration_existing');")
    baseline = db.sql("SELECT version,sha256 FROM newim_meta.migrations ORDER BY version")
    owned = db.commands.directory / 'migration-copy'
    shutil.copytree(DB_DIR/'migrations', owned)
    file = owned/'003_conversation_sync.sql'
    original = file.read_text()
    marker = '-- SYNC_MIGRATION_FAULT_POINT'
    if original.count(marker) != 1:
        raise Failure('expected exactly one sync migration fault marker')
    file.write_text(original.replace(marker, 'SELECT 1/0;'))
    db.sql(migration_sql(directory=owned), error='22012')
    def preserved():
        if db.sql("SELECT version,sha256 FROM newim_meta.migrations ORDER BY version") != baseline:
            raise Failure('migration failure changed historical ledger')
        if db.sql("SELECT count(*) FROM newim.im_conversation_members WHERE user_id='migration_existing'").strip() != '1':
            raise Failure('migration failure changed populated source')
        if db.sql("SELECT count(*) FROM pg_tables WHERE schemaname='newim' AND tablename LIKE 'im_conversation_sync_%'").strip() != '0':
            raise Failure('failed migration left sync tables')
    preserved()
    blocker = db.session("BEGIN; SELECT pg_advisory_xact_lock(731003);")
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=731003 AND granted)")
    file.write_text(original.replace(marker, "SET application_name='sync_migration_victim'; SELECT pg_advisory_xact_lock(731003);"))
    victim = db.session(migration_sql(directory=owned))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='sync_migration_victim' AND wait_event='advisory')")
    db.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='sync_migration_victim'")
    victim.finish(error='57P01')
    blocker.send('ROLLBACK;')
    blocker.finish()
    preserved()
    db.sql(migration_sql())
    db.sql(migration_sql())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT/'build'/'sync'/(args.suite+'-'+uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        keys = ('PATH','HOME','GOROOT','GOPATH','GOMODCACHE','GOCACHE','GOTMPDIR','GOENV','GOFLAGS','GOEXPERIMENT','GOVERSION','CGO_ENABLED','GOOS','GOARCH','GOTOOLCHAIN','CC','CXX')
        recorded = {}
        for key in keys:
            if key not in env:
                continue
            value = env[key]
            if any(part in key for part in ('TOKEN','SECRET','PASSWORD','CREDENTIAL','AUTH','KEY')):
                value = 'sha256:'+hashlib.sha256(value.encode()).hexdigest()
            recorded[key] = value
        (directory/'build-environment.json').write_text(json.dumps({
            'cwd': str(ROOT), 'environment': recorded}, indent=2)+'\n')
        binary = directory/'conversationsync.test'
        argv = ['go','test','-c','-tags=integration','-o',str(binary),'./tests/integration/conversationsync']
        start = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        except subprocess.TimeoutExpired:
            commands.record(argv,124,time.monotonic()-start,'cross-compile')
            raise Failure('cross compilation timeout')
        commands.record(argv,result.returncode,time.monotonic()-start,'cross-compile')
        (directory/'compile.log').write_bytes(result.stdout+result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end='')
            raise Failure('integration cross compilation failed')
        with Database(commands,image,platform) as db:
            if args.suite == 'migrations':
                migration_faults(db)
            else:
                db.sql(migration_sql())
            commands.run(['docker','cp',str(binary),db.name+':/tmp/conversationsync.test'],label='copy-owned-test-binary')
            if args.suite == 'recovery':
                run_test(db,TESTS[args.suite],'prepare')
                commands.run(['docker','kill','--signal=KILL',db.name],label='crash-database')
                commands.run(['docker','start',db.name],label='restart-database')
                db.ready()
                run_test(db,TESTS[args.suite],'restart')
                dump = commands.run(['docker','exec',db.name,'pg_dump','-U','newim_test','-d','newim_test','--format=custom','--no-owner'],timeout=60,label='dump-populated-sync')
                commands.run(['docker','exec',db.name,'createdb','-U','newim_test','sync_restore'],label='create-restore-database')
                commands.run(['docker','exec','-i',db.name,'pg_restore','-U','newim_test','-d','sync_restore','--exit-on-error','--no-owner'],data=dump.stdout,timeout=60,label='restore-populated-sync')
                run_test(db,TESTS[args.suite],'restore')
            else:
                run_test(db,TESTS[args.suite])
            if args.suite == 'query-plan':
                commands.run(['docker','cp',db.name+':/tmp/nim-syn-003-query-plans.json',str(directory/'query-plans.json')],label='copy-query-plans')
        print('Sync suite passed; command records: '+str(directory))
    except (Failure,OSError) as exc:
        print('Sync suite failed: '+str(exc)+'; command records: '+str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
