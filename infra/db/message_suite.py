"""Run tagged message send integration tests in an owned PostgreSQL container."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
import uuid

from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestMessageCheck',
    'recovery': 'TestMessageRecovery',
    'errors': 'TestMessageErrors',
}


def run_test(db, name, phase='single'):
    argv = ['docker', 'exec', '-e', 'NEWIM_MESSAGE_PHASE='+phase, db.name,
            '/tmp/message.test']
    listing = db.commands.run(argv+['-test.list', '^'+name+'$'], label='list-'+phase)
    if listing.stdout.decode().splitlines() != [name]:
        raise Failure('message integration discovery must identify exactly one test')
    result = db.commands.run(argv+['-test.v', '-test.count=1', '-test.timeout=180s',
                                  '-test.run', '^'+name+'$'], timeout=195,
                             label='message-integration-'+phase, check=False)
    (db.commands.directory / ('test-'+phase+'.log')).write_bytes(result.stdout+result.stderr)
    print(result.stdout.decode(), end='', flush=True)
    combined = result.stdout + result.stderr
    if (result.returncode or b'--- SKIP:' in combined or
            not re.search(rb'^--- PASS: '+name.encode()+rb'\b', result.stdout, re.M) or
            not re.search(rb'^PASS$', result.stdout, re.M)):
        raise Failure('message integration execution failed or skipped: '+phase)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT/'build'/'message'/(args.suite+'-'+uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        keys = ('PATH', 'HOME', 'GOROOT', 'GOPATH', 'GOMODCACHE', 'GOCACHE', 'GOTMPDIR',
                'GOENV', 'GOFLAGS', 'GOEXPERIMENT', 'GOVERSION', 'CGO_ENABLED', 'GOOS',
                'GOARCH', 'GOTOOLCHAIN', 'CC', 'CXX')
        recorded = {}
        for key in keys:
            if key not in env:
                continue
            value = env[key]
            if any(part in key for part in ('TOKEN', 'SECRET', 'PASSWORD', 'CREDENTIAL', 'AUTH', 'KEY')):
                value = 'sha256:'+hashlib.sha256(value.encode()).hexdigest()
            recorded[key] = value
        (directory/'build-environment.json').write_text(json.dumps({
            'cwd': str(ROOT), 'environment': recorded}, indent=2)+'\n')
        binary = directory/'message.test'
        argv = ['go', 'test', '-c', '-tags=integration', '-o', str(binary), './tests/integration/message']
        started = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        except subprocess.TimeoutExpired:
            commands.record(argv, 124, time.monotonic()-started, 'message-cross-compile')
            raise Failure('message integration cross compilation timeout')
        commands.record(argv, result.returncode, time.monotonic()-started, 'message-cross-compile')
        (directory/'compile.log').write_bytes(result.stdout+result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end='')
            raise Failure('message integration cross compilation failed')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql())
            commands.run(['docker', 'cp', str(binary), db.name+':/tmp/message.test'],
                         label='copy-owned-message-test-binary')
            if args.suite == 'recovery':
                run_test(db, TESTS[args.suite], 'prepare')
                hold = db.session("SET application_name='message_restart_hold'; BEGIN; SELECT newim.persist_message('recovery_restart_before_user','recovery_restart_before_client','recovery_restart_before_conversation','restart_before_server','text',1,1800000000000,convert_to('{\"text\":\"restart before commit\"}','UTF8'),'restart_before_event'); SELECT pg_sleep(60); COMMIT;")
                db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='message_restart_hold' AND wait_event='PgSleep')")
                commands.run(['docker', 'kill', '--signal=KILL', db.name], label='crash-message-before-commit')
                hold.cancel()
                commands.run(['docker', 'start', db.name], label='restart-message-after-uncommitted')
                db.ready()
                run_test(db, TESTS[args.suite], 'restart')
                run_test(db, TESTS[args.suite], 'restarted')
                dump = commands.run(['docker', 'exec', db.name, 'pg_dump', '-U', 'newim_test',
                                     '-d', 'newim_test', '--format=custom', '--no-owner'],
                                    timeout=60, label='dump-populated-message')
                commands.run(['docker', 'exec', db.name, 'createdb', '-U', 'newim_test', 'message_restore'],
                             label='create-message-restore-database')
                commands.run(['docker', 'exec', '-i', db.name, 'pg_restore', '-U', 'newim_test',
                              '-d', 'message_restore', '--exit-on-error', '--no-owner'],
                             data=dump.stdout, timeout=60, label='restore-populated-message')
                run_test(db, TESTS[args.suite], 'restore')
            else:
                run_test(db, TESTS[args.suite])
        print('Message suite passed; command records: '+str(directory))
    except (Failure, OSError) as exc:
        print('Message suite failed: '+str(exc)+'; command records: '+str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
