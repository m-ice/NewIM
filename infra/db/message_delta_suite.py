"""Run internal message-delta integration tests in an owned PostgreSQL container."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'authz': 'TestAuthz',
    'delta': 'TestDelta',
    'query-plan': 'TestQueryPlan',
    'recovery': 'TestRecovery',
    'redaction': 'TestRedaction',
}
TARGETS = [
    ('messagesync', './tests/integration/messagesync', 'messagesync.test'),
    ('storage', './server/storage/messagesync', 'messagesync-storage.test'),
]

def migration(db):
    db.sql(migration_sql())


def record_environment(directory: Path):
    env = os.environ.copy()
    env.update(CGO_ENABLED='0', GOOS='linux', GOARCH='arm64' if os.uname().machine in ('arm64', 'aarch64') else 'amd64', GOTOOLCHAIN='local')
    keys = ('PATH','HOME','GOROOT','GOPATH','GOMODCACHE','GOCACHE','GOTMPDIR','GOENV','GOFLAGS','GOEXPERIMENT','GOVERSION','CGO_ENABLED','GOOS','GOARCH','GOTOOLCHAIN','CC','CXX')
    recorded = {}
    for key in keys:
        if key not in env:
            continue
        value = env[key]
        if any(part in key for part in ('TOKEN','SECRET','PASSWORD','CREDENTIAL','AUTH','KEY')):
            value = 'sha256:'+hashlib.sha256(value.encode()).hexdigest()
        recorded[key] = value
    (directory/'build-environment.json').write_text(json.dumps({'cwd': str(ROOT), 'environment': recorded}, indent=2)+'\n')
    return env


def compile_binaries(commands: Commands, directory: Path, env):
    binaries = {}
    for label, package, name in TARGETS:
        binary = directory/name
        argv = ['go','test','-c','-tags=integration','-o',str(binary),package]
        started = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        except subprocess.TimeoutExpired as exc:
            commands.record(argv, 124, time.monotonic()-started, 'compile-'+label)
            raise Failure(label+' compilation timeout') from exc
        commands.record(argv, result.returncode, time.monotonic()-started, 'compile-'+label)
        (directory/('compile-'+label+'.log')).write_bytes(result.stdout+result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end='')
            raise Failure(label+' compilation failed')
        binaries[label] = binary
    return binaries


def run_test(db, binary, name, phase=None, label=None, timeout=240):
    argv = ['docker','exec']
    if phase:
        argv += ['-e','NEWIM_MESSAGE_DELTA_PHASE='+phase]
    argv += [db.name, '/tmp/'+Path(binary).name]
    listing = db.commands.run(argv+['-test.list','^'+name+'$'], label='list-'+label)
    if listing.stdout.decode().splitlines() != [name]:
        raise Failure('integration discovery must identify exactly one test')
    result = db.commands.run(argv+['-test.v','-test.count=1','-test.timeout=180s','-test.run','^'+name+'$'],
                             timeout=timeout, label='integration-'+label, check=False)
    logname = 'test-'+label+'.log'
    (db.commands.directory/logname).write_bytes(result.stdout+result.stderr)
    print(result.stdout.decode(), end='', flush=True)
    combined = result.stdout+result.stderr
    if (result.returncode or b'--- SKIP:' in combined or
            not re.search(rb'^--- PASS: '+name.encode()+rb'\b', result.stdout, re.M) or
            not re.search(rb'^PASS$', result.stdout, re.M)):
        raise Failure('integration execution failed or skipped: '+label)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT/'build'/'message-delta'/(args.suite+'-'+uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = record_environment(directory)
        env['GOARCH'] = 'arm64' if platform == 'linux/arm64/v8' else 'amd64'
        binaries = compile_binaries(commands, directory, env)
        with Database(commands, image, platform) as db:
            migration(db)
            commands.run(['docker','cp',str(binaries['messagesync']),db.name+':/tmp/messagesync.test'], label='copy-main-binary')
            commands.run(['docker','cp',str(binaries['storage']),db.name+':/tmp/messagesync-storage.test'], label='copy-storage-binary')
            if args.suite == 'recovery':
                run_test(db, binaries['messagesync'], TESTS[args.suite], phase='prepare', label='prepare')
                commands.run(['docker','kill','--signal=KILL',db.name], label='crash-database')
                commands.run(['docker','start',db.name], label='restart-database')
                db.ready()
                run_test(db, binaries['messagesync'], TESTS[args.suite], phase='restart', label='restart')
                run_test(db, binaries['storage'], 'TestReadTransactionGuard', label='transaction-guard')
            else:
                run_test(db, binaries['messagesync'], TESTS[args.suite], label=args.suite)
            if args.suite == 'query-plan':
                commands.run(['docker','cp',db.name+':/tmp/nim-syn-005-query-plans.json',str(directory/'query-plans.json')], label='copy-query-plans')
            if args.suite == 'redaction':
                commands.run(['docker','cp',db.name+':/tmp/nim-syn-005-redaction.json',str(directory/'redaction.json')], label='copy-redaction-evidence')
                log = (directory/'test-redaction.log').read_text()
                for fragment in ('payload_token_secret_dsn_identifier_sql_driver',):
                    if fragment in log:
                        raise Failure('redaction sentinel appeared in test log')
        print('Message delta suite passed; command records: '+str(directory))
    except (Failure,OSError) as exc:
        print('Message delta suite failed: '+str(exc)+'; command records: '+str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
