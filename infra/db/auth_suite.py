"""Run tagged auth integration tests in an owned durable PostgreSQL container."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid

from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestAuthCheck',
    'recovery': 'TestAuthRecovery',
    'policy': 'TestAuthPolicy',
    'http': 'TestAuthHTTP',
    'route': 'TestAuthRoute',
    'logout': 'TestAuthLogout',
    'logout-restore': 'TestAuthLogoutRestore',
}


def result_errors(name, listing, stdout, returncode):
    errors = []
    if listing != [name]:
        errors.append('discovery must identify exactly one named test')
    combined = stdout
    if returncode:
        errors.append('test process returned nonzero')
    if b'--- SKIP:' in combined:
        errors.append('test output contains a skip')
    if b'--- FAIL:' in combined:
        errors.append('test output contains a failure')
    if not re.search(rb'^--- PASS: '+re.escape(name.encode())+rb'\b', combined, re.M):
        errors.append('exact named test did not pass')
    if not re.search(rb'^PASS$', combined, re.M):
        errors.append('test output has no final PASS')
    return errors


def self_test():
    name = 'TestAuthLogout'
    passing = b'=== RUN   '+name.encode()+b'\n--- PASS: '+name.encode()+b' (0.01s)\nPASS\n'
    cases = (
        ('passing', [name], passing, 0),
        ('empty', [], passing, 0),
        ('extra', [name, 'TestExtra'], passing, 0),
        ('skip', [name], passing.replace(b'--- PASS:', b'--- SKIP:'), 0),
        ('subtest-skip', [name], passing+b'    --- SKIP: TestAuthLogout/child\n', 0),
        ('missing-pass', [name], passing.replace(b'--- PASS: '+name.encode(), b'--- RUN: '+name.encode()), 0),
        ('nonzero', [name], passing, 1),
    )
    for label, listing, stdout, returncode in cases:
        errors = result_errors(name, listing, stdout, returncode)
        if (label == 'passing') == bool(errors):
            print('auth suite self-test failed: '+label, file=sys.stderr)
            return 1
    print('auth suite self-test: OK')
    return 0


def dump_and_restore(db):
    dump = db.commands.run(['docker', 'exec', db.name, 'pg_dump', '-U', 'newim_test',
                            '-d', 'newim_test', '--format=custom', '--no-owner'],
                           timeout=60, label='dump-populated-auth')
    db.commands.run(['docker', 'exec', db.name, 'createdb', '-U', 'newim_test', 'auth_restore'],
                    label='create-auth-restore-database')
    db.commands.run(['docker', 'exec', '-i', db.name, 'pg_restore', '-U', 'newim_test',
                     '-d', 'auth_restore', '--exit-on-error', '--no-owner'],
                    data=dump.stdout, timeout=60, label='restore-populated-auth')


def run_test(db, name, phase='single', env=None):
    argv = ['docker', 'exec', '-e', 'NEWIM_AUTH_PHASE='+phase]
    for key, value in (env or {}).items():
        argv += ['-e', key+'='+value]
    argv += [db.name, '/tmp/auth.test']
    listing = db.commands.run(argv+['-test.list', '^'+name+'$'], label='list-'+phase)
    result = db.commands.run(argv+['-test.v', '-test.count=1', '-test.timeout=180s',
                                  '-test.run', '^'+name+'$'], timeout=195,
                             label='auth-integration-'+phase, check=False)
    (db.commands.directory / ('test-'+phase+'.log')).write_bytes(result.stdout+result.stderr)
    print(result.stdout.decode(), end='', flush=True)
    errors = result_errors(name, listing.stdout.decode().splitlines(), result.stdout+result.stderr, result.returncode)
    if errors:
        raise Failure('auth integration execution failed or skipped: '+phase+': '+', '.join(errors))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', nargs='?', choices=TESTS)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.suite is None:
        parser.error('suite is required unless --self-test is used')
    directory = ROOT/'build'/'auth'/(args.suite+'-'+uuid.uuid4().hex)
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
        binary = directory/'auth.test'
        argv = ['go', 'test', '-c', '-tags=integration', '-o', str(binary), './tests/integration/auth']
        started = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        except subprocess.TimeoutExpired:
            commands.record(argv, 124, time.monotonic()-started, 'auth-cross-compile')
            raise Failure('auth integration cross compilation timeout')
        commands.record(argv, result.returncode, time.monotonic()-started, 'auth-cross-compile')
        (directory/'compile.log').write_bytes(result.stdout+result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end='')
            raise Failure('auth integration cross compilation failed')
        server_binary = None
        if args.suite == 'route':
            server_binary = directory/'newim-server'
            server_argv = ['go', 'build', '-o', str(server_binary), './server/cmd/newim-server']
            started = time.monotonic()
            result = subprocess.run(server_argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
            commands.record(server_argv, result.returncode, time.monotonic()-started, 'auth-route-server-build')
            (directory/'server-build.log').write_bytes(result.stdout+result.stderr)
            if result.returncode:
                print(result.stderr.decode(), end='')
                raise Failure('auth route server binary build failed')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql())
            commands.run(['docker', 'cp', str(binary), db.name+':/tmp/auth.test'],
                         label='copy-owned-auth-test-binary')
            if server_binary is not None:
                commands.run(['docker', 'cp', str(server_binary), db.name+':/tmp/newim-server'],
                             label='copy-owned-auth-route-server')
            if args.suite == 'recovery':
                run_test(db, TESTS[args.suite], 'prepare')
                commands.run(['docker', 'kill', '--signal=KILL', db.name], label='crash-auth-database')
                commands.run(['docker', 'start', db.name], label='restart-auth-database')
                db.ready()
                run_test(db, TESTS[args.suite], 'restart')
                dump_and_restore(db)
                run_test(db, TESTS[args.suite], 'restore')
            elif args.suite == 'logout':
                run_test(db, TESTS['logout'], 'prepare')
                dump_and_restore(db)
                run_test(db, TESTS['logout-restore'], 'restore')
            elif args.suite == 'route':
                run_test(db, TESTS[args.suite], env={'NEWIM_SERVER_BINARY': '/tmp/newim-server'})
            else:
                run_test(db, TESTS[args.suite])
        print('Auth suite passed; command records: '+str(directory))
    except (Failure, OSError) as exc:
        print('Auth suite failed: '+str(exc)+'; command records: '+str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
