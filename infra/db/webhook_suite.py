"""Run tagged webhook integration tests in an owned durable PostgreSQL container."""
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
    'recovery': 'TestWebhookRecovery',
    'protocol': 'TestWebhookProtocol',
    'security': 'TestWebhookSecurity',
    'redaction': 'TestWebhookRedaction',
}


def run_test(db, name):
    argv = ['docker', 'exec', db.name, '/tmp/webhook.test']
    listing = db.commands.run(argv+['-test.list', '^'+name+'$'], label='list-webhook')
    if listing.stdout.decode().splitlines() != [name]:
        raise Failure('webhook integration discovery must identify exactly one test')
    result = db.commands.run(argv+['-test.v', '-test.count=1', '-test.timeout=180s',
                                  '-test.run', '^'+name+'$'], timeout=195,
                             label='webhook-integration', check=False)
    (db.commands.directory/'test.log').write_bytes(result.stdout+result.stderr)
    print(result.stdout.decode(), end='', flush=True)
    combined = result.stdout + result.stderr
    if (result.returncode or b'--- SKIP:' in combined or
            not re.search(rb'^--- PASS: '+name.encode()+rb'\b', result.stdout, re.M) or
            not re.search(rb'^PASS$', result.stdout, re.M)):
        raise Failure('webhook integration execution failed or skipped')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT/'build'/'webhook'/(args.suite+'-'+uuid.uuid4().hex)
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
        binary = directory/'webhook.test'
        argv = ['go', 'test', '-c', '-tags=integration', '-o', str(binary), './tests/integration/webhook']
        started = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        except subprocess.TimeoutExpired:
            commands.record(argv, 124, time.monotonic()-started, 'webhook-cross-compile')
            raise Failure('webhook integration cross compilation timeout')
        commands.record(argv, result.returncode, time.monotonic()-started, 'webhook-cross-compile')
        (directory/'compile.log').write_bytes(result.stdout+result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end='')
            raise Failure('webhook integration cross compilation failed')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql())
            commands.run(['docker', 'cp', str(binary), db.name+':/tmp/webhook.test'],
                         label='copy-owned-webhook-test-binary')
            run_test(db, TESTS[args.suite])
        print('Webhook suite passed; command records: '+str(directory))
    except (Failure, OSError) as exc:
        print('Webhook suite failed: '+str(exc)+'; command records: '+str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
