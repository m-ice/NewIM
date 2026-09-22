"""Real PostgreSQL media acceptance suites. 每个套件使用独立持久卷。"""
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
    'db': '^TestMediaDB$',
    'security': '^TestMedia(Security|ProcessRestart|AmbiguousCommitLeavesPendingAndRecovers)$',
    'authz': '^TestMediaAuthz$',
    'check': '^TestMedia(DB|Security|Authz|ProcessRestart|AmbiguousCommitLeavesPendingAndRecovers)$',
}


def run_selected(db, pattern):
    result = db.commands.run([
        'docker', 'exec', db.name, '/tmp/media.test', '-test.v', '-test.count=1',
        '-test.timeout=180s', '-test.run', pattern,
    ], timeout=210, label='media-integration', check=False)
    (db.commands.directory / 'test.log').write_bytes(result.stdout + result.stderr)
    print(result.stdout.decode(), end='', flush=True)
    combined = result.stdout + result.stderr
    if (result.returncode or b'--- SKIP:' in combined or b'--- FAIL:' in combined or
            not re.search(rb'^--- PASS:', result.stdout, re.M) or
            not re.search(rb'^PASS$', result.stdout, re.M)):
        raise Failure('media integration execution failed or skipped')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT / 'build' / 'media' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'media.test'
        argv = ['go', 'test', '-c', '-tags=integration', '-o', str(binary), './tests/integration/media']
        started = time.monotonic()
        result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        commands.record(argv, result.returncode, time.monotonic() - started, 'media-cross-compile')
        (directory / 'compile.log').write_bytes(result.stdout + result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end='')
            raise Failure('media integration cross compilation failed')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql())
            commands.run(['docker', 'cp', str(binary), db.name + ':/tmp/media.test'],
                         label='copy-owned-media-test-binary')
            run_selected(db, TESTS[args.suite])
        print('Media suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('Media suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
