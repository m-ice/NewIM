"""Run TLS local-media tests with an owned PostgreSQL fixture."""
from __future__ import annotations

import argparse
import os
import uuid

from auth_suite import restart_database, result_errors
from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestMediaHTTPCheck',
    'security': 'TestMediaHTTPSecurity',
    'recovery': 'TestMediaHTTPRecovery',
}


def run_test(db, name, phase='single'):
    argv = ['docker', 'exec', '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_MEDIA_HTTP_PHASE=' + phase, db.name, '/tmp/mediahttp.test']
    pattern = '^' + name + '$'
    listing = db.commands.run(argv + ['-test.list', pattern], label='list-' + phase)
    result = db.commands.run(argv + ['-test.v', '-test.count=1', '-test.timeout=120s',
                                     '-test.run', pattern], timeout=135,
                             label='media-http-integration-' + phase, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / ('test-' + phase + '.log')).write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    problems = result_errors(name, listing.stdout.decode().splitlines(), output, result.returncode)
    if problems:
        raise Failure('TLS media test failed or skipped: ' + phase + ': ' + ', '.join(problems))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT / 'build' / 'media-http' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'mediahttp.test'
        gateway = directory / 'newim-gateway'
        build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(binary),
                         './tests/integration/mediahttp'], env, 'media-http-cross-compile')
        build(commands, ['go', 'build', '-o', str(gateway), './server/cmd/newim-gateway'],
              env, 'media-http-gateway-build')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql(14))
            for path in (binary, gateway):
                commands.run(['docker', 'cp', str(path), db.name + ':/tmp/' + path.name],
                             label='copy-owned-' + path.name)
            commands.run(['docker', 'exec', db.name, 'chmod', '0755',
                          '/tmp/mediahttp.test', '/tmp/newim-gateway'], label='make-media-http-executable')
            if args.suite == 'recovery':
                run_test(db, TESTS[args.suite], 'prepare')
                restart_database(db)
                run_test(db, TESTS[args.suite], 'restart')
            else:
                run_test(db, TESTS[args.suite])
        print('TLS media suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('TLS media suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
