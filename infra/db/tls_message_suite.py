"""Run real TLS private-message gates in an owned, digest-verified PostgreSQL container."""
from __future__ import annotations

import argparse
import os
import uuid

from auth_suite import restart_database, result_errors
from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestTLSMessageCheck',
    'security': 'TestTLSMessageSecurity',
    'recovery': 'TestTLSMessageRecovery',
}


def run_test(db, name, phase='single'):
    argv = ['docker', 'exec', '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_TLS_MESSAGE_PHASE=' + phase, db.name, '/tmp/tlsmessage.test']
    listing = db.commands.run(argv + ['-test.list', '^' + name + '$'], label='list-' + phase)
    result = db.commands.run(argv + ['-test.v', '-test.count=1', '-test.timeout=120s',
                                     '-test.run', '^' + name + '$'], timeout=135,
                             label='tls-message-integration-' + phase, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / ('test-' + phase + '.log')).write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    problems = result_errors(name, listing.stdout.decode().splitlines(), output, result.returncode)
    if problems:
        raise Failure('TLS message test failed or skipped: ' + phase + ': ' + ', '.join(problems))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT / 'build' / 'tls-message' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'tlsmessage.test'
        gateway = directory / 'newim-gateway'
        build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(binary),
                         './tests/integration/tlsmessage'], env, 'tls-message-cross-compile')
        build(commands, ['go', 'build', '-o', str(gateway), './server/cmd/newim-gateway'],
              env, 'tls-message-gateway-build')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql(11))
            for path in (binary, gateway):
                commands.run(['docker', 'cp', str(path), db.name + ':/tmp/' + path.name],
                             label='copy-owned-' + path.name)
            commands.run(['docker', 'exec', db.name, 'chmod', '0755',
                          '/tmp/tlsmessage.test', '/tmp/newim-gateway'], label='make-tls-message-executable')
            if args.suite == 'recovery':
                run_test(db, TESTS[args.suite], 'prepare')
                restart_database(db)
                run_test(db, TESTS[args.suite], 'restart')
            else:
                run_test(db, TESTS[args.suite])
        print('TLS message suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('TLS message suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
