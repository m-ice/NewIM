"""Run user-profile integration against owned PostgreSQL and real HTTP/TLS processes."""
from __future__ import annotations

import argparse
import os
import uuid

from auth_suite import result_errors
from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestUserCheck',
    'security': 'TestUserSecurity',
}


def run_test(db, test_name):
    argv = ['docker', 'exec',
            '-e', 'NEWIM_SERVER_BINARY=/tmp/newim-server',
            '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_USER_MIGRATION_SQL=/tmp/user-migration.sql',
            db.name, '/tmp/user.test']
    listing = db.commands.run(argv + ['-test.list', '^' + test_name + '$'], label='list-user-test')
    result = db.commands.run(argv + ['-test.v', '-test.count=1', '-test.timeout=120s',
                                     '-test.run', '^' + test_name + '$'], timeout=135,
                             label='user-integration', check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / 'test.log').write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    problems = result_errors(test_name, listing.stdout.decode().splitlines(), output, result.returncode)
    if problems:
        raise Failure('user test failed or skipped: ' + ', '.join(problems))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT / 'build' / 'user' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'user.test'
        servers = ('newim-server', 'newim-gateway')
        build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(binary),
                         './tests/integration/user'], env, 'user-cross-compile')
        for server in servers:
            build(commands, ['go', 'build', '-o', str(directory / server), './server/cmd/' + server],
                  env, server + '-build')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql(9 if args.suite == 'check' else 10))
            paths = (binary.name, *servers)
            for name in paths:
                commands.run(['docker', 'cp', str(directory / name), db.name + ':/tmp/' + name],
                             label='copy-owned-' + name)
            commands.run(['docker', 'exec', db.name, 'chmod', '0755', *('/tmp/' + name for name in paths)],
                         label='make-user-fixtures-executable')
            if args.suite == 'check':
                migration = directory / 'user-migration.sql'
                migration.write_text(migration_sql(10))
                commands.run(['docker', 'cp', str(migration), db.name + ':/tmp/user-migration.sql'],
                             label='copy-pinned-user-migration')
            run_test(db, TESTS[args.suite])
        print('User suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('User suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
