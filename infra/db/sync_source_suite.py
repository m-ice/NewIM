"""Run real conversation source gates in an owned, digest-verified PostgreSQL container."""
from __future__ import annotations

import argparse
import os
import uuid

from auth_suite import restart_database, result_errors
from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestSyncSourceCheck',
    'security': 'TestSyncSourceSecurity',
    'recovery': 'TestSyncSourceRecovery',
}


def run_test(db, name, phase='single'):
    argv = ['docker', 'exec', '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_SERVER_BINARY=/tmp/newim-server',
            '-e', 'NEWIM_SYNC_BACKFILL_BINARY=/tmp/newim-sync-backfill',
            '-e', 'NEWIM_SYNC_SOURCE_PHASE=' + phase, db.name, '/tmp/syncsource.test']
    listing = db.commands.run(argv + ['-test.list', '^' + name + '$'], label='list-' + phase)
    result = db.commands.run(argv + ['-test.v', '-test.count=1', '-test.timeout=120s',
                                     '-test.run', '^' + name + '$'], timeout=135,
                             label='sync-source-integration-' + phase, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / ('test-' + phase + '.log')).write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    problems = result_errors(name, listing.stdout.decode().splitlines(), output, result.returncode)
    if problems:
        raise Failure('conversation source test failed or skipped: ' + phase + ': ' + ', '.join(problems))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT / 'build' / 'sync-source' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'syncsource.test'
        servers = ('newim-gateway', 'newim-server', 'newim-sync-backfill')
        build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(binary),
                         './tests/integration/syncsource'], env, 'sync-source-cross-compile')
        for name in servers:
            build(commands, ['go', 'build', '-o', str(directory / name), './server/cmd/' + name],
                  env, 'sync-source-' + name + '-build')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql(13 if args.suite == 'recovery' else 14))
            paths = (binary.name, *servers)
            for name in paths:
                commands.run(['docker', 'cp', str(directory / name), db.name + ':/tmp/' + name],
                             label='copy-owned-' + name)
            commands.run(['docker', 'exec', db.name, 'chmod', '0755', *('/tmp/' + name for name in paths)],
                         label='make-sync-source-executable')
            if args.suite == 'recovery':
                migration = directory / 'sync-source-migration.sql'
                migration.write_text(migration_sql(14))
                commands.run(['docker', 'cp', str(migration), db.name + ':/tmp/sync-source-migration.sql'],
                             label='copy-pinned-sync-source-migration')
                run_test(db, TESTS[args.suite], 'prepare')
                restart_database(db)
                run_test(db, TESTS[args.suite], 'restart')
            else:
                run_test(db, TESTS[args.suite])
        print('Conversation source suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('Conversation source suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
