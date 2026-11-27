"""Run real TLS conversation sync gates in an owned, digest-verified PostgreSQL container."""
from __future__ import annotations

import argparse
import os
import uuid

from auth_suite import restart_database, result_errors
from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    'check': 'TestSyncHTTPCheck',
    'security': 'TestSyncHTTPSecurity',
    'recovery': 'TestSyncHTTPRecovery',
    'read-watermark-check': 'TestReadWatermarkHTTPCheck',
    'read-watermark-security': 'TestReadWatermarkHTTPSecurity',
    'read-watermark-recovery': 'TestReadWatermarkHTTPRecovery',
    'read-receipt-check': 'TestReadReceiptHTTPCheck',
    'read-receipt-security': 'TestReadReceiptHTTPSecurity',
    'read-receipt-recovery': 'TestReadReceiptHTTPRecovery',
}


def run_test(db, name, phase='single'):
    argv = ['docker', 'exec', '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_SERVER_BINARY=/tmp/newim-server',
            '-e', 'NEWIM_SYNC_BACKFILL_BINARY=/tmp/newim-sync-backfill',
            '-e', 'NEWIM_SYNC_HTTP_PHASE=' + phase, db.name, '/tmp/conversationsynchttp.test']
    listing = db.commands.run(argv + ['-test.list', '^' + name + '$'], label='list-' + phase)
    result = db.commands.run(argv + ['-test.v', '-test.count=1', '-test.timeout=120s',
                                     '-test.run', '^' + name + '$'], timeout=135,
                             label='sync-http-integration-' + phase, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / ('test-' + phase + '.log')).write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    problems = result_errors(name, listing.stdout.decode().splitlines(), output, result.returncode)
    if problems:
        raise Failure('TLS conversation sync test failed or skipped: ' + phase + ': ' + ', '.join(problems))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    directory = ROOT / 'build' / 'sync-http' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'conversationsynchttp.test'
        read_suite = args.suite.startswith('read-')
        servers = ('newim-gateway', 'newim-server')
        if not read_suite:
            servers += ('newim-sync-backfill',)
        build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(binary),
                         './tests/integration/conversationsynchttp'], env, 'sync-http-cross-compile')
        for name in servers:
            build(commands, ['go', 'build', '-o', str(directory / name), './server/cmd/' + name],
                  env, 'sync-http-' + name + '-build')
        with Database(commands, image, platform) as db:
            db.sql(migration_sql(15 if read_suite else 14))
            paths = (binary.name, *servers)
            for name in paths:
                commands.run(['docker', 'cp', str(directory / name), db.name + ':/tmp/' + name],
                             label='copy-owned-' + name)
            commands.run(['docker', 'exec', db.name, 'chmod', '0755', *('/tmp/' + name for name in paths)],
                         label='make-sync-http-executable')
            if args.suite.endswith('recovery'):
                run_test(db, TESTS[args.suite], 'prepare')
                restart_database(db)
                run_test(db, TESTS[args.suite], 'restart')
            else:
                run_test(db, TESTS[args.suite])
        print('TLS conversation sync suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('TLS conversation sync suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
