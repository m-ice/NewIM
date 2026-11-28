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
    'read-sync-check': 'TestReadSyncHTTPCheck',
    'read-sync-security': 'TestReadSyncHTTPSecurity',
    'read-sync-recovery': 'TestReadSyncHTTPRecovery',
    'read-sync-query-plan': 'TestReadSyncHTTPWide',
    'conversation-account-sync-check': 'TestAccountSettingsHTTPCheck',
    'conversation-account-sync-security': 'TestAccountSettingsHTTPSecurity',
    'conversation-account-sync-recovery': 'TestAccountSettingsHTTPRecovery',
    'conversation-account-sync-query-plan': 'TestAccountSettingsHTTPWide',
    'message-event-check': 'TestMessageVisibilityProjection',
    'message-event-recovery': 'TestMessageVisibilityUpgrade',
    'push-delivery-check': 'TestPushDelivery',
    'message-trace-check': 'TestMessageTrace',
    'push-token-check': 'TestPushTokenRotation',
    'push-recovery': 'TestPushRecovery',
}
SDK_TESTS = {
    'conversation-account-sdk-check': 'tls_sqlite_conversation_account_sync',
    'message-sdk-sync-check': 'message_sync::tls_sqlite_message_sync',
    'message-sdk-sync-convergence': 'message_sync::tls_sqlite_message_convergence',
    'message-sdk-sync-recovery': 'message_sync::tls_sqlite_message_recovery',
    'sdk-connection-integration': 'connection::tls_sdk_connection',
    'message-event-integration': 'connection::tls_sdk_message_visibility',
    'push-sdk-check': 'connection::tls_sdk_push_click',
    'sdk-observability-check': 'connection::tls_sdk_observability',
}


def run_test(db, name, phase='single', binary='conversationsynchttp.test'):
    argv = ['docker', 'exec', '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_SERVER_BINARY=/tmp/newim-server',
            '-e', 'NEWIM_SYNC_BACKFILL_BINARY=/tmp/newim-sync-backfill',
            '-e', 'NEWIM_SYNC_HTTP_PHASE=' + phase, db.name, '/tmp/' + binary]
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


def run_sdk_bridge(db, name, label):
    result = db.commands.run([
        'env', 'NEWIM_ACCOUNT_SDK_BRIDGE_CONTAINER=' + db.name,
        'python3', 'sdk/storage/sqlite/engine.py', 'run',
        'cargo', 'test', '-p', 'newim-store-sqlite', '--locked', '--offline',
        '--test', 'account_sdk_bridge', '--', '--ignored', '--exact', name,
    ], label=label, timeout=360, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / 'test-sdk.log').write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    if (result.returncode or b'running 1 test' not in output
            or ('test ' + name + ' ... ok').encode() not in output
            or b'test result: ok. 1 passed; 0 failed; 0 ignored' not in output):
        raise Failure('native SQLite sync bridge did not pass its exact test')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=(*TESTS, *SDK_TESTS))
    args = parser.parse_args()
    directory = ROOT / 'build' / 'sync-http' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        binary = directory / 'conversationsynchttp.test'
        read_sync = args.suite.startswith('read-sync-')
        read_suite = args.suite.startswith('read-')
        account_sync = args.suite.startswith('conversation-account-')
        sdk_bridge = SDK_TESTS.get(args.suite)
        query_plan = args.suite.endswith('query-plan') or args.suite == 'message-event-check'
        servers = ('newim-gateway', 'newim-server')
        if not read_suite or read_sync:
            servers += ('newim-sync-backfill',)
        build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(binary),
                         './tests/integration/conversationsynchttp'], env, 'sync-http-cross-compile')
        paths = (binary.name, *servers)
        if query_plan:
            plan = directory / 'readsyncplan.test'
            build(commands, ['go', 'test', '-c', '-tags=integration', '-o', str(plan),
                             './server/storage/conversationsync'], env, 'sync-query-plan-cross-compile')
            paths += (plan.name,)
        for name in servers:
            build(commands, ['go', 'build', '-o', str(directory / name), './server/cmd/' + name],
                  env, 'sync-http-' + name + '-build')
        with Database(commands, image, platform) as db:
            head_suite = args.suite.startswith('push-') or args.suite in ('message-trace-check', 'sdk-observability-check')
            target = 22 if head_suite else 21 if args.suite.startswith('message-event-') else (
                18 if account_sync or sdk_bridge else 17 if read_sync else 15 if read_suite else 14
            )
            upgrades_visibility = args.suite == 'message-event-recovery'
            db.sql(migration_sql(20 if upgrades_visibility else target))
            for name in paths:
                commands.run(['docker', 'cp', str(directory / name), db.name + ':/tmp/' + name],
                             label='copy-owned-' + name)
            commands.run(['docker', 'exec', db.name, 'chmod', '0755', *('/tmp/' + name for name in paths)],
                         label='make-sync-http-executable')
            if sdk_bridge:
                label = 'conversation-account-sdk-bridge' if account_sync else args.suite + '-bridge'
                run_sdk_bridge(db, sdk_bridge, label)
            elif args.suite.endswith('recovery'):
                run_test(db, TESTS[args.suite], 'prepare')
                restart_database(db)
                if upgrades_visibility:
                    db.sql(migration_sql(21))
                run_test(db, TESTS[args.suite], 'restart')
            else:
                run_test(db, TESTS[args.suite])
                if query_plan:
                    if args.suite == 'message-event-check':
                        test = 'TestMessageVisibilityQueryPlan'
                    else:
                        test = 'TestAccountSettingsSourceQueryPlan' if account_sync else 'TestReadSyncQueryPlan'
                    run_test(db, test, 'plan', plan.name)
        print('TLS conversation sync suite passed; command records: ' + str(directory))
    except (Failure, OSError) as exc:
        print('TLS conversation sync suite failed: ' + str(exc) + '; command records: ' + str(directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
