"""Run real TLS conversation sync gates in an owned, digest-verified PostgreSQL container."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform as host_platform
import sys
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
    'reliability-transport': 'TestTransportReliability',
    'reliability-push-timeout': 'TestPushReceiverTimeout',
    'performance-baseline': 'TestPerformanceBaseline',
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


def run_native_bridge(db):
    result = db.commands.run([
        sys.executable, '-B', 'sdk/native/tests/tls_host.py', db.name,
    ], label='native-c-abi-tls-bridge', timeout=210, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / 'test-sdk.log').write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    if result.returncode or b'native C ABI, SQLite and TLS recovery passed' not in result.stdout:
        raise Failure('native C SDK bridge did not complete its TLS fixture')


def performance_source(commands):
    modified = commands.run(['git', 'status', '--porcelain', '--untracked-files=no'],
                            label='baseline-tracked-state').stdout
    untracked = commands.run(['git', 'ls-files', '--others', '--exclude-standard', '--',
                              'core', 'server', 'tests', 'infra'], label='baseline-source-state').stdout
    if modified or untracked:
        raise Failure('performance baseline requires an unchanged committed source tree')
    revision, tree = commands.run(['git', 'rev-parse', 'HEAD', 'HEAD^{tree}'],
                                  label='baseline-source-revision').stdout.decode().splitlines()
    return {'revision': revision, 'tree': tree}


def performance_host(commands):
    system = host_platform.system()
    if system == 'Darwin':
        cpu, memory = commands.run(['/usr/sbin/sysctl', '-n', 'machdep.cpu.brand_string', 'hw.memsize'],
                                   label='baseline-host-hardware').stdout.decode().splitlines()
    elif system == 'Linux':
        cpu_info = Path('/proc/cpuinfo').read_text(encoding='utf-8')
        fields = dict(line.split(':', 1) for line in cpu_info.splitlines() if ':' in line)
        fields = {key.strip(): value.strip() for key, value in fields.items()}
        cpu = fields.get('model name') or fields.get('Hardware') or host_platform.machine()
        memory = os.sysconf('SC_PHYS_PAGES') * os.sysconf('SC_PAGE_SIZE')
    else:
        raise Failure('no hardware metadata recorder for this baseline host platform')
    return {'os': system, 'arch': host_platform.machine(), 'kernel': host_platform.release(),
            'cpu_model': cpu.strip(), 'memory_bytes': int(memory), 'logical_cpus': os.cpu_count()}


def save_performance_report(db, source, binaries):
    commands, directory = db.commands, db.commands.directory
    if performance_source(commands) != source:
        raise Failure('source changed during the performance run')
    network, cpu, memory, pids = commands.run([
        'docker', 'inspect', '--type', 'container', '--format',
        '{{.HostConfig.NetworkMode}} {{.HostConfig.NanoCpus}} {{.HostConfig.Memory}} {{.HostConfig.PidsLimit}}',
        db.container_id,
    ], label='baseline-executed-container-config').stdout.decode().split()
    versions = commands.run(['go', 'version', '-m', str(directory / 'newim-gateway')],
                            label='baseline-binary-dependencies').stdout.decode()
    dependencies = {
        fields[1]: fields[2] for line in versions.splitlines() if len(fields := line.split()) >= 3
        and fields[0] == 'dep' and fields[1] in ('github.com/coder/websocket', 'github.com/jackc/pgx/v5')
    }
    docker_version = commands.run(['docker', 'version', '--format', '{{.Server.Version}}'],
                                  label='baseline-docker-version').stdout.decode().strip()
    metadata = {
        **source, 'host': performance_host(commands), 'docker_version': docker_version,
        'container': {'network': network, 'nano_cpus': int(cpu), 'memory_bytes': int(memory), 'pids': int(pids)},
        'image': json.loads((directory / 'image.json').read_text()),
        'binary_sha256': {name: hashlib.sha256((directory / name).read_bytes()).hexdigest() for name in binaries},
        'dependencies': dependencies,
    }
    context = directory / 'performance-context.json'
    context.write_text(json.dumps(metadata, indent=2) + '\n')
    result = commands.run([
        sys.executable, '-B', 'tests/performance/report.py', '--context', str(context),
        '--log', str(directory / 'test-single.log'), '--output', str(directory / 'performance-report.json'),
    ], label='baseline-report', check=False)
    print((result.stdout + result.stderr).decode(errors='replace'), end='', flush=True)
    if result.returncode:
        raise Failure('performance report is incomplete or inconsistent')
    digest = hashlib.sha256((directory / 'performance-report.json').read_bytes()).hexdigest()
    print(f'performance report sha256: {digest}', flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=(*TESTS, *SDK_TESTS, 'native-sdk-bridge-integration'))
    args = parser.parse_args()
    directory = ROOT / 'build' / 'sync-http' / (args.suite + '-' + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        native = args.suite == 'native-sdk-bridge-integration'
        measuring = args.suite == 'performance-baseline'
        source = performance_source(commands) if measuring else None
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
        if not measuring and (not read_suite or read_sync):
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
            head_suite = native or measuring or args.suite.startswith(('push-', 'reliability-')) or args.suite in (
                'message-trace-check', 'sdk-observability-check'
            )
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
            if native:
                run_native_bridge(db)
            elif sdk_bridge:
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
                if measuring:
                    save_performance_report(db, source, paths)
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
