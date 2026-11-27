"""Run media TLS gates against a pinned S3 service and owned PostgreSQL."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import tarfile
import tempfile
import uuid

from auth_suite import restart_database, result_errors
from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, DB_DIR, Failure, ROOT, image_identity

LOCK = json.loads((DB_DIR / 'media_s3_fixture.json').read_text())
TESTS = {
    'check': 'TestMediaHTTPCheck',
    'security': 'TestMediaS3HTTPSecurity',
    'recovery': 'TestMediaHTTPRecovery',
}

BUCKET = 'newim-media-fixture'
HOST = 'objects.newim.test'
MISMATCH_HOST = 'unlisted.newim.test'
PORT = '8443'
REGION = 'us-east-1'
CLIENT_DIR = '/tmp/newim-s3-client'
PROVIDER_DIR = '/tmp/newim-s3-provider'


def service_image(commands, platform):
    locked = LOCK['platforms'][platform]
    repository = LOCK['repository']
    inspect = ['docker', 'image', 'inspect']

    def installed():
        for selector in (repository + '@' + locked['manifest'], locked['config'], repository + '@' + LOCK['index']):
            result = commands.run(inspect + [selector, '--format', '{{json .}}'],
                                  label='s3-image-inspect', check=False)
            if result.returncode == 0:
                return json.loads(result.stdout)
        return None

    image = installed()
    if image is None:
        commands.run(['docker', 'pull', '--platform', platform, repository + '@' + LOCK['index']],
                     timeout=180, label='pull-s3-image-index')
        image = installed()
    if image is None:
        raise Failure('the pinned S3 fixture image is unavailable')
    aliases = {repository, repository.removeprefix('docker.io/')}
    digests = {ref.split('@', 1)[1] for ref in (image.get('RepoDigests') or [])
               if '@' in ref and ref.split('@', 1)[0] in aliases}
    descriptor = image.get('Descriptor') or {}
    expected_arch = platform.split('/')[1]
    binding = (LOCK['index'], locked['manifest'])
    # Docker reports either the platform config ID or an OCI index/manifest ID.
    identity = image.get('Id')
    if (image.get('Os') != 'linux' or image.get('Architecture') != expected_arch
            or identity not in (locked['config'], locked['manifest'], LOCK['index'])
            or image.get('RootFS', {}).get('Layers') != locked['diff_ids']
            or (image.get('Config', {}).get('Labels') or {}).get('org.opencontainers.image.revision') != LOCK['revision']
            or (descriptor and descriptor.get('digest') not in (locked['manifest'], LOCK['index']))
            or not any(ref in digests for ref in binding)):
        raise Failure('local S3 fixture image differs from the reviewed platform lock')
    record = {'repository': repository, 'index': LOCK['index'], 'manifest': locked['manifest'],
              'config': locked['config'], 'actual_image_id': identity, 'platform': platform,
              'version': LOCK['version'], 'revision': LOCK['revision']}
    (commands.directory / 's3-image.json').write_text(json.dumps(record, indent=2) + '\n')
    return repository + '@' + next(digest for digest in binding if digest in digests)


def write_config(directory, commands):
    access = secrets.token_hex(10)
    secret = secrets.token_hex(20)
    identity = {'name': 'fixture', 'credentials': [{'accessKey': access, 'secretKey': secret}],
                'actions': [action + ':' + BUCKET for action in ('Read', 'Write', 'List')]}
    (directory / 'iam.json').write_text(json.dumps({'identities': [identity]}))
    (directory / 'fixture.json').write_text(json.dumps({
        'bucket': BUCKET, 'region': REGION, 'endpoint': 'https://' + HOST + ':' + PORT,
        'mismatchEndpoint': 'https://' + MISMATCH_HOST + ':' + PORT, 'caFile': CLIENT_DIR + '/cert.pem',
        'accessKey': access, 'secretKey': secret,
    }))
    (directory / 'tls.cnf').write_text(f'''[req]
prompt = no
distinguished_name = subject
x509_extensions = extensions

[subject]
CN = {HOST}

[extensions]
subjectAltName = DNS:{HOST}
basicConstraints = critical,CA:TRUE
keyUsage = critical,digitalSignature,keyEncipherment,keyCertSign
extendedKeyUsage = serverAuth
''')
    for name in ('iam.json', 'fixture.json', 'tls.cnf'):
        (directory / name).chmod(0o600)
    commands.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-sha256', '-nodes', '-days', '2',
                  '-config', str(directory / 'tls.cnf'), '-out', str(directory / 'cert.pem'),
                  '-keyout', str(directory / 'key.pem')], label='generate-s3-fixture-certificate')


def install_config(commands, container, directory, target, names, owner):
    archive_path = directory / 'settings.tar'

    def owned(member):
        member.uid = member.gid = owner
        member.uname = member.gname = ''
        member.mode = 0o700 if member.isdir() else 0o600
        return member

    with tarfile.open(archive_path, 'w') as archive:
        archive.add(directory, arcname=Path(target).name, recursive=False, filter=owned)
        for name in names:
            archive.add(directory / name, arcname=Path(target).name + '/' + name, filter=owned)
    try:
        commands.run(['docker', 'cp', '-a', '-', container + ':/tmp'],
                     data=archive_path.read_bytes(), label='install-owned-s3-settings')
    finally:
        archive_path.unlink()


class ObjectFixture:
    label = 'newim.media-s3-run'

    def __init__(self, commands, image, platform, settings):
        self.commands, self.image, self.platform, self.settings = commands, image, platform, settings
        self.run_id = uuid.uuid4().hex
        self.name = 'newim-media-s3-' + self.run_id
        self.network = self.name + '-net'
        self.volume = self.name + '-data'

    def __enter__(self):
        c = self.commands
        label = self.label + '=' + self.run_id
        try:
            c.run(['docker', 'network', 'create', '--internal', '--label', label, self.network],
                  label='create-owned-internal-s3-network')
            network = c.run(['docker', 'network', 'inspect', self.network, '--format', '{{.Internal}}'],
                            label='verify-s3-network-isolation')
            if network.stdout.strip() != b'true':
                raise Failure('the S3 fixture network is not internal')
            c.run(['docker', 'volume', 'create', '--label', label, self.volume], label='create-owned-s3-volume')
            c.run(['docker', 'create', '--name', self.name, '--label', label, '--pull', 'never',
                   '--platform', self.platform, '--network', self.network,
                   '--network-alias', HOST, '--network-alias', MISMATCH_HOST,
                   '--memory', '768m', '--cpus', '2', '--pids-limit', '128', '--stop-timeout', '5',
                   '--mount', 'type=volume,source=' + self.volume + ',target=/data',
                   self.image, 'mini', '-ip=127.0.0.1', '-ip.bind=0.0.0.0', '-dir=/data',
                   '-master.telemetry=false', '-s3.autoCreateBucket=false', '-bucket=' + BUCKET,
                   '-s3.config=' + PROVIDER_DIR + '/iam.json', '-s3.port.https=' + PORT,
                   '-s3.cert.file=' + PROVIDER_DIR + '/cert.pem', '-s3.key.file=' + PROVIDER_DIR + '/key.pem'],
                  timeout=45, label='create-owned-s3-container')
            install_config(c, self.name, self.settings, PROVIDER_DIR, ('iam.json', 'cert.pem', 'key.pem'), 1000)
            c.run(['docker', 'start', self.name], timeout=30, label='start-owned-s3-container')
            version = c.run(['docker', 'exec', self.name, 'weed', 'version'], label='s3-runtime-version')
            if (' ' + LOCK['version'] + ' ').encode() not in version.stdout or LOCK['revision'][:9].encode() not in version.stdout:
                raise Failure('the S3 fixture runtime version differs from the image lock')
            (c.directory / 's3-version.txt').write_bytes(version.stdout)
            return self
        except BaseException:
            self.close()
            raise

    def restart(self):
        self.commands.run(['docker', 'kill', '--signal=KILL', self.name], label='crash-owned-s3-container')
        self.commands.run(['docker', 'start', self.name], timeout=30, label='restart-owned-s3-container')

    def close(self):
        c = self.commands
        for kind, name, labels, rm_args in (
                ('container', self.name, '.Config.Labels', ['-f']),
                ('volume', self.volume, '.Labels', []),
                ('network', self.network, '.Labels', [])):
            fmt = '{{index ' + labels + ' "' + self.label + '"}}'
            entry = c.run(['docker', kind, 'inspect', name, '--format', fmt],
                          label='verify-s3-' + kind + '-owner', check=False)
            if entry.returncode:
                continue
            if entry.stdout.decode().strip() != self.run_id:
                raise Failure('unexpected fixture ' + kind + ' ownership; preserved')
            c.run(['docker', kind, 'rm', *rm_args, name], label='remove-owned-s3-' + kind)

    def __exit__(self, *_):
        self.close()


def run_test(db, name, phase, label):
    argv = ['docker', 'exec', '-e', 'NEWIM_GATEWAY_BINARY=/tmp/newim-gateway',
            '-e', 'NEWIM_TEST_S3_SETTINGS=' + CLIENT_DIR + '/fixture.json',
            '-e', 'NEWIM_MEDIA_HTTP_PHASE=' + phase, db.name, '/tmp/mediahttp.test']
    pattern = '^' + name + '$'
    listing = db.commands.run(argv + ['-test.list', pattern], label='list-' + label)
    result = db.commands.run(argv + ['-test.v', '-test.count=1', '-test.timeout=180s',
                                     '-test.run', pattern], timeout=195, label=label, check=False)
    output = result.stdout + result.stderr
    (db.commands.directory / (label + '.log')).write_bytes(output)
    print(output.decode(errors='replace'), end='', flush=True)
    errors = result_errors(name, listing.stdout.decode().splitlines(), output, result.returncode)
    if errors:
        raise Failure(label + ' failed or skipped: ' + ', '.join(errors))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=TESTS)
    args = parser.parse_args()
    commands = Commands(ROOT / 'build' / 'media-s3' / (args.suite + '-' + uuid.uuid4().hex))
    c = commands
    try:
        db_image, platform = image_identity(c)
        object_image = service_image(c, platform)
        env = os.environ.copy()
        env.update(CGO_ENABLED='0', GOOS='linux', GOARCH=platform.split('/')[1], GOTOOLCHAIN='local')
        gateway = c.directory / 'newim-gateway'
        binary = c.directory / 'mediahttp.test'
        build(c, ['go', 'test', '-c', '-tags=integration', '-o', str(binary), './tests/integration/mediahttp'],
              env, 'media-s3-cross-compile')
        build(c, ['go', 'build', '-o', str(gateway), './server/cmd/newim-gateway'],
              env, 'media-s3-gateway-build')
        with tempfile.TemporaryDirectory(prefix='newim-media-s3-fixture-') as private:
            settings = Path(private)
            write_config(settings, c)
            with ObjectFixture(c, object_image, platform, settings) as objects, \
                    Database(c, db_image, platform, network=objects.network) as db:
                db.sql(migration_sql(14))
                install_config(c, db.name, settings, CLIENT_DIR, ('fixture.json', 'cert.pem'), 0)
                for path in (binary, gateway):
                    c.run(['docker', 'cp', str(path), db.name + ':/tmp/' + path.name],
                          label='copy-owned-' + path.name)
                c.run(['docker', 'exec', db.name, 'chmod', '0755', '/tmp/mediahttp.test', '/tmp/newim-gateway'],
                      label='make-media-s3-binaries-executable')
                run_test(db, 'TestMediaS3ProviderReady', 'single', 'provider-ready')
                if args.suite == 'recovery':
                    run_test(db, TESTS[args.suite], 'prepare', 'media-s3-prepare')
                    objects.restart()
                    restart_database(db)
                    run_test(db, 'TestMediaS3ProviderReady', 'single', 'provider-restarted')
                    run_test(db, TESTS[args.suite], 'restart', 'media-s3-restart')
                else:
                    run_test(db, TESTS[args.suite], 'single', 'media-s3-' + args.suite)
        print('TLS S3 media suite passed; command records: ' + str(c.directory))
    except (Failure, OSError) as exc:
        print('TLS S3 media suite failed: ' + str(exc) + '; command records: ' + str(c.directory))
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
