"""Bounded isolated PostgreSQL test tooling. 只管理本次创建的资源，不接触用户数据库。"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
DB_DIR = ROOT / 'infra' / 'db'
LOCK = json.loads((DB_DIR / 'image-lock.json').read_text())
MAXIMUM = 9223372036854775807


class Failure(RuntimeError):
    pass


class Commands:
    def __init__(self, directory: Path):
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=False)
        self.records: list[dict] = []

    def record(self, argv, code, elapsed, label, input_bytes=b''):
        self.records.append({'argv': argv, 'cwd': str(ROOT), 'exit_code': code,
                             'elapsed_seconds': round(elapsed, 3), 'label': label,
                             'stdin_sha256': hashlib.sha256(input_bytes).hexdigest()})
        (self.directory / 'commands.json').write_text(json.dumps(self.records, indent=2) + '\n')

    def run(self, argv, *, data=b'', timeout=30, label='command', check=True):
        start = time.monotonic()
        try:
            result = subprocess.run(argv, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    cwd=ROOT, timeout=timeout, check=False)
            code = result.returncode
        except subprocess.TimeoutExpired as exc:
            self.record(argv, 124, time.monotonic()-start, label, data)
            raise Failure(f'{label}: deadline exceeded ({timeout}s)') from exc
        self.record(argv, code, time.monotonic()-start, label, data)
        if check and code:
            # Never include SQL/output containing payloads, IDs, tokens or signed URLs.
            if label == 'image-inspect-after-pull' and b'unknown flag: --platform' in result.stderr:
                raise Failure(f'{label}: Docker CLI does not support inspect --platform (exit {code})')
            raise Failure(f'{label}: command exit {code}')
        return result


def native_platform():
    name = platform.machine().lower()
    if name in ('aarch64', 'arm64'):
        return 'linux/arm64/v8'
    if name in ('x86_64', 'amd64'):
        return 'linux/amd64'
    raise Failure('unsupported native database test architecture')


def locked_metadata(target):
    """Verify index → manifest → config and layers. 校验保存的官方原始元数据链。"""
    locked = LOCK['platforms'][target]
    def read(digest):
        raw = (DB_DIR / 'image-metadata' / (digest.removeprefix('sha256:')+'.json')).read_bytes()
        if 'sha256:'+hashlib.sha256(raw).hexdigest() != digest:
            raise Failure('image metadata checksum mismatch')
        return json.loads(raw)
    index, manifest, config = read(LOCK['index']), read(locked['manifest']), read(locked['config'])
    architecture = 'arm64' if target == 'linux/arm64/v8' else 'amd64'
    matches = [m for m in index['manifests'] if m['digest'] == locked['manifest']]
    if (len(matches) != 1 or matches[0]['platform']['os'] != 'linux'
            or matches[0]['platform']['architecture'] != architecture
            or manifest['config']['digest'] != locked['config']
            or manifest['layers'] != locked['layers']
            or config['rootfs']['diff_ids'] != locked['diff_ids']
            or config['architecture'] != architecture or config['os'] != 'linux'):
        raise Failure('image metadata identity chain mismatch')
    return locked


def verify_inspected_image(actual, target, locked):
    expected_arch = 'arm64' if target == 'linux/arm64/v8' else 'amd64'
    if (actual.get('Os') != 'linux' or actual.get('Architecture') != expected_arch
            or (expected_arch == 'arm64' and actual.get('Variant') not in ('v8', None, ''))
            or not isinstance(actual.get('RootFS'), dict)
            or actual['RootFS'].get('Layers') != locked['diff_ids']):
        raise Failure('local image descriptor/config/platform/rootfs does not match reviewed image lock')
    if 'Descriptor' in actual:
        descriptor = actual['Descriptor']
        if (not isinstance(descriptor, dict) or descriptor.get('digest') != locked['manifest']
                or actual.get('Id') not in (locked['config'], locked['manifest'])):
            raise Failure('present image descriptor/config does not match reviewed image lock')
        return 'oci-descriptor'
    # Docker 28 classic: actual config identity binds execution settings and DiffIDs.
    # 缺失 Descriptor 时仅允许官方仓库 digest 与精确 config；不凭相同标签降级。
    aliases = ('postgres', 'library/postgres', 'docker.io/library/postgres')
    expected = {repo+'@'+digest for repo in aliases for digest in (LOCK['index'], locked['manifest'])}
    digests = actual.get('RepoDigests')
    if (actual.get('Id') != locked['config'] or not isinstance(digests, list)
            or not any(isinstance(d, str) and d in expected for d in digests)):
        raise Failure('classic image config/official repository digest does not match reviewed image lock')
    return 'classic-config'


def image_identity(commands: Commands, *, prepare=False):
    target = native_platform()
    locked = locked_metadata(target)
    explicit = 'NEWIM_DB_IMAGE' in os.environ
    selector = os.environ.get('NEWIM_DB_IMAGE', locked['manifest'])
    if selector.startswith('-') or len(selector) > 512:
        raise Failure('invalid local image selector')
    result = commands.run(['docker', 'image', 'inspect', selector, '--format', '{{json .}}'],
                          label='image-inspect', check=False)
    if result.returncode and explicit:
        raise Failure('explicit local image selector unavailable; no substitution allowed')
    if result.returncode:
        # Classic images are addressable by config ID, not the OCI manifest ID.
        result = commands.run(['docker', 'image', 'inspect', locked['config'], '--format', '{{json .}}'],
                              label='classic-config-inspect', check=False)
    if result.returncode and prepare:
        commands.run(['docker', 'pull', '--platform', target,
                      LOCK['repository']+'@'+LOCK['index']], timeout=180, label='locked-image-pull')
        selector = LOCK['repository']+'@'+LOCK['index']
        result = commands.run(['docker', 'image', 'inspect', selector,
                               '--format', '{{json .}}'], label='image-inspect-after-pull')
    elif result.returncode:
        raise Failure('locked image absent; run make db-prepare first')
    actual = json.loads(result.stdout)
    # A prepared multiarch index may resolve to an index descriptor. Resolve its
    # locked platform by digest; never accept a tag just because its name matches.
    if isinstance(actual.get('Descriptor'), dict) and actual['Descriptor'].get('digest') == LOCK['index']:
        result = commands.run(['docker', 'image', 'inspect', locked['manifest'],
                               '--format', '{{json .}}'], label='platform-image-inspect')
        actual = json.loads(result.stdout)
    verification = verify_inspected_image(actual, target, locked)
    identity = {'platform': target, 'official_index': LOCK['index'],
                'official_manifest': locked['manifest'], 'official_config': locked['config'],
                'actual_image_id': actual['Id'], 'verification': verification,
                'actual_repo_digests': actual.get('RepoDigests', []),
                'actual_size': actual.get('Size'), 'diff_ids': locked['diff_ids']}
    (commands.directory / 'image.json').write_text(json.dumps(identity, indent=2)+'\n')
    return actual['Id'], target


class Database:
    def __init__(self, commands: Commands, image, target):
        self.commands = commands
        self.image, self.target = image, target
        self.run_id = uuid.uuid4().hex
        self.name = 'newim-db-'+self.run_id
        self.volume = self.name+'-data'
        self.container_id = None
        self.created_volume = False
        self.sessions = []

    def __enter__(self):
        try:
            # Track the exact UUID name even if Docker times out after creating it.
            self.created_volume = True
            self.commands.run(['docker', 'volume', 'create', '--label', 'newim.db-run='+self.run_id,
                               self.volume], label='create-owned-volume')
            result = self.commands.run([
                'docker', 'run', '-d', '--name', self.name, '--label', 'newim.db-run='+self.run_id,
                '--pull', 'never', '--platform', self.target, '--network', 'none',
                '--memory', '512m', '--cpus', '2', '--pids-limit', '128', '--stop-timeout', '10',
                '--mount', 'type=volume,source='+self.volume+',target=/var/lib/postgresql',
                '-e', 'POSTGRES_HOST_AUTH_METHOD=trust', '-e', 'POSTGRES_USER=newim_test',
                '-e', 'POSTGRES_DB=newim_test', self.image,
                '-c', 'listen_addresses=', '-c', 'fsync=on', '-c', 'full_page_writes=on',
                '-c', 'synchronous_commit=on', '-c', 'log_statement=none',
                '-c', 'log_min_messages=panic', '-c', 'log_min_error_statement=panic',
                '-c', 'log_parameter_max_length=0', '-c', 'log_parameter_max_length_on_error=0',
                '-c', 'log_error_verbosity=terse', '-c', 'max_connections=40'],
                timeout=45, label='start-isolated-database')
            self.container_id = result.stdout.decode().strip()
            if not re.fullmatch(r'[0-9a-f]{64}', self.container_id):
                raise Failure('Docker did not return a container ID')
            self.ready()
            version = self.sql("SELECT current_setting('server_version_num');").strip()
            if version != str(LOCK['server_version_num']):
                raise Failure('actual SQL PostgreSQL version differs from image lock')
            if self.sql("SELECT current_setting('fsync'), current_setting('full_page_writes'), current_setting('synchronous_commit');").strip() != 'on|on|on':
                raise Failure('durability settings disabled')
            return self
        except BaseException:
            self.close()
            raise

    def ready(self):
        deadline = time.monotonic()+60
        while time.monotonic() < deadline:
            # The entrypoint's temporary init server also accepts connections.
            # Wait until PID 1 is the final postgres, not the initialization shell.
            process = self.commands.run(['docker', 'exec', self.name, 'cat', '/proc/1/comm'],
                                        timeout=5, label='final-server-process', check=False)
            if process.returncode or process.stdout.strip() != b'postgres':
                time.sleep(0.2)
                continue
            result = self.commands.run(['docker', 'exec', self.name, 'pg_isready', '-q', '-U',
                                       'newim_test', '-d', 'newim_test'], timeout=5,
                                       label='database-readiness', check=False)
            if result.returncode == 0:
                return
            time.sleep(0.2)
        raise Failure('database readiness deadline exceeded')

    def argv(self, database='newim_test'):
        return ['docker', 'exec', '-i', self.name, 'psql', '-X', '-q', '-A', '-t',
                '-v', 'ON_ERROR_STOP=1', '-v', 'VERBOSITY=sqlstate',
                '-v', 'SHOW_CONTEXT=never', '-U', 'newim_test', '-d', database]

    @staticmethod
    def prefix():
        return "SET statement_timeout = '12s'; SET lock_timeout = '8s';\n"

    def sql(self, statement, *, database='newim_test', error=None, timeout=20):
        data = (self.prefix()+statement+'\n').encode()
        r = self.commands.run(self.argv(database), data=data, timeout=timeout,
                              label='sql-expected-'+error if error else 'sql', check=False)
        if error:
            if r.returncode == 0 or not re.search(rb'\b'+error.encode()+rb'\b', r.stderr):
                raise Failure('expected SQLSTATE '+error+' not observed')
        elif r.returncode:
            state = re.search(rb'ERROR:\s+([0-9A-Z]{5})\b', r.stderr)
            raise Failure('SQL failed: '+(state[1].decode() if state else 'unclassified'))
        return r.stdout.decode()

    def session(self, sql):
        session = Session(self, sql)
        self.sessions.append(session)
        return session

    def wait_sql(self, statement, expected='t', seconds=6):
        deadline = time.monotonic()+seconds
        while time.monotonic() < deadline:
            if self.sql(statement).strip() == expected:
                return
            time.sleep(0.05)
        raise Failure('database barrier deadline exceeded')

    def close(self):
        for session in self.sessions:
            session.cancel()
        # Names are UUID-owned; verify labels before removing even these objects.
        if self.container_id:
            result = self.commands.run(['docker', 'container', 'inspect', self.container_id,
                                       '--format', '{{index .Config.Labels "newim.db-run"}}'],
                                       label='verify-owned-container', check=False)
            if result.returncode == 0 and result.stdout.decode().strip() == self.run_id:
                self.commands.run(['docker', 'rm', '-f', self.container_id], timeout=30,
                                  label='remove-owned-container')
            elif result.returncode == 0:
                raise Failure('container ownership mismatch; preserved')
        elif self.created_volume:
            # docker run can create the named container before a client timeout.
            result = self.commands.run(['docker', 'container', 'inspect', self.name,
                                       '--format', '{{index .Config.Labels "newim.db-run"}}'],
                                       label='inspect-incomplete-owned-start', check=False)
            if result.returncode == 0 and result.stdout.decode().strip() == self.run_id:
                self.commands.run(['docker', 'rm', '-f', self.name], timeout=30,
                                  label='remove-incomplete-owned-start')
        if self.created_volume:
            result = self.commands.run(['docker', 'volume', 'inspect', self.volume,
                                       '--format', '{{index .Labels "newim.db-run"}}'],
                                       label='verify-owned-volume', check=False)
            if result.returncode == 0 and result.stdout.decode().strip() == self.run_id:
                self.commands.run(['docker', 'volume', 'rm', self.volume], timeout=30,
                                  label='remove-owned-volume')
            elif result.returncode == 0:
                raise Failure('volume ownership mismatch; preserved')
        self.container_id = None
        self.created_volume = False

    def __exit__(self, *_):
        self.close()


class Session:
    def __init__(self, db, sql):
        self.db = db
        self.sql_bytes = (db.prefix()+sql+'\n').encode()
        self.started = time.monotonic()
        self.process = subprocess.Popen(db.argv(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, cwd=ROOT)
        self.process.stdin.write(self.sql_bytes)
        self.process.stdin.flush()
        self.finished = False

    def send(self, sql):
        data = (sql+'\n').encode()
        self.sql_bytes += data
        self.process.stdin.write(data)
        self.process.stdin.flush()

    def finish(self, error=None, timeout=20):
        self.process.stdin.close()
        self.process.stdin = None
        try:
            out, err = self.process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            self.cancel()
            raise Failure('independent SQL session timed out') from exc
        self.finished = True
        code = self.process.returncode
        self.db.commands.record(self.db.argv(), code, time.monotonic()-self.started,
                                'independent-sql-session', self.sql_bytes)
        if error:
            if code == 0 or not re.search(rb'\b'+error.encode()+rb'\b', err):
                raise Failure('session expected SQLSTATE '+error+' not observed')
        elif code:
            raise Failure('independent SQL session failed')
        return out.decode()

    def cancel(self):
        if not self.finished:
            if self.process.poll() is None:
                self.process.kill()
            self.process.communicate()
            self.db.commands.record(self.db.argv(), self.process.returncode,
                                    time.monotonic()-self.started,
                                    'cancelled-independent-sql-session', self.sql_bytes)
            self.finished = True
