"""故障输入与验收门禁回归测试；不启动业务或 Agent。"""
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from control import eligible, validate, within
from check_scope import scope_errors


class Contracts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        for directory in ('.codex/agents', 'tasks', 'workstreams', 'docs', 'reference'):
            shutil.copytree(ROOT / directory, self.root / directory)
        shutil.copy(ROOT / 'AGENTS.md', self.root / 'AGENTS.md')
        self.manifest = json.loads((self.root / 'tasks/manifest.json').read_text())
        self.tasks = {t['id']: t for t in self.manifest['tasks']}
        self.task = self.tasks['NIM-CTL-001']

    def check(self):
        (self.root / 'tasks/manifest.json').write_text(json.dumps(self.manifest))
        return validate(self.root)[1]

    def rejects(self, text):
        self.assertTrue(any(text in e for e in self.check()), self.check())

    def test_repository_valid(self):
        self.assertEqual(self.check(), [])

    def test_repository_location_required(self):
        del self.manifest['repository']
        self.rejects('repository must define')

    def test_repository_location_absolute(self):
        self.manifest['repository']['local_path'] = '../NewIM'
        self.rejects('local_path must be an absolute path')

    def test_duplicate_id(self):
        self.manifest['tasks'].append(copy.deepcopy(self.task))
        self.rejects('duplicate task id')

    def test_missing_id(self):
        del self.task['id']
        self.rejects('invalid/missing id')

    def test_bad_root(self):
        self.manifest = []
        self.rejects('manifest must be an object')

    def test_empty_tasks(self):
        self.manifest['tasks'] = []
        self.rejects('nonempty array')

    def test_bad_status(self):
        self.task['status'] = 'finished'
        self.rejects('invalid status')

    def test_wrong_types_are_diagnostics(self):
        original = copy.deepcopy(self.task)
        for key in ('title', 'phase', 'owner', 'status', 'depends_on', 'reviewers', 'acceptance', 'flags', 'write_scopes', 'evidence_path'):
            for value in (None, 4, {}, [None]):
                with self.subTest(key=key, value=value):
                    self.task.clear()
                    self.task.update(copy.deepcopy(original))
                    self.task[key] = value
                    self.assertTrue(self.check())

    def test_unknown_dependency(self):
        self.task['depends_on'] = ['NIM-UNKNOWN-999']
        self.rejects('unknown dependency')

    def test_self_dependency(self):
        self.task['depends_on'] = [self.task['id']]
        self.rejects('self dependency')

    def test_cycle(self):
        self.task['depends_on'] = ['NIM-FND-001']
        self.tasks['NIM-FND-001']['depends_on'] = [self.task['id']]
        self.rejects('cycle')

    def test_unknown_reviewer(self):
        self.task['reviewers'] = ['nobody']
        self.rejects('unknown reviewer')

    def test_flag_requires_security_review(self):
        self.task['flags'] = ['security']
        self.rejects('requires security reviewer')

    def test_missing_flags(self):
        del self.task['flags']
        self.rejects('missing')

    def test_empty_acceptance(self):
        self.task['acceptance'] = []
        self.rejects('acceptance must be')

    def test_path_traversal(self):
        self.task['write_scopes'] = ['../outside/']
        self.rejects('invalid write scope')

    def test_check_coverage(self):
        self.task['acceptance_checks'].pop()
        self.rejects('cover each acceptance')

    def test_command_requires_argv(self):
        self.task['acceptance_checks'][0]['argv'] = 'echo pass'
        self.rejects('argv array')

    def test_pending_cannot_start(self):
        self.task['acceptance_checks'][0] = {'criterion': self.task['acceptance'][0], 'kind': 'pending', 'reason': 'not ready'}
        self.rejects('resolve pending')

    def test_manual_only_implementation_rejected(self):
        for check in self.task['acceptance_checks']:
            check.update(kind='manual', reason='review')
        self.rejects('at least one executable check')

    def test_active_dependencies_must_be_done(self):
        self.task['depends_on'] = ['NIM-FND-001']
        self.rejects('unfinished dependencies')

    def test_blocker_required(self):
        self.task['status'] = 'blocked'
        self.rejects('requires dependencies/manual blocker')

    def test_release_coverage(self):
        self.tasks['NIM-REL-001']['depends_on'].remove('NIM-SDK-002')
        self.rejects("release omits tasks ['NIM-SDK-002']")

    def test_foundation_dependency_required(self):
        self.tasks['NIM-SDK-001']['depends_on'].remove('NIM-FND-002')
        self.rejects('implementation must depend')

    def test_workstream_mismatch(self):
        p = self.root / 'workstreams/foundation.json'
        stream = json.loads(p.read_text())
        stream['tasks'].remove('NIM-CTL-001')
        p.write_text(json.dumps(stream))
        self.rejects('each task exactly once')

    def test_done_without_evidence(self):
        self.task['status'] = 'done'
        self.rejects('evidence:')

    def evidence(self):
        self.task['status'] = 'done'
        folder = self.root / 'tasks/evidence/NIM-CTL-001'
        folder.mkdir(parents=True)
        report = folder / 'report.md'
        report.write_text('Test report for isolated validator fixture.\n')
        ref = {'report': str(report.relative_to(self.root)), 'sha256': hashlib.sha256(report.read_bytes()).hexdigest()}
        sha = 'a' * 40
        record = {'task_id': self.task['id'], 'commit': sha,
                  'checks': [{**ref, 'criterion': c['criterion'], 'result': 'pass', 'argv': c['argv'], 'exit_code': 0} for c in self.task['acceptance_checks']],
                  'reviews': [{**ref, 'role': role, 'reviewer': 'test-fixture-reviewer', 'result': 'approved', 'commit': sha} for role in self.task['reviewers']]}
        return record

    def save_evidence(self, record):
        (self.root / self.task['evidence_path']).write_text(json.dumps(record))

    def test_complete_evidence_passes(self):
        self.save_evidence(self.evidence())
        self.assertEqual(self.check(), [])

    def test_wrong_commit_rejected(self):
        record = self.evidence()
        record['reviews'][0]['commit'] = 'b' * 40
        self.save_evidence(record)
        self.rejects('matching commit')

    def test_missing_review_rejected(self):
        record = self.evidence()
        record['reviews'].pop()
        self.save_evidence(record)
        self.rejects('each required reviewer')

    def test_tampered_report_rejected(self):
        record = self.evidence()
        self.save_evidence(record)
        (self.root / record['checks'][0]['report']).write_text('tampered')
        self.rejects('SHA-256 mismatch')

    def test_failed_command_rejected(self):
        record = self.evidence()
        record['checks'][0]['exit_code'] = 1
        self.save_evidence(record)
        self.rejects('exit 0')

    def test_wrong_command_rejected(self):
        record = self.evidence()
        record['checks'][0]['argv'] = ['true']
        self.save_evidence(record)
        self.rejects('match argv')

    def test_report_symlink_escape(self):
        record = self.evidence()
        report = self.root / record['checks'][0]['report']
        report.unlink()
        report.symlink_to(ROOT / 'README.md')
        self.save_evidence(record)
        self.rejects('report escapes repository')

    def test_manual_block_never_ready(self):
        self.task.update(status='blocked', blocker={'kind': 'manual', 'reason': 'decision needed'})
        self.assertFalse(eligible(self.task, self.tasks))

    def test_dependency_block_can_become_ready(self):
        self.task.update(status='blocked', blocker={'kind': 'dependencies', 'reason': 'wait'}, depends_on=['NIM-FND-001'])
        self.assertFalse(eligible(self.task, self.tasks))
        self.tasks['NIM-FND-001']['status'] = 'done'
        self.assertTrue(eligible(self.task, self.tasks))


class Scopes(unittest.TestCase):
    def setUp(self):
        self.base = {'tasks': [{'id': 'NIM-TST-001', 'status': 'ready', 'depends_on': [], 'write_scopes': ['server/auth/']} ]}
        self.head = copy.deepcopy(self.base)

    def test_scope_cli_with_real_commits_and_event(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            shutil.copytree(ROOT / 'scripts', root / 'scripts')
            (root / 'tasks').mkdir()
            (root / 'tasks/manifest.json').write_text(json.dumps(self.base))
            def git(*args):
                return subprocess.check_output(['git', '-C', temp, *args], stderr=subprocess.PIPE).decode().strip()
            git('init')
            git('config', 'user.email', 'fixture@example.invalid')
            git('config', 'user.name', 'Fixture')
            git('add', '.')
            git('commit', '-m', 'base')
            base = git('rev-parse', 'HEAD')
            (root / 'server/auth').mkdir(parents=True)
            (root / 'server/auth/a.py').write_text('value = 1\n')
            git('add', '.')
            git('commit', '-m', 'implementation')
            head = git('rev-parse', 'HEAD')
            event = root / 'event.json'
            payload = {'pull_request': {'base': {'sha': base}, 'head': {'sha': head}, 'body': 'Task: NIM-TST-001'}}
            event.write_text(json.dumps(payload))
            command = [sys.executable, '-B', str(root / 'scripts/check_scope.py'), '--event', str(event)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            payload['pull_request']['body'] = 'No task declaration'
            event.write_text(json.dumps(payload))
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('declare at least one', result.stderr)

    def test_path_boundaries(self):
        self.assertTrue(within('server/auth/a.py', ['server/auth/']))
        for path in ('server/authentication/a.py', '../server/auth/a.py', '/server/auth/a.py'):
            self.assertFalse(within(path, ['server/auth/']))

    def test_allowed_change(self):
        self.assertEqual(scope_errors(self.base, self.head, ['NIM-TST-001'], ['server/auth/a.py']), [])

    def test_scope_expansion_cannot_authorize_itself(self):
        self.head['tasks'][0]['write_scopes'].append('server/message/')
        self.assertTrue(scope_errors(self.base, self.head, ['NIM-TST-001'], ['server/message/a.py']))

    def test_new_task_requires_prior_registration(self):
        self.assertTrue(scope_errors(self.base, self.head, ['NIM-TST-002'], ['server/auth/a.py']))

    def test_evidence_only_for_selected_task(self):
        self.assertEqual(scope_errors(self.base, self.head, ['NIM-TST-001'], ['tasks/evidence/NIM-TST-001/report.md']), [])
        self.assertTrue(scope_errors(self.base, self.head, ['NIM-TST-001'], ['tasks/evidence/NIM-TST-002/report.md']))

    def test_rename_checks_old_and_new_paths_with_git(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            def git(*args):
                return subprocess.check_output(['git', '-C', temp, *args], stderr=subprocess.PIPE).decode().strip()
            git('init')
            git('config', 'user.email', 'fixture@example.invalid')
            git('config', 'user.name', 'Fixture')
            (root / 'outside').mkdir()
            (root / 'outside/a.py').write_text('content\n')
            git('add', '.')
            git('commit', '-m', 'base')
            base = git('rev-parse', 'HEAD')
            (root / 'server/auth').mkdir(parents=True)
            (root / 'outside/a.py').rename(root / 'server/auth/a.py')
            git('add', '-A')
            git('commit', '-m', 'rename')
            changed = git('diff', '--no-renames', '--name-only', base, 'HEAD').splitlines()
            self.assertIn('outside/a.py', changed)
            self.assertTrue(scope_errors(self.base, self.head, ['NIM-TST-001'], changed))


if __name__ == '__main__':
    unittest.main()
