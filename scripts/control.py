"""任务控制面校验 / Validate task contracts without executing acceptance commands."""
from pathlib import Path, PurePosixPath
import hashlib
import json
import re
import tomllib

ROOT = Path(__file__).resolve().parents[1]
STATUSES = {'ready', 'blocked', 'in_progress', 'review', 'done'}
TASK_ID = re.compile(r'NIM-[A-Z]+-[0-9]{3}')
SHA = re.compile(r'[0-9a-f]{40}|[0-9a-f]{64}')


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def strings(value, allow_empty=False):
    return (isinstance(value, list) and (allow_empty or bool(value))
            and all(nonempty(x) for x in value) and len(value) == len(set(value)))


def safe_path(value):
    if not nonempty(value) or '\\' in value:
        return False
    path = PurePosixPath(value)
    return (not path.is_absolute() and '..' not in path.parts
            and value.rstrip('/') == str(path) and str(path) != '.')


def within(path, scopes):
    """按路径边界匹配 / Match directories without accepting sibling prefixes."""
    return safe_path(path) and any(
        path.startswith(s) if s.endswith('/') else path == s for s in scopes
    )


def eligible(task, tasks):
    return (task['status'] == 'ready' or (
        task['status'] == 'blocked' and task['blocker']['kind'] == 'dependencies'
    )) and all(tasks[d]['status'] == 'done' for d in task['depends_on'])


def load_json(path):
    return json.loads(path.read_text(encoding='utf-8'))


def validate_evidence(root, task, errors):
    label = task['id']
    path = root / task['evidence_path']
    try:
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('evidence escapes repository')
        record = load_json(path)
        if not isinstance(record, dict):
            raise ValueError('evidence must be an object')
    except (OSError, ValueError) as exc:
        errors.append(f'{label}: evidence: {exc}')
        return
    commit = record.get('commit')
    if record.get('task_id') != label or not isinstance(commit, str) or not SHA.fullmatch(commit):
        errors.append(f'{label}: evidence requires matching task_id and full commit SHA')

    def report(item):
        rel = item.get('report')
        if not safe_path(rel):
            errors.append(f'{label}: invalid report path')
            return
        target = root / rel
        try:
            if not target.resolve().is_relative_to(root.resolve()):
                raise ValueError('report escapes repository')
            content = target.read_bytes()
            if not content.strip() or hashlib.sha256(content).hexdigest() != item.get('sha256'):
                raise ValueError('empty report or SHA-256 mismatch')
        except (OSError, ValueError) as exc:
            errors.append(f'{label}: report {rel}: {exc}')

    results = record.get('checks')
    checks = task['acceptance_checks']
    if not isinstance(results, list) or len(results) != len(checks):
        errors.append(f'{label}: evidence must cover every acceptance check')
    else:
        for planned, actual in zip(checks, results):
            if not isinstance(actual, dict):
                errors.append(f'{label}: check evidence must be an object')
                continue
            if actual.get('criterion') != planned['criterion'] or actual.get('result') != 'pass':
                errors.append(f'{label}: criterion mismatch or check did not pass')
            if planned['kind'] == 'command' and (
                actual.get('argv') != planned['argv'] or type(actual.get('exit_code')) is not int
                or actual['exit_code'] != 0
            ):
                errors.append(f'{label}: command evidence must match argv and exit 0')
            report(actual)
    reviews = record.get('reviews')
    if not isinstance(reviews, list):
        errors.append(f'{label}: missing review evidence')
        return
    roles = []
    for review in reviews:
        if not isinstance(review, dict):
            errors.append(f'{label}: invalid review evidence')
            continue
        roles.append(review.get('role'))
        if (review.get('result') != 'approved' or review.get('commit') != commit
                or not nonempty(review.get('reviewer'))):
            errors.append(f'{label}: review requires identity, approval and matching commit')
        report(review)
    if sorted(str(x) for x in roles) != sorted(task['reviewers']):
        errors.append(f'{label}: evidence must contain each required reviewer role exactly once')


def validate(root=ROOT):
    """返回清单与错误，不写文件 / Return manifest and diagnostics without writes."""
    errors = []
    agents = set()
    for path in sorted((root / '.codex/agents').glob('*.toml')):
        agents.add(path.stem)
        try:
            agent = tomllib.loads(path.read_text(encoding='utf-8'))
            for key in ('name', 'description', 'developer_instructions'):
                if not nonempty(agent.get(key)):
                    errors.append(f'{path.name}: missing/string required: {key}')
        except (OSError, ValueError) as exc:
            errors.append(f'{path.name}: invalid TOML: {exc}')
    if not agents:
        errors.append('no agent definitions')
    try:
        manifest = load_json(root / 'tasks/manifest.json')
    except (OSError, ValueError) as exc:
        return {}, errors + [f'manifest invalid: {exc}']
    if not isinstance(manifest, dict):
        return {}, errors + ['manifest must be an object']
    if type(manifest.get('schema_version')) is not int or manifest['schema_version'] != 2:
        errors.append('schema_version must be 2')
    if not strings(manifest.get('status_values')) or set(manifest['status_values']) != STATUSES:
        errors.append('status_values must match the supported states')
    repository = manifest.get('repository')
    if not isinstance(repository, dict):
        errors.append('repository must define local_path, origin and control_source_path')
    else:
        for key in ('local_path', 'control_source_path'):
            value = repository.get(key)
            if not nonempty(value) or not Path(value).is_absolute():
                errors.append(f'repository.{key} must be an absolute path')
        if not nonempty(repository.get('origin')):
            errors.append('repository.origin must be a nonempty string')
    # Paths describe the local checkout; CI does not require this workstation to exist.
    raw = manifest.get('tasks')
    if not isinstance(raw, list) or not raw:
        return manifest, errors + ['tasks must be a nonempty array']
    tasks = {}
    required = {'id', 'title', 'description', 'phase', 'status', 'owner', 'reviewers',
                'depends_on', 'write_scopes', 'acceptance', 'flags', 'blocker',
                'acceptance_checks', 'evidence_path'}
    for index, task in enumerate(raw):
        if not isinstance(task, dict):
            errors.append(f'task[{index}] must be an object')
            continue
        label = task.get('id')
        if not isinstance(label, str) or not TASK_ID.fullmatch(label):
            errors.append(f'task[{index}]: invalid/missing id')
            continue
        if label in tasks:
            errors.append(f'{label}: duplicate task id')
            continue
        tasks[label] = task
        before_shape = len(errors)
        missing = required - task.keys()
        if missing:
            errors.append(f'{label}: missing {sorted(missing)}')
        for key in ('title', 'phase', 'status', 'owner', 'evidence_path'):
            if not nonempty(task.get(key)):
                errors.append(f'{label}: {key} must be a nonempty string')
        if not isinstance(task.get('description'), str):
            errors.append(f'{label}: description must be a string')
        for key in ('reviewers', 'depends_on', 'write_scopes', 'acceptance', 'flags'):
            if not strings(task.get(key), allow_empty=key in ('depends_on', 'flags')):
                errors.append(f'{label}: {key} must be an array of unique nonempty strings')
        if len(errors) != before_shape:
            continue
        if task['status'] not in STATUSES:
            errors.append(f'{label}: invalid status')
        if not isinstance(task['owner'], str) or task['owner'] not in agents:
            errors.append(f'{label}: unknown owner')
        for reviewer in task['reviewers']:
            if reviewer not in agents:
                errors.append(f'{label}: unknown reviewer {reviewer}')
        for flag, role in (('security', 'security'), ('license', 'license'), ('reliability', 'qa')):
            if flag in task['flags'] and role not in task['reviewers']:
                errors.append(f'{label}: {flag} flag requires {role} reviewer')
        if any(not safe_path(s) for s in task['write_scopes']):
            errors.append(f'{label}: invalid write scope')
        if not safe_path(task['evidence_path']) or task['evidence_path'] != f'tasks/evidence/{label}.json':
            errors.append(f'{label}: evidence_path must be tasks/evidence/{label}.json')
        blocker = task['blocker']
        if task['status'] == 'blocked':
            if (not isinstance(blocker, dict) or blocker.get('kind') not in ('dependencies', 'manual')
                    or not nonempty(blocker.get('reason'))):
                errors.append(f'{label}: blocked requires dependencies/manual blocker with reason')
            elif blocker['kind'] == 'dependencies' and not task['depends_on']:
                errors.append(f'{label}: dependency blocker has no dependencies')
        elif blocker is not None:
            errors.append(f'{label}: only blocked tasks may have a blocker')
        checks = task['acceptance_checks']
        if not isinstance(checks, list) or len(checks) != len(task['acceptance']):
            errors.append(f'{label}: acceptance_checks must cover each acceptance criterion')
            continue
        if (task['status'] in ('in_progress', 'review', 'done') and label != 'NIM-FND-001'
                and not any(isinstance(c, dict) and c.get('kind') == 'command' for c in checks)):
            errors.append(f'{label}: implementation requires at least one executable check')
        for criterion, check in zip(task['acceptance'], checks):
            if not isinstance(check, dict) or check.get('criterion') != criterion:
                errors.append(f'{label}: check criterion/order mismatch')
                continue
            kind = check.get('kind')
            if kind == 'command':
                argv = check.get('argv')
                if not isinstance(argv, list) or not argv or not all(nonempty(x) for x in argv):
                    errors.append(f'{label}: command requires nonempty argv array')
            elif kind in ('manual', 'pending'):
                if not nonempty(check.get('reason')):
                    errors.append(f'{label}: {kind} check requires reason')
                if kind == 'pending' and task['status'] in ('in_progress', 'review', 'done'):
                    errors.append(f'{label}: resolve pending checks before implementation/review/done')
            else:
                errors.append(f'{label}: unknown check kind')
    # Do not traverse a graph until all its nodes have valid shapes.
    if errors:
        return manifest, errors
    for label, task in tasks.items():
        for dependency in task['depends_on']:
            if dependency not in tasks:
                errors.append(f'{label}: unknown dependency {dependency}')
        if label in task['depends_on']:
            errors.append(f'{label}: self dependency')
    if errors:
        return manifest, errors
    visiting, visited = set(), set()

    def visit(label):
        if label in visiting:
            errors.append(f'task dependency cycle at {label}')
            return
        if label in visited:
            return
        visiting.add(label)
        for dependency in tasks[label]['depends_on']:
            visit(dependency)
        visiting.remove(label)
        visited.add(label)

    for label in tasks:
        visit(label)
    if errors:
        return manifest, errors

    def ancestors(label):
        result, pending = set(), list(tasks[label]['depends_on'])
        while pending:
            dependency = pending.pop()
            if dependency not in result:
                result.add(dependency)
                pending.extend(tasks[dependency]['depends_on'])
        return result

    for label, task in tasks.items():
        if task['status'] in ('ready', 'in_progress', 'review', 'done'):
            if any(tasks[d]['status'] != 'done' for d in task['depends_on']):
                errors.append(f'{label}: active/done task has unfinished dependencies')
        if task['phase'] not in ('foundation', 'protocol', 'release') and 'NIM-FND-002' not in ancestors(label):
            errors.append(f'{label}: implementation must depend on NIM-FND-002')
        if task['phase'] == 'release':
            missing = {n for n, t in tasks.items() if t['phase'] != 'release'} - ancestors(label)
            if missing:
                errors.append(f'{label}: release omits tasks {sorted(missing)}')
        if task['status'] == 'done':
            validate_evidence(root, task, errors)
    releases = [t for t in tasks.values() if t['phase'] == 'release']
    if not releases:
        errors.append('at least one release task is required')
    memberships = []
    for path in sorted((root / 'workstreams').glob('*.json')):
        try:
            stream = load_json(path)
            if not isinstance(stream, dict) or not strings(stream.get('tasks')):
                raise ValueError('workstream requires nonempty unique tasks')
            if stream.get('phase') != path.stem:
                errors.append(f'{path.name}: phase must match filename')
            for label in stream['tasks']:
                memberships.append(label)
                if label not in tasks or tasks[label]['phase'] != stream.get('phase'):
                    errors.append(f'{path.name}: unknown task or wrong phase: {label}')
        except (OSError, ValueError) as exc:
            errors.append(f'{path.name}: invalid workstream: {exc}')
    if sorted(memberships) != sorted(tasks):
        errors.append('workstreams must include each task exactly once')
    for rel in ('AGENTS.md', 'reference/NewIM_自研IM技术基线与风险清单.txt',
                'docs/05-quality-gates.md', 'docs/06-licensing-boundary.md'):
        if not (root / rel).is_file():
            errors.append(f'missing {rel}')
    return manifest, errors
