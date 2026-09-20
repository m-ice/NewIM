#!/usr/bin/env python3
"""检查 PR 两端授权范围 / Check changed paths against both base and head contracts."""
import argparse
import json
import re
import subprocess
from pathlib import Path
from control import ROOT, SHA, within


def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args], stderr=subprocess.PIPE)


def scope_errors(base_manifest, head_manifest, task_ids, changed):
    errors = []
    scope_sets = []
    for label, manifest in (('base', base_manifest), ('head', head_manifest)):
        tasks = {t['id']: t for t in manifest['tasks']}
        scopes = []
        for task_id in task_ids:
            task = tasks.get(task_id)
            if task is None:
                errors.append(f'{label}: task {task_id} does not exist; register it in an earlier PR')
                continue
            if task['status'] == 'blocked' and task.get('blocker', {}).get('kind') == 'manual':
                errors.append(f'{label}: task {task_id} is manually blocked')
            if any(d not in tasks or tasks[d]['status'] != 'done' for d in task['depends_on']):
                errors.append(f'{label}: task {task_id} has unfinished dependencies')
            scopes.extend(task['write_scopes'])
            # Evidence is the only implicit write permission, limited to this task.
            scopes.extend([f'tasks/evidence/{task_id}.json', f'tasks/evidence/{task_id}/'])
        scope_sets.append((label, scopes))
    for path in changed:
        for label, scopes in scope_sets:
            if not within(path, scopes):
                errors.append(f'{label}: changed path outside declared task scopes: {path}')
    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base')
    parser.add_argument('--head')
    parser.add_argument('--task', action='append', default=[])
    parser.add_argument('--event', help='GitHub pull_request event JSON; reads Task: NIM-XXX-001 lines')
    args = parser.parse_args()
    try:
        if args.event:
            event = json.loads(Path(args.event).read_text(encoding='utf-8'))
            pr = event['pull_request']
            args.base, args.head = pr['base']['sha'], pr['head']['sha']
            args.task = re.findall(r'^Task:\s*(NIM-[A-Z]+-[0-9]{3})\s*$', pr.get('body') or '', re.MULTILINE)
        if not args.task:
            raise ValueError('declare at least one Task: NIM-XXX-001 in PR body or pass --task')
        if not all(isinstance(x, str) and SHA.fullmatch(x) for x in (args.base, args.head)):
            raise ValueError('base/head must be full commit SHAs')
        manifests = [json.loads(git('show', f'{sha}:tasks/manifest.json')) for sha in (args.base, args.head)]
        changed = git('diff', '--no-renames', '--name-only', '-z', args.base, args.head).decode('utf-8').split('\0')
        errors = scope_errors(*manifests, args.task, [p for p in changed if p])
        if errors:
            raise ValueError('\n'.join(errors))
        print('OK: changed paths fit task scopes at both base and head')
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f'ERROR: {exc}\n')


if __name__ == '__main__':
    main()
