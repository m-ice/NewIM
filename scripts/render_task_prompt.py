#!/usr/bin/env python3
import argparse
import json
from control import eligible, validate


def main():
    parser = argparse.ArgumentParser(description='Render only dependency-ready, unblocked tasks.')
    parser.add_argument('task_id')
    args = parser.parse_args()
    manifest, errors = validate()
    if errors:
        raise SystemExit('\n'.join(errors))
    tasks = {task['id']: task for task in manifest['tasks']}
    task = tasks.get(args.task_id)
    if task is None:
        raise SystemExit('unknown task id')
    if not eligible(task, tasks):
        raise SystemExit('task is not eligible: check status, blockers and dependencies')
    if any(check['kind'] == 'pending' for check in task['acceptance_checks']):
        raise SystemExit('resolve pending acceptance checks in a control-plane change first')
    lines = [f"# Execute {task['id']}: {task['title']}",
             f"Product repository / working directory: {manifest['repository']['local_path']}",
             f"Git origin: {manifest['repository']['origin']}",
             'All write scopes are relative to the product repository root.',
             'Read docs/16-project-location.md, root and nearest AGENTS.md, relevant specs and ADRs first.',
             f"Accountable specialist: {task['owner']}",
             f"Description: {task['description']}",
             f"Allowed write scopes: {', '.join(task['write_scopes'])}",
             f"Reviewers: {', '.join(task['reviewers'])}",
             f"Flags: {', '.join(task['flags']) or 'none'}", 'Acceptance checks:']
    for check in task['acceptance_checks']:
        detail = json.dumps(check['argv'], ensure_ascii=False) if check['kind'] == 'command' else check['reason']
        lines.append(f"- {check['criterion']} [{check['kind']}]: {detail}")
    lines += [f"Completion evidence: {task['evidence_path']}",
              'Follow docs/15-control-plane-checks.md. Never mark done without matching check and review evidence.',
              'Do not widen scopes silently. Record blockers; do not invent a toolchain or a passing result.']
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
