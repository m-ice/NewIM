#!/usr/bin/env python3
from control import eligible, validate


def main():
    manifest, errors = validate()
    if errors:
        raise SystemExit('\n'.join(errors))
    tasks = {task['id']: task for task in manifest['tasks']}
    for task in tasks.values():
        if eligible(task, tasks):
            print(f"{task['id']}\t{task['owner']}\t{task['title']}")


if __name__ == '__main__':
    main()
