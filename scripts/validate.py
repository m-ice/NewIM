#!/usr/bin/env python3
"""校验控制面，不执行产品代码 / Validate the control plane only."""
from control import validate


def main():
    manifest, errors = validate()
    if errors:
        for error in errors:
            print(f'ERROR: {error}')
        return 1
    print(f"OK: {len(manifest['tasks'])} tasks; contracts, graph, workstreams and done evidence valid")
    print('Product builds/tests and authenticity of human reviews are not certified by this check.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
