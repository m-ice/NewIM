#!/usr/bin/env python3
"""Fail-closed runner for named bearer HTTP security tests."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

TESTS = ("TestSessionHandlerContract", "TestSessionHandlerSecurity")
ROOT = Path(__file__).resolve().parents[3]


def validation_errors(events: list[dict], returncode: int) -> list[str]:
    errors = []
    if returncode != 0:
        errors.append("go test failed")

    def has(test: str, action: str) -> bool:
        return any(event.get("Action") == action and event.get("Test") == test for event in events)

    for test in TESTS:
        if not has(test, "run") or not has(test, "pass"):
            errors.append(f"{test} did not run and pass")
        if has(test, "skip") or has(test, "fail"):
            errors.append(f"{test} skipped or failed")
    return errors


def run_tests() -> int:
    pattern = "^(TestSessionHandlerContract|TestSessionHandlerSecurity)$"
    argv = [
        "go", "test", "-json", "-race", "-shuffle=on", "-count=1",
        "-run", pattern, "./server/auth/bearerhttp",
    ]
    try:
        result = subprocess.run(argv, cwd=ROOT, capture_output=True, check=False)
    except OSError as exc:
        print(f"security gate could not start go test: {exc}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)

    events = []
    for line in result.stdout.decode("utf-8", "replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    errors = validation_errors(events, result.returncode)
    for error in errors:
        print(f"security gate: {error}", file=sys.stderr)
    return 1 if errors else 0


def self_test() -> int:
    pass_events = [
        {"Action": "run", "Test": test} for test in TESTS
    ] + [
        {"Action": "pass", "Test": test} for test in TESTS
    ]
    cases = [
        ("passing", pass_events, 0, False),
        ("empty", [], 0, True),
        ("skipped", [{"Action": "skip", "Test": TESTS[0]}], 0, True),
        ("failed", [{"Action": "fail", "Test": TESTS[0]}], 1, True),
    ]
    for name, events, returncode, want_error in cases:
        errors = validation_errors(events, returncode)
        if bool(errors) != want_error:
            print(f"security gate self-test {name} failed: {errors}", file=sys.stderr)
            return 1
    return 0


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    if sys.argv[1:]:
        print("security gate accepts no arguments except --self-test", file=sys.stderr)
        return 2
    return run_tests()


if __name__ == "__main__":
    raise SystemExit(main())
