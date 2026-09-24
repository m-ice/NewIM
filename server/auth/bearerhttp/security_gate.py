#!/usr/bin/env python3
"""Fail-closed runner for named bearer HTTP security tests."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

SESSION_TESTS = ("TestSessionHandlerContract", "TestSessionHandlerSecurity")
LOGOUT_TESTS = ("TestTokenRevocationHandlerContract", "TestTokenRevocationHandlerSecurity")
SUITES = {"session": SESSION_TESTS, "logout": LOGOUT_TESTS}
ROOT = Path(__file__).resolve().parents[3]


def validation_errors(events: list[dict], returncode: int, tests: tuple[str, ...]) -> list[str]:
    errors = []
    if returncode:
        errors.append("go test failed")

    def has(test: str, action: str) -> bool:
        return any(event.get("Action") == action and event.get("Test") == test for event in events)

    skipped = [event.get("Test", "") for event in events if event.get("Action") == "skip"]
    if skipped:
        errors.append("skipped tests observed: " + ", ".join(skipped))
    failed = [event.get("Test", "") for event in events if event.get("Action") == "fail"]
    if failed:
        errors.append("failed test events observed: " + ", ".join(failed))
    for test in tests:
        if not has(test, "run"):
            errors.append(f"{test} did not run")
        if not has(test, "pass"):
            errors.append(f"{test} did not pass")
    return errors


def run_tests(tests: tuple[str, ...]) -> int:
    pattern = "^(" + "|".join(tests) + ")$"
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
    errors = validation_errors(events, result.returncode, tests)
    for error in errors:
        print(f"security gate: {error}", file=sys.stderr)
    return 1 if errors else 0


def self_test() -> int:
    name = "TestTokenRevocationHandlerContract"
    passing = [{"Action": "run", "Test": name}, {"Action": "pass", "Test": name}]
    cases = [
        ("passing", passing, 0, False),
        ("empty", [], 0, True),
        ("skipped", [{"Action": "skip", "Test": name}], 0, True),
        ("subtest-skip", passing + [{"Action": "skip", "Test": name + "/child"}], 0, True),
        ("failed", [{"Action": "fail", "Test": name}], 1, True),
        ("missing-pass", [{"Action": "run", "Test": name}], 0, True),
    ]
    for label, events, returncode, want_error in cases:
        errors = validation_errors(events, returncode, (name,))
        if bool(errors) != want_error:
            print(f"security gate self-test {label} failed: {errors}", file=sys.stderr)
            return 1
    print("security gate self-test: OK")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--suite", choices=SUITES, default="session")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    return run_tests(SUITES[args.suite])


if __name__ == "__main__":
    raise SystemExit(main())
