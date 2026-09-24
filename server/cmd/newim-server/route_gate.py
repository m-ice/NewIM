#!/usr/bin/env python3
"""Fail-closed JSON gate for route and process lifecycle tests."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

TESTS = (
    "TestAPIRouteComposition",
    "TestAPIRouteValidation",
    "TestAuthRuntimeDisabledWithoutDSN",
    "TestAuthRuntimeRejectsUnsafeStartupBeforeOpen",
    "TestAuthRuntimeRejectsMalformedAndLocalSocketDSN",
    "TestJoinRuntimeServerAndAuthDeadlineDoesNotCloseBeforeDrain",
    "TestJoinRuntimeServerAndAuthClosesAfterServerDrain",
    "TestNewIMServerSecondBindFailure",
)
ROOT = Path(__file__).resolve().parents[3]


def validation_errors(events: list[dict], returncode: int) -> list[str]:
    errors = []
    if returncode != 0:
        errors.append("go test failed")
    skipped = [event.get("Test", "") for event in events if event.get("Action") == "skip"]
    failed = [event.get("Test", "") for event in events if event.get("Action") == "fail"]
    if skipped:
        errors.append("skipped tests observed: " + ", ".join(skipped))
    if failed:
        errors.append("failed tests observed: " + ", ".join(failed))

    def has(test: str, action: str) -> bool:
        return any(event.get("Action") == action and event.get("Test") == test for event in events)

    for test in TESTS:
        if not has(test, "run") or not has(test, "pass"):
            errors.append(f"{test} did not run and pass")
    return errors


def run_tests() -> int:
    argv = [
        "go", "test", "-json", "-race", "-shuffle=on", "-count=1",
        "./server/api", "./server/cmd/newim-server",
    ]
    try:
        result = subprocess.run(argv, cwd=ROOT, capture_output=True, check=False)
    except OSError as exc:
        print(f"route gate could not start go test: {exc}", file=sys.stderr)
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
        print(f"route gate: {error}", file=sys.stderr)
    return 1 if errors else 0


def self_test() -> int:
    passing = [{"Action": "run", "Test": test} for test in TESTS]
    passing += [{"Action": "pass", "Test": test} for test in TESTS]
    cases = [
        ("passing", passing, 0, False),
        ("empty", [], 0, True),
        ("subtest-skip", passing + [{"Action": "skip", "Test": "TestAPIRouteValidation/child"}], 0, True),
        ("failure", [{"Action": "fail", "Test": TESTS[0]}], 1, True),
    ]
    for name, events, returncode, want_error in cases:
        errors = validation_errors(events, returncode)
        if bool(errors) != want_error:
            print(f"route gate self-test {name} failed: {errors}", file=sys.stderr)
            return 1
    return 0


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    if sys.argv[1:]:
        print("route gate accepts no arguments except --self-test", file=sys.stderr)
        return 2
    return run_tests()


if __name__ == "__main__":
    raise SystemExit(main())
