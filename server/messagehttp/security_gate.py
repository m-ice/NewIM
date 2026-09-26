#!/usr/bin/env python3
"""Fail-closed gate for message HTTP unit and real-process security tests."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

TESTS = (
    "TestNewHandlerRejectsInvalidConfiguration",
    "TestHandlerSuccessUsesTokenScopedIdentityAndProtocolACK",
    "TestHandlerStrictHeaders",
    "TestHandlerMethodRouteAndQueryPrecedence",
    "TestHandlerBodyBoundaries",
    "TestHandlerProtocolValidation",
    "TestHandlerAuthenticationErrorMapping",
    "TestHandlerSendErrorMapping",
    "TestNewHandlerRejectsTypedNilDependencies",
    "TestHandlerSafeNilRequestAndHandler",
    "TestHandlerRecoversAndRedactsPanics",
    "TestHandlerRejectsNonACKSuccessFrames",
    "TestHandlerRejectsMiscorrelatedACK",
)
ROOT = Path(__file__).resolve().parents[2]


def unit_errors(events: list[dict], returncode: int) -> list[str]:
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
            errors.append(test + " did not run and pass")
    return errors


def suite_errors(output: bytes, returncode: int) -> list[str]:
    errors = []
    if returncode != 0:
        errors.append("message HTTP integration suite failed")
    if b"--- SKIP:" in output:
        errors.append("message HTTP integration suite skipped a test")
    if b"--- FAIL:" in output:
        errors.append("message HTTP integration suite failed a test")
    if b"Message HTTP suite passed;" not in output:
        errors.append("message HTTP integration suite did not report exact PASS")
    return errors


def run_unit_tests() -> int:
    argv = ["go", "test", "-json", "-race", "-shuffle=on", "-count=1", "./server/messagehttp"]
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, check=False)
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
    errors = unit_errors(events, result.returncode)
    for error in errors:
        print("message HTTP security gate: " + error, file=sys.stderr)
    return 1 if errors else 0


def run_integration_suite() -> int:
    argv = ["python3", "-B", "infra/db/message_http_suite.py", "security"]
    result = subprocess.run(argv, cwd=ROOT, capture_output=True, check=False)
    sys.stdout.buffer.write(result.stdout)
    sys.stderr.buffer.write(result.stderr)
    errors = suite_errors(result.stdout + result.stderr, result.returncode)
    for error in errors:
        print("message HTTP security gate: " + error, file=sys.stderr)
    return 1 if errors else 0


def self_test() -> int:
    passing_events = [{"Action": "run", "Test": test} for test in TESTS]
    passing_events += [{"Action": "pass", "Test": test} for test in TESTS]
    cases = (
        ("passing", passing_events, 0, False),
        ("empty", [], 0, True),
        ("all-skip", [{"Action": "skip", "Test": TESTS[0]}], 0, True),
        ("subtest-skip", passing_events + [{"Action": "skip", "Test": TESTS[0] + "/child"}], 0, True),
        ("missing-pass", [{"Action": "run", "Test": TESTS[0]}], 0, True),
        ("nonzero", passing_events, 1, True),
    )
    for label, events, returncode, want_error in cases:
        errors = unit_errors(events, returncode)
        if bool(errors) != want_error:
            print("message HTTP security gate self-test failed: " + label, file=sys.stderr)
            return 1
    passing_suite = b"Message HTTP suite passed; command records: x\n"
    suite_cases = (
        ("suite-passing", passing_suite, 0, False),
        ("suite-empty-target", b"", 0, True),
        ("suite-skip", passing_suite + b"--- SKIP: TestMessageHTTPSecurity\n", 0, True),
        ("suite-missing-pass", b"ok\n", 0, True),
        ("suite-nonzero", passing_suite, 1, True),
    )
    for label, output, returncode, want_error in suite_cases:
        errors = suite_errors(output, returncode)
        if bool(errors) != want_error:
            print("message HTTP security gate self-test failed: " + label, file=sys.stderr)
            return 1
    print("message HTTP security gate self-test: OK")
    return 0


def main() -> int:
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    if sys.argv[1:]:
        print("message HTTP security gate accepts no arguments except --self-test", file=sys.stderr)
        return 2
    unit = run_unit_tests()
    integration = run_integration_suite()
    return 1 if unit or integration else 0


if __name__ == "__main__":
    raise SystemExit(main())
