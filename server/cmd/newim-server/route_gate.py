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


def main() -> int:
    if sys.argv[1:]:
        print("route gate accepts no arguments", file=sys.stderr)
        return 2
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

    def has(test: str, action: str) -> bool:
        return any(event.get("Action") == action and event.get("Test") == test for event in events)

    errors = []
    if result.returncode != 0:
        errors.append("go test failed")
    for test in TESTS:
        if not has(test, "run") or not has(test, "pass"):
            errors.append(f"{test} did not run and pass")
        if has(test, "skip") or has(test, "fail"):
            errors.append(f"{test} skipped or failed")
    for error in errors:
        print(f"route gate: {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
