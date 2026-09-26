"""Run the real loopback message HTTP integration gates in owned PostgreSQL."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
import uuid

from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity

TESTS = {
    "check": ("TestMessageHTTPCheck", "TestMessageHTTPIdentity"),
    "identity": ("TestMessageHTTPIdentity",),
    "security": "TestMessageHTTPSecurity",
}


def result_errors(name, listing, stdout, stderr, returncode):
    errors = []
    combined = stdout + stderr
    if listing != [name]:
        errors.append("discovery must identify exactly one named test")
    if returncode:
        errors.append("test process returned nonzero")
    if b"--- SKIP:" in combined:
        errors.append("test output contains a skip")
    if b"--- FAIL:" in combined:
        errors.append("test output contains a failure")
    if not re.search(rb"^--- PASS: " + re.escape(name.encode()) + rb"\b", stdout, re.M):
        errors.append("exact named test did not pass")
    if not re.search(rb"^PASS$", stdout, re.M):
        errors.append("test output has no final PASS")
    return errors


def self_test():
    name = "TestMessageHTTPCheck"
    passing = b"=== RUN   " + name.encode() + b"\n--- PASS: " + name.encode() + b" (0.01s)\nPASS\n"
    cases = (
        ("passing", [name], passing, b"", 0),
        ("empty", [], passing, b"", 0),
        ("extra", [name, "TestExtra"], passing, b"", 0),
        ("skip", [name], passing.replace(b"--- PASS:", b"--- SKIP:"), b"", 0),
        ("subtest-skip", [name], passing + b"    --- SKIP: TestMessageHTTPCheck/child\n", b"", 0),
        ("missing-pass", [name], passing.replace(b"--- PASS:", b"--- RUN:"), b"", 0),
        ("nonzero", [name], passing, b"", 1),
    )
    for label, listing, stdout, stderr, returncode in cases:
        errors = result_errors(name, listing, stdout, stderr, returncode)
        if (label == "passing") == bool(errors):
            print("message HTTP suite self-test failed: " + label, file=os.sys.stderr)
            return 1
    print("message HTTP suite self-test: OK")
    return 0


def build_binaries(commands, env, directory):
    test_binary = directory / "messagehttp.test"
    server_binary = directory / "newim-server"
    wrapper_binary = directory / "loss-wrapper"
    builds = (
        ("message-http-cross-compile", ["go", "test", "-c", "-tags=integration", "-o", str(test_binary), "./tests/integration/messagehttp"]),
        ("message-http-server-build", ["go", "build", "-o", str(server_binary), "./server/cmd/newim-server"]),
        ("message-http-loss-wrapper-build", ["go", "build", "-tags=integration", "-o", str(wrapper_binary), "./tests/integration/messagehttp/loss-wrapper"]),
    )
    for label, argv in builds:
        started = time.monotonic()
        try:
            result = subprocess.run(argv, cwd=ROOT, env=env, capture_output=True, timeout=180, check=False)
        except subprocess.TimeoutExpired:
            commands.record(argv, 124, time.monotonic() - started, label)
            raise Failure(label + " timeout")
        commands.record(argv, result.returncode, time.monotonic() - started, label)
        (directory / (label + ".log")).write_bytes(result.stdout + result.stderr)
        if result.returncode:
            print(result.stderr.decode(), end="")
            raise Failure(label + " failed")
    return test_binary, server_binary, wrapper_binary


def run_test(db, name, phase="single"):
    argv = ["docker", "exec", "-e", "NEWIM_MESSAGE_HTTP_PHASE=" + phase,
            "-e", "NEWIM_SERVER_BINARY=/tmp/newim-server",
            "-e", "NEWIM_LOSS_WRAPPER_BINARY=/tmp/loss-wrapper",
            "-e", "NEWIM_MESSAGE_HTTP_MIGRATION_SQL=/tmp/message-http-migration.sql",
            db.name, "/tmp/messagehttp.test"]
    listing = db.commands.run(argv + ["-test.list", "^" + name + "$"], label="list-" + phase)
    if listing.stdout.decode().splitlines() != [name]:
        raise Failure("message HTTP discovery must identify exactly one named test")
    result = db.commands.run(argv + ["-test.v", "-test.count=1", "-test.timeout=240s",
                                     "-test.run", "^" + name + "$"], timeout=300,
                             label="message-http-integration-" + phase, check=False)
    log = result.stdout + result.stderr
    (db.commands.directory / ("test-" + phase + ".log")).write_bytes(log)
    print(result.stdout.decode(), end="", flush=True)
    errors = result_errors(name, listing.stdout.decode().splitlines(), result.stdout, result.stderr, result.returncode)
    if errors:
        raise Failure("message HTTP integration failed or skipped: " + phase + ": " + ", ".join(errors))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("suite", nargs="?", choices=TESTS)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.suite is None:
        parser.error("suite is required unless --self-test is used")

    directory = ROOT / "build" / "message-http" / (args.suite + "-" + uuid.uuid4().hex)
    commands = Commands(directory)
    try:
        image, platform = image_identity(commands)
        env = os.environ.copy()
        env.update(CGO_ENABLED="0", GOOS="linux", GOARCH=platform.split("/")[1], GOTOOLCHAIN="local")
        keys = ("PATH", "HOME", "GOROOT", "GOPATH", "GOMODCACHE", "GOCACHE", "GOTMPDIR",
                "GOENV", "GOFLAGS", "GOEXPERIMENT", "GOVERSION", "CGO_ENABLED", "GOOS",
                "GOARCH", "GOTOOLCHAIN", "CC", "CXX")
        recorded = {}
        for key in keys:
            if key not in env:
                continue
            value = env[key]
            if any(part in key for part in ("TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "AUTH", "KEY")):
                value = "sha256:" + hashlib.sha256(value.encode()).hexdigest()
            recorded[key] = value
        (directory / "build-environment.json").write_text(json.dumps({
            "cwd": str(ROOT), "environment": recorded}, indent=2) + "\n")

        test_binary, server_binary, wrapper_binary = build_binaries(commands, env, directory)
        migration_file = directory / "migration.sql"
        migration_file.write_text(migration_sql())
        with Database(commands, image, platform) as db:
            db.sql(migration_sql())
            commands.run(["docker", "cp", str(test_binary), db.name + ":/tmp/messagehttp.test"],
                         label="copy-owned-message-http-test-binary")
            commands.run(["docker", "cp", str(server_binary), db.name + ":/tmp/newim-server"],
                         label="copy-owned-message-http-server")
            commands.run(["docker", "cp", str(wrapper_binary), db.name + ":/tmp/loss-wrapper"],
                         label="copy-owned-message-http-loss-wrapper")
            commands.run(["docker", "cp", str(migration_file), db.name + ":/tmp/message-http-migration.sql"],
                         label="copy-owned-message-http-migration")
            commands.run(["docker", "exec", db.name, "chmod", "0755", "/tmp/messagehttp.test", "/tmp/newim-server", "/tmp/loss-wrapper"],
                         label="make-message-http-binaries-executable")
            tests = TESTS[args.suite]
            if isinstance(tests, str):
                tests = (tests,)
            for test_name in tests:
                run_test(db, test_name, args.suite)
        print("Message HTTP suite passed; command records: " + str(directory))
    except (Failure, OSError) as exc:
        print("Message HTTP suite failed: " + str(exc) + "; command records: " + str(directory))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
