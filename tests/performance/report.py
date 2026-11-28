"""Validate the isolated NewIM baseline and derive comparable latency and rate fields."""

import argparse
import hashlib
import json
import re
from pathlib import Path


MARKER = b"NEWIM_PERF_RAW "
NANOSECONDS_PER_SECOND = 1_000_000_000
PROFILE = {
    "devices": 16,
    "connection_batch": 4,
    "message_sends": 48,
    "text_bytes": 64,
    "history_readers": 4,
    "page_limit": 8,
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def positive_integer(value):
    return type(value) is int and value > 0


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def rate(count, window_ns):
    return count * NANOSECONDS_PER_SECOND / window_ns


def stage(value, expected, sequential=False):
    require(isinstance(value, dict), "missing operation sample")
    window = value.get("window_ns")
    durations = value.get("latencies_ns")
    require(positive_integer(window), "invalid measurement window")
    require(isinstance(durations, list) and len(durations) == expected, "incomplete sample population")
    require(all(positive_integer(item) and item <= window for item in durations), "invalid latency sample")
    if sequential:
        require(sum(durations) <= window, "serial latencies exceed the measured window")
    ordered = sorted(durations)
    percentiles = {}
    for p in (50, 95, 99):
        rank = (p * len(ordered) + 99) // 100
        percentiles[f"p{p}"] = ordered[rank - 1] / 1_000_000
    return {
        "count": len(durations),
        "window_ns": window,
        "latency_ms": percentiles,
        "operations_per_second": rate(len(durations), window),
    }


def read_raw(log):
    matches = [line[len(MARKER):] for line in log.splitlines() if line.startswith(MARKER)]
    require(len(matches) == 1, "expected exactly one completed baseline record")
    return json.loads(matches[0])


def validate_context(context, raw):
    require(isinstance(context, dict), "invalid execution context")
    for name in ("revision", "tree"):
        require(re.fullmatch("(?:[0-9a-f]{40}|[0-9a-f]{64})", context.get(name, "")), f"missing exact source {name}")
    host, container, image = (context.get(key, {}) for key in ("host", "container", "image"))
    require(all(nonempty(host.get(key)) for key in ("os", "arch", "kernel", "cpu_model")),
            "host hardware identity is incomplete")
    require(positive_integer(host.get("logical_cpus")) and positive_integer(host.get("memory_bytes")),
            "host hardware limits are incomplete")
    require(container == {"network": "none", "nano_cpus": 2_000_000_000, "memory_bytes": 536_870_912, "pids": 128},
            "unexpected isolation or container configuration")
    require(nonempty(context.get("docker_version")), "Docker engine version is missing")
    for name in ("actual_image_id", "official_manifest", "official_config"):
        require(re.fullmatch("sha256:[0-9a-f]{64}", image.get(name, "")), "incomplete locked image identity")
    runtime_platform = "/".join(image.get("platform", "").split("/")[:2])
    require(runtime_platform in ("linux/amd64", "linux/arm64") and runtime_platform == raw.get("runtime_platform"),
            "executed architecture differs from the locked image")
    binaries = context.get("binary_sha256", {})
    require(set(binaries) == {"newim-server", "newim-gateway", "conversationsynchttp.test"} and
            all(re.fullmatch("[0-9a-f]{64}", digest) for digest in binaries.values()), "incomplete binary identity")
    require(all(nonempty(context.get("dependencies", {}).get(dep)) for dep in
                ("github.com/coder/websocket", "github.com/jackc/pgx/v5")), "missing server dependency versions")


def build_report(raw, context, log_digest):
    require(isinstance(raw, dict), "invalid baseline record")
    require(all(type(raw.get(key)) is int and raw[key] == value for key, value in PROFILE.items()),
            "baseline work profile differs from the declared configuration")
    validate_context(context, raw)
    for name in ("go_version", "server_version", "postgres_version"):
        require(nonempty(raw.get(name)), f"missing {name}")
    for name in ("runtime_cpus", "go_max_procs", "replacement_start_ns"):
        require(positive_integer(raw.get(name)), f"invalid {name}")
    require(raw.get("postgres_max_connections") == 40 and
            all(raw.get("postgres_" + key) == "on" for key in ("synchronous_commit", "fsync", "full_page_writes")),
            "database durability configuration differs from the baseline")

    devices, sends = PROFILE["devices"], PROFILE["message_sends"]
    history = sends + 1
    expected_pages = (history + PROFILE["page_limit"] - 1) // PROFILE["page_limit"] * PROFILE["history_readers"]
    for key, expected in (
        ("active_subscriptions", devices),
        ("reconnected_subscriptions", devices),
        ("read_messages", history * PROFILE["history_readers"]),
        ("committed_message_writes", sends),
        ("committed_outbox_writes", sends),
    ):
        require(type(raw.get(key)) is int and raw[key] == expected, f"incomplete measurement: {key}")

    expected_counts = {"login": devices, "connect": devices, "send": sends,
                       "sync_page": expected_pages, "reconnect": devices}
    sample_groups = raw.get("samples", {})
    require(isinstance(sample_groups, dict) and set(sample_groups) == set(expected_counts),
            "missing or unexpected operation population")
    operations = {
        name: stage(sample_groups[name], count, sequential=name in ("login", "send", "sync_page"))
        for name, count in expected_counts.items()
    }
    read_window = operations["sync_page"]["window_ns"]
    write_window = operations["send"]["window_ns"]
    return {
        "schema_version": 1,
        "source_log_sha256": log_digest,
        "context": context,
        "measurements": raw,
        "operations": operations,
        "read_messages_per_second": rate(raw["read_messages"], read_window),
        "committed_message_writes_per_second": rate(raw["committed_message_writes"], write_window),
        "committed_outbox_writes_per_second": rate(raw["committed_outbox_writes"], write_window),
    }


def self_test():
    require(stage({"window_ns": 300, "latencies_ns": [5, 10, 100]}, 3, True)["latency_ms"] ==
            {"p50": 0.00001, "p95": 0.0001, "p99": 0.0001}, "nearest-rank regression")
    require(rate(5, 2 * NANOSECONDS_PER_SECOND) == 2.5, "rate conversion regression")
    require(read_raw(b"=== RUN\nNEWIM_PERF_RAW {\"result\":1}\nPASS\n") == {"result": 1}, "record parse regression")

    bad = [
        (stage, ({"window_ns": 15, "latencies_ns": [5]}, 2)),
        (stage, ({"window_ns": 5, "latencies_ns": [6]}, 1)),
        (stage, ({"window_ns": 8, "latencies_ns": [5, 5]}, 2, True)),
        (stage, ({"window_ns": True, "latencies_ns": [1]}, 1)),
        (read_raw, (b"PASS\n",)),
        (read_raw, (b"NEWIM_PERF_RAW {}\nNEWIM_PERF_RAW {}\n",)),
        (build_report, ({}, {}, "")),
        (build_report, (PROFILE, {}, "")),
    ]
    sample = {**PROFILE, "active_subscriptions": 16, "reconnected_subscriptions": 16,
              "read_messages": 196, "committed_message_writes": 48, "committed_outbox_writes": 48,
              "go_version": "go-test", "server_version": "test", "postgres_version": "test",
              "runtime_platform": "linux/arm64", "runtime_cpus": 2, "go_max_procs": 2, "replacement_start_ns": 1,
              "postgres_max_connections": 40, "postgres_synchronous_commit": "on", "postgres_fsync": "on",
              "postgres_full_page_writes": "on"}
    sample["samples"] = {name: {"window_ns": 1000, "latencies_ns": [1] * count} for name, count in
                         (("login", 16), ("connect", 16), ("send", 48), ("sync_page", 28), ("reconnect", 16))}
    digest = "a" * 64
    metadata = {
        "revision": "a" * 40, "tree": "b" * 40, "docker_version": "test",
        "host": {"os": "test", "arch": "arm64", "kernel": "test", "cpu_model": "test",
                 "logical_cpus": 2, "memory_bytes": 1},
        "container": {"network": "none", "nano_cpus": 2_000_000_000, "memory_bytes": 536_870_912, "pids": 128},
        "image": {"platform": "linux/arm64/v8", **{key: "sha256:" + digest for key in
                  ("actual_image_id", "official_manifest", "official_config")}},
        "binary_sha256": dict.fromkeys(("newim-server", "newim-gateway", "conversationsynchttp.test"), digest),
        "dependencies": dict.fromkeys(("github.com/coder/websocket", "github.com/jackc/pgx/v5"), "test"),
    }
    report = build_report(sample, metadata, digest)
    require(report["operations"]["send"]["count"] == 48 and
            report["read_messages_per_second"] == rate(196, 1000), "complete report regression")
    bad.extend([
        (build_report, ({**sample, "read_messages": 195}, metadata, digest)),
        (build_report, (sample, {**metadata, "revision": "a" * 41}, digest)),
        (build_report, (sample, {**metadata, "container": {}}, digest)),
        (build_report, ({**sample, "committed_outbox_writes": 47}, metadata, digest)),
    ])
    for fn, args in bad:
        try:
            fn(*args)
        except ValueError:
            continue
        raise ValueError(f"{fn.__name__} accepted an incomplete or invalid measurement")
    print("performance report self-test passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--log", type=Path)
    parser.add_argument("--context", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not (args.log and args.context and args.output):
        parser.error("the measured log, captured context and output path are required")
    try:
        log = args.log.read_bytes()
        report = build_report(read_raw(log), json.loads(args.context.read_text()), hashlib.sha256(log).hexdigest())
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    except (ValueError, OSError, TypeError) as err:
        parser.exit(1, f"performance report rejected: {err}\n")
    for name, values in report["operations"].items():
        latency = values["latency_ms"]
        print(f"{name}: n={values['count']} rate={values['operations_per_second']:.2f}/s "
              f"p50={latency['p50']:.3f}ms p95={latency['p95']:.3f}ms p99={latency['p99']:.3f}ms")
    for field in ("read_messages_per_second", "committed_message_writes_per_second", "committed_outbox_writes_per_second"):
        print(f"{field}={report[field]:.2f}")
    print(f"performance report: {args.output}")


if __name__ == "__main__":
    main()
