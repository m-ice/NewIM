"""Build and exercise the C host against the native Rust static library."""

from pathlib import Path
import argparse
import os
import re
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]


def build_host(directory: Path) -> Path:
    cmd = ["python3", "sdk/storage/sqlite/engine.py", "run", "cargo", "rustc",
           "-p", "newim-sdk-native", "--lib", "--locked", "--offline", "--", "--print", "native-static-libs"]
    cargo_dir = ROOT / "target"
    env = {**os.environ, "CARGO_TARGET_DIR": str(cargo_dir)}
    result = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True, check=False)
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", flush=True)
    result.check_returncode()
    matches = re.findall(r"^note: native-static-libs: (.+)$", result.stderr, re.M)
    if len(matches) != 1:
        raise RuntimeError("missing or ambiguous native static library linker flags")
    binary = directory / "native-host"
    subprocess.run([
        "cc", "-std=c11", "-Wall", "-Wextra", "-Werror", "-I", "sdk/native/include",
        "sdk/native/tests/host.c", str(cargo_dir / "debug" / "libnewim_native.a"),
        *shlex.split(matches[0]), "-o", str(binary),
    ], cwd=ROOT, check=True)
    return binary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    if args.output_dir:
        path = args.output_dir.resolve()
        path.mkdir(parents=True, exist_ok=True)
        print(build_host(path), flush=True)
        return
    with tempfile.TemporaryDirectory(prefix="newim-native-abi-") as directory:
        directory = Path(directory).resolve()
        host = build_host(directory)
        store = directory / "account"
        store.mkdir(mode=0o700)
        subprocess.run([str(host), "--check", str(store), "native_test_account"], check=True, timeout=30)


if __name__ == "__main__":
    main()
