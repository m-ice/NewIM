#!/bin/sh
set -eu

# 禁止 Go 自动切换版本。 / Prevent implicit Go toolchain selection.
export GOTOOLCHAIN=local
go_version=$(go env GOVERSION)
if [ "$go_version" != 'go1.27.1' ]; then
    printf '%s\n' 'NewIM requires Go 1.27.1.' >&2
    exit 1
fi
rust_version=$(rustc --version)
case "$rust_version" in
    'rustc 1.98.1 ('*) ;;
    *) printf '%s\n' 'NewIM requires Rust 1.98.1.' >&2; exit 1 ;;
esac
cargo_version=$(cargo --version)
case "$cargo_version" in
    'cargo 1.98.1 ('*) ;;
    *) printf '%s\n' 'NewIM requires Cargo 1.98.1.' >&2; exit 1 ;;
esac
go version
printf '%s\n' "$rust_version" "$cargo_version"
