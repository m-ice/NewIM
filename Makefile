.PHONY: build check toolchain

export GOTOOLCHAIN := local

toolchain:
	@sh infra/build/verify-toolchain.sh

build: toolchain
	mkdir -p build
	go build -trimpath -o build/ ./...
	cargo build --workspace --locked --offline
	cargo build --workspace --locked --offline --target wasm32-unknown-unknown

check: toolchain
	@set -eu; unformatted=$$(gofmt -l server); if [ -n "$$unformatted" ]; then printf '%s\n' "$$unformatted"; exit 1; fi
	go vet ./...
	go test -count=1 ./...
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked --offline -- -D warnings
	cargo test --workspace --locked --offline
