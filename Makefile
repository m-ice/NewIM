.PHONY: build check toolchain protocol-golden protocol-unknown-fields protocol-unknown-type protocol-limits

export GOTOOLCHAIN := local

toolchain:
	@sh infra/build/verify-toolchain.sh

build: toolchain
	mkdir -p build
	go build -trimpath -o build/ ./...
	python3 sdk/storage/sqlite/engine.py run cargo build --workspace --locked --offline
	cargo build -p newim-sdk-core -p newim-protocol --locked --offline --target wasm32-unknown-unknown

check: toolchain
	@set -eu; unformatted=$$(gofmt -l core server tests); if [ -n "$$unformatted" ]; then printf '%s\n' "$$unformatted"; exit 1; fi
	go vet ./...
	go test -count=1 ./...
	cargo fmt --all -- --check
	python3 sdk/storage/sqlite/engine.py run cargo clippy --workspace --all-targets --locked --offline -- -D warnings
	python3 sdk/storage/sqlite/engine.py run cargo test --workspace --locked --offline

protocol-golden: toolchain
	go test -count=1 -run '^TestGolden$$' ./tests/compatibility
	cargo test -p newim-protocol --locked --test fixtures golden -- --exact

protocol-unknown-fields: toolchain
	go test -count=1 -run '^TestUnknownFields$$' ./tests/compatibility
	cargo test -p newim-protocol --locked --test fixtures unknown_fields -- --exact

protocol-unknown-type: toolchain
	go test -count=1 -run '^TestUnknownType$$' ./tests/compatibility
	cargo test -p newim-protocol --locked --test fixtures unknown_type -- --exact

protocol-limits: toolchain
	go test -count=1 -run '^TestLimits$$' ./tests/compatibility
	cargo test -p newim-protocol --locked --test fixtures limits -- --exact

.PHONY: store-prepare store-engine store-idempotency store-migrations store-maintenance store-recovery
store-prepare:
	python3 sdk/storage/sqlite/engine.py prepare

store-engine:
	python3 sdk/storage/sqlite/engine.py build

store-idempotency: toolchain
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test idempotency

store-migrations: toolchain
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test migrations

store-maintenance: toolchain
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test maintenance

store-recovery: toolchain
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test recovery
