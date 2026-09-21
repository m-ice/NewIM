.PHONY: build check toolchain protocol-golden protocol-unknown-fields protocol-unknown-type protocol-limits

export GOTOOLCHAIN := local

toolchain:
	@sh infra/build/verify-toolchain.sh

build: toolchain
	mkdir -p build
	go build -trimpath -o build/ ./...
	cargo build --workspace --locked
	cargo build --workspace --locked --target wasm32-unknown-unknown

check: toolchain
	@set -eu; unformatted=$$(gofmt -l core server tests); if [ -n "$$unformatted" ]; then printf '%s\n' "$$unformatted"; exit 1; fi
	go vet ./...
	go test -count=1 ./...
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked -- -D warnings
	cargo test --workspace --locked

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

.PHONY: db-prepare db-schema db-migrations db-sequence db-repair
db-prepare:
	python3 -B infra/db/test.py prepare

db-schema: toolchain
	python3 -B infra/db/test.py schema

db-migrations:
	python3 -B infra/db/test.py migrations

db-sequence:
	python3 -B infra/db/test.py sequence

db-repair:
	python3 -B infra/db/test.py repair
