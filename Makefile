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

.PHONY: send-protocol-golden send-protocol-errors send-protocol-limits

send-protocol-golden: toolchain
	go test -count=1 ./tests/compatibility/send-ack/golden
	cargo test -p newim-protocol --locked --test send_golden

send-protocol-errors: toolchain
	go test -count=1 ./tests/compatibility/send-ack/errors
	cargo test -p newim-protocol --locked --test send_errors

send-protocol-limits: toolchain
	go test -count=1 ./tests/compatibility/send-ack/limits
	cargo test -p newim-protocol --locked --test send_limits

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

.PHONY: sync-bootstrap sync-delta sync-cursor sync-query-plan sync-recovery sync-migrations sync-check
sync-bootstrap:
	python3 -B infra/db/sync_suite.py bootstrap

sync-delta:
	python3 -B infra/db/sync_suite.py delta

sync-cursor:
	python3 -B infra/db/sync_suite.py cursor

sync-query-plan:
	python3 -B infra/db/sync_suite.py query-plan

sync-recovery:
	python3 -B infra/db/sync_suite.py recovery

sync-migrations:
	python3 -B infra/db/sync_suite.py migrations

sync-check: sync-bootstrap sync-delta sync-cursor sync-query-plan sync-recovery sync-migrations
