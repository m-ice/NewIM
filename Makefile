.PHONY: build check toolchain protocol-golden protocol-unknown-fields protocol-unknown-type protocol-limits media-protocol media-db media-security media-authz media-check

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

media-protocol: toolchain
	go test -count=1 ./tests/compatibility/media
	cargo test -p newim-protocol --locked --offline --test media_protocol
	cargo test -p newim-sdk-core --locked --offline --test media_flow

media-db: toolchain
	python3 -B infra/db/media_suite.py db

media-security: toolchain
	python3 -B infra/db/media_suite.py security

media-authz: toolchain
	python3 -B infra/db/media_suite.py authz

media-check: media-protocol media-db media-security media-authz


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

.PHONY: sdk-outbox-restart sdk-outbox-retry sdk-outbox-ack sdk-outbox-terminal sdk-outbox-check
define require-outbox-test
	@set -eu; list=$$($(1)); case "$$list" in *"$(2): test"*) ;; *) echo "missing outbox test: $(2)" >&2; exit 1;; esac
endef

sdk-outbox-restart: toolchain
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --list,outbox::conformance::restart_resumes_same_intent)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --exact outbox::conformance::restart_resumes_same_intent
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,restart_resumes_same_pending)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact restart_resumes_same_pending

sdk-outbox-retry: toolchain
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --list,outbox::conformance::retry_deadline_and_exhaustion)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --exact outbox::conformance::retry_deadline_and_exhaustion
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,retry_state_survives_reopen_and_exhausts)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact retry_state_survives_reopen_and_exhausts

sdk-outbox-ack: toolchain
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --list,outbox::conformance::ack_batch_is_exact_and_recoverable)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --exact outbox::conformance::ack_batch_is_exact_and_recoverable
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,ack_is_atomic_and_existing_is_reconciled)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact ack_is_atomic_and_existing_is_reconciled
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,commit_outcome_unknown_requires_authoritative_reload)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact commit_outcome_unknown_requires_authoritative_reload

sdk-outbox-terminal: toolchain
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --list,outbox::conformance::generation_terminal_and_explicit_removal)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib -- --exact outbox::conformance::generation_terminal_and_explicit_removal
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,rollback_preserves_pending_and_terminal_cas_removes_exactly)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact rollback_preserves_pending_and_terminal_cas_removes_exactly
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,pending_cas_is_exact_and_revision_checked)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact pending_cas_is_exact_and_revision_checked

sdk-outbox-check: sdk-outbox-restart sdk-outbox-retry sdk-outbox-ack sdk-outbox-terminal toolchain
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --test store_conformance
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-sdk-core --locked --offline --lib
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test idempotency
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --lib
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow

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

.PHONY: auth-check auth-recovery auth-policy
auth-check: toolchain
	python3 -B infra/db/auth_suite.py check

auth-recovery: toolchain
	python3 -B infra/db/auth_suite.py recovery

auth-policy: toolchain
	python3 -B infra/db/auth_suite.py policy

.PHONY: message-check message-recovery message-errors
message-check: toolchain
	python3 -B infra/db/message_suite.py check

message-recovery: toolchain
	python3 -B infra/db/message_suite.py recovery

message-errors: toolchain
	python3 -B infra/db/message_suite.py errors
