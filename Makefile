.PHONY: build check docs-check api-check api-recovery api-security toolchain protocol-golden protocol-unknown-fields protocol-unknown-type protocol-limits webhook-envelope webhook-envelope-security webhook-protocol webhook-recovery webhook-security webhook-redaction media-protocol media-db media-security media-authz media-check message-http-check message-http-security message-http-gate-self-test message-observability-check message-observability-metrics message-observability-security message-observability-recovery

export GOTOOLCHAIN := local

toolchain:
	@sh infra/build/verify-toolchain.sh

docs-check:
	python3 -B tests/docs/check_current_docs.py

api-check: toolchain
	go test -race -shuffle=on -count=1 ./server/api/... ./server/cmd/newim-server/...

api-recovery: toolchain
	@go test -list '^TestServerRecovery$$' ./server/api | grep -qx 'TestServerRecovery'
	@go test -list '^TestNewIMServerProcess$$' ./server/cmd/newim-server | grep -qx 'TestNewIMServerProcess'
	@go test -list '^TestNewIMServerSecondBindFailure$$' ./server/cmd/newim-server | grep -qx 'TestNewIMServerSecondBindFailure'
	go test -race -shuffle=on -count=1 -run '^(TestServerRecovery|TestNewIMServerProcess|TestNewIMServerSecondBindFailure)$$' ./server/api ./server/cmd/newim-server

api-security: toolchain
	@go test -list '^TestHTTPSecurity$$' ./server/api | grep -qx 'TestHTTPSecurity'
	@go test -list '^TestImportBoundary$$' ./server/api | grep -qx 'TestImportBoundary'
	go test -race -shuffle=on -count=1 -run '^(TestHTTPSecurity|TestImportBoundary)$$' ./server/api

build: toolchain
	mkdir -p build
	go build -trimpath -o build/ ./...
	python3 sdk/storage/sqlite/engine.py run cargo build --workspace --locked --offline
	cargo build -p newim-sdk-core -p newim-protocol --locked --offline --target wasm32-unknown-unknown

check: toolchain docs-check message-observability-check
	@set -eu; unformatted=$$(gofmt -l core server tests); if [ -n "$$unformatted" ]; then printf '%s\n' "$$unformatted"; exit 1; fi
	go vet ./...
	go test -race -shuffle=on -count=1 ./...
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

webhook-envelope: toolchain
	go test -race -shuffle=on -count=1 ./tests/compatibility/webhook -run '^(TestWebhookEnvelope|TestWebhookSigningAndVerification|TestWebhookKeyRotationUse)$$'

webhook-envelope-security: toolchain
	go test -race -shuffle=on -count=1 ./tests/compatibility/webhook -run '^(TestWebhookSecurity|TestWebhookStrictInputs|TestWebhookClockBoundaries)$$'

webhook-protocol: toolchain
	go test -race -shuffle=on -count=1 ./server/webhook -run '^(TestWorkerDeliversSignedRequest|TestWorkerNonceIsFreshPerAttempt|TestBuildEnvelopePayloadContract)$$'
	python3 -B infra/db/webhook_suite.py protocol

webhook-recovery: toolchain
	go test -race -shuffle=on -count=1 ./server/webhook -run '^(TestWorkerRunStopsOnCancellation|TestWorkerRetryBackoffBounds|TestWorkerStorageFailureIsReturned|TestWorkerDrainsAboveHighWater|TestWorkerRetriesAndDeadLetters|TestRetryOutcomeDeadLettersAtMax)$$'
	go test -race -shuffle=on -count=1 ./server/cmd/newim-server -run '^(TestWebhookRuntimeConfigFailsClosed|TestJoinRuntimeAndServerShutdownOrder)$$'
	python3 -B infra/db/webhook_suite.py recovery
	python3 -B infra/db/webhook_suite.py lifecycle

webhook-security: toolchain
	go test -race -shuffle=on -count=1 ./server/webhook -run '^(TestSecureClient.*|TestWorkerConfigRejectsLeaseShorterThanRequest|TestRetryAfterParsing)$$'
	python3 -B infra/db/webhook_suite.py security

webhook-redaction: toolchain
	go test -race -shuffle=on -count=1 ./server/webhook -run '^(TestWorkerRedactsDoerError|TestLogObserverRedactsSensitiveFields)$$'
	python3 -B infra/db/webhook_suite.py redaction

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

.PHONY: media-http-check media-http-security media-http-recovery
media-http-check: toolchain
	python3 -B infra/db/media_http_suite.py check

media-http-security: toolchain
	go test -race -shuffle=on -count=1 ./server/credentialfile ./server/media/...
	python3 -B infra/db/media_http_suite.py security

media-http-recovery: toolchain
	python3 -B infra/db/media_http_suite.py recovery

.PHONY: media-s3-check media-s3-security media-s3-recovery
media-s3-check: toolchain
	python3 -B infra/db/media_s3_suite.py check

media-s3-security: toolchain
	go test -race -shuffle=on -count=1 ./server/media/... ./server/cmd/newim-gateway
	python3 -B infra/db/media_s3_suite.py security

media-s3-recovery: toolchain
	python3 -B infra/db/media_s3_suite.py recovery


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
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,pending_state_change_invalidates_stale_enqueue_replay)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact pending_state_change_invalidates_stale_enqueue_replay
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,retry_wait_stale_enqueue_replay_preserves_deadline)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact retry_wait_stale_enqueue_replay_preserves_deadline
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,removed_terminal_pending_cannot_be_recreated_by_stale_enqueue_replay)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact removed_terminal_pending_cannot_be_recreated_by_stale_enqueue_replay
	$(call require-outbox-test,python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --list,unrelated_trim_replay_keeps_its_original_receipt_after_pending_mutation)
	python3 sdk/storage/sqlite/engine.py run cargo test -p newim-store-sqlite --locked --offline --test outbox_flow -- --exact unrelated_trim_replay_keeps_its_original_receipt_after_pending_mutation
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

.PHONY: auth-check auth-recovery auth-policy auth-http-check auth-http-security auth-route-check auth-issuer-check auth-admission-check auth-logout-check auth-logout-security
auth-check: toolchain
	python3 -B infra/db/auth_suite.py check

auth-recovery: toolchain
	python3 -B infra/db/auth_suite.py recovery

auth-policy: toolchain
	python3 -B infra/db/auth_suite.py policy

auth-http-check: toolchain
	go test -race -shuffle=on -count=1 ./server/auth/bearerhttp
	python3 -B infra/db/auth_suite.py http

auth-http-security: toolchain
	python3 -B server/auth/bearerhttp/security_gate.py --self-test
	python3 -B server/auth/bearerhttp/security_gate.py

auth-route-check: toolchain
	python3 -B server/auth/bearerhttp/security_gate.py --self-test
	python3 -B server/auth/bearerhttp/security_gate.py
	python3 -B server/cmd/newim-server/route_gate.py --self-test
	python3 -B server/cmd/newim-server/route_gate.py
	python3 -B infra/db/auth_suite.py route

auth-issuer-check: toolchain
	python3 -B infra/db/auth_suite.py issuer

auth-admission-check: toolchain
	python3 -B infra/db/auth_suite.py admission

.PHONY: auth-account-revoke-check auth-account-revoke-security auth-account-revoke-recovery
auth-account-revoke-check: toolchain
	python3 -B infra/db/account_revocation_suite.py check

auth-account-revoke-security: toolchain
	python3 -B infra/db/account_revocation_suite.py security

auth-account-revoke-recovery: toolchain
	python3 -B infra/db/account_revocation_suite.py recovery

auth-logout-check: toolchain
	python3 -B infra/db/auth_suite.py --self-test
	python3 -B infra/db/auth_suite.py logout

auth-logout-security: toolchain
	python3 -B server/auth/bearerhttp/security_gate.py --self-test
	python3 -B server/auth/bearerhttp/security_gate.py --suite logout

.PHONY: gateway-check gateway-security gateway-recovery
gateway-check: toolchain
	go test -race -shuffle=on -count=1 ./server/gateway ./server/cmd/newim-gateway
	python3 -B infra/db/gateway_suite.py check

gateway-security: toolchain
	python3 -B infra/db/gateway_suite.py security

gateway-recovery: toolchain
	python3 -B infra/db/gateway_suite.py recovery

.PHONY: user-check user-security
user-check: toolchain
	python3 -B infra/db/user_suite.py check

user-security: toolchain
	python3 -B infra/db/user_suite.py security

.PHONY: friend-check friend-security friend-recovery
friend-check: toolchain
	python3 -B infra/db/friend_suite.py check

friend-security: toolchain
	python3 -B infra/db/friend_suite.py security

friend-recovery: toolchain
	python3 -B infra/db/friend_suite.py recovery

.PHONY: tls-message-check tls-message-security tls-message-recovery
tls-message-check: toolchain
	python3 -B infra/db/tls_message_suite.py check

tls-message-security: toolchain
	python3 -B infra/db/tls_message_suite.py security

tls-message-recovery: toolchain
	python3 -B infra/db/tls_message_suite.py recovery

.PHONY: group-check group-security group-recovery
group-check: toolchain
	python3 -B infra/db/group_suite.py check

group-security: toolchain
	python3 -B infra/db/group_suite.py security

group-recovery: toolchain
	python3 -B infra/db/group_suite.py recovery

.PHONY: sync-source-check sync-source-security sync-source-recovery
sync-source-check: toolchain
	python3 -B infra/db/sync_source_suite.py check

sync-source-security: toolchain
	python3 -B infra/db/sync_source_suite.py security

sync-source-recovery: toolchain
	python3 -B infra/db/sync_source_suite.py recovery

.PHONY: sync-http-check sync-http-security sync-http-recovery
sync-http-check: toolchain
	python3 -B infra/db/sync_http_suite.py check

sync-http-security: toolchain
	go test -race -shuffle=on -count=1 ./server/credentialfile ./server/auth/login ./server/sync/conversationhttp
	python3 -B infra/db/sync_http_suite.py security

sync-http-recovery: toolchain
	python3 -B infra/db/sync_http_suite.py recovery

.PHONY: read-watermark-check read-watermark-security read-watermark-recovery
read-watermark-check: toolchain
	python3 -B infra/db/sync_http_suite.py read-watermark-check

read-watermark-security: toolchain
	go test -race -shuffle=on -count=1 ./server/read ./server/readhttp ./server/storage/read ./server/storage/authsession
	python3 -B infra/db/sync_http_suite.py read-watermark-security

read-watermark-recovery: toolchain
	python3 -B infra/db/sync_http_suite.py read-watermark-recovery

.PHONY: message-delta-http-check message-delta-http-security message-delta-http-recovery
message-delta-http-check: toolchain
	python3 -B infra/db/message_delta_http_suite.py check

message-delta-http-security: toolchain
	go test -race -shuffle=on -count=1 ./server/sync/conversationhttp
	python3 -B infra/db/message_delta_http_suite.py security

message-delta-http-recovery: toolchain
	python3 -B infra/db/message_delta_http_suite.py recovery

.PHONY: message-check message-recovery message-errors
message-check: toolchain
	python3 -B infra/db/message_suite.py check

message-recovery: toolchain
	python3 -B infra/db/message_suite.py recovery

message-errors: toolchain
	python3 -B infra/db/message_suite.py errors

message-http-check: toolchain
	python3 -B infra/db/message_http_suite.py check

.PHONY: private-message-policy-check
private-message-policy-check: toolchain
	python3 -B infra/db/message_http_suite.py private-policy

message-http-security: toolchain
	python3 -B server/messagehttp/security_gate.py --self-test
	python3 -B server/messagehttp/security_gate.py

message-http-gate-self-test: toolchain
	python3 -B infra/db/message_http_suite.py --self-test
	python3 -B server/messagehttp/security_gate.py --self-test

message-observability-check: toolchain
	go test -race -shuffle=on -count=1 ./server/api ./server/message ./server/messagehttp ./server/webhook

message-observability-metrics: toolchain
	go test -race -shuffle=on -count=1 -run '^(TestMetricsRegistry|TestObserverDispatcher|TestSlow|TestSourceTimeout|TestBoundedSink|TestAttachedSource|TestMessageLifecycle|TestMessageReplay|TestTraceID|TestPanickingObserver|TestBlockedObserver|TestOperationAndCode|TestPanickingWebhook|TestBlockedWebhook|TestWebhookExhausted)' ./server/api ./server/message ./server/webhook

message-observability-security: toolchain
	go test -race -shuffle=on -count=1 ./server/api ./server/message ./server/webhook
	python3 -B server/messagehttp/security_gate.py --self-test
	python3 -B server/messagehttp/security_gate.py

message-observability-recovery: toolchain
	go test -race -shuffle=on -count=1 ./server/api ./server/message ./server/storage/message ./server/storage/webhook ./server/webhook
	python3 -B infra/db/message_suite.py observability
	python3 -B infra/db/webhook_suite.py observability


.PHONY: message-delta-authz message-delta message-delta-query-plan message-delta-recovery message-delta-redaction message-delta-check
message-delta-authz:
	python3 -B infra/db/message_delta_suite.py authz

message-delta:
	python3 -B infra/db/message_delta_suite.py delta

message-delta-query-plan:
	python3 -B infra/db/message_delta_suite.py query-plan

message-delta-recovery:
	python3 -B infra/db/message_delta_suite.py recovery

message-delta-redaction:
	python3 -B infra/db/message_delta_suite.py redaction

message-delta-check: toolchain message-delta-authz message-delta message-delta-query-plan message-delta-recovery message-delta-redaction
