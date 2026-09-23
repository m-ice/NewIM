-- Additive webhook fan-out, endpoint revision and fenced delivery state.
-- 追加 Webhook fan-out、endpoint revision 与租约投递状态；不删除或重写历史投递。
ALTER TABLE newim.im_outbox_events
  ADD COLUMN webhook_fanout_at timestamptz;

-- Pre-006 events are cut over as already fanned out; new events remain pending.
-- 006 前事件按已完成 fan-out 截断；新事件保持 pending。
UPDATE newim.im_outbox_events
   SET webhook_fanout_at = clock_timestamp()
 WHERE webhook_fanout_at IS NULL;

CREATE INDEX im_outbox_events_webhook_pending_idx
  ON newim.im_outbox_events (created_at, event_id)
  WHERE webhook_fanout_at IS NULL;

CREATE TABLE newim.im_webhook_endpoints (
  destination_id newim.identifier PRIMARY KEY,
  status text COLLATE "C" NOT NULL
    CHECK (status IN ('active', 'revoked')),
  active_revision bigint NOT NULL CHECK (active_revision > 0),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  revoked_at timestamptz,
  CONSTRAINT im_webhook_endpoints_state_check CHECK (
    (status = 'active' AND revoked_at IS NULL)
    OR (status = 'revoked' AND revoked_at IS NOT NULL)
  ),
  CONSTRAINT im_webhook_endpoints_updated_check CHECK (updated_at >= created_at)
);

-- Revision rows are immutable by contract; rotation appends a new revision and
-- moves the endpoint pointer instead of updating URL, key or secret material.
-- revision 行按契约不可变；轮换追加新 revision，只移动 endpoint 指针。
CREATE TABLE newim.im_webhook_endpoint_revisions (
  destination_id newim.identifier NOT NULL
    REFERENCES newim.im_webhook_endpoints (destination_id),
  revision bigint NOT NULL CHECK (revision > 0),
  url text COLLATE "C" NOT NULL
    CHECK (char_length(url) BETWEEN 1 AND 2048 AND url !~ '[[:space:][:cntrl:]]'),
  key_id text COLLATE "C" NOT NULL
    CHECK (key_id ~ '^[A-Za-z0-9_-]{1,64}$'),
  secret_nonce bytea NOT NULL
    CHECK (octet_length(secret_nonce) = 12),
  secret_ciphertext bytea NOT NULL
    CHECK (octet_length(secret_ciphertext) BETWEEN 1 AND 8192),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (destination_id, revision)
);

ALTER TABLE newim.im_webhook_deliveries
  ADD COLUMN endpoint_revision bigint,
  ADD COLUMN status text COLLATE "C",
  ADD COLUMN next_attempt_at timestamptz,
  ADD COLUMN lease_owner text COLLATE "C",
  ADD COLUMN lease_token text COLLATE "C",
  ADD COLUMN lease_expires_at timestamptz,
  ADD COLUMN last_http_status integer,
  ADD COLUMN last_error_code text COLLATE "C",
  ADD COLUMN created_at timestamptz,
  ADD COLUMN updated_at timestamptz;

-- Preserve every pre-006 row as a non-claimable dead-letter legacy delivery.
-- 保留所有 006 前投递行，并标记为不可领取的 dead-letter legacy delivery。
UPDATE newim.im_webhook_deliveries
   SET status = 'dead_letter',
       created_at = clock_timestamp(),
       updated_at = clock_timestamp()
 WHERE status IS NULL;

ALTER TABLE newim.im_webhook_deliveries
  ALTER COLUMN status SET DEFAULT 'pending',
  ALTER COLUMN status SET NOT NULL,
  ALTER COLUMN next_attempt_at SET DEFAULT clock_timestamp(),
  ALTER COLUMN created_at SET DEFAULT clock_timestamp(),
  ALTER COLUMN created_at SET NOT NULL,
  ALTER COLUMN updated_at SET DEFAULT clock_timestamp(),
  ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE newim.im_webhook_deliveries
  ADD CONSTRAINT im_webhook_deliveries_endpoint_revision_fk
  FOREIGN KEY (destination_id, endpoint_revision)
  REFERENCES newim.im_webhook_endpoint_revisions (destination_id, revision),
  ADD CONSTRAINT im_webhook_deliveries_status_check CHECK (
    status IN ('pending', 'retry', 'leased', 'delivered', 'dead_letter', 'cancelled')
  ),
  ADD CONSTRAINT im_webhook_deliveries_revision_check CHECK (
    endpoint_revision IS NULL OR endpoint_revision > 0
  ),
  ADD CONSTRAINT im_webhook_deliveries_legacy_check CHECK (
    endpoint_revision IS NOT NULL OR status = 'dead_letter'
  ),
  ADD CONSTRAINT im_webhook_deliveries_state_check CHECK (
    (status IN ('pending', 'retry')
      AND next_attempt_at IS NOT NULL
      AND lease_owner IS NULL
      AND lease_token IS NULL
      AND lease_expires_at IS NULL
      AND completed_at IS NULL)
    OR (status = 'leased'
      AND lease_owner IS NOT NULL
      AND lease_token IS NOT NULL
      AND lease_expires_at IS NOT NULL
      AND completed_at IS NULL)
    OR (status IN ('delivered', 'dead_letter', 'cancelled')
      AND lease_owner IS NULL
      AND lease_token IS NULL
      AND lease_expires_at IS NULL)
  ),
  ADD CONSTRAINT im_webhook_deliveries_lease_owner_check CHECK (
    lease_owner IS NULL
    OR (char_length(lease_owner) BETWEEN 1 AND 128 AND lease_owner !~ '[[:cntrl:]]')
  ),
  ADD CONSTRAINT im_webhook_deliveries_lease_token_check CHECK (
    lease_token IS NULL
    OR (char_length(lease_token) BETWEEN 1 AND 128 AND lease_token !~ '[[:cntrl:]]')
  ),
  ADD CONSTRAINT im_webhook_deliveries_http_status_check CHECK (
    last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599
  ),
  ADD CONSTRAINT im_webhook_deliveries_error_code_check CHECK (
    last_error_code IS NULL OR last_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
  ),
  ADD CONSTRAINT im_webhook_deliveries_updated_check CHECK (updated_at >= created_at);

CREATE INDEX im_webhook_deliveries_claim_idx
  ON newim.im_webhook_deliveries (next_attempt_at, delivery_id)
  WHERE status IN ('pending', 'retry');
CREATE INDEX im_webhook_deliveries_lease_idx
  ON newim.im_webhook_deliveries (lease_expires_at, delivery_id)
  WHERE status = 'leased';
CREATE INDEX im_webhook_deliveries_destination_idx
  ON newim.im_webhook_deliveries (destination_id, status, next_attempt_at, delivery_id);

COMMENT ON COLUMN newim.im_outbox_events.webhook_fanout_at IS
  'Webhook-only fan-out completion marker; consumers must never update completed_at for webhook isolation. 仅 Webhook fan-out 使用，不得改写共享 completed_at。';
COMMENT ON TABLE newim.im_webhook_endpoints IS
  'Trusted internal endpoint state and current append-only revision pointer. 可信内部 endpoint 状态与当前追加式 revision 指针。';
COMMENT ON TABLE newim.im_webhook_endpoint_revisions IS
  'Append-only endpoint URL/key-id/encrypted-secret revisions; plaintext secrets and logging are forbidden. 追加式 URL/key-id/加密 secret revision；禁止明文密钥与日志输出。';
COMMENT ON COLUMN newim.im_webhook_endpoint_revisions.url IS
  'Validated canonical endpoint URL; operational logs and metrics must use a non-reversible short identifier. 已校验 canonical URL；运行日志与指标只能使用不可逆短标识。';
COMMENT ON COLUMN newim.im_webhook_deliveries.endpoint_revision IS
  'Frozen revision snapshot; NULL is permitted only for pre-006 dead-letter legacy rows. 冻结 revision 快照；仅 006 前 dead-letter legacy 行可为 NULL。';
COMMENT ON COLUMN newim.im_webhook_deliveries.lease_token IS
  'Fencing token required for attempt accounting and terminal completion. 尝试计数与终态完成必须校验的 fencing token。';
-- WEBHOOK_MIGRATION_FAULT_POINT
