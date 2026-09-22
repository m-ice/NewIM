-- Additive opaque-token storage; raw bearer material is never persisted.
-- 追加不透明令牌存储；原始承载令牌绝不落库。
CREATE TABLE newim.im_auth_tokens (
  token_id text COLLATE "C" PRIMARY KEY
    CHECK (token_id ~ '^[0-9a-f]{32}$'),
  token_digest bytea NOT NULL
    CHECK (octet_length(token_digest) = 32),
  session_id newim.identifier NOT NULL REFERENCES newim.im_sessions (session_id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  expires_at timestamptz NOT NULL,
  revoked_at timestamptz,
  CONSTRAINT im_auth_tokens_digest_key UNIQUE (token_digest),
  CONSTRAINT im_auth_tokens_expiry_check CHECK (expires_at > created_at),
  CONSTRAINT im_auth_tokens_revocation_check CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);
CREATE INDEX im_auth_tokens_session_idx ON newim.im_auth_tokens (session_id, token_id);
COMMENT ON TABLE newim.im_auth_tokens IS
  'SHA-256 token digest only; raw token/secret/digest logging is forbidden. 仅存 SHA-256 摘要，禁止记录原始令牌/密钥/摘要。';
-- AUTH_MIGRATION_FAULT_POINT
