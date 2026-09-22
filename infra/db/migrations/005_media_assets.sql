-- Additive provider-neutral media metadata and digest-only upload grants.
-- 追加供应商无关的媒体元数据与仅摘要上传凭据；不保存对象或签名 URL。
ALTER TABLE newim.im_sessions
  ADD CONSTRAINT im_sessions_identity_key UNIQUE (user_id, device_id, session_id);
ALTER TABLE newim.im_auth_tokens
  ADD CONSTRAINT im_auth_tokens_token_session_key UNIQUE (token_id, session_id);

CREATE TABLE newim.im_media_assets (
  media_key newim.identifier PRIMARY KEY,
  owner_user_id newim.identifier NOT NULL,
  device_id newim.identifier NOT NULL,
  session_id newim.identifier NOT NULL,
  connection_id newim.identifier NOT NULL,
  token_id text COLLATE "C" NOT NULL
    CHECK (token_id ~ '^[0-9a-f]{32}$'),
  conversation_id newim.identifier NOT NULL,
  media_kind text COLLATE "C" NOT NULL
    CHECK (media_kind IN ('image', 'video', 'audio', 'file')),
  content_type text COLLATE "C" NOT NULL
    CHECK (content_type ~ '^[a-z0-9][a-z0-9!#$&^_.+-]{0,62}/[a-z0-9][a-z0-9!#$&^_.+-]{0,63}$'),
  declared_size_bytes bigint NOT NULL
    CHECK (declared_size_bytes BETWEEN 1 AND 104857600),
  actual_size_bytes bigint
    CHECK (actual_size_bytes BETWEEN 1 AND 104857600),
  sha256 bytea NOT NULL
    CHECK (octet_length(sha256) = 32),
  upload_grant_id text COLLATE "C" NOT NULL
    CHECK (upload_grant_id ~ '^[0-9a-f]{32}$'),
  upload_grant_digest bytea NOT NULL
    CHECK (octet_length(upload_grant_digest) = 32),
  upload_expires_at timestamptz NOT NULL,
  completed_at timestamptz,
  state text COLLATE "C" NOT NULL
    CHECK (state IN ('pending', 'ready')),
  CONSTRAINT im_media_assets_grant_id_key UNIQUE (upload_grant_id),
  CONSTRAINT im_media_assets_grant_digest_key UNIQUE (upload_grant_digest),
  CONSTRAINT im_media_assets_ready_shape_check CHECK (
    (state = 'pending' AND actual_size_bytes IS NULL AND completed_at IS NULL)
    OR (state = 'ready' AND actual_size_bytes IS NOT NULL AND actual_size_bytes = declared_size_bytes AND completed_at IS NOT NULL)
  ),
  CONSTRAINT im_media_assets_device_fk
    FOREIGN KEY (owner_user_id, device_id)
    REFERENCES newim.im_devices (user_id, device_id),
  CONSTRAINT im_media_assets_session_identity_fk
    FOREIGN KEY (owner_user_id, device_id, session_id)
    REFERENCES newim.im_sessions (user_id, device_id, session_id),
  CONSTRAINT im_media_assets_token_session_fk
    FOREIGN KEY (token_id, session_id)
    REFERENCES newim.im_auth_tokens (token_id, session_id),
  CONSTRAINT im_media_assets_membership_fk
    FOREIGN KEY (conversation_id, owner_user_id)
    REFERENCES newim.im_conversation_members (conversation_id, user_id)
);

CREATE INDEX im_media_assets_ready_idx
  ON newim.im_media_assets (conversation_id, media_key) WHERE state = 'ready';
CREATE INDEX im_media_assets_expiry_idx
  ON newim.im_media_assets (upload_expires_at) WHERE state = 'pending';

COMMENT ON TABLE newim.im_media_assets IS
  'Media metadata and SHA-256 grant digest only; signed/object URLs are forbidden. 仅保存媒体元数据和凭据摘要，禁止签名/对象 URL。';
COMMENT ON COLUMN newim.im_media_assets.upload_grant_digest IS
  'SHA-256 digest of the complete raw grant; raw secret/logging is forbidden. 完整凭据的 SHA-256 摘要，禁止原始密钥与日志。';
COMMENT ON COLUMN newim.im_media_assets.connection_id IS
  'Trusted transport connection identity; no connection table exists in this migration set. 可信传输连接标识；当前迁移集无连接表。';
-- MEDIA_MIGRATION_FAULT_POINT
