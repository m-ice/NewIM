-- Account existence means initialized projection, not automatic backfill.
-- 账户存在表示投影已初始化；迁移不为既有账户伪造空投影。
CREATE TABLE newim.im_conversation_sync_accounts (
  user_id newim.identifier PRIMARY KEY REFERENCES newim.im_users,
  epoch bigint NOT NULL DEFAULT 1 CHECK (epoch > 0),
  last_change_seq bigint NOT NULL DEFAULT 0 CHECK (last_change_seq >= 0),
  min_valid_seq bigint NOT NULL DEFAULT 0 CHECK (min_valid_seq >= 0),
  CHECK (min_valid_seq <= last_change_seq)
);
CREATE TABLE newim.im_conversation_sync_keys (
  user_id newim.identifier NOT NULL REFERENCES newim.im_conversation_sync_accounts,
  conversation_id newim.identifier NOT NULL REFERENCES newim.im_conversations,
  first_change_seq bigint NOT NULL CHECK (first_change_seq > 0),
  PRIMARY KEY (user_id, conversation_id)
);
CREATE TABLE newim.im_conversation_sync_changes (
  user_id newim.identifier NOT NULL,
  change_seq bigint NOT NULL CHECK (change_seq > 0),
  conversation_id newim.identifier NOT NULL,
  kind text NOT NULL CHECK (kind IN ('upsert', 'remove')),
  latest_seq bigint,
  latest_server_msg_id newim.identifier,
  PRIMARY KEY (user_id, change_seq),
  FOREIGN KEY (user_id, conversation_id)
    REFERENCES newim.im_conversation_sync_keys (user_id, conversation_id),
  FOREIGN KEY (conversation_id, latest_server_msg_id)
    REFERENCES newim.im_messages (conversation_id, server_msg_id),
  CHECK ((kind = 'upsert' AND latest_seq IS NOT NULL AND latest_seq >= 0)
      OR (kind = 'remove' AND latest_seq IS NULL AND latest_server_msg_id IS NULL))
);
CREATE INDEX im_conversation_sync_changes_version_idx
  ON newim.im_conversation_sync_changes (user_id, conversation_id, change_seq DESC);
-- SYNC_MIGRATION_FAULT_POINT
