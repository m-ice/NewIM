CREATE SCHEMA newim;
REVOKE ALL ON SCHEMA newim FROM PUBLIC;
CREATE DOMAIN newim.identifier AS text COLLATE "C"
  CHECK (VALUE ~ '^[A-Za-z0-9_-]{1,128}$');
CREATE DOMAIN newim.message_type AS text COLLATE "C"
  CHECK (VALUE ~ '^[a-z][a-z0-9_]{0,63}$');

CREATE TABLE newim.im_users (
  user_id newim.identifier PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE newim.im_devices (
  user_id newim.identifier NOT NULL REFERENCES newim.im_users,
  device_id newim.identifier NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (user_id, device_id)
);
CREATE TABLE newim.im_sessions (
  session_id newim.identifier PRIMARY KEY,
  user_id newim.identifier NOT NULL,
  device_id newim.identifier NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  revoked_at timestamptz,
  FOREIGN KEY (user_id, device_id) REFERENCES newim.im_devices (user_id, device_id)
);
CREATE INDEX im_sessions_user_idx ON newim.im_sessions (user_id, session_id);
CREATE TABLE newim.im_conversations (
  conversation_id newim.identifier PRIMARY KEY,
  last_seq bigint NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
  latest_server_msg_id newim.identifier,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE newim.im_conversation_members (
  conversation_id newim.identifier NOT NULL REFERENCES newim.im_conversations,
  user_id newim.identifier NOT NULL REFERENCES newim.im_users,
  PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX im_conversation_members_user_idx
  ON newim.im_conversation_members (user_id, conversation_id);
CREATE TABLE newim.im_messages (
  server_msg_id newim.identifier PRIMARY KEY,
  sender_id newim.identifier NOT NULL REFERENCES newim.im_users,
  client_msg_id newim.identifier NOT NULL,
  conversation_id newim.identifier NOT NULL REFERENCES newim.im_conversations,
  conversation_seq bigint NOT NULL CHECK (conversation_seq >= 0),
  protocol_version integer NOT NULL CHECK (protocol_version = 1),
  schema_version integer NOT NULL CHECK (schema_version > 0),
  message_type newim.message_type NOT NULL,
  server_time_ms bigint NOT NULL CHECK (server_time_ms >= 0),
  payload_bytes bytea NOT NULL CHECK (octet_length(payload_bytes) BETWEEN 2 AND 65536),
  CONSTRAINT im_messages_sender_client_key UNIQUE (sender_id, client_msg_id),
  CONSTRAINT im_messages_conversation_seq_key UNIQUE (conversation_id, conversation_seq),
  CONSTRAINT im_messages_conversation_server_key UNIQUE (conversation_id, server_msg_id)
);
ALTER TABLE newim.im_conversations ADD CONSTRAINT im_conversations_latest_fk
  FOREIGN KEY (conversation_id, latest_server_msg_id)
  REFERENCES newim.im_messages (conversation_id, server_msg_id);
CREATE TABLE newim.im_read_states (
  conversation_id newim.identifier NOT NULL,
  user_id newim.identifier NOT NULL,
  read_seq bigint NOT NULL DEFAULT 0 CHECK (read_seq >= 0),
  PRIMARY KEY (conversation_id, user_id),
  FOREIGN KEY (conversation_id, user_id)
    REFERENCES newim.im_conversation_members (conversation_id, user_id)
);
CREATE TABLE newim.im_blocks (
  user_id newim.identifier NOT NULL REFERENCES newim.im_users,
  blocked_user_id newim.identifier NOT NULL REFERENCES newim.im_users,
  PRIMARY KEY (user_id, blocked_user_id),
  CHECK (user_id <> blocked_user_id)
);
COMMENT ON COLUMN newim.im_messages.payload_bytes IS
  'Validated raw payload JSON, including opaque numeric tokens; codec validation remains mandatory. 已校验原始负载，禁止 jsonb/浮点转换。';
COMMENT ON COLUMN newim.im_messages.server_time_ms IS
  'Wire millisecond integer in the full signed BIGINT nonnegative domain. 保留完整毫秒数值域，不转为 timestamp。';
