CREATE TABLE newim.im_outbox_events (
  event_id newim.identifier PRIMARY KEY,
  server_msg_id newim.identifier NOT NULL UNIQUE REFERENCES newim.im_messages,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  completed_at timestamptz
);
CREATE INDEX im_outbox_events_pending_idx
  ON newim.im_outbox_events (available_at, event_id) WHERE completed_at IS NULL;
CREATE TABLE newim.im_webhook_deliveries (
  delivery_id newim.identifier PRIMARY KEY,
  event_id newim.identifier NOT NULL REFERENCES newim.im_outbox_events,
  destination_id newim.identifier NOT NULL,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  completed_at timestamptz,
  UNIQUE (event_id, destination_id)
);
CREATE TABLE newim.im_push_tokens (
  user_id newim.identifier NOT NULL,
  device_id newim.identifier NOT NULL,
  provider newim.identifier NOT NULL,
  token_ciphertext bytea NOT NULL CHECK (octet_length(token_ciphertext) BETWEEN 1 AND 8192),
  PRIMARY KEY (user_id, device_id, provider),
  FOREIGN KEY (user_id, device_id) REFERENCES newim.im_devices (user_id, device_id)
);
COMMENT ON TABLE newim.im_push_tokens IS
  'Opaque encrypted token supplied by a separately reviewed platform service; no encryption implementation here. 仅保存外部加密结果。';
COMMENT ON TABLE newim.im_outbox_events IS
  'Durable identity/reference only; dispatcher and event wire policy are separate. 不声明恰好一次投递。';

CREATE FUNCTION newim.persist_message(
  p_sender newim.identifier, p_client newim.identifier,
  p_conversation newim.identifier, p_server newim.identifier,
  p_type newim.message_type, p_schema integer, p_time bigint,
  p_payload bytea, p_event newim.identifier
) RETURNS newim.im_messages
LANGUAGE plpgsql SECURITY INVOKER SET search_path = pg_catalog, newim AS $$
DECLARE
  stored newim.im_messages;
  allocated bigint;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION USING ERRCODE = 'NI003', MESSAGE = 'STORAGE_ISOLATION_UNSUPPORTED';
  END IF;
  SELECT * INTO stored FROM newim.im_messages
    WHERE sender_id = p_sender AND client_msg_id = p_client;
  IF FOUND THEN RETURN stored; END IF;
  -- All writes are inside this subtransaction: a duplicate race rolls them back.
  -- 写入位于同一子事务；并发幂等冲突不会保留已增加的序列。
  BEGIN
    PERFORM 1 FROM newim.im_conversations WHERE conversation_id = p_conversation FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE = 'NI001', MESSAGE = 'STORAGE_CONVERSATION_MISSING';
    END IF;
    SELECT * INTO stored FROM newim.im_messages
      WHERE sender_id = p_sender AND client_msg_id = p_client;
    IF FOUND THEN RETURN stored; END IF;
    UPDATE newim.im_conversations SET last_seq = last_seq + 1
      WHERE conversation_id = p_conversation AND last_seq < 9223372036854775807
      RETURNING last_seq INTO allocated;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE = 'NI002', MESSAGE = 'STORAGE_SEQUENCE_EXHAUSTED';
    END IF;
    INSERT INTO newim.im_messages
      (server_msg_id, sender_id, client_msg_id, conversation_id, conversation_seq,
       protocol_version, schema_version, message_type, server_time_ms, payload_bytes)
    VALUES (p_server, p_sender, p_client, p_conversation, allocated, 1, p_schema, p_type, p_time, p_payload)
    RETURNING * INTO stored;
    UPDATE newim.im_conversations SET latest_server_msg_id = stored.server_msg_id
      WHERE conversation_id = p_conversation;
    INSERT INTO newim.im_outbox_events (event_id, server_msg_id) VALUES (p_event, stored.server_msg_id);
  EXCEPTION WHEN unique_violation THEN
    -- The same retry can conflict with both unique keys; PostgreSQL may report either.
    -- 同一重试可同时命中主键和幂等键；以提交后的身份查询判断，不能依赖约束报错顺序。
    SELECT * INTO stored FROM newim.im_messages
      WHERE sender_id = p_sender AND client_msg_id = p_client;
    IF NOT FOUND THEN RAISE; END IF;
  END;
  RETURN stored;
END;
$$;
REVOKE ALL ON FUNCTION newim.persist_message(newim.identifier,newim.identifier,newim.identifier,newim.identifier,newim.message_type,integer,bigint,bytea,newim.identifier) FROM PUBLIC;
COMMENT ON FUNCTION newim.persist_message(newim.identifier,newim.identifier,newim.identifier,newim.identifier,newim.message_type,integer,bigint,bytea,newim.identifier) IS
  'Caller must authorize and codec-validate; outer COMMIT defines persistence. 调用方负责权限与协议校验，返回行不等于外层事务已提交。';
