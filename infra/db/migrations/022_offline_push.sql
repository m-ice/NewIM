CREATE TABLE newim.im_gateway_subscriptions (
  connection_id newim.identifier PRIMARY KEY,
  user_id newim.identifier NOT NULL,
  device_id newim.identifier NOT NULL,
  session_id newim.identifier NOT NULL,
  token_id text COLLATE "C" NOT NULL,
  conversation_id newim.identifier NOT NULL,
  refreshed_at timestamptz NOT NULL CHECK (isfinite(refreshed_at)),
  expires_at timestamptz NOT NULL CHECK (isfinite(expires_at) AND expires_at > refreshed_at),
  FOREIGN KEY (user_id, device_id, session_id) REFERENCES newim.im_sessions (user_id, device_id, session_id),
  FOREIGN KEY (token_id, session_id) REFERENCES newim.im_auth_tokens (token_id, session_id),
  FOREIGN KEY (conversation_id, user_id) REFERENCES newim.im_conversation_members (conversation_id, user_id)
);

CREATE INDEX im_gateway_subscriptions_target_idx
  ON newim.im_gateway_subscriptions (conversation_id, user_id, device_id, expires_at);
CREATE INDEX im_gateway_subscriptions_expiry_idx
  ON newim.im_gateway_subscriptions (expires_at, connection_id);

CREATE SEQUENCE newim.im_push_registration_version AS bigint;

ALTER TABLE newim.im_push_tokens
  ADD COLUMN session_id newim.identifier,
  ADD COLUMN registration_version bigint,
  ADD COLUMN key_id newim.identifier,
  ADD COLUMN token_nonce bytea,
  ADD COLUMN registered_at timestamptz CHECK (isfinite(registered_at)),
  ADD COLUMN invalidated_at timestamptz CHECK (isfinite(invalidated_at)),
  ADD CONSTRAINT im_push_tokens_session_fk
    FOREIGN KEY (user_id, device_id, session_id) REFERENCES newim.im_sessions (user_id, device_id, session_id),
  ADD CONSTRAINT im_push_tokens_registration_shape CHECK (
    num_nonnulls(session_id, registration_version, key_id, token_nonce, registered_at, invalidated_at) = 0
    OR (
      num_nonnulls(session_id, registration_version, key_id, token_nonce, registered_at) = 5
      AND provider IN ('apns', 'fcm')
      AND registration_version > 0
      AND octet_length(token_nonce) = 12 AND octet_length(token_ciphertext) > 16
      AND (invalidated_at IS NULL OR invalidated_at >= registered_at)
    )
  );

CREATE UNIQUE INDEX im_push_tokens_active_device_idx
  ON newim.im_push_tokens (user_id, device_id)
  WHERE registration_version IS NOT NULL AND invalidated_at IS NULL;

COMMENT ON TABLE newim.im_push_tokens IS
  'Legacy opaque rows stay readable. Only current versioned registrations are used for delivery. 旧密文保留，只有版本化注册参与通知。';

CREATE TABLE newim.im_push_state (
  singleton boolean PRIMARY KEY CHECK (singleton),
  enabled_from timestamptz NOT NULL CHECK (isfinite(enabled_from))
);

ALTER TABLE newim.im_outbox_events
  ADD COLUMN push_fanout_at timestamptz CHECK (isfinite(push_fanout_at));

CREATE INDEX im_outbox_push_pending_idx
  ON newim.im_outbox_events (created_at, event_id) WHERE push_fanout_at IS NULL;

CREATE TABLE newim.im_push_deliveries (
  event_id newim.identifier NOT NULL REFERENCES newim.im_outbox_events (event_id),
  user_id newim.identifier NOT NULL,
  device_id newim.identifier NOT NULL,
  session_id newim.identifier NOT NULL,
  provider newim.identifier NOT NULL CHECK (provider IN ('apns', 'fcm')),
  registration_version bigint NOT NULL CHECK (registration_version > 0),
  status text COLLATE "C" NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'leased', 'accepted', 'cancelled', 'failed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 8),
  available_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(available_at)),
  lease_id newim.identifier,
  lease_until timestamptz CHECK (isfinite(lease_until)),
  PRIMARY KEY (event_id, user_id, device_id),
  FOREIGN KEY (user_id, device_id, session_id) REFERENCES newim.im_sessions (user_id, device_id, session_id),
  CONSTRAINT im_push_delivery_lease_shape CHECK (
    (status = 'leased' AND attempts > 0 AND lease_id IS NOT NULL AND lease_until IS NOT NULL)
    OR (status <> 'leased' AND lease_id IS NULL AND lease_until IS NULL)
  )
);

CREATE INDEX im_push_deliveries_pending_idx
  ON newim.im_push_deliveries (available_at, event_id, user_id, device_id) WHERE status = 'pending';
CREATE INDEX im_push_deliveries_lease_idx
  ON newim.im_push_deliveries (lease_until, event_id, user_id, device_id) WHERE status = 'leased';
