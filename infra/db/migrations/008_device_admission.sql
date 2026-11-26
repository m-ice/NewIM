ALTER TABLE newim.im_devices
  ADD COLUMN platform text COLLATE "C" CHECK (platform ~ '^[a-z][a-z0-9_-]{0,31}$'),
  ADD COLUMN last_active_at timestamptz CHECK (isfinite(last_active_at));

ALTER TABLE newim.im_sessions
  ADD COLUMN admitted_at timestamptz CHECK (isfinite(admitted_at));

CREATE INDEX im_sessions_live_device_idx
  ON newim.im_sessions (user_id, device_id, session_id) WHERE revoked_at IS NULL;

CREATE TABLE newim.im_session_revocation_events (
  event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  user_id newim.identifier NOT NULL,
  device_id newim.identifier NOT NULL,
  session_id newim.identifier NOT NULL UNIQUE REFERENCES newim.im_sessions (session_id),
  revoked_at timestamptz NOT NULL CHECK (isfinite(revoked_at)),
  reason text COLLATE "C" NOT NULL CHECK (reason = 'device_limit'),
  FOREIGN KEY (user_id, device_id) REFERENCES newim.im_devices (user_id, device_id)
);

CREATE INDEX im_session_revocation_events_account_idx
  ON newim.im_session_revocation_events (user_id, event_id);
