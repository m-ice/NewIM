ALTER TABLE newim.im_session_revocation_events
  DROP CONSTRAINT im_session_revocation_events_reason_check,
  ADD CONSTRAINT im_session_revocation_events_reason_check
    CHECK (reason IN ('device_limit','password_changed','signed_out_all','account_blocked'));

CREATE TABLE newim.im_account_revocation_requests (
  user_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  request_id newim.identifier NOT NULL,
  reason text COLLATE "C" NOT NULL CHECK (reason IN ('password_changed','signed_out_all','account_blocked')),
  revoked_sessions bigint NOT NULL CHECK (revoked_sessions >= 0),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(recorded_at)),
  PRIMARY KEY (user_id, request_id)
);
