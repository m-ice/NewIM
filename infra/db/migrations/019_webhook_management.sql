CREATE TABLE newim.im_webhook_management_audit (
  audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor text COLLATE "C" NOT NULL DEFAULT 'deployment' CHECK (actor = 'deployment'),
  action text COLLATE "C" NOT NULL CHECK (action IN ('create', 'update', 'rotate', 'revoke', 'replay')),
  request_id newim.identifier NOT NULL,
  destination_id newim.identifier NOT NULL,
  revision bigint NOT NULL CHECK (revision > 0),
  recorded_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(recorded_at)),
  delivery_id newim.identifier REFERENCES newim.im_webhook_deliveries (delivery_id),
  previous_status text COLLATE "C",
  previous_attempts integer,
  previous_http_status integer CHECK (previous_http_status BETWEEN 100 AND 599),
  previous_error_code text COLLATE "C",
  previous_completed_at timestamptz,
  FOREIGN KEY (destination_id, revision)
    REFERENCES newim.im_webhook_endpoint_revisions (destination_id, revision),
  CONSTRAINT im_webhook_management_audit_replay_shape CHECK (
    (action = 'replay' AND delivery_id IS NOT NULL
      AND previous_status IS NOT DISTINCT FROM 'dead_letter'
      AND previous_attempts IS NOT NULL AND previous_attempts BETWEEN 1 AND 8
      AND previous_error_code IS NOT NULL
      AND previous_error_code IN ('WEBHOOK_HTTP_PERMANENT', 'WEBHOOK_DELIVERY_DEAD_LETTER')
      AND previous_completed_at IS NOT NULL AND isfinite(previous_completed_at))
    OR (action <> 'replay' AND num_nonnulls(delivery_id, previous_status, previous_attempts,
      previous_http_status, previous_error_code, previous_completed_at) = 0)
  )
);

CREATE UNIQUE INDEX im_webhook_management_replay_request_idx
  ON newim.im_webhook_management_audit (request_id) WHERE action = 'replay';
CREATE UNIQUE INDEX im_webhook_management_replay_delivery_idx
  ON newim.im_webhook_management_audit (delivery_id) WHERE action = 'replay';
CREATE INDEX im_webhook_deliveries_management_idx
  ON newim.im_webhook_deliveries (destination_id, delivery_id);
CREATE INDEX im_webhook_endpoints_active_idx
  ON newim.im_webhook_endpoints (destination_id) WHERE status = 'active' AND revoked_at IS NULL;
CREATE INDEX im_webhook_revisions_key_idx
  ON newim.im_webhook_endpoint_revisions (destination_id, key_id);
