ALTER TABLE newim.im_push_tokens
  DROP CONSTRAINT im_push_tokens_registration_shape,
  ADD CONSTRAINT im_push_tokens_registration_shape CHECK (
    num_nonnulls(session_id, registration_version, key_id, token_nonce, registered_at, invalidated_at) = 0
    OR (
      num_nonnulls(session_id, registration_version, key_id, token_nonce, registered_at) = 5
      AND provider IN ('apns', 'fcm', 'pushkit')
      AND registration_version > 0
      AND octet_length(token_nonce) = 12 AND octet_length(token_ciphertext) > 16
      AND (invalidated_at IS NULL OR invalidated_at >= registered_at)
    )
  );

ALTER TABLE newim.im_push_deliveries
  DROP CONSTRAINT im_push_deliveries_provider_check,
  ADD CONSTRAINT im_push_deliveries_provider_check CHECK (
    provider IN ('apns', 'fcm', 'pushkit')
  );
