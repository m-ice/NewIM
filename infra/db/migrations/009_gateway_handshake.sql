CREATE TABLE newim.im_gateway_handshakes (
  token_id text COLLATE "C" NOT NULL REFERENCES newim.im_auth_tokens(token_id),
  nonce_hash bytea NOT NULL CHECK (octet_length(nonce_hash) = 32),
  expires_at timestamptz NOT NULL CHECK (isfinite(expires_at)),
  PRIMARY KEY (token_id, nonce_hash)
);

CREATE INDEX im_gateway_handshakes_expiry_idx
  ON newim.im_gateway_handshakes (expires_at, token_id, nonce_hash);
