CREATE TABLE newim.im_friend_edges (
  owner_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  peer_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  remark text NOT NULL DEFAULT '' CHECK (octet_length(remark) <= 512),
  PRIMARY KEY (owner_id, peer_id),
  CHECK (owner_id <> peer_id)
);

CREATE TABLE newim.im_friend_applications (
  applicant_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  request_id newim.identifier NOT NULL,
  recipient_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  message text NOT NULL CHECK (octet_length(message) <= 512),
  state text COLLATE "C" NOT NULL CHECK (state IN ('pending','accepted','rejected')),
  PRIMARY KEY (applicant_id, request_id),
  CHECK (applicant_id <> recipient_id)
);

CREATE INDEX im_friend_applications_incoming_idx
  ON newim.im_friend_applications (recipient_id, applicant_id, request_id);

CREATE UNIQUE INDEX im_friend_applications_pending_idx
  ON newim.im_friend_applications (applicant_id, recipient_id) WHERE state = 'pending';

CREATE TABLE newim.im_friend_sync_accounts (
  owner_id newim.identifier PRIMARY KEY REFERENCES newim.im_users (user_id),
  latest_revision bigint NOT NULL DEFAULT 0 CHECK (latest_revision >= 0)
);

CREATE TABLE newim.im_friend_sync_changes (
  owner_id newim.identifier NOT NULL REFERENCES newim.im_friend_sync_accounts (owner_id),
  revision bigint NOT NULL CHECK (revision > 0),
  kind text COLLATE "C" NOT NULL,
  peer_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  payload jsonb,
  PRIMARY KEY (owner_id, revision),
  CHECK (owner_id <> peer_id),
  CHECK (
    (kind = 'friend_removed' AND payload IS NULL) OR
    (kind = 'friend_upserted' AND (payload #>> '{profile,userId}' = peer_id) IS TRUE) OR
    (kind = 'application_changed' AND (
      (payload ->> 'applicantId' = owner_id AND payload ->> 'recipientId' = peer_id) OR
      (payload ->> 'applicantId' = peer_id AND payload ->> 'recipientId' = owner_id)
    ) IS TRUE)
  )
);
