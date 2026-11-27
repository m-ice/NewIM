ALTER TABLE newim.im_conversation_members
  ADD COLUMN group_role text COLLATE "C" CHECK (group_role IN ('owner','admin','member')),
  ADD COLUMN mute_until timestamptz CHECK (isfinite(mute_until)),
  ADD COLUMN history_after_seq bigint NOT NULL DEFAULT 0 CHECK (history_after_seq >= 0),
  ADD COLUMN departed_at timestamptz CHECK (isfinite(departed_at)),
  ADD COLUMN departed_through_seq bigint,
  ADD CONSTRAINT im_conversation_members_departure_shape CHECK (
    (departed_at IS NULL AND departed_through_seq IS NULL)
    OR
    (departed_at IS NOT NULL AND departed_through_seq IS NOT NULL
      AND departed_through_seq >= history_after_seq)
  ),
  ADD CONSTRAINT im_conversation_members_legacy_shape CHECK (
    group_role IS NOT NULL OR (mute_until IS NULL AND history_after_seq = 0 AND departed_at IS NULL)
  );

CREATE INDEX im_conversation_members_active_group_idx
  ON newim.im_conversation_members (conversation_id, user_id)
  WHERE group_role IS NOT NULL AND departed_at IS NULL;

CREATE UNIQUE INDEX im_conversation_members_active_owner_idx
  ON newim.im_conversation_members (conversation_id)
  WHERE group_role = 'owner' AND departed_at IS NULL;

CREATE TABLE newim.im_groups (
  group_id newim.identifier PRIMARY KEY REFERENCES newim.im_conversations (conversation_id),
  name text NOT NULL CHECK (octet_length(name) > 0),
  face_url text NOT NULL DEFAULT '',
  description text NOT NULL DEFAULT '',
  notification text NOT NULL DEFAULT '',
  ex text NOT NULL DEFAULT '',
  join_mode smallint NOT NULL DEFAULT 0 CHECK (join_mode BETWEEN 0 AND 2),
  member_limit integer NOT NULL DEFAULT 1000 CHECK (member_limit BETWEEN 1 AND 1000),
  members_can_invite boolean NOT NULL DEFAULT true,
  history_enabled boolean NOT NULL DEFAULT true,
  muted boolean NOT NULL DEFAULT false,
  dissolved_at timestamptz CHECK (isfinite(dissolved_at))
);

CREATE TABLE newim.im_group_requests (
  group_id newim.identifier NOT NULL REFERENCES newim.im_groups (group_id),
  actor_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  request_id newim.identifier NOT NULL,
  user_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  kind text COLLATE "C" NOT NULL CHECK (kind IN ('join','invite')),
  state text COLLATE "C" NOT NULL CHECK (state IN ('pending','accepted','rejected')),
  message text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(created_at)),
  responded_at timestamptz CHECK (isfinite(responded_at)),
  responded_by newim.identifier REFERENCES newim.im_users (user_id),
  PRIMARY KEY (group_id, actor_id, request_id),
  CHECK ((kind = 'join' AND actor_id = user_id) OR (kind = 'invite' AND actor_id <> user_id)),
  CHECK (
    (state = 'pending' AND responded_at IS NULL AND responded_by IS NULL)
    OR
    (state IN ('accepted','rejected') AND responded_at IS NOT NULL AND responded_by IS NOT NULL)
  )
);

CREATE UNIQUE INDEX im_group_requests_pending_idx
  ON newim.im_group_requests (group_id, user_id) WHERE state = 'pending';

CREATE TABLE newim.im_group_sync_accounts (
  owner_id newim.identifier PRIMARY KEY REFERENCES newim.im_users (user_id),
  latest_revision bigint NOT NULL DEFAULT 0 CHECK (latest_revision >= 0)
);

CREATE TABLE newim.im_group_sync_changes (
  owner_id newim.identifier NOT NULL REFERENCES newim.im_group_sync_accounts (owner_id),
  revision bigint NOT NULL CHECK (revision > 0),
  group_id newim.identifier NOT NULL REFERENCES newim.im_groups (group_id),
  kind text COLLATE "C" NOT NULL CHECK (kind IN ('group_changed','group_removed','request_changed')),
  payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
  PRIMARY KEY (owner_id, revision)
);
