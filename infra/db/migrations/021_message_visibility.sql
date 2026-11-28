CREATE TABLE newim.im_message_visibility_events (
  conversation_id newim.identifier NOT NULL,
  visibility_revision bigint NOT NULL CHECK (visibility_revision > 0),
  target_seq bigint NOT NULL CHECK (target_seq > 0),
  actor_user_id newim.identifier NOT NULL REFERENCES newim.im_users (user_id),
  request_id newim.identifier NOT NULL,
  action text COLLATE "C" NOT NULL,
  audience_user_id newim.identifier REFERENCES newim.im_users (user_id),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (conversation_id, visibility_revision),
  UNIQUE (actor_user_id, request_id),
  FOREIGN KEY (conversation_id, target_seq)
    REFERENCES newim.im_messages (conversation_id, conversation_seq),
  CONSTRAINT im_message_visibility_audience_shape CHECK (
    (action = 'delete_for_me' AND audience_user_id IS NOT NULL AND audience_user_id = actor_user_id)
    OR (action IN ('recall', 'admin_remove') AND audience_user_id IS NULL)
  )
);

CREATE UNIQUE INDEX im_message_visibility_global_target_idx
  ON newim.im_message_visibility_events (conversation_id, target_seq)
  WHERE audience_user_id IS NULL;
CREATE UNIQUE INDEX im_message_visibility_personal_target_idx
  ON newim.im_message_visibility_events (conversation_id, audience_user_id, target_seq)
  WHERE audience_user_id IS NOT NULL;
CREATE INDEX im_message_visibility_global_revision_idx
  ON newim.im_message_visibility_events (conversation_id, visibility_revision)
  WHERE audience_user_id IS NULL;
CREATE INDEX im_message_visibility_personal_revision_idx
  ON newim.im_message_visibility_events (conversation_id, audience_user_id, visibility_revision)
  WHERE audience_user_id IS NOT NULL;

ALTER TABLE newim.im_conversation_sync_changes
  ADD COLUMN unread_count bigint,
  ADD COLUMN visibility_revision bigint,
  ADD CONSTRAINT im_conversation_sync_changes_visibility_shape CHECK (
    num_nonnulls(unread_count, visibility_revision) = 0
    OR (
      kind = 'upsert' AND num_nonnulls(unread_count, visibility_revision) = 2
      AND unread_count BETWEEN 0 AND latest_seq
      AND visibility_revision >= 0
    )
  );
