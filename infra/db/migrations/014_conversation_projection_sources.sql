ALTER TABLE newim.im_conversation_sync_accounts
  ADD COLUMN source_ready boolean NOT NULL DEFAULT false,
  ADD COLUMN source_after_key newim.identifier,
  ADD CONSTRAINT im_conversation_sync_source_progress CHECK (
    NOT source_ready OR source_after_key IS NULL
  );

CREATE INDEX im_conversation_members_sync_active_idx
  ON newim.im_conversation_members (conversation_id, user_id)
  WHERE departed_at IS NULL;

CREATE INDEX im_conversation_members_sync_user_active_idx
  ON newim.im_conversation_members (user_id, conversation_id)
  WHERE departed_at IS NULL;
