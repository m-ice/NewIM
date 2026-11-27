CREATE TABLE newim.im_conversation_account_settings (
  conversation_id newim.identifier NOT NULL,
  user_id newim.identifier NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  pinned boolean NOT NULL DEFAULT false,
  muted boolean NOT NULL DEFAULT false,
  draft bytea NOT NULL DEFAULT ''::bytea CHECK (octet_length(draft) <= 4096),
  clear_through_seq bigint NOT NULL DEFAULT 0 CHECK (clear_through_seq >= 0),
  deleted_through_seq bigint CHECK (
    deleted_through_seq >= 0 AND deleted_through_seq <= clear_through_seq
  ),
  PRIMARY KEY (conversation_id, user_id),
  FOREIGN KEY (conversation_id, user_id)
    REFERENCES newim.im_conversation_members (conversation_id, user_id)
);

ALTER TABLE newim.im_conversation_sync_changes
  ADD COLUMN settings_version bigint,
  ADD COLUMN settings_pinned boolean,
  ADD COLUMN settings_muted boolean,
  ADD COLUMN settings_draft bytea,
  ADD COLUMN settings_clear_through_seq bigint,
  ADD CONSTRAINT im_conversation_sync_changes_settings_shape CHECK (
    num_nonnulls(settings_version, settings_pinned, settings_muted, settings_draft, settings_clear_through_seq) = 0
    OR (
      kind = 'upsert'
      AND num_nonnulls(settings_version, settings_pinned, settings_muted, settings_draft, settings_clear_through_seq) = 5
      AND settings_version > 0
      AND octet_length(settings_draft) <= 4096
      AND settings_clear_through_seq >= 0
    )
  );
