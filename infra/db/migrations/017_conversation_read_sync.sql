ALTER TABLE newim.im_conversation_sync_accounts
  ADD COLUMN read_state_epoch bigint CHECK (
    read_state_epoch > 0 AND read_state_epoch <= epoch
  );

ALTER TABLE newim.im_conversation_sync_changes
  ADD COLUMN last_read_seq bigint CHECK (
    last_read_seq IS NULL OR (kind = 'upsert' AND last_read_seq >= 0)
  );
