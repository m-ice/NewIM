ALTER TABLE conversation_sync_items ADD COLUMN unread_count INTEGER;
ALTER TABLE conversation_sync_items ADD COLUMN visibility_revision INTEGER CHECK(
  (unread_count IS NULL AND visibility_revision IS NULL)
  OR (latest_seq IS NOT NULL AND unread_count IS NOT NULL AND visibility_revision IS NOT NULL
      AND unread_count BETWEEN 0 AND max(latest_seq-coalesce(last_read_seq,0),0)
      AND visibility_revision>=0)
);
