ALTER TABLE conversation_sync_items ADD COLUMN unread_count INTEGER;
ALTER TABLE conversation_sync_items ADD COLUMN visibility_revision INTEGER CHECK(
  (unread_count IS NULL AND visibility_revision IS NULL)
  OR (latest_seq IS NOT NULL AND unread_count IS NOT NULL AND visibility_revision IS NOT NULL
      AND unread_count BETWEEN 0 AND max(latest_seq-coalesce(last_read_seq,0),0)
      AND visibility_revision>=0)
);

ALTER TABLE message_sync ADD COLUMN visibility_revision INTEGER NOT NULL DEFAULT 0 CHECK(visibility_revision>=0);

CREATE TABLE message_visibility (
  conversation_id TEXT NOT NULL CHECK(length(CAST(conversation_id AS BLOB)) BETWEEN 1 AND 128),
  sequence INTEGER NOT NULL CHECK(sequence>0),
  server_id TEXT NOT NULL UNIQUE CHECK(length(CAST(server_id AS BLOB)) BETWEEN 1 AND 128),
  action INTEGER NOT NULL CHECK(action IN (1,2,3)),
  PRIMARY KEY(conversation_id,sequence)
) STRICT, WITHOUT ROWID;
