CREATE TABLE message_sync (
 conversation_id TEXT PRIMARY KEY CHECK(length(CAST(conversation_id AS BLOB)) BETWEEN 1 AND 128),
 continuous_seq INTEGER NOT NULL CHECK(continuous_seq>=0),
 history_after_seq INTEGER NOT NULL CHECK(history_after_seq>=0),
 highest_observed_seq INTEGER NOT NULL CHECK(highest_observed_seq>=continuous_seq),
 phase INTEGER NOT NULL CHECK(phase BETWEEN 0 AND 4),
 started_at_ms INTEGER CHECK(started_at_ms>=0),
 failures INTEGER CHECK(failures BETWEEN 0 AND 8),
 page_limit INTEGER CHECK(page_limit BETWEEN 1 AND 64),
 wake_at_ms INTEGER CHECK(wake_at_ms>=0),
 attempt_id INTEGER CHECK(attempt_id>0),
 generation INTEGER CHECK(generation>0),
 attempt_after_seq INTEGER CHECK(attempt_after_seq>=0),
 expires_at_ms INTEGER CHECK(expires_at_ms>=0),
 CHECK((phase=0 AND started_at_ms IS NULL AND failures IS NULL AND page_limit IS NULL) OR
       (phase<>0 AND started_at_ms IS NOT NULL AND failures IS NOT NULL AND page_limit IS NOT NULL)),
 CHECK((phase=1 AND wake_at_ms IS NOT NULL) OR (phase<>1 AND wake_at_ms IS NULL)),
 CHECK((phase=2 AND attempt_id IS NOT NULL AND generation IS NOT NULL AND
        attempt_after_seq IS NOT NULL AND expires_at_ms IS NOT NULL AND expires_at_ms>=started_at_ms) OR
       (phase<>2 AND attempt_id IS NULL AND generation IS NULL AND
        attempt_after_seq IS NULL AND expires_at_ms IS NULL))
) STRICT, WITHOUT ROWID;
