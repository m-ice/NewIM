CREATE TABLE conversation_sync_state (
 id INTEGER PRIMARY KEY CHECK(id=1),
 round INTEGER NOT NULL CHECK(round>0),
 phase INTEGER NOT NULL CHECK(phase BETWEEN 0 AND 3),
 cursor BLOB,
 published_round INTEGER CHECK(published_round>0),
 CHECK((phase=0 AND cursor IS NULL) OR
       (phase<>0 AND cursor IS NOT NULL AND length(cursor) BETWEEN 1 AND 2048)),
 CHECK((phase<2 AND (published_round IS NULL OR published_round<round)) OR
       (phase>=2 AND published_round IS NOT NULL AND published_round=round))
) STRICT;
INSERT INTO conversation_sync_state VALUES(1,1,0,NULL,NULL);

CREATE TABLE conversation_sync_items (
 round INTEGER NOT NULL CHECK(round>0),
 conversation_id TEXT NOT NULL CHECK(length(CAST(conversation_id AS BLOB)) BETWEEN 1 AND 128),
 revision INTEGER NOT NULL CHECK(revision>0),
 latest_seq INTEGER CHECK(latest_seq>=0),
 latest_server_id TEXT CHECK(length(CAST(latest_server_id AS BLOB)) BETWEEN 1 AND 128),
 last_read_seq INTEGER CHECK(last_read_seq>=0),
 settings_version INTEGER CHECK(settings_version>=0),
 pinned INTEGER CHECK(pinned IN (0,1)),
 muted INTEGER CHECK(muted IN (0,1)),
 draft TEXT CHECK(length(CAST(draft AS BLOB))<=4096),
 clear_through_seq INTEGER CHECK(clear_through_seq>=0),
 known_clear_through_seq INTEGER NOT NULL CHECK(known_clear_through_seq>=coalesce(clear_through_seq,0)),
 PRIMARY KEY(round,conversation_id),
 CHECK((latest_seq IS NULL AND latest_server_id IS NULL AND last_read_seq IS NULL AND settings_version IS NULL) OR
       (latest_seq IS NOT NULL AND ((latest_seq=0)=(latest_server_id IS NULL)) AND
        (latest_seq=0 OR last_read_seq IS NULL OR last_read_seq<=latest_seq))),
 CHECK((settings_version IS NULL AND pinned IS NULL AND muted IS NULL AND draft IS NULL AND clear_through_seq IS NULL) OR
       (settings_version IS NOT NULL AND pinned IS NOT NULL AND muted IS NOT NULL AND draft IS NOT NULL AND clear_through_seq IS NOT NULL))
) STRICT, WITHOUT ROWID;

CREATE TABLE conversation_sync_history (
 conversation_id TEXT PRIMARY KEY CHECK(length(CAST(conversation_id AS BLOB)) BETWEEN 1 AND 128),
 clear_through_seq INTEGER NOT NULL CHECK(clear_through_seq>0)
) STRICT, WITHOUT ROWID;
