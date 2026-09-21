CREATE TABLE store_metadata (
 id INTEGER PRIMARY KEY CHECK(id=1), account TEXT NOT NULL, instance TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation>0), revision INTEGER NOT NULL CHECK(revision>=0),
 last_operation INTEGER NOT NULL DEFAULT 0, last_request BLOB NOT NULL DEFAULT X'',
 last_revision INTEGER NOT NULL DEFAULT 0, last_affected INTEGER NOT NULL DEFAULT 0,
 recovery_required INTEGER NOT NULL DEFAULT 0 CHECK(recovery_required IN (0,1))
);
CREATE TABLE messages (
 server_id TEXT PRIMARY KEY, sender_id TEXT NOT NULL, client_id TEXT NOT NULL,
 conversation_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence>=0),
 server_time INTEGER NOT NULL CHECK(server_time>=0), schema_version INTEGER NOT NULL CHECK(schema_version>0 AND schema_version<=2147483647),
 message_type TEXT NOT NULL, payload BLOB CHECK(payload IS NULL OR length(payload)<=65536),
 UNIQUE(sender_id,client_id), UNIQUE(conversation_id,sequence)
);
CREATE TABLE conversations (id TEXT PRIMARY KEY, unread INTEGER NOT NULL CHECK(unread>=0), summary BLOB NOT NULL CHECK(length(summary)<=65536));
CREATE TABLE sync_state (id INTEGER PRIMARY KEY CHECK(id=1), cursor BLOB NOT NULL CHECK(length(cursor)<=65536));
INSERT INTO sync_state VALUES(1,X'');
CREATE TABLE pending_outbox (sender_id TEXT NOT NULL, client_id TEXT NOT NULL, conversation_id TEXT NOT NULL, payload BLOB NOT NULL CHECK(length(payload)<=65536), PRIMARY KEY(sender_id,client_id));
CREATE TABLE users_cache (id TEXT PRIMARY KEY, value BLOB NOT NULL CHECK(length(value)<=65536));
