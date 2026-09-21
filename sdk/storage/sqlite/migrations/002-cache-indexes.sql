CREATE INDEX messages_payload_trim ON messages(conversation_id,sequence) WHERE payload IS NOT NULL;
CREATE INDEX pending_page ON pending_outbox((sender_id || ':' || client_id));
