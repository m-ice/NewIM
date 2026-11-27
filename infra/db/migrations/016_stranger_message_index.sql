CREATE INDEX im_messages_conversation_sender_idx
  ON newim.im_messages (conversation_id, sender_id);
