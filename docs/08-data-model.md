# Minimum data model

Core entities: `im_users`, `im_devices`, `im_sessions`, `im_conversations`, `im_conversation_members`, `im_messages`, `im_read_states`, `im_outbox_events`, `im_webhook_deliveries`, `im_push_tokens`, `im_blocks`; moderation state is optional but expected.

Required constraints/indexes include global unique `server_msg_id`, unique `(sender_id, client_msg_id)`, and an index/uniqueness strategy around `(conversation_id, conversation_seq)`.

Redis is never the permanent message source of truth. Media binary data is not stored unbounded in the client message DB; store media keys and metadata, with signed URLs resolved temporarily.
