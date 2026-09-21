-- Reviewed forward repair for derived pointers only. 不删除消息或重编号；锁顺序与写入一致。
BEGIN;
SET LOCAL lock_timeout = '8s';
SET LOCAL statement_timeout = '12s';
SELECT conversation_id FROM newim.im_conversations ORDER BY conversation_id FOR UPDATE;
UPDATE newim.im_conversations c
SET latest_server_msg_id = (SELECT m.server_msg_id FROM newim.im_messages m
  WHERE m.conversation_id=c.conversation_id ORDER BY m.conversation_seq DESC LIMIT 1);
-- Counter gaps are preserved. Never lower an allocator or silently repair a counter behind data.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM newim.im_messages m JOIN newim.im_conversations c USING(conversation_id)
            WHERE m.conversation_seq>c.last_seq) THEN
  RAISE EXCEPTION USING ERRCODE='NR001', MESSAGE='REPAIR_COUNTER_BEHIND_MESSAGES';
 END IF;
END $$;
COMMIT;
