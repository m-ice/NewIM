ALTER TABLE newim.im_conversations
  ADD COLUMN kind text,
  ADD COLUMN direct_user_low newim.identifier REFERENCES newim.im_users,
  ADD COLUMN direct_user_high newim.identifier REFERENCES newim.im_users,
  ADD CONSTRAINT im_conversations_direct_pair_shape CHECK (
    CASE kind
      WHEN 'direct' THEN direct_user_low IS NOT NULL AND direct_user_high IS NOT NULL
        AND direct_user_low < direct_user_high
      WHEN 'group' THEN direct_user_low IS NULL AND direct_user_high IS NULL
      ELSE kind IS NULL AND direct_user_low IS NULL AND direct_user_high IS NULL
    END
  );

CREATE UNIQUE INDEX im_conversations_direct_pair_key
  ON newim.im_conversations (direct_user_low, direct_user_high)
  WHERE kind = 'direct';
