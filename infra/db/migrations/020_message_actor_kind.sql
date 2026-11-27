CREATE TABLE newim.im_user_actor_types (
  user_id newim.identifier PRIMARY KEY REFERENCES newim.im_users (user_id),
  actor_type text COLLATE "C" NOT NULL CHECK (actor_type IN ('BOT', 'SYSTEM', 'AI_AGENT'))
);

COMMENT ON TABLE newim.im_user_actor_types IS
  'Immutable actor assignment written at account creation. An absent row denotes HUMAN.';
