# Message registry

Initial logical types: `text`, `image`, `audio`, `gift`, `match`, `activity_invite`, `system_notice`, `profile_card`, `location`, `call_signal`.

Each registered type defines validator, renderer contract, push preview policy, moderation policy, unread policy and persistence policy. Avoid an unbounded `custom` type whose JSON semantics are undocumented.
