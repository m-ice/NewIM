CREATE TABLE newim.im_user_profiles (
  user_id newim.identifier PRIMARY KEY REFERENCES newim.im_users (user_id),
  nickname text NOT NULL DEFAULT '' CHECK (octet_length(nickname) <= 128),
  face_url text NOT NULL DEFAULT '' CHECK (octet_length(face_url) <= 2048),
  ex text NOT NULL DEFAULT '' CHECK (octet_length(ex) <= 4096)
);
