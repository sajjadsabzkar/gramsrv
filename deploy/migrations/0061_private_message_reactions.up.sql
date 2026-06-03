-- 0061_private_message_reactions: persist emoji reactions for private message boxes.

CREATE TABLE IF NOT EXISTS private_message_reactions (
  message_sender_id bigint NOT NULL,
  private_message_id bigint NOT NULL,
  user_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  reaction_type text NOT NULL,
  reaction_value text NOT NULL,
  big boolean NOT NULL DEFAULT false,
  reaction_date integer NOT NULL,
  chosen_order integer NOT NULL DEFAULT 1,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (message_sender_id, private_message_id, user_id, reaction_type, reaction_value),
  FOREIGN KEY (message_sender_id, private_message_id)
    REFERENCES private_messages(sender_user_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS private_message_reactions_message_idx
  ON private_message_reactions (message_sender_id, private_message_id, reaction_date DESC, user_id);
