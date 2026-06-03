CREATE INDEX IF NOT EXISTS channel_message_reactions_unread_owner_idx
    ON channel_message_reactions (channel_id, sender_user_id, message_id DESC)
    WHERE unread;
