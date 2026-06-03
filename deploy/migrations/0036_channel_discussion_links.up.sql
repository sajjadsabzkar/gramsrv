-- 0036_channel_discussion_links: persist bidirectional broadcast <-> discussion group links.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS linked_chat_id BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS channels_linked_chat_idx
    ON channels (linked_chat_id)
    WHERE linked_chat_id <> 0 AND NOT deleted;
