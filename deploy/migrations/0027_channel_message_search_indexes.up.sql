-- 0027_channel_message_search_indexes: bounded in-channel text search.
--
-- TDesktop uses messages.search with inputPeerChannel for in-chat search.
-- Keep text lookup indexed on the single-copy channel_messages table.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS channel_messages_body_trgm_idx
    ON channel_messages USING gin (body gin_trgm_ops)
    WHERE NOT deleted AND body <> '';
