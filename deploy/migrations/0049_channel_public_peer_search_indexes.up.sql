-- 0049_channel_public_peer_search_indexes: indexed public channel peer lookup for contacts.search.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS channels_public_username_trgm_idx
    ON channels USING gin (lower(username) gin_trgm_ops)
    WHERE NOT deleted AND COALESCE(username, '') <> '';

CREATE INDEX IF NOT EXISTS channels_public_title_trgm_idx
    ON channels USING gin (lower(title) gin_trgm_ops)
    WHERE NOT deleted AND COALESCE(username, '') <> '';
