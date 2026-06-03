-- 0018_user_search_indexes: keep TDesktop global user search bounded as users grow.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS users_phone_prefix_idx
    ON users (phone text_pattern_ops);

CREATE INDEX IF NOT EXISTS users_username_lower_trgm_idx
    ON users USING gin (lower(username) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS users_name_lower_trgm_idx
    ON users USING gin (lower(trim(first_name || ' ' || last_name)) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS contacts_user_saved_name_trgm_idx
    ON contacts USING gin (lower(trim(contact_first_name || ' ' || contact_last_name)) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS message_boxes_body_trgm_idx
    ON message_boxes USING gin (body gin_trgm_ops)
    WHERE NOT deleted AND body <> '';
