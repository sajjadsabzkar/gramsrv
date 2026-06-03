-- 0060_user_last_seen: persist exact last seen timestamps for UserStatusOffline.was_online.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS last_seen_at BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS users_last_seen_idx
    ON users (last_seen_at DESC)
    WHERE last_seen_at > 0;
