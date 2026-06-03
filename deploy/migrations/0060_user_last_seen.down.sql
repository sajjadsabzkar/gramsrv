DROP INDEX IF EXISTS users_last_seen_idx;

ALTER TABLE users
    DROP COLUMN IF EXISTS last_seen_at;
