-- 0019_usernames: primary username lifecycle.

CREATE UNIQUE INDEX IF NOT EXISTS users_username_lower_unique_idx
    ON users (lower(username))
    WHERE username <> '';
