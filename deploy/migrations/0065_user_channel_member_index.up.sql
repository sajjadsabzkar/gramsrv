CREATE TABLE IF NOT EXISTS user_channel_member_index (
    user_id     BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id  BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    status      VARCHAR(16) NOT NULL,
    megagroup   BOOLEAN     NOT NULL DEFAULT false,
    broadcast   BOOLEAN     NOT NULL DEFAULT false,
    deleted     BOOLEAN     NOT NULL DEFAULT false,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, channel_id),
    CONSTRAINT user_channel_member_index_status_check
        CHECK (status IN ('active', 'left', 'kicked', 'banned'))
);

CREATE INDEX IF NOT EXISTS user_channel_member_index_common_idx
    ON user_channel_member_index (user_id, channel_id)
    WHERE status = 'active' AND megagroup AND NOT broadcast AND NOT deleted;

INSERT INTO user_channel_member_index (
    user_id, channel_id, status, megagroup, broadcast, deleted
)
SELECT m.user_id, m.channel_id, m.status, c.megagroup, c.broadcast, c.deleted
FROM channel_members m
JOIN channels c ON c.id = m.channel_id
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    status = EXCLUDED.status,
    megagroup = EXCLUDED.megagroup,
    broadcast = EXCLUDED.broadcast,
    deleted = EXCLUDED.deleted,
    updated_at = now();
