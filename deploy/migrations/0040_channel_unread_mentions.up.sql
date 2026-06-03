CREATE TABLE IF NOT EXISTS channel_unread_mentions (
    user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id     BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    message_id     INT         NOT NULL,
    top_message_id INT         NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, channel_id, message_id),
    FOREIGN KEY (channel_id, message_id) REFERENCES channel_messages(channel_id, id) ON DELETE CASCADE
) PARTITION BY HASH (user_id);

CREATE INDEX IF NOT EXISTS channel_unread_mentions_peer_idx
    ON channel_unread_mentions (user_id, channel_id, top_message_id, message_id DESC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_unread_mentions_p%s PARTITION OF channel_unread_mentions FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
