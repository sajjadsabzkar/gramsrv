ALTER TABLE channel_messages
    ADD COLUMN IF NOT EXISTS views_count INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS channel_message_viewers (
    channel_id     BIGINT      NOT NULL,
    message_id     INT         NOT NULL,
    viewer_user_id BIGINT      NOT NULL,
    viewed_at      INT         NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, message_id, viewer_user_id),
    CONSTRAINT channel_message_viewers_positive_check CHECK (
        channel_id > 0 AND message_id > 0 AND viewer_user_id > 0
    )
) PARTITION BY HASH (channel_id);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_message_viewers_p%s PARTITION OF channel_message_viewers FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
