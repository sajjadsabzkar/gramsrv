-- 0034_channel_invite_importers: invite management read model.

ALTER TABLE channel_invites
    ADD COLUMN IF NOT EXISTS requested_count INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS channel_invite_importers (
    channel_id  BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    invite_id   BIGINT      NOT NULL,
    user_id     BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    date        INT         NOT NULL,
    requested   BOOLEAN     NOT NULL DEFAULT false,
    approved_by BIGINT      NOT NULL DEFAULT 0,
    via_chatlist BOOLEAN    NOT NULL DEFAULT false,
    about       TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, user_id)
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_invite_importers_link_idx
    ON channel_invite_importers (channel_id, invite_id, requested, date DESC, user_id DESC);

CREATE INDEX IF NOT EXISTS channel_invite_importers_requested_idx
    ON channel_invite_importers (channel_id, requested, date DESC, user_id DESC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_invite_importers_p%s PARTITION OF channel_invite_importers FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
