-- 0047_dialog_drafts: persistent cloud drafts for private dialogs and channels.

CREATE TABLE IF NOT EXISTS dialog_drafts (
    user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    peer_type      VARCHAR(16) NOT NULL,
    peer_id        BIGINT      NOT NULL,
    top_message_id INT         NOT NULL DEFAULT 0,
    date           INT         NOT NULL DEFAULT 0,
    draft          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, peer_type, peer_id, top_message_id),
    CONSTRAINT dialog_drafts_peer_type_check CHECK (peer_type IN ('user', 'channel')),
    CONSTRAINT dialog_drafts_top_message_id_check CHECK (top_message_id >= 0)
) PARTITION BY HASH (user_id);

CREATE INDEX IF NOT EXISTS dialog_drafts_user_date_idx
    ON dialog_drafts (user_id, date DESC, peer_type, peer_id, top_message_id);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS dialog_drafts_p%s PARTITION OF dialog_drafts FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
