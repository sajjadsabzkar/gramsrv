-- 0026_channel_read_participants: store channel read receipt dates and expose TDesktop read mark config.

ALTER TABLE channel_members
    ADD COLUMN IF NOT EXISTS read_inbox_date INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS channel_members_read_participants_idx
    ON channel_members (channel_id, read_inbox_max_id, user_id)
    WHERE status = 'active';

UPDATE app_configs
SET hash = 2,
    config_json = config_json
        || jsonb_build_object(
            'chat_read_mark_size_threshold', 50,
            'chat_read_mark_expire_period', 604800,
            'pm_read_date_expire_period', 604800
        ),
    updated_at = now()
WHERE client = 'tdesktop';
