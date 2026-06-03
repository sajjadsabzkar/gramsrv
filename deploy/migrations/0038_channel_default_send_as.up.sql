ALTER TABLE channel_dialogs
    ADD COLUMN IF NOT EXISTS default_send_as_peer_type VARCHAR(16),
    ADD COLUMN IF NOT EXISTS default_send_as_peer_id BIGINT;

ALTER TABLE channel_dialogs
    DROP CONSTRAINT IF EXISTS channel_dialogs_default_send_as_peer_type_check;

ALTER TABLE channel_dialogs
    ADD CONSTRAINT channel_dialogs_default_send_as_peer_type_check
    CHECK (
        default_send_as_peer_type IS NULL
        OR default_send_as_peer_type IN ('user', 'channel')
    );
