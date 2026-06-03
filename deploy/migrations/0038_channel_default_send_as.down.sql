ALTER TABLE channel_dialogs
    DROP CONSTRAINT IF EXISTS channel_dialogs_default_send_as_peer_type_check;

ALTER TABLE channel_dialogs
    DROP COLUMN IF EXISTS default_send_as_peer_id,
    DROP COLUMN IF EXISTS default_send_as_peer_type;
