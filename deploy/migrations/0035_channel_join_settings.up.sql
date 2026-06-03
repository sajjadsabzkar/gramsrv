-- 0035_channel_join_settings: public join/send gating flags.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS join_to_send BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS join_request BOOLEAN NOT NULL DEFAULT false;
