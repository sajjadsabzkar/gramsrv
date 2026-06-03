-- 0059_channel_photo: denormalized current channel/supergroup avatar for ChatPhoto rendering.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS photo_id       BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS photo_dc_id    INT    NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS photo_stripped BYTEA  NOT NULL DEFAULT ''::bytea;
