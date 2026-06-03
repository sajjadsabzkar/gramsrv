-- 0059_channel_photo down: drop denormalized channel avatar columns.

ALTER TABLE channels
    DROP COLUMN IF EXISTS photo_stripped,
    DROP COLUMN IF EXISTS photo_dc_id,
    DROP COLUMN IF EXISTS photo_id;
