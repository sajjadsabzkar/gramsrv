-- 0034_channel_invite_importers rollback.

DROP TABLE IF EXISTS channel_invite_importers;

ALTER TABLE channel_invites
    DROP COLUMN IF EXISTS requested_count;
