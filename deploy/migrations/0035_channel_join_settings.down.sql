-- 0035_channel_join_settings rollback.

ALTER TABLE channels
    DROP COLUMN IF EXISTS join_request,
    DROP COLUMN IF EXISTS join_to_send;
