DROP TABLE IF EXISTS channel_message_viewers CASCADE;

ALTER TABLE channel_messages
    DROP COLUMN IF EXISTS views_count;
