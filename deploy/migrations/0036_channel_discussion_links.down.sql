DROP INDEX IF EXISTS channels_linked_chat_idx;

ALTER TABLE channels
    DROP COLUMN IF EXISTS linked_chat_id;
