ALTER TABLE channel_dialogs
    ADD COLUMN IF NOT EXISTS view_forum_as_messages BOOLEAN NOT NULL DEFAULT false;
