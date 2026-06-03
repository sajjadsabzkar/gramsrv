ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS participants_hidden BOOLEAN NOT NULL DEFAULT false;
