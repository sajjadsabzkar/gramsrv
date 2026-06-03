-- 0025_channel_member_slowmode_backfill: idempotent compatibility migration for
-- developer databases that applied an earlier 0022 channel schema draft.

ALTER TABLE channel_members
    ADD COLUMN IF NOT EXISTS slowmode_last_send_date INT NOT NULL DEFAULT 0;
