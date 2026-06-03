-- 0031_channel_available_min_pts: channelDifference visibility floor per member.

ALTER TABLE channel_members
    ADD COLUMN IF NOT EXISTS available_min_pts INT NOT NULL DEFAULT 0;
