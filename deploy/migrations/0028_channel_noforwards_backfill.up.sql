-- 0028_channel_noforwards_backfill: ensure older developer databases have
-- the channel content-protection flag used by messages.toggleNoForwards.
--
-- Fresh databases already get this column from 0022.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS noforwards BOOLEAN NOT NULL DEFAULT false;
