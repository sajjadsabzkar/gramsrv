-- 0029_channel_available_reactions: persisted reaction policy for
-- messages.setChatAvailableReactions and channels.getFullChannel.
--
-- Fresh databases already get this column from 0022.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS available_reactions JSONB NOT NULL DEFAULT '{}'::jsonb;
