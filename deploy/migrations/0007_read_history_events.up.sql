-- 0007_read_history_events: persist readHistory update events for getDifference.
--
-- updateReadHistoryInbox：真正发生已读推进时递增 pts，并给其它 session / getDifference 留可补偿事件。

ALTER TABLE update_events
    ADD COLUMN IF NOT EXISTS peer_type VARCHAR(16),
    ADD COLUMN IF NOT EXISTS peer_id BIGINT,
    ADD COLUMN IF NOT EXISTS max_id INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS still_unread_count INT NOT NULL DEFAULT 0;

ALTER TABLE update_events
    DROP CONSTRAINT IF EXISTS update_events_type_check;

ALTER TABLE update_events
    ADD CONSTRAINT update_events_type_check
    CHECK (event_type IN ('new_message', 'read_history_inbox'));

ALTER TABLE update_events
    DROP CONSTRAINT IF EXISTS update_events_peer_type_check;

ALTER TABLE update_events
    ADD CONSTRAINT update_events_peer_type_check
    CHECK (peer_type IS NULL OR peer_type IN ('user'));
