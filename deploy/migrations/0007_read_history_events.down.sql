ALTER TABLE update_events
    DROP CONSTRAINT IF EXISTS update_events_peer_type_check;

ALTER TABLE update_events
    DROP CONSTRAINT IF EXISTS update_events_type_check;

ALTER TABLE update_events
    ADD CONSTRAINT update_events_type_check
    CHECK (event_type IN ('new_message'));

ALTER TABLE update_events
    DROP COLUMN IF EXISTS still_unread_count,
    DROP COLUMN IF EXISTS max_id,
    DROP COLUMN IF EXISTS peer_id,
    DROP COLUMN IF EXISTS peer_type;
