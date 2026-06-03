ALTER TABLE user_update_events
    DROP CONSTRAINT IF EXISTS user_update_events_type_check;

ALTER TABLE user_update_events
    ADD CONSTRAINT user_update_events_type_check CHECK (
        event_type IN ('new_message', 'read_history_inbox', 'noop')
    );

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS event_bool;
