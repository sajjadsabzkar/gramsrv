DROP INDEX IF EXISTS channel_invites_hash_lookup_idx;

ALTER TABLE channel_update_events
    DROP CONSTRAINT IF EXISTS channel_update_events_type_check;

ALTER TABLE channel_update_events
    ADD CONSTRAINT channel_update_events_type_check CHECK (
        event_type IN (
            'new_channel_message',
            'edit_channel_message',
            'delete_channel_messages',
            'channel_participant',
            'noop'
        )
    );

ALTER TABLE channels
    DROP COLUMN IF EXISTS pinned_message_id;
