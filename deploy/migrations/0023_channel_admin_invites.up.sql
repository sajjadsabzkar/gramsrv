-- 0023_channel_admin_invites: metadata needed by channel admin/pin/invite RPCs.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS pinned_message_id INT NOT NULL DEFAULT 0;

ALTER TABLE channel_update_events
    DROP CONSTRAINT IF EXISTS channel_update_events_type_check;

ALTER TABLE channel_update_events
    ADD CONSTRAINT channel_update_events_type_check CHECK (
        event_type IN (
            'new_channel_message',
            'edit_channel_message',
            'delete_channel_messages',
            'channel_participant',
            'pinned_channel_messages',
            'noop'
        )
    );

CREATE INDEX IF NOT EXISTS channel_invites_hash_lookup_idx
    ON channel_invite_hashes (hash, channel_id, invite_id);
