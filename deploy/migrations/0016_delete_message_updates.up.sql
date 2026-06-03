-- 0016_delete_message_updates: durable owner-view delete message updates.
--
-- message_ids carries updateDeleteMessages.messages for offline getDifference
-- and reliable online outbox delivery.

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS message_ids JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE user_update_events
    DROP CONSTRAINT IF EXISTS user_update_events_type_check;

ALTER TABLE user_update_events
    ADD CONSTRAINT user_update_events_type_check CHECK (
        event_type IN (
            'new_message',
            'read_history_inbox',
            'contacts_reset',
            'dialog_pinned',
            'pinned_dialogs',
            'dialog_unread_mark',
            'peer_settings',
            'delete_messages',
            'noop'
        )
    );

CREATE INDEX IF NOT EXISTS message_boxes_private_sender_live_idx
    ON message_boxes (message_sender_id, private_message_id)
    WHERE NOT deleted;
