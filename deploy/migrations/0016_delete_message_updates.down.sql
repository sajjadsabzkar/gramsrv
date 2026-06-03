DROP INDEX IF EXISTS message_boxes_private_sender_live_idx;

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
            'noop'
        )
    );

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS message_ids;
