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
            'dialog_filter',
            'dialog_filter_order',
            'dialog_filters',
            'folder_peers',
            'noop'
        )
    );

DROP INDEX IF EXISTS message_boxes_private_sender_owner_idx;
DROP INDEX IF EXISTS user_update_events_read_outbox_idx;
DROP INDEX IF EXISTS message_boxes_read_receipt_idx;

ALTER TABLE message_boxes
    DROP COLUMN IF EXISTS edit_date;

ALTER TABLE private_messages
    DROP COLUMN IF EXISTS edit_date;

ALTER TABLE users
    DROP COLUMN IF EXISTS about;
