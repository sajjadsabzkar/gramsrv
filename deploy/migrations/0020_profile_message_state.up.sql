-- 0020_profile_message_state: profile about, message edits, and read outbox updates.

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS about VARCHAR(255) NOT NULL DEFAULT '';

ALTER TABLE private_messages
    ADD COLUMN IF NOT EXISTS edit_date INT NOT NULL DEFAULT 0;

ALTER TABLE message_boxes
    ADD COLUMN IF NOT EXISTS edit_date INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS message_boxes_read_receipt_idx
    ON message_boxes (owner_user_id, peer_type, peer_id, box_id DESC)
    WHERE NOT deleted AND NOT outgoing;

CREATE INDEX IF NOT EXISTS message_boxes_private_sender_owner_idx
    ON message_boxes (message_sender_id, private_message_id, owner_user_id)
    WHERE NOT deleted;

CREATE INDEX IF NOT EXISTS user_update_events_read_outbox_idx
    ON user_update_events (user_id, peer_type, peer_id, max_id, date)
    WHERE event_type = 'read_history_outbox';

ALTER TABLE user_update_events
    DROP CONSTRAINT IF EXISTS user_update_events_type_check;

ALTER TABLE user_update_events
    ADD CONSTRAINT user_update_events_type_check CHECK (
        event_type IN (
            'new_message',
            'read_history_inbox',
            'read_history_outbox',
            'edit_message',
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
