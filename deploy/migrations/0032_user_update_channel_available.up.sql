-- 0032_user_update_channel_available: durable account update for channel local clear.

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
            'channel_available_messages',
            'noop'
        )
    );
