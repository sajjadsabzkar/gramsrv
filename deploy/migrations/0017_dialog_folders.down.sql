-- 0017_dialog_folders rollback.

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

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS tags_enabled;

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS filter_id;

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS folder_peers;

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS filter_order;

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS dialog_filter;

DROP TABLE IF EXISTS dialog_filter_settings CASCADE;

DROP TABLE IF EXISTS dialog_filters CASCADE;

DROP INDEX IF EXISTS dialogs_user_folder_top_message_idx;

ALTER TABLE dialogs
    DROP CONSTRAINT IF EXISTS dialogs_folder_id_check;

ALTER TABLE dialogs
    DROP COLUMN IF EXISTS folder_id;
