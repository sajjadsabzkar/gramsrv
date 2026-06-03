-- 0014_settings_update_events: durable updates for contacts/dialog settings.
--
-- Online push is not enough: offline sessions must recover contact resets,
-- dialog pin order changes, manual unread marks, and peer settings changes
-- through updates.getDifference.

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS event_bool BOOLEAN NOT NULL DEFAULT false;

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
