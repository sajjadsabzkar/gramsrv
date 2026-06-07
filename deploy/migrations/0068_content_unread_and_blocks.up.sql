ALTER TABLE message_boxes
    ADD COLUMN IF NOT EXISTS media_unread BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS reaction_unread BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE channel_unread_mentions
    ADD COLUMN IF NOT EXISTS media_unread BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE user_update_events
    DROP CONSTRAINT IF EXISTS user_update_events_type_check;

ALTER TABLE user_update_events
    ADD CONSTRAINT user_update_events_type_check CHECK (
        event_type IN (
            'new_message',
            'read_history_inbox',
            'read_history_outbox',
            'read_message_contents',
            'edit_message',
            'message_reactions',
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
            'channel_view_forum_as_messages',
            'noop'
        )
    );

CREATE TABLE IF NOT EXISTS contact_blocks (
    owner_user_id   BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    blocked_user_id BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    date            INT         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_user_id, blocked_user_id)
) PARTITION BY HASH (owner_user_id);

CREATE INDEX IF NOT EXISTS contact_blocks_owner_date_idx
    ON contact_blocks (owner_user_id, date DESC, blocked_user_id DESC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS contact_blocks_p%s PARTITION OF contact_blocks FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
