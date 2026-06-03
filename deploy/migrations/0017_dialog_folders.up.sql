-- 0017_dialog_folders: archive folder, custom dialog filters, and durable folder updates.

ALTER TABLE dialogs
    ADD COLUMN IF NOT EXISTS folder_id INT NOT NULL DEFAULT 0;

ALTER TABLE dialogs
    DROP CONSTRAINT IF EXISTS dialogs_folder_id_check;

ALTER TABLE dialogs
    ADD CONSTRAINT dialogs_folder_id_check CHECK (folder_id >= 0);

CREATE INDEX IF NOT EXISTS dialogs_user_folder_top_message_idx
    ON dialogs (user_id, folder_id, pinned DESC, top_message_date DESC, top_message_id DESC, peer_id DESC);

CREATE TABLE IF NOT EXISTS dialog_filters (
    user_id      BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    filter_id    INT         NOT NULL,
    is_chatlist  BOOLEAN     NOT NULL DEFAULT false,
    filter       JSONB       NOT NULL,
    order_value  INT         NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, filter_id),
    CONSTRAINT dialog_filters_id_check CHECK (filter_id >= 2),
    CONSTRAINT dialog_filters_filter_object_check CHECK (jsonb_typeof(filter) = 'object')
) PARTITION BY HASH (user_id);

CREATE INDEX IF NOT EXISTS dialog_filters_user_order_idx
    ON dialog_filters (user_id, order_value, filter_id);

CREATE TABLE IF NOT EXISTS dialog_filter_settings (
    user_id      BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tags_enabled BOOLEAN     NOT NULL DEFAULT false,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id)
) PARTITION BY HASH (user_id);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS dialog_filters_p%s PARTITION OF dialog_filters FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS dialog_filter_settings_p%s PARTITION OF dialog_filter_settings FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS dialog_filter JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS filter_order JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS folder_peers JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS filter_id INT NOT NULL DEFAULT 0;

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS tags_enabled BOOLEAN NOT NULL DEFAULT false;

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
