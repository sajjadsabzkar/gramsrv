DROP INDEX IF EXISTS dialogs_user_pinned_order_idx;

ALTER TABLE dialogs
    DROP COLUMN IF EXISTS hidden_peer_settings_bar,
    DROP COLUMN IF EXISTS unread_mark,
    DROP COLUMN IF EXISTS pinned_order;

DROP INDEX IF EXISTS contacts_user_name_idx;

ALTER TABLE contacts
    DROP COLUMN IF EXISTS stories_hidden,
    DROP COLUMN IF EXISTS close_friend,
    DROP COLUMN IF EXISTS note_entities,
    DROP COLUMN IF EXISTS note,
    DROP COLUMN IF EXISTS contact_last_name,
    DROP COLUMN IF EXISTS contact_first_name,
    DROP COLUMN IF EXISTS contact_phone;
