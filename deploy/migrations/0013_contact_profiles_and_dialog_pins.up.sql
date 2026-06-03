-- 0013_contact_profiles_and_dialog_pins: owner-scoped contact profile fields and pin order.
--
-- contact_* columns are deliberately scoped to (user_id, contact_user_id): they model the
-- current account's saved name/phone/note for a peer and must not mutate users global data.

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS contact_phone VARCHAR(32) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS contact_first_name VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS contact_last_name VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS note TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS note_entities JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS close_friend BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS stories_hidden BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS contacts_user_name_idx
    ON contacts (user_id, contact_first_name, contact_last_name, contact_user_id);

ALTER TABLE dialogs
    ADD COLUMN IF NOT EXISTS pinned_order INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS unread_mark BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS hidden_peer_settings_bar BOOLEAN NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS dialogs_user_pinned_order_idx
    ON dialogs (user_id, pinned, pinned_order, top_message_date DESC, top_message_id DESC, peer_id DESC)
    WHERE pinned;
