DROP INDEX IF EXISTS messages_owner_date_idx;
DROP INDEX IF EXISTS messages_owner_dialog_idx;
DROP TABLE IF EXISTS messages;

DROP INDEX IF EXISTS dialogs_user_top_message_idx;
ALTER TABLE dialogs DROP COLUMN IF EXISTS top_message_date;

DELETE FROM users WHERE id = 777000;
ALTER TABLE users
    DROP COLUMN IF EXISTS support,
    DROP COLUMN IF EXISTS verified;
