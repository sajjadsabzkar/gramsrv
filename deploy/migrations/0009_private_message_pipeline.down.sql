-- 0009 rollback: remove second-stage private message pipeline tables.
DROP TABLE IF EXISTS dispatch_outbox;
DROP TABLE IF EXISTS user_update_events;
DROP TABLE IF EXISTS dialogs;
DROP TABLE IF EXISTS message_boxes;
DROP TABLE IF EXISTS private_messages;

ALTER TABLE IF EXISTS dialogs_legacy RENAME TO dialogs;
ALTER TABLE IF EXISTS messages_legacy RENAME TO messages;

ALTER TABLE update_states DROP CONSTRAINT IF EXISTS update_states_pkey;
ALTER TABLE update_states DROP COLUMN IF EXISTS user_id;
ALTER TABLE update_states ADD PRIMARY KEY (auth_key_id);
