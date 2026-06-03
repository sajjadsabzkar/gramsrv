-- 0057_media down: drop media catalogs and message media columns; restore body checks.

ALTER TABLE channel_messages DROP CONSTRAINT IF EXISTS channel_messages_content_check;
ALTER TABLE channel_messages
    ADD CONSTRAINT channel_messages_content_check
    CHECK (body <> '' OR action <> '{}'::jsonb);

ALTER TABLE private_messages DROP CONSTRAINT IF EXISTS private_messages_nonempty_body;
ALTER TABLE private_messages
    ADD CONSTRAINT private_messages_nonempty_body
    CHECK (body <> '');

ALTER TABLE channel_messages DROP COLUMN IF EXISTS media;
ALTER TABLE message_boxes DROP COLUMN IF EXISTS media;
ALTER TABLE private_messages DROP COLUMN IF EXISTS media;

DROP TABLE IF EXISTS profile_photos;
DROP TABLE IF EXISTS available_reactions;
DROP TABLE IF EXISTS sticker_sets;
DROP TABLE IF EXISTS photos;
DROP TABLE IF EXISTS documents;
DROP TABLE IF EXISTS file_blobs;
DROP TABLE IF EXISTS upload_parts;
