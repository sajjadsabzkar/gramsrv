ALTER TABLE channels
    DROP COLUMN IF EXISTS emoji_status_until,
    DROP COLUMN IF EXISTS emoji_status_document_id,
    DROP COLUMN IF EXISTS profile_color_background_emoji_id,
    DROP COLUMN IF EXISTS profile_color,
    DROP COLUMN IF EXISTS profile_color_set,
    DROP COLUMN IF EXISTS color_background_emoji_id,
    DROP COLUMN IF EXISTS color,
    DROP COLUMN IF EXISTS color_set;
