-- 0067_seed_document_id_namespace: normalize imported sticker/reaction
-- document ids into telesrv-owned storage ids.
--
-- The seed export keeps upstream Telegram document ids in JSON and filenames.
-- Those ids are source identifiers, not identities owned by this server.  Older
-- local databases stored them directly; normalize every catalog/message
-- reference so RPC, media snapshots and upload.getFile all use one server id.

CREATE OR REPLACE FUNCTION pg_temp.telesrv_seed_document_storage_id(source_id BIGINT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT CASE
        WHEN source_id > 4000000000000000000 THEN source_id - 4000000000000000000
        ELSE source_id
    END
$$;

CREATE OR REPLACE FUNCTION pg_temp.telesrv_seed_document_id_array(ids JSONB)
RETURNS JSONB
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(
        jsonb_agg(
            CASE
                WHEN value::text ~ '^-?[0-9]+$'
                    THEN to_jsonb(pg_temp.telesrv_seed_document_storage_id((value::text)::BIGINT))
                ELSE value
            END
            ORDER BY ord
        ),
        '[]'::jsonb
    )
    FROM jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(COALESCE(ids, 'null'::jsonb)) = 'array' THEN ids
            ELSE '[]'::jsonb
        END
    ) WITH ORDINALITY AS e(value, ord)
$$;

CREATE OR REPLACE FUNCTION pg_temp.telesrv_seed_sticker_packs_document_ids(packs JSONB)
RETURNS JSONB
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(
        jsonb_agg(
            CASE
                WHEN pack ? 'document_ids' THEN
                    jsonb_set(pack, '{document_ids}', pg_temp.telesrv_seed_document_id_array(pack->'document_ids'), false)
                WHEN pack ? 'documents' THEN
                    jsonb_set(pack, '{documents}', pg_temp.telesrv_seed_document_id_array(pack->'documents'), false)
                ELSE pack
            END
            ORDER BY ord
        ),
        '[]'::jsonb
    )
    FROM jsonb_array_elements(
        CASE
            WHEN jsonb_typeof(COALESCE(packs, 'null'::jsonb)) = 'array' THEN packs
            ELSE '[]'::jsonb
        END
    ) WITH ORDINALITY AS p(pack, ord)
$$;

DROP TABLE IF EXISTS seed_document_id_map;
CREATE TEMP TABLE seed_document_id_map AS
SELECT id AS old_id, pg_temp.telesrv_seed_document_storage_id(id) AS new_id
FROM documents
WHERE id > 4000000000000000000;

INSERT INTO documents (
    id, access_hash, file_reference, date, mime_type, size, dc_id, attributes, thumbs, created_at
)
SELECT
    m.new_id, d.access_hash, d.file_reference, d.date, d.mime_type, d.size, d.dc_id,
    d.attributes, d.thumbs, d.created_at
FROM documents d
JOIN seed_document_id_map m ON m.old_id = d.id
WHERE m.old_id <> m.new_id
ON CONFLICT (id) DO UPDATE SET
    access_hash = EXCLUDED.access_hash,
    file_reference = EXCLUDED.file_reference,
    date = EXCLUDED.date,
    mime_type = EXCLUDED.mime_type,
    size = EXCLUDED.size,
    dc_id = EXCLUDED.dc_id,
    attributes = EXCLUDED.attributes,
    thumbs = EXCLUDED.thumbs;

DELETE FROM documents d
USING seed_document_id_map m
WHERE d.id = m.old_id
  AND m.old_id <> m.new_id;

DROP TABLE IF EXISTS seed_file_blob_map;
CREATE TEMP TABLE seed_file_blob_map AS
SELECT
    location_key AS old_key,
    'doc:' ||
        pg_temp.telesrv_seed_document_storage_id((substring(location_key FROM '^doc:([0-9]+)'))::BIGINT)::TEXT ||
        COALESCE(substring(location_key FROM '^doc:[0-9]+(:.*)$'), '') AS new_key
FROM file_blobs
WHERE substring(location_key FROM '^doc:([0-9]+)') IS NOT NULL
  AND (substring(location_key FROM '^doc:([0-9]+)'))::BIGINT > 4000000000000000000;

INSERT INTO file_blobs (location_key, backend, object_key, size, sha256, mime_type, created_at)
SELECT m.new_key, b.backend, b.object_key, b.size, b.sha256, b.mime_type, b.created_at
FROM file_blobs b
JOIN seed_file_blob_map m ON m.old_key = b.location_key
WHERE m.old_key <> m.new_key
ON CONFLICT (location_key) DO UPDATE SET
    backend = EXCLUDED.backend,
    object_key = EXCLUDED.object_key,
    size = EXCLUDED.size,
    sha256 = EXCLUDED.sha256,
    mime_type = EXCLUDED.mime_type;

DELETE FROM file_blobs b
USING seed_file_blob_map m
WHERE b.location_key = m.old_key
  AND m.old_key <> m.new_key;

UPDATE sticker_sets
SET thumb_document_id = pg_temp.telesrv_seed_document_storage_id(thumb_document_id),
    document_ids = pg_temp.telesrv_seed_document_id_array(document_ids),
    packs = pg_temp.telesrv_seed_sticker_packs_document_ids(packs);

UPDATE available_reactions
SET static_icon_id = pg_temp.telesrv_seed_document_storage_id(static_icon_id),
    appear_animation_id = pg_temp.telesrv_seed_document_storage_id(appear_animation_id),
    select_animation_id = pg_temp.telesrv_seed_document_storage_id(select_animation_id),
    activate_animation_id = pg_temp.telesrv_seed_document_storage_id(activate_animation_id),
    effect_animation_id = pg_temp.telesrv_seed_document_storage_id(effect_animation_id),
    around_animation_id = pg_temp.telesrv_seed_document_storage_id(around_animation_id),
    center_icon_id = pg_temp.telesrv_seed_document_storage_id(center_icon_id);

UPDATE private_messages
SET media = jsonb_set(
    media,
    '{document,id}',
    to_jsonb(pg_temp.telesrv_seed_document_storage_id((media #>> '{document,id}')::BIGINT)),
    false
)
WHERE media->>'kind' = 'document'
  AND (media #>> '{document,id}') ~ '^[0-9]+$'
  AND (media #>> '{document,id}')::BIGINT > 4000000000000000000;

UPDATE message_boxes
SET media = jsonb_set(
    media,
    '{document,id}',
    to_jsonb(pg_temp.telesrv_seed_document_storage_id((media #>> '{document,id}')::BIGINT)),
    false
)
WHERE media->>'kind' = 'document'
  AND (media #>> '{document,id}') ~ '^[0-9]+$'
  AND (media #>> '{document,id}')::BIGINT > 4000000000000000000;

UPDATE channel_messages
SET media = jsonb_set(
    media,
    '{document,id}',
    to_jsonb(pg_temp.telesrv_seed_document_storage_id((media #>> '{document,id}')::BIGINT)),
    false
)
WHERE media->>'kind' = 'document'
  AND (media #>> '{document,id}') ~ '^[0-9]+$'
  AND (media #>> '{document,id}')::BIGINT > 4000000000000000000;

UPDATE channels
SET color_background_emoji_id = pg_temp.telesrv_seed_document_storage_id(color_background_emoji_id),
    profile_color_background_emoji_id = pg_temp.telesrv_seed_document_storage_id(profile_color_background_emoji_id),
    emoji_status_document_id = pg_temp.telesrv_seed_document_storage_id(emoji_status_document_id),
    available_reactions = CASE
        WHEN available_reactions ? 'CustomEmojiIDs' THEN
            jsonb_set(
                available_reactions,
                '{CustomEmojiIDs}',
                pg_temp.telesrv_seed_document_id_array(available_reactions->'CustomEmojiIDs'),
                false
            )
        ELSE available_reactions
    END;

DROP TABLE IF EXISTS seed_file_blob_map;
DROP TABLE IF EXISTS seed_document_id_map;
