-- 0057_media: media pipeline storage — upload parts, file blobs, documents,
-- photos, sticker sets, available reactions, profile photos; message media column.
--
-- Registry tables (documents/photos/sticker_sets/available_reactions/file_blobs)
-- are low-cardinality global catalogs and are not partitioned. upload_parts is
-- transient (assembled then deleted). Blob bytes live on the configured backend
-- (localfs); file_blobs only indexes location_key -> backend/object_key.

-- 上传分片：upload.saveFilePart / saveBigFilePart 累积，组装成 blob 后清理。
CREATE TABLE IF NOT EXISTS upload_parts (
    owner_user_id BIGINT      NOT NULL,
    file_id       BIGINT      NOT NULL,
    part          INT         NOT NULL,
    total_parts   INT         NOT NULL DEFAULT 0,
    is_big        BOOLEAN     NOT NULL DEFAULT false,
    bytes         BYTEA       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_user_id, file_id, part)
);

-- file_blobs：可下载二进制对象的索引（location_key -> backend/object_key）。
CREATE TABLE IF NOT EXISTS file_blobs (
    location_key TEXT        PRIMARY KEY,
    backend      TEXT        NOT NULL DEFAULT 'localfs',
    object_key   TEXT        NOT NULL,
    size         BIGINT      NOT NULL DEFAULT 0,
    sha256       BYTEA       NOT NULL DEFAULT ''::bytea,
    mime_type    TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- documents：Telegram 文档（贴纸/gif/文件/视频/音频/自定义 emoji）元数据。
CREATE TABLE IF NOT EXISTS documents (
    id             BIGINT      PRIMARY KEY,
    access_hash    BIGINT      NOT NULL DEFAULT 0,
    file_reference BYTEA       NOT NULL DEFAULT '',
    date           INT         NOT NULL DEFAULT 0,
    mime_type      TEXT        NOT NULL DEFAULT '',
    size           BIGINT      NOT NULL DEFAULT 0,
    dc_id          INT         NOT NULL DEFAULT 0,
    attributes     JSONB       NOT NULL DEFAULT '[]'::jsonb,
    thumbs         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- photos：Telegram 照片（头像/图片消息）元数据。
CREATE TABLE IF NOT EXISTS photos (
    id             BIGINT      PRIMARY KEY,
    access_hash    BIGINT      NOT NULL DEFAULT 0,
    file_reference BYTEA       NOT NULL DEFAULT '',
    date           INT         NOT NULL DEFAULT 0,
    dc_id          INT         NOT NULL DEFAULT 0,
    has_stickers   BOOLEAN     NOT NULL DEFAULT false,
    sizes          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- sticker_sets：贴纸 / 自定义 emoji 集（含有序文档 id 与 packs）。
CREATE TABLE IF NOT EXISTS sticker_sets (
    id                BIGINT      PRIMARY KEY,
    access_hash       BIGINT      NOT NULL DEFAULT 0,
    short_name        TEXT        NOT NULL DEFAULT '',
    title             TEXT        NOT NULL DEFAULT '',
    count             INT         NOT NULL DEFAULT 0,
    hash              INT         NOT NULL DEFAULT 0,
    set_kind          TEXT        NOT NULL DEFAULT 'stickers',
    official          BOOLEAN     NOT NULL DEFAULT false,
    animated          BOOLEAN     NOT NULL DEFAULT false,
    videos            BOOLEAN     NOT NULL DEFAULT false,
    emojis            BOOLEAN     NOT NULL DEFAULT false,
    masks             BOOLEAN     NOT NULL DEFAULT false,
    installed         BOOLEAN     NOT NULL DEFAULT false,
    archived          BOOLEAN     NOT NULL DEFAULT false,
    installed_date    INT         NOT NULL DEFAULT 0,
    thumb_document_id BIGINT      NOT NULL DEFAULT 0,
    thumbs            JSONB       NOT NULL DEFAULT '[]'::jsonb,
    thumb_dc_id       INT         NOT NULL DEFAULT 0,
    thumb_version     INT         NOT NULL DEFAULT 0,
    document_ids      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    packs             JSONB       NOT NULL DEFAULT '[]'::jsonb,
    sort_order        INT         NOT NULL DEFAULT 0,
    system_key        TEXT        NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS sticker_sets_short_name_idx
    ON sticker_sets (short_name) WHERE short_name <> '';
CREATE INDEX IF NOT EXISTS sticker_sets_kind_order_idx
    ON sticker_sets (set_kind, sort_order, id);
CREATE INDEX IF NOT EXISTS sticker_sets_system_key_idx
    ON sticker_sets (system_key) WHERE system_key <> '';

-- available_reactions：messages.getAvailableReactions 目录（引用真实文档 id）。
CREATE TABLE IF NOT EXISTS available_reactions (
    reaction              TEXT    PRIMARY KEY,
    title                 TEXT    NOT NULL DEFAULT '',
    inactive              BOOLEAN NOT NULL DEFAULT false,
    premium               BOOLEAN NOT NULL DEFAULT false,
    static_icon_id        BIGINT  NOT NULL DEFAULT 0,
    appear_animation_id   BIGINT  NOT NULL DEFAULT 0,
    select_animation_id   BIGINT  NOT NULL DEFAULT 0,
    activate_animation_id BIGINT  NOT NULL DEFAULT 0,
    effect_animation_id   BIGINT  NOT NULL DEFAULT 0,
    around_animation_id   BIGINT  NOT NULL DEFAULT 0,
    center_icon_id        BIGINT  NOT NULL DEFAULT 0,
    sort_order            INT     NOT NULL DEFAULT 0
);

-- profile_photos：用户/频道头像历史（current = active 中 sort_order 最大者）。
CREATE TABLE IF NOT EXISTS profile_photos (
    owner_peer_type TEXT        NOT NULL,
    owner_peer_id   BIGINT      NOT NULL,
    photo_id        BIGINT      NOT NULL,
    date            INT         NOT NULL DEFAULT 0,
    active          BOOLEAN     NOT NULL DEFAULT true,
    sort_order      BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_peer_type, owner_peer_id, photo_id),
    CONSTRAINT profile_photos_peer_type_check CHECK (owner_peer_type IN ('user', 'channel'))
);

CREATE INDEX IF NOT EXISTS profile_photos_current_idx
    ON profile_photos (owner_peer_type, owner_peer_id, sort_order DESC)
    WHERE active;

-- 消息媒体快照列：避免历史读取再 join documents/photos。
ALTER TABLE private_messages
    ADD COLUMN IF NOT EXISTS media JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE message_boxes
    ADD COLUMN IF NOT EXISTS media JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE channel_messages
    ADD COLUMN IF NOT EXISTS media JSONB NOT NULL DEFAULT '{}'::jsonb;

-- 放宽 body 非空约束：允许「仅媒体」消息（图片/文件/贴纸，无文字 caption）。
ALTER TABLE private_messages DROP CONSTRAINT IF EXISTS private_messages_nonempty_body;
ALTER TABLE private_messages
    ADD CONSTRAINT private_messages_nonempty_body
    CHECK (body <> '' OR media <> '{}'::jsonb);

ALTER TABLE channel_messages DROP CONSTRAINT IF EXISTS channel_messages_content_check;
ALTER TABLE channel_messages
    ADD CONSTRAINT channel_messages_content_check
    CHECK (body <> '' OR action <> '{}'::jsonb OR media <> '{}'::jsonb);
