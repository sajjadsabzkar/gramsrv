-- 0022_channels: supergroup/channel storage.
--
-- Channel messages are single-copy. Per-user dialog/read state is stored separately.
-- Channel pts is scoped by channel_id and persisted in channel_update_events.

CREATE TABLE IF NOT EXISTS channels (
    id                    BIGINT      NOT NULL,
    access_hash           BIGINT      NOT NULL,
    creator_user_id       BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    title                 TEXT        NOT NULL,
    about                 TEXT        NOT NULL DEFAULT '',
    username              TEXT,
    broadcast             BOOLEAN     NOT NULL DEFAULT false,
    megagroup             BOOLEAN     NOT NULL DEFAULT false,
    forum                 BOOLEAN     NOT NULL DEFAULT false,
    forum_tabs            BOOLEAN     NOT NULL DEFAULT false,
    noforwards            BOOLEAN     NOT NULL DEFAULT false,
    join_to_send          BOOLEAN     NOT NULL DEFAULT false,
    join_request          BOOLEAN     NOT NULL DEFAULT false,
    signatures            BOOLEAN     NOT NULL DEFAULT false,
    pre_history_hidden    BOOLEAN     NOT NULL DEFAULT false,
    participants_hidden   BOOLEAN     NOT NULL DEFAULT false,
    antispam              BOOLEAN     NOT NULL DEFAULT false,
    linked_chat_id        BIGINT      NOT NULL DEFAULT 0,
    slowmode_seconds      INT         NOT NULL DEFAULT 0,
    default_banned_rights JSONB       NOT NULL DEFAULT '{}'::jsonb,
    available_reactions   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    color_set             BOOLEAN     NOT NULL DEFAULT false,
    color                 INT         NOT NULL DEFAULT 0,
    color_background_emoji_id BIGINT  NOT NULL DEFAULT 0,
    profile_color_set     BOOLEAN     NOT NULL DEFAULT false,
    profile_color         INT         NOT NULL DEFAULT 0,
    profile_color_background_emoji_id BIGINT NOT NULL DEFAULT 0,
    emoji_status_document_id BIGINT   NOT NULL DEFAULT 0,
    emoji_status_until    INT         NOT NULL DEFAULT 0,
    participants_count    INT         NOT NULL DEFAULT 0,
    admins_count          INT         NOT NULL DEFAULT 0,
    kicked_count          INT         NOT NULL DEFAULT 0,
    banned_count          INT         NOT NULL DEFAULT 0,
    top_message_id        INT         NOT NULL DEFAULT 0,
    pts                   INT         NOT NULL DEFAULT 0,
    admin_log_seq         BIGINT      NOT NULL DEFAULT 0,
    ttl_period            INT         NOT NULL DEFAULT 0,
    date                  INT         NOT NULL,
    deleted               BOOLEAN     NOT NULL DEFAULT false,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id),
    CONSTRAINT channels_kind_check CHECK (
        ((broadcast AND NOT megagroup AND NOT forum)
        OR (megagroup AND NOT broadcast))
        AND (NOT forum_tabs OR forum)
    ),
    CONSTRAINT channels_title_nonempty_check CHECK (title <> '')
) PARTITION BY HASH (id);

CREATE UNIQUE INDEX IF NOT EXISTS channels_access_hash_idx
    ON channels (id, access_hash);
CREATE INDEX IF NOT EXISTS channels_creator_idx
    ON channels (creator_user_id, id DESC)
    WHERE NOT deleted;
CREATE INDEX IF NOT EXISTS channels_linked_chat_idx
    ON channels (linked_chat_id)
    WHERE linked_chat_id <> 0 AND NOT deleted;

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channels_p%s PARTITION OF channels FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

-- PostgreSQL global unique indexes on partitioned tables must include the partition key.
-- Keep username uniqueness in a compact lookup table instead of relying on channels(username).
CREATE TABLE IF NOT EXISTS channel_usernames (
    username_lower TEXT        PRIMARY KEY,
    channel_id     BIGINT      NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT channel_usernames_nonempty_check CHECK (username_lower <> '')
);

CREATE TABLE IF NOT EXISTS channel_members (
    channel_id         BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id            BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    inviter_user_id    BIGINT      NOT NULL DEFAULT 0,
    role               VARCHAR(16) NOT NULL DEFAULT 'member',
    status             VARCHAR(16) NOT NULL DEFAULT 'active',
    joined_at          INT         NOT NULL DEFAULT 0,
    left_at            INT         NOT NULL DEFAULT 0,
    admin_rights       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    banned_rights      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    rank               TEXT        NOT NULL DEFAULT '',
    available_min_id   INT         NOT NULL DEFAULT 0,
    available_min_pts  INT         NOT NULL DEFAULT 0,
    read_inbox_max_id  INT         NOT NULL DEFAULT 0,
    read_inbox_date    INT         NOT NULL DEFAULT 0,
    read_outbox_max_id INT         NOT NULL DEFAULT 0,
    unread_mark        BOOLEAN     NOT NULL DEFAULT false,
    slowmode_last_send_date INT     NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, user_id),
    CONSTRAINT channel_members_role_check CHECK (role IN ('creator', 'admin', 'member')),
    CONSTRAINT channel_members_status_check CHECK (status IN ('active', 'left', 'kicked', 'banned'))
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_members_user_active_idx
    ON channel_members (user_id, channel_id)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS channel_members_user_left_idx
    ON channel_members (user_id, left_at DESC, channel_id DESC)
    WHERE status = 'left';
CREATE INDEX IF NOT EXISTS channel_members_channel_role_idx
    ON channel_members (channel_id, role, user_id)
    WHERE status = 'active';
CREATE INDEX IF NOT EXISTS channel_members_read_participants_idx
    ON channel_members (channel_id, read_inbox_max_id, user_id)
    WHERE status = 'active';

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_members_p%s PARTITION OF channel_members FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS channel_messages (
    channel_id          BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    id                  INT         NOT NULL,
    random_id           BIGINT      NOT NULL DEFAULT 0,
    sender_user_id      BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    from_peer_type      VARCHAR(16) NOT NULL DEFAULT 'user',
    from_peer_id        BIGINT      NOT NULL,
    send_as_peer_type   VARCHAR(16),
    send_as_peer_id     BIGINT,
    message_date        INT         NOT NULL,
    edit_date           INT         NOT NULL DEFAULT 0,
    post                BOOLEAN     NOT NULL DEFAULT false,
    silent              BOOLEAN     NOT NULL DEFAULT false,
    noforwards          BOOLEAN     NOT NULL DEFAULT false,
    body                TEXT        NOT NULL DEFAULT '',
    entities            JSONB       NOT NULL DEFAULT '[]'::jsonb,
    reply_to            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    reply_to_msg_id     INT         NOT NULL DEFAULT 0,
    reply_to_peer_type  VARCHAR(16) NOT NULL DEFAULT '',
    reply_to_peer_id    BIGINT      NOT NULL DEFAULT 0,
    reply_to_top_id     INT         NOT NULL DEFAULT 0,
    fwd_from            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    discussion_channel_id BIGINT    NOT NULL DEFAULT 0,
    discussion_message_id INT       NOT NULL DEFAULT 0,
    action              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    pts                 INT         NOT NULL,
    deleted             BOOLEAN     NOT NULL DEFAULT false,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, id),
    CONSTRAINT channel_messages_peer_type_check CHECK (
        from_peer_type IN ('user', 'channel')
        AND (send_as_peer_type IS NULL OR send_as_peer_type IN ('user', 'channel'))
        AND (reply_to_peer_type = '' OR reply_to_peer_type IN ('user', 'channel'))
    ),
    CONSTRAINT channel_messages_content_check CHECK (body <> '' OR action <> '{}'::jsonb)
) PARTITION BY HASH (channel_id);

CREATE UNIQUE INDEX IF NOT EXISTS channel_messages_random_idx
    ON channel_messages (channel_id, sender_user_id, random_id)
    WHERE random_id <> 0;
CREATE INDEX IF NOT EXISTS channel_messages_history_idx
    ON channel_messages (channel_id, id DESC)
    WHERE NOT deleted;
CREATE INDEX IF NOT EXISTS channel_messages_sender_history_idx
    ON channel_messages (channel_id, sender_user_id, id DESC)
    WHERE NOT deleted;
CREATE INDEX IF NOT EXISTS channel_messages_date_idx
    ON channel_messages (channel_id, message_date DESC, id DESC)
    WHERE NOT deleted;
CREATE INDEX IF NOT EXISTS channel_messages_reply_thread_idx
    ON channel_messages (channel_id, reply_to_top_id, id DESC)
    WHERE reply_to_top_id > 0 AND NOT deleted;
CREATE INDEX IF NOT EXISTS channel_messages_discussion_ref_idx
    ON channel_messages (discussion_channel_id, discussion_message_id)
    WHERE discussion_channel_id <> 0 AND discussion_message_id <> 0 AND NOT deleted;

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_messages_p%s PARTITION OF channel_messages FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS channel_update_events (
    channel_id     BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    pts            INT         NOT NULL,
    pts_count      INT         NOT NULL DEFAULT 1,
    date           INT         NOT NULL,
    event_type     VARCHAR(32) NOT NULL,
    message_id     INT         NOT NULL DEFAULT 0,
    message_ids    JSONB       NOT NULL DEFAULT '[]'::jsonb,
    sender_user_id BIGINT      NOT NULL DEFAULT 0,
    user_ids       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, pts),
    CONSTRAINT channel_update_events_pts_count_check CHECK (pts_count > 0),
    CONSTRAINT channel_update_events_type_check CHECK (
        event_type IN (
            'new_channel_message',
            'edit_channel_message',
            'delete_channel_messages',
            'channel_participant',
            'pinned_channel_messages',
            'noop'
        )
    )
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_update_events_scan_idx
    ON channel_update_events (channel_id, pts ASC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_update_events_p%s PARTITION OF channel_update_events FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS channel_admin_log_events (
    channel_id           BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    id                   BIGINT      NOT NULL,
    actor_user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    event_date           INT         NOT NULL,
    event_type           VARCHAR(48) NOT NULL,
    prev_string          TEXT        NOT NULL DEFAULT '',
    new_string           TEXT        NOT NULL DEFAULT '',
    prev_bool            BOOLEAN     NOT NULL DEFAULT false,
    new_bool             BOOLEAN     NOT NULL DEFAULT false,
    prev_int             INT         NOT NULL DEFAULT 0,
    new_int              INT         NOT NULL DEFAULT 0,
    prev_participant     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    new_participant      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    participant          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    message              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    prev_message         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    new_message          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    query                TEXT        NOT NULL DEFAULT '',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, id),
    CONSTRAINT channel_admin_log_events_type_check CHECK (
        event_type IN (
            'change_title',
            'change_username',
            'change_linked_chat',
            'toggle_signatures',
            'toggle_pre_history_hidden',
            'toggle_forum',
            'toggle_anti_spam',
            'toggle_slow_mode',
            'participant_invite',
            'participant_join',
            'participant_leave',
            'participant_promote',
            'participant_demote',
            'participant_ban',
            'participant_unban',
            'participant_kick',
            'participant_unkick',
            'update_pinned',
            'send_message',
            'edit_message',
            'delete_message'
        )
    )
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_admin_log_events_scan_idx
    ON channel_admin_log_events (channel_id, id DESC);
CREATE INDEX IF NOT EXISTS channel_admin_log_events_actor_idx
    ON channel_admin_log_events (channel_id, actor_user_id, id DESC);
CREATE INDEX IF NOT EXISTS channel_admin_log_events_type_idx
    ON channel_admin_log_events (channel_id, event_type, id DESC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_admin_log_events_p%s PARTITION OF channel_admin_log_events FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS channel_dialogs (
    user_id                  BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel_id               BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    folder_id                INT         NOT NULL DEFAULT 0,
    top_message_id           INT         NOT NULL DEFAULT 0,
    top_message_date         INT         NOT NULL DEFAULT 0,
    read_inbox_max_id        INT         NOT NULL DEFAULT 0,
    read_outbox_max_id       INT         NOT NULL DEFAULT 0,
    unread_count             INT         NOT NULL DEFAULT 0,
    unread_mentions_count    INT         NOT NULL DEFAULT 0,
    unread_reactions_count   INT         NOT NULL DEFAULT 0,
    pinned                   BOOLEAN     NOT NULL DEFAULT false,
    pinned_order             INT         NOT NULL DEFAULT 0,
    unread_mark              BOOLEAN     NOT NULL DEFAULT false,
    view_forum_as_messages   BOOLEAN     NOT NULL DEFAULT false,
    notify_settings          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, channel_id),
    CONSTRAINT channel_dialogs_folder_id_check CHECK (folder_id >= 0)
) PARTITION BY HASH (user_id);

CREATE INDEX IF NOT EXISTS channel_dialogs_user_top_idx
    ON channel_dialogs (user_id, folder_id, pinned DESC, pinned_order DESC, top_message_date DESC, top_message_id DESC, channel_id DESC);
CREATE INDEX IF NOT EXISTS channel_dialogs_pinned_idx
    ON channel_dialogs (user_id, pinned)
    WHERE pinned;

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_dialogs_p%s PARTITION OF channel_dialogs FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;

CREATE TABLE IF NOT EXISTS channel_invites (
    channel_id    BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    invite_id     BIGINT      NOT NULL,
    hash          TEXT        NOT NULL,
    admin_user_id BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    title         TEXT        NOT NULL DEFAULT '',
    permanent     BOOLEAN     NOT NULL DEFAULT false,
    revoked       BOOLEAN     NOT NULL DEFAULT false,
    request_needed BOOLEAN    NOT NULL DEFAULT false,
    expire_date   INT,
    usage_limit   INT,
    usage_count   INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, invite_id)
) PARTITION BY HASH (channel_id);

-- Keep invite hash uniqueness outside the partitioned table because PostgreSQL
-- requires global unique indexes on partitions to include the partition key.
CREATE TABLE IF NOT EXISTS channel_invite_hashes (
    hash       TEXT        PRIMARY KEY,
    channel_id BIGINT      NOT NULL,
    invite_id  BIGINT      NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT channel_invite_hashes_nonempty_check CHECK (hash <> '')
);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_invites_p%s PARTITION OF channel_invites FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
