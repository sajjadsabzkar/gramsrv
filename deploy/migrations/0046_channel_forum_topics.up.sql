-- 0046_channel_forum_topics: forum topic read model.
--
-- Topic ids are the channel service message ids produced by
-- messages.createForumTopic. Messages stay single-copy in channel_messages;
-- this table stores the bounded topic index/read counters TDesktop needs for
-- messages.getForumTopics/getForumTopicsByID.

CREATE TABLE IF NOT EXISTS channel_forum_topics (
    channel_id                BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    topic_id                  INT         NOT NULL,
    creator_user_id           BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    title                     TEXT        NOT NULL DEFAULT '',
    icon_color                INT         NOT NULL DEFAULT 0,
    icon_emoji_id             BIGINT      NOT NULL DEFAULT 0,
    title_missing             BOOLEAN     NOT NULL DEFAULT false,
    closed                    BOOLEAN     NOT NULL DEFAULT false,
    hidden                    BOOLEAN     NOT NULL DEFAULT false,
    pinned                    BOOLEAN     NOT NULL DEFAULT false,
    pinned_order              INT         NOT NULL DEFAULT 0,
    date                      INT         NOT NULL,
    top_message_id            INT         NOT NULL DEFAULT 0,
    read_inbox_max_id         INT         NOT NULL DEFAULT 0,
    read_outbox_max_id        INT         NOT NULL DEFAULT 0,
    unread_count              INT         NOT NULL DEFAULT 0,
    unread_mentions_count     INT         NOT NULL DEFAULT 0,
    unread_reactions_count    INT         NOT NULL DEFAULT 0,
    unread_poll_votes_count   INT         NOT NULL DEFAULT 0,
    deleted                   BOOLEAN     NOT NULL DEFAULT false,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, topic_id),
    CONSTRAINT channel_forum_topics_title_check CHECK (title <> '' OR title_missing),
    CONSTRAINT channel_forum_topics_ids_check CHECK (topic_id > 0 AND top_message_id >= 0)
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_forum_topics_page_idx
    ON channel_forum_topics (channel_id, pinned DESC, pinned_order DESC, date DESC, topic_id DESC)
    WHERE NOT deleted;

CREATE INDEX IF NOT EXISTS channel_forum_topics_title_idx
    ON channel_forum_topics (channel_id, lower(title), date DESC, topic_id DESC)
    WHERE NOT deleted;

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_forum_topics_p%s PARTITION OF channel_forum_topics FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
