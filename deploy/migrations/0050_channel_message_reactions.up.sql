CREATE TABLE IF NOT EXISTS channel_message_reactions (
    channel_id      BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    message_id      INT         NOT NULL,
    reacted_user_id BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    sender_user_id  BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    reaction_type   VARCHAR(16) NOT NULL,
    reaction_value  TEXT        NOT NULL,
    big             BOOLEAN     NOT NULL DEFAULT false,
    unread          BOOLEAN     NOT NULL DEFAULT false,
    chosen_order    INT         NOT NULL DEFAULT 1,
    reaction_date   INT         NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, message_id, reacted_user_id, reaction_type, reaction_value),
    FOREIGN KEY (channel_id, message_id) REFERENCES channel_messages(channel_id, id) ON DELETE CASCADE,
    CONSTRAINT channel_message_reactions_type_check CHECK (reaction_type IN ('emoji')),
    CONSTRAINT channel_message_reactions_value_check CHECK (reaction_value <> ''),
    CONSTRAINT channel_message_reactions_order_check CHECK (chosen_order > 0)
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_message_reactions_msg_date_idx
    ON channel_message_reactions (channel_id, message_id, reaction_date DESC, reacted_user_id DESC, reaction_value ASC);

CREATE INDEX IF NOT EXISTS channel_message_reactions_value_date_idx
    ON channel_message_reactions (channel_id, message_id, reaction_type, reaction_value, reaction_date DESC, reacted_user_id DESC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_message_reactions_p%s PARTITION OF channel_message_reactions FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
