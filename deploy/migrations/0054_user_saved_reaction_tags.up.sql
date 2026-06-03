CREATE TABLE IF NOT EXISTS user_saved_reaction_tags (
    user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reaction_type  VARCHAR(16) NOT NULL,
    reaction_value TEXT        NOT NULL,
    title          TEXT        NOT NULL DEFAULT '',
    reaction_count INT         NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, reaction_type, reaction_value),
    CHECK (reaction_type IN ('emoji')),
    CHECK (reaction_value <> ''),
    CHECK (reaction_count >= 0),
    CHECK (char_length(title) <= 12)
);

CREATE INDEX IF NOT EXISTS user_saved_reaction_tags_user_order_idx
    ON user_saved_reaction_tags (user_id, reaction_count DESC, updated_at DESC, reaction_value ASC);
