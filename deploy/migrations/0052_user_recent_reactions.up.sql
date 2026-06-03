CREATE TABLE IF NOT EXISTS user_recent_reactions (
    user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reaction_type  VARCHAR(16) NOT NULL,
    reaction_value TEXT        NOT NULL,
    reaction_date  INT         NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, reaction_type, reaction_value),
    CHECK (reaction_type IN ('emoji')),
    CHECK (reaction_value <> '')
);

CREATE INDEX IF NOT EXISTS user_recent_reactions_user_date_idx
    ON user_recent_reactions (user_id, reaction_date DESC, updated_at DESC, reaction_value ASC);
