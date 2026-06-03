CREATE TABLE IF NOT EXISTS user_top_reactions (
    user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reaction_type  VARCHAR(16) NOT NULL,
    reaction_value TEXT        NOT NULL,
    reaction_count INT         NOT NULL DEFAULT 0,
    reaction_date  INT         NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, reaction_type, reaction_value),
    CHECK (reaction_type IN ('emoji')),
    CHECK (reaction_value <> ''),
    CHECK (reaction_count >= 0)
);

CREATE INDEX IF NOT EXISTS user_top_reactions_user_rank_idx
    ON user_top_reactions (user_id, reaction_count DESC, reaction_date DESC, updated_at DESC, reaction_value ASC);
