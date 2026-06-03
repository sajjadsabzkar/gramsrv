CREATE TABLE IF NOT EXISTS account_reaction_settings (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    messages_notify_from TEXT NOT NULL DEFAULT 'contacts',
    stories_notify_from TEXT NOT NULL DEFAULT 'contacts',
    poll_votes_notify_from TEXT NOT NULL DEFAULT 'contacts',
    show_previews BOOLEAN NOT NULL DEFAULT TRUE,
    default_reaction_type TEXT NOT NULL DEFAULT 'emoji',
    default_reaction_value TEXT NOT NULL DEFAULT '👍',
    paid_privacy_kind TEXT NOT NULL DEFAULT 'default',
    paid_privacy_peer_type TEXT,
    paid_privacy_peer_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (messages_notify_from IN ('none', 'contacts', 'all')),
    CHECK (stories_notify_from IN ('none', 'contacts', 'all')),
    CHECK (poll_votes_notify_from IN ('none', 'contacts', 'all')),
    CHECK (default_reaction_type IN ('emoji')),
    CHECK (default_reaction_value <> ''),
    CHECK (paid_privacy_kind IN ('default', 'anonymous', 'peer')),
    CHECK (
        (paid_privacy_kind = 'peer' AND paid_privacy_peer_type IN ('user', 'channel') AND paid_privacy_peer_id IS NOT NULL)
        OR (paid_privacy_kind <> 'peer' AND paid_privacy_peer_type IS NULL AND paid_privacy_peer_id IS NULL)
    )
);
