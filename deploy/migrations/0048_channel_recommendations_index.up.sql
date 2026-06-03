CREATE INDEX IF NOT EXISTS channels_public_broadcast_recommendations_idx
    ON channels (participants_count DESC, date DESC, id DESC)
    WHERE broadcast AND NOT megagroup AND NOT deleted AND COALESCE(username, '') <> '';
