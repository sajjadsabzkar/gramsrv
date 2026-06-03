-- 0030_user_update_channel_peers rollback.

ALTER TABLE user_update_events
    DROP CONSTRAINT IF EXISTS user_update_events_peer_type_check;

ALTER TABLE user_update_events
    ADD CONSTRAINT user_update_events_peer_type_check
    CHECK (peer_type IS NULL OR peer_type IN ('user'));
