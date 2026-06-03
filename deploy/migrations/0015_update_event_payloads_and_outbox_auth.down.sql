ALTER TABLE dispatch_outbox
    DROP COLUMN IF EXISTS exclude_auth_key_id;

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS peer_settings;

ALTER TABLE user_update_events
    DROP COLUMN IF EXISTS event_peers;
