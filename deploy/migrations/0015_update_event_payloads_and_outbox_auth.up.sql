-- 0015_update_event_payloads_and_outbox_auth: keep setting-update payloads durable.
--
-- event_peers carries ordered dialog peers for updatePinnedDialogs.order.
-- peer_settings carries updatePeerSettings flags.
-- exclude_auth_key_id makes outbox exclusion precise for same session_id across auth keys.

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS event_peers JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE user_update_events
    ADD COLUMN IF NOT EXISTS peer_settings JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE dispatch_outbox
    ADD COLUMN IF NOT EXISTS exclude_auth_key_id BIGINT NOT NULL DEFAULT 0;
