CREATE TABLE IF NOT EXISTS user_update_watermarks (
    user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    contiguous_pts INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS user_update_events_retention_idx
    ON user_update_events (user_id, date, pts);

CREATE INDEX IF NOT EXISTS dispatch_outbox_failed_cleanup_idx
    ON dispatch_outbox (status, updated_at, target_user_id, id)
    WHERE status = 'failed';
