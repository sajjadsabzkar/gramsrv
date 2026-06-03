-- 0006_update_events: minimal auth-key update queue for getDifference补偿.

CREATE TABLE IF NOT EXISTS update_events (
    auth_key_id BIGINT      NOT NULL REFERENCES auth_keys(auth_key_id) ON DELETE CASCADE,
    pts         INT         NOT NULL,
    pts_count   INT         NOT NULL DEFAULT 1,
    date        INT         NOT NULL,
    event_type  VARCHAR(32) NOT NULL,
    message_id  INT         REFERENCES messages(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (auth_key_id, pts),
    CONSTRAINT update_events_type_check CHECK (event_type IN ('new_message'))
);

CREATE INDEX IF NOT EXISTS update_events_auth_pts_idx
    ON update_events (auth_key_id, pts);
