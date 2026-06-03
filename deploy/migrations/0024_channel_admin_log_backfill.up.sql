-- 0024_channel_admin_log_backfill: bring existing developer DBs forward after
-- channel settings/admin-log fields were added to the initial 0022 draft.
--
-- Fresh databases already get these objects from 0022; every statement here is
-- idempotent so old local databases can migrate without reset.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS pre_history_hidden BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS slowmode_seconds INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS admin_log_seq BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS channel_admin_log_events (
    channel_id           BIGINT      NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    id                   BIGINT      NOT NULL,
    actor_user_id        BIGINT      NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    event_date           INT         NOT NULL,
    event_type           VARCHAR(48) NOT NULL,
    prev_string          TEXT        NOT NULL DEFAULT '',
    new_string           TEXT        NOT NULL DEFAULT '',
    prev_bool            BOOLEAN     NOT NULL DEFAULT false,
    new_bool             BOOLEAN     NOT NULL DEFAULT false,
    prev_int             INT         NOT NULL DEFAULT 0,
    new_int              INT         NOT NULL DEFAULT 0,
    prev_participant     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    new_participant      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    participant          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    message              JSONB       NOT NULL DEFAULT '{}'::jsonb,
    prev_message         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    new_message          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    query                TEXT        NOT NULL DEFAULT '',
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, id),
    CONSTRAINT channel_admin_log_events_type_check CHECK (
        event_type IN (
            'change_title',
            'change_username',
            'change_linked_chat',
            'toggle_signatures',
            'toggle_pre_history_hidden',
            'toggle_forum',
            'toggle_anti_spam',
            'toggle_slow_mode',
            'participant_invite',
            'participant_join',
            'participant_leave',
            'participant_promote',
            'participant_demote',
            'participant_ban',
            'participant_unban',
            'participant_kick',
            'participant_unkick',
            'update_pinned',
            'send_message',
            'edit_message',
            'delete_message'
        )
    )
) PARTITION BY HASH (channel_id);

CREATE INDEX IF NOT EXISTS channel_admin_log_events_scan_idx
    ON channel_admin_log_events (channel_id, id DESC);
CREATE INDEX IF NOT EXISTS channel_admin_log_events_actor_idx
    ON channel_admin_log_events (channel_id, actor_user_id, id DESC);
CREATE INDEX IF NOT EXISTS channel_admin_log_events_type_idx
    ON channel_admin_log_events (channel_id, event_type, id DESC);

DO $$
DECLARE
    i int;
BEGIN
    FOR i IN 0..63 LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS channel_admin_log_events_p%s PARTITION OF channel_admin_log_events FOR VALUES WITH (MODULUS 64, REMAINDER %s)',
            lpad(i::text, 2, '0'),
            i
        );
    END LOOP;
END $$;
