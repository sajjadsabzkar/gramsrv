-- 0055_channel_paid_translation_settings: persist Layer 225 channel setting flags.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS autotranslation BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS restricted_sponsored BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS broadcast_messages_allowed BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS send_paid_messages_stars BIGINT NOT NULL DEFAULT 0;

ALTER TABLE channels
    ADD CONSTRAINT channels_send_paid_messages_stars_nonnegative_check CHECK (send_paid_messages_stars >= 0);

ALTER TABLE channel_admin_log_events
    DROP CONSTRAINT IF EXISTS channel_admin_log_events_type_check;

ALTER TABLE channel_admin_log_events
    ADD CONSTRAINT channel_admin_log_events_type_check CHECK (
        event_type IN (
            'change_title',
            'change_username',
            'change_linked_chat',
            'toggle_signatures',
            'toggle_pre_history_hidden',
            'toggle_forum',
            'toggle_autotranslation',
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
    );
