-- 0055_channel_paid_translation_settings rollback.

ALTER TABLE channel_admin_log_events
    DROP CONSTRAINT IF EXISTS channel_admin_log_events_type_check;

DELETE FROM channel_admin_log_events
WHERE event_type = 'toggle_autotranslation';

ALTER TABLE channel_admin_log_events
    ADD CONSTRAINT channel_admin_log_events_type_check CHECK (
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
    );

ALTER TABLE channels
    DROP CONSTRAINT IF EXISTS channels_send_paid_messages_stars_nonnegative_check,
    DROP COLUMN IF EXISTS send_paid_messages_stars,
    DROP COLUMN IF EXISTS broadcast_messages_allowed,
    DROP COLUMN IF EXISTS restricted_sponsored,
    DROP COLUMN IF EXISTS autotranslation;
