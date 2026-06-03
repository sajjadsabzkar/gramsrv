-- 0044_channel_antispam: persist native antispam state and expose TDesktop config.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS antispam BOOLEAN NOT NULL DEFAULT false;

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

UPDATE app_configs
SET hash = 4,
    config_json = config_json
        || jsonb_build_object(
            'telegram_antispam_group_size_min', 200,
            'telegram_antispam_user_id', '5434988373'
        ),
    updated_at = now()
WHERE client = 'tdesktop';
