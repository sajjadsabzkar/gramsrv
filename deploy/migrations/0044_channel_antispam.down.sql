-- 0044_channel_antispam rollback.

UPDATE app_configs
SET hash = 3,
    config_json = config_json
        - 'telegram_antispam_group_size_min'
        - 'telegram_antispam_user_id',
    updated_at = now()
WHERE client = 'tdesktop' AND hash = 4;

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
    DROP COLUMN IF EXISTS antispam;
