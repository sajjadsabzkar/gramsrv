-- 0045_channel_forum_tabs rollback.

ALTER TABLE channel_admin_log_events
    DROP CONSTRAINT IF EXISTS channel_admin_log_events_type_check;

DELETE FROM channel_admin_log_events
WHERE event_type = 'toggle_forum';

ALTER TABLE channel_admin_log_events
    ADD CONSTRAINT channel_admin_log_events_type_check CHECK (
        event_type IN (
            'change_title',
            'change_username',
            'change_linked_chat',
            'toggle_signatures',
            'toggle_pre_history_hidden',
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
    DROP CONSTRAINT IF EXISTS channels_kind_check;

ALTER TABLE channels
    ADD CONSTRAINT channels_kind_check CHECK (
        (broadcast AND NOT megagroup AND NOT forum)
        OR (megagroup AND NOT broadcast)
    );

ALTER TABLE channels
    DROP COLUMN IF EXISTS forum_tabs;
