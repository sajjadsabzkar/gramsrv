-- 0045_channel_forum_tabs: persist Layer 225 forum layout and admin-log action.

ALTER TABLE channels
    ADD COLUMN IF NOT EXISTS forum_tabs BOOLEAN NOT NULL DEFAULT false;

UPDATE channels
SET forum_tabs = false
WHERE NOT forum AND forum_tabs;

ALTER TABLE channels
    DROP CONSTRAINT IF EXISTS channels_kind_check;

ALTER TABLE channels
    ADD CONSTRAINT channels_kind_check CHECK (
        ((broadcast AND NOT megagroup AND NOT forum)
        OR (megagroup AND NOT broadcast))
        AND (NOT forum_tabs OR forum)
    );

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
