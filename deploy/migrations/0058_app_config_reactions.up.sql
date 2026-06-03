-- 0058_app_config_reactions: expose default reaction + reaction limits for TDesktop.

UPDATE app_configs
SET hash = 5,
    config_json = config_json || jsonb_build_object(
        'reactions_default', jsonb_build_object('_', 'reactionEmoji', 'emoticon', '👍'),
        'reactions_uniq_max', 11,
        'reactions_user_max_default', 1,
        'reactions_in_chat_max', 3
    ),
    updated_at = now()
WHERE client = 'tdesktop';
