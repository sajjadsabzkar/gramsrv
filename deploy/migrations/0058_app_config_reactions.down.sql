-- 0058_app_config_reactions down: drop reaction config keys, restore prior hash.

UPDATE app_configs
SET hash = 3,
    config_json = config_json
        - 'reactions_default'
        - 'reactions_uniq_max'
        - 'reactions_user_max_default'
        - 'reactions_in_chat_max',
    updated_at = now()
WHERE client = 'tdesktop';
