-- 0033_app_config_quote_length rollback.

UPDATE app_configs
SET hash = 2,
    config_json = config_json - 'quote_length_max',
    updated_at = now()
WHERE client = 'tdesktop';
