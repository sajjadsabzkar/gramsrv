-- 0033_app_config_quote_length: expose TDesktop quote reply length limit explicitly.

UPDATE app_configs
SET hash = 3,
    config_json = config_json || jsonb_build_object('quote_length_max', 1024),
    updated_at = now()
WHERE client = 'tdesktop';
