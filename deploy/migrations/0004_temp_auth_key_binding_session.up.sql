-- 0004_temp_auth_key_binding_session: persist validated bind_auth_key_inner session id.

ALTER TABLE temp_auth_key_bindings
    ADD COLUMN IF NOT EXISTS temp_session_id BIGINT NOT NULL DEFAULT 0;
