-- name: GetAuthKey :one
SELECT auth_key_id, body, server_salt, created_at
FROM auth_keys
WHERE auth_key_id = $1;

-- name: UpsertAuthKey :exec
INSERT INTO auth_keys (auth_key_id, body, server_salt)
VALUES ($1, $2, $3)
ON CONFLICT (auth_key_id) DO UPDATE
SET body = EXCLUDED.body, server_salt = EXCLUDED.server_salt;
