-- name: GetUpdateState :one
SELECT auth_key_id, user_id, pts, qts, date, seq
FROM update_states
WHERE auth_key_id = $1
  AND user_id = $2;

-- name: UpsertUpdateState :exec
INSERT INTO update_states (auth_key_id, user_id, pts, qts, date, seq)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (auth_key_id, user_id) DO UPDATE SET
  pts = EXCLUDED.pts,
  qts = EXCLUDED.qts,
  date = EXCLUDED.date,
  seq = EXCLUDED.seq,
  updated_at = now();

-- name: DeleteUpdateState :exec
DELETE FROM update_states
WHERE auth_key_id = $1
  AND user_id = $2;

-- name: DeleteUpdateStatesByAuthKey :exec
DELETE FROM update_states
WHERE auth_key_id = $1;
