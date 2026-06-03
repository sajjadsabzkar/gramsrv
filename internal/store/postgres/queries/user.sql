-- name: GetUserByID :one
SELECT * FROM users WHERE id = $1;

-- name: GetUsersByIDs :many
SELECT *
FROM users
WHERE id = ANY(sqlc.arg(ids)::bigint[])
ORDER BY id;

-- name: GetUserByPhone :one
SELECT * FROM users WHERE phone = $1;

-- name: GetUsersByPhones :many
SELECT *
FROM users
WHERE phone = ANY(sqlc.arg(phones)::text[])
ORDER BY id;

-- name: GetUserByUsername :one
SELECT * FROM users WHERE lower(username) = lower($1) AND username <> '';

-- name: SearchUsers :many
WITH matched AS (
  SELECT
    u.id,
    u.access_hash,
    COALESCE(NULLIF(c.contact_phone, ''), u.phone)::text AS phone,
    COALESCE(NULLIF(c.contact_first_name, ''), u.first_name)::text AS first_name,
    COALESCE(c.contact_last_name, u.last_name)::text AS last_name,
    u.about,
    u.username,
    u.country_code,
    u.verified,
    u.support,
    u.last_seen_at,
    (c.contact_user_id IS NOT NULL)::boolean AS contact,
    COALESCE(c.mutual, false)::boolean AS mutual,
    CASE
      WHEN sqlc.arg(phone_query)::text <> '' AND u.phone = sqlc.arg(phone_query)::text THEN 0
      WHEN lower(u.username) = sqlc.arg(query_lower)::text THEN 1
      WHEN lower(COALESCE(NULLIF(c.contact_first_name, ''), u.first_name)) = sqlc.arg(query_lower)::text THEN 2
      WHEN lower(u.first_name) = sqlc.arg(query_lower)::text THEN 3
      WHEN c.contact_user_id IS NOT NULL THEN 4
      ELSE 5
    END AS rank
  FROM users u
  LEFT JOIN contacts c ON c.user_id = sqlc.arg(current_user_id)::bigint AND c.contact_user_id = u.id
  WHERE u.id <> sqlc.arg(current_user_id)::bigint
    AND sqlc.arg(query_lower)::text <> ''
    AND (
      (sqlc.arg(phone_query)::text <> '' AND u.phone LIKE sqlc.arg(phone_query)::text || '%')
      OR lower(u.username) LIKE sqlc.arg(query_like)::text || '%' ESCAPE '\'
      OR lower(u.first_name) LIKE '%' || sqlc.arg(query_like)::text || '%' ESCAPE '\'
      OR lower(u.last_name) LIKE '%' || sqlc.arg(query_like)::text || '%' ESCAPE '\'
      OR lower(trim(u.first_name || ' ' || u.last_name)) LIKE '%' || sqlc.arg(query_like)::text || '%' ESCAPE '\'
      OR lower(c.contact_first_name) LIKE '%' || sqlc.arg(query_like)::text || '%' ESCAPE '\'
      OR lower(c.contact_last_name) LIKE '%' || sqlc.arg(query_like)::text || '%' ESCAPE '\'
      OR lower(trim(c.contact_first_name || ' ' || c.contact_last_name)) LIKE '%' || sqlc.arg(query_like)::text || '%' ESCAPE '\'
    )
)
SELECT
  id,
  access_hash,
  phone,
  first_name,
  last_name,
  about,
  username,
  country_code,
  verified,
  support,
  last_seen_at,
  contact,
  mutual
FROM matched
ORDER BY contact DESC, rank, id
LIMIT sqlc.arg(limit_count);

-- name: CreateUser :one
INSERT INTO users (access_hash, phone, first_name, last_name, username, country_code)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: UpdateUserUsername :one
UPDATE users
SET username = $2,
    updated_at = now()
WHERE id = $1
RETURNING *;

-- name: UpdateUserLastSeen :exec
UPDATE users
SET last_seen_at = GREATEST(last_seen_at, sqlc.arg(last_seen_at)::bigint),
    updated_at = now()
WHERE id = sqlc.arg(id)::bigint;

-- name: UpdateUserProfile :one
UPDATE users
SET first_name = $2,
    last_name = $3,
    about = $4,
    updated_at = now()
WHERE id = $1
RETURNING *;
