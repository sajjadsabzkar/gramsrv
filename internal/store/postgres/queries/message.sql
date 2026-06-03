-- name: CreateMessage :one
WITH pm AS (
  INSERT INTO private_messages (
    sender_user_id,
    recipient_user_id,
    random_id,
    message_date,
    body,
    entities
  ) VALUES (
    sqlc.arg(from_user_id),
    sqlc.arg(owner_user_id),
    0,
    sqlc.arg(message_date),
    sqlc.arg(body),
    sqlc.arg(entities_json)::jsonb
  )
  RETURNING id, sender_user_id
),
box AS (
  INSERT INTO message_boxes (
    owner_user_id,
    box_id,
    private_message_id,
    message_sender_id,
    peer_type,
    peer_id,
    from_user_id,
    message_date,
    outgoing,
    body,
    entities,
    pts
  )
  SELECT
    sqlc.arg(owner_user_id),
    sqlc.arg(box_id),
    pm.id,
    pm.sender_user_id,
    sqlc.arg(peer_type),
    sqlc.arg(peer_id),
    sqlc.arg(from_user_id),
    sqlc.arg(message_date),
    sqlc.arg(outgoing),
    sqlc.arg(body),
    sqlc.arg(entities_json)::jsonb,
    sqlc.arg(pts)
  FROM pm
  RETURNING
    box_id,
    private_message_id,
    owner_user_id,
    peer_type,
    peer_id,
    from_user_id,
    message_date,
    edit_date,
    outgoing,
    body,
    entities::text AS entities_json,
    pts
)
SELECT
  box_id,
  private_message_id,
  owner_user_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities_json,
  pts
FROM box;

-- name: CreatePrivateMessage :one
INSERT INTO private_messages (
  sender_user_id,
  recipient_user_id,
  random_id,
  message_date,
  body,
  entities,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  media
) VALUES (
  $1, $2, $3, $4, $5, sqlc.arg(entities_json)::jsonb,
  sqlc.arg(silent)::boolean,
  sqlc.arg(noforwards)::boolean,
  sqlc.arg(reply_to_msg_id)::int,
  sqlc.arg(reply_to_peer_type)::text,
  sqlc.arg(reply_to_peer_id)::bigint,
  sqlc.arg(reply_to_top_id)::int,
  sqlc.arg(quote_text)::text,
  sqlc.arg(quote_entities_json)::jsonb,
  sqlc.arg(quote_offset)::int,
  sqlc.arg(fwd_from_peer_type)::text,
  sqlc.arg(fwd_from_peer_id)::bigint,
  sqlc.arg(fwd_from_name)::text,
  sqlc.arg(fwd_date)::int,
  sqlc.arg(media_json)::jsonb
)
ON CONFLICT (sender_user_id, random_id) WHERE random_id <> 0 DO NOTHING
RETURNING
  id,
  sender_user_id,
  recipient_user_id,
  random_id,
  message_date,
  edit_date,
  body,
  entities::text AS entities_json;

-- name: GetPrivateMessageByRandomID :one
SELECT
  id,
  sender_user_id,
  recipient_user_id,
  random_id,
  message_date,
  edit_date,
  body,
  entities::text AS entities_json
FROM private_messages
WHERE sender_user_id = $1
  AND random_id = $2
  AND random_id <> 0;

-- name: CreateMessageBox :one
INSERT INTO message_boxes (
  owner_user_id,
  box_id,
  private_message_id,
  message_sender_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  outgoing,
  body,
  entities,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, sqlc.arg(entities_json)::jsonb,
  sqlc.arg(silent)::boolean,
  sqlc.arg(noforwards)::boolean,
  sqlc.arg(reply_to_msg_id)::int,
  sqlc.arg(reply_to_peer_type)::text,
  sqlc.arg(reply_to_peer_id)::bigint,
  sqlc.arg(reply_to_top_id)::int,
  sqlc.arg(quote_text)::text,
  sqlc.arg(quote_entities_json)::jsonb,
  sqlc.arg(quote_offset)::int,
  sqlc.arg(fwd_from_peer_type)::text,
  sqlc.arg(fwd_from_peer_id)::bigint,
  sqlc.arg(fwd_from_name)::text,
  sqlc.arg(fwd_date)::int,
  sqlc.arg(pts)::int,
  sqlc.arg(media_json)::jsonb
)
RETURNING
  box_id,
  private_message_id,
  owner_user_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities::text AS entities_json,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities::text AS quote_entities_json,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media::text AS media_json;

-- name: GetMessageBoxByPrivateMessage :one
SELECT
  box_id,
  private_message_id,
  owner_user_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities::text AS entities_json,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities::text AS quote_entities_json,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media::text AS media_json
FROM message_boxes
WHERE owner_user_id = $1
  AND private_message_id = $2
  AND NOT deleted;

-- name: GetMessageBoxForReply :one
SELECT
  box_id,
  private_message_id,
  message_sender_id
FROM message_boxes
WHERE owner_user_id = sqlc.arg(owner_user_id)::bigint
  AND peer_type = sqlc.arg(peer_type)::text
  AND peer_id = sqlc.arg(peer_id)::bigint
  AND box_id = sqlc.arg(box_id)::int
  AND NOT deleted
LIMIT 1;

-- name: GetMessageBoxesForForward :many
WITH requested AS (
  SELECT
    id::int AS box_id,
    ord::int AS ord
  FROM unnest(sqlc.arg(box_ids)::int[]) WITH ORDINALITY AS r(id, ord)
)
SELECT
  r.ord,
  m.box_id,
  m.private_message_id,
  m.owner_user_id,
  m.message_sender_id,
  m.peer_type,
  m.peer_id,
  m.from_user_id,
  m.message_date,
  m.edit_date,
  m.outgoing,
  m.body,
  m.entities::text AS entities_json,
  m.silent,
  m.noforwards,
  m.reply_to_msg_id,
  m.reply_to_peer_type,
  m.reply_to_peer_id,
  m.reply_to_top_id,
  m.quote_text,
  m.quote_entities::text AS quote_entities_json,
  m.quote_offset,
  m.fwd_from_peer_type,
  m.fwd_from_peer_id,
  m.fwd_from_name,
  m.fwd_date,
  m.pts,
  m.media::text AS media_json
FROM requested r
JOIN message_boxes m
  ON m.owner_user_id = sqlc.arg(owner_user_id)::bigint
 AND m.peer_type = sqlc.arg(peer_type)::text
 AND m.peer_id = sqlc.arg(peer_id)::bigint
 AND m.box_id = r.box_id
 AND NOT m.deleted
ORDER BY r.ord ASC;

-- name: MaxMessageBoxID :one
SELECT COALESCE(MAX(box_id), 0)::int AS max_box_id
FROM message_boxes
WHERE owner_user_id = $1;

-- name: ListMessagesByUser :many
WITH load_params AS (
  SELECT
    sqlc.arg(offset_id)::int AS offset_id,
    sqlc.arg(offset_date)::int AS offset_date,
    sqlc.arg(add_offset)::int AS add_offset,
    sqlc.arg(limit_count)::int AS limit_count,
    CASE
      WHEN sqlc.arg(add_offset)::int >= 0 THEN 'backward'
      WHEN sqlc.arg(add_offset)::int + sqlc.arg(limit_count)::int > 0 THEN 'around'
      ELSE 'forward'
    END::text AS load_type
),
base AS NOT MATERIALIZED (
  SELECT
    m.box_id,
    m.private_message_id,
    m.owner_user_id,
    m.peer_type,
    m.peer_id,
    m.from_user_id,
    m.message_date,
    m.edit_date,
    m.outgoing,
    m.body,
    m.entities::text AS entities_json,
    m.silent,
    m.noforwards,
    m.reply_to_msg_id,
    m.reply_to_peer_type,
    m.reply_to_peer_id,
    m.reply_to_top_id,
    m.quote_text,
    m.quote_entities::text AS quote_entities_json,
    m.quote_offset,
    m.fwd_from_peer_type,
    m.fwd_from_peer_id,
    m.fwd_from_name,
    m.fwd_date,
    m.pts,
    m.media::text AS media_json,
    COALESCE(peer_u.id, 0)::bigint AS peer_user_id,
    COALESCE(peer_u.access_hash, 0)::bigint AS peer_access_hash,
    COALESCE(peer_u.phone, '')::text AS peer_phone,
    COALESCE(peer_u.first_name, '')::text AS peer_first_name,
    COALESCE(peer_u.last_name, '')::text AS peer_last_name,
    COALESCE(peer_u.username, '')::text AS peer_username,
    COALESCE(peer_u.country_code, '')::text AS peer_country_code,
    COALESCE(peer_u.verified, false)::boolean AS peer_verified,
    COALESCE(peer_u.support, false)::boolean AS peer_support,
    COALESCE(peer_u.last_seen_at, 0)::bigint AS peer_last_seen_at,
    COALESCE(from_u.id, 0)::bigint AS from_user_user_id,
    COALESCE(from_u.access_hash, 0)::bigint AS from_user_access_hash,
    COALESCE(from_u.phone, '')::text AS from_user_phone,
    COALESCE(from_u.first_name, '')::text AS from_user_first_name,
    COALESCE(from_u.last_name, '')::text AS from_user_last_name,
    COALESCE(from_u.username, '')::text AS from_user_username,
    COALESCE(from_u.country_code, '')::text AS from_user_country_code,
    COALESCE(from_u.verified, false)::boolean AS from_user_verified,
    COALESCE(from_u.support, false)::boolean AS from_user_support,
    COALESCE(from_u.last_seen_at, 0)::bigint AS from_user_last_seen_at
  FROM message_boxes m
  LEFT JOIN users peer_u ON m.peer_type = 'user' AND peer_u.id = m.peer_id
  LEFT JOIN users from_u ON from_u.id = m.from_user_id
  WHERE m.owner_user_id = $1
    AND NOT m.deleted
    AND (
      NOT sqlc.arg(has_peer)::boolean
      OR (m.peer_type = sqlc.arg(peer_type)::text AND m.peer_id = sqlc.arg(peer_id)::bigint)
    )
    AND (
      sqlc.arg(query)::text = ''
      OR m.body ILIKE ('%' || sqlc.arg(query)::text || '%')
    )
    AND (sqlc.arg(max_id)::int <= 0 OR m.box_id < sqlc.arg(max_id)::int)
    AND (sqlc.arg(min_id)::int <= 0 OR m.box_id > sqlc.arg(min_id)::int)
),
total AS (
  SELECT count(*)::int AS total_count
  FROM base
  WHERE sqlc.arg(need_total_count)::boolean
),
backward AS (
  SELECT b.*
  FROM base b
  CROSS JOIN load_params p
  WHERE p.load_type = 'backward'
    AND (
      (p.offset_date > 0 AND b.message_date < p.offset_date)
      OR (p.offset_date <= 0 AND (p.offset_id <= 0 OR b.box_id < p.offset_id))
    )
  ORDER BY b.box_id DESC
  OFFSET GREATEST((SELECT add_offset FROM load_params), 0)
  LIMIT (SELECT limit_count FROM load_params)
),
around_forward AS (
  SELECT f.*
  FROM (
    SELECT b.*
    FROM base b
    CROSS JOIN load_params p
    WHERE p.load_type = 'around'
      AND (
        (p.offset_date > 0 AND b.message_date >= p.offset_date)
        OR (p.offset_date <= 0 AND p.offset_id > 0 AND b.box_id > p.offset_id)
      )
    ORDER BY b.box_id ASC
    LIMIT LEAST(-(SELECT add_offset FROM load_params), (SELECT limit_count FROM load_params))
  ) f
),
around_backward AS (
  SELECT b.*
  FROM base b
  CROSS JOIN load_params p
  WHERE p.load_type = 'around'
    AND (
      (p.offset_date > 0 AND b.message_date < p.offset_date)
      OR (p.offset_date <= 0 AND (p.offset_id <= 0 OR b.box_id <= p.offset_id))
    )
  ORDER BY b.box_id DESC
  LIMIT GREATEST((SELECT limit_count + add_offset FROM load_params), 0)
),
forward AS (
  SELECT f.*
  FROM (
    SELECT b.*
    FROM base b
    CROSS JOIN load_params p
    WHERE p.load_type = 'forward'
      AND (
        (p.offset_date > 0 AND b.message_date >= p.offset_date)
        OR (p.offset_date <= 0 AND p.offset_id > 0 AND b.box_id > p.offset_id)
      )
    ORDER BY b.box_id ASC
    LIMIT (SELECT limit_count FROM load_params)
  ) f
),
paged AS (
  SELECT * FROM backward
  UNION ALL
  SELECT * FROM around_forward
  UNION ALL
  SELECT * FROM around_backward
  UNION ALL
  SELECT * FROM forward
)
SELECT
  box_id,
  private_message_id,
  owner_user_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities_json,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities_json,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media_json,
  peer_user_id,
  peer_access_hash,
  peer_phone,
  peer_first_name,
  peer_last_name,
  peer_username,
  peer_country_code,
  peer_verified,
  peer_support,
  peer_last_seen_at,
  from_user_user_id,
  from_user_access_hash,
  from_user_phone,
  from_user_first_name,
  from_user_last_name,
  from_user_username,
  from_user_country_code,
  from_user_verified,
  from_user_support,
  from_user_last_seen_at,
  COALESCE(total.total_count, 0)::int AS total_count
FROM paged
CROSS JOIN total
ORDER BY box_id DESC;

-- name: GetMessageBoxesByIDs :many
SELECT
  wanted.box_id AS requested_box_id,
  m.box_id,
  m.private_message_id,
  m.owner_user_id,
  m.peer_type,
  m.peer_id,
  m.from_user_id,
  m.message_date,
  m.edit_date,
  m.outgoing,
  m.body,
  m.entities::text AS entities_json,
  m.silent,
  m.noforwards,
  m.reply_to_msg_id,
  m.reply_to_peer_type,
  m.reply_to_peer_id,
  m.reply_to_top_id,
  m.quote_text,
  m.quote_entities::text AS quote_entities_json,
  m.quote_offset,
  m.fwd_from_peer_type,
  m.fwd_from_peer_id,
  m.fwd_from_name,
  m.fwd_date,
  m.pts,
  m.media::text AS media_json,
  COALESCE(peer_u.id, 0)::bigint AS peer_user_id,
  COALESCE(peer_u.access_hash, 0)::bigint AS peer_access_hash,
  COALESCE(peer_u.phone, '')::text AS peer_phone,
  COALESCE(peer_u.first_name, '')::text AS peer_first_name,
  COALESCE(peer_u.last_name, '')::text AS peer_last_name,
  COALESCE(peer_u.username, '')::text AS peer_username,
  COALESCE(peer_u.country_code, '')::text AS peer_country_code,
  COALESCE(peer_u.verified, false)::boolean AS peer_verified,
  COALESCE(peer_u.support, false)::boolean AS peer_support,
  COALESCE(peer_u.last_seen_at, 0)::bigint AS peer_last_seen_at,
  COALESCE(from_u.id, 0)::bigint AS from_user_user_id,
  COALESCE(from_u.access_hash, 0)::bigint AS from_user_access_hash,
  COALESCE(from_u.phone, '')::text AS from_user_phone,
  COALESCE(from_u.first_name, '')::text AS from_user_first_name,
  COALESCE(from_u.last_name, '')::text AS from_user_last_name,
  COALESCE(from_u.username, '')::text AS from_user_username,
  COALESCE(from_u.country_code, '')::text AS from_user_country_code,
  COALESCE(from_u.verified, false)::boolean AS from_user_verified,
  COALESCE(from_u.support, false)::boolean AS from_user_support,
  COALESCE(from_u.last_seen_at, 0)::bigint AS from_user_last_seen_at
FROM unnest(@box_ids::int[]) WITH ORDINALITY AS wanted(box_id, ord)
JOIN message_boxes m
  ON m.owner_user_id = sqlc.arg(owner_user_id)::bigint
 AND m.box_id = wanted.box_id
 AND NOT m.deleted
LEFT JOIN users peer_u ON m.peer_type = 'user' AND peer_u.id = m.peer_id
LEFT JOIN users from_u ON from_u.id = m.from_user_id
ORDER BY wanted.ord ASC;

-- name: GetMessageBoxForEdit :one
SELECT
  box_id,
  private_message_id,
  owner_user_id,
  message_sender_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities::text AS entities_json,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities::text AS quote_entities_json,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media::text AS media_json
FROM message_boxes
WHERE owner_user_id = sqlc.arg(owner_user_id)::bigint
  AND box_id = sqlc.arg(box_id)::int
  AND peer_type = sqlc.arg(peer_type)::text
  AND peer_id = sqlc.arg(peer_id)::bigint
  AND NOT deleted
LIMIT 1
FOR UPDATE;

-- name: ListVisibleMessageBoxesByPrivateMessage :many
SELECT
  box_id,
  private_message_id,
  owner_user_id,
  message_sender_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities::text AS entities_json,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities::text AS quote_entities_json,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media::text AS media_json
FROM message_boxes
WHERE message_sender_id = sqlc.arg(message_sender_id)::bigint
  AND private_message_id = sqlc.arg(private_message_id)::bigint
  AND NOT deleted
ORDER BY owner_user_id ASC, box_id ASC
FOR UPDATE;

-- name: UpdatePrivateMessageEdit :exec
UPDATE private_messages
SET body = sqlc.arg(body)::text,
    entities = sqlc.arg(entities_json)::jsonb,
    edit_date = sqlc.arg(edit_date)::int
WHERE sender_user_id = sqlc.arg(sender_user_id)::bigint
  AND id = sqlc.arg(private_message_id)::bigint;

-- name: UpdateMessageBoxEdit :one
UPDATE message_boxes
SET body = sqlc.arg(body)::text,
    entities = sqlc.arg(entities_json)::jsonb,
    edit_date = sqlc.arg(edit_date)::int,
    pts = sqlc.arg(pts)::int
WHERE owner_user_id = sqlc.arg(owner_user_id)::bigint
  AND box_id = sqlc.arg(box_id)::int
  AND NOT deleted
RETURNING
  box_id,
  private_message_id,
  owner_user_id,
  message_sender_id,
  peer_type,
  peer_id,
  from_user_id,
  message_date,
  edit_date,
  outgoing,
  body,
  entities::text AS entities_json,
  silent,
  noforwards,
  reply_to_msg_id,
  reply_to_peer_type,
  reply_to_peer_id,
  reply_to_top_id,
  quote_text,
  quote_entities::text AS quote_entities_json,
  quote_offset,
  fwd_from_peer_type,
  fwd_from_peer_id,
  fwd_from_name,
  fwd_date,
  pts,
  media::text AS media_json;

-- name: GetDialogReadStateForUpdate :one
SELECT
  user_id,
  peer_type,
  peer_id,
  top_message_id,
  read_inbox_max_id,
  unread_count
FROM dialogs
WHERE user_id = sqlc.arg(user_id)::bigint
  AND peer_type = sqlc.arg(peer_type)::text
  AND peer_id = sqlc.arg(peer_id)::bigint
FOR UPDATE;

-- name: LatestIncomingReadReceiptCandidate :one
SELECT
  m.message_sender_id,
  m.private_message_id,
  sender_box.owner_user_id AS sender_owner_user_id,
  sender_box.box_id AS sender_box_id
FROM message_boxes m
JOIN message_boxes sender_box
  ON sender_box.message_sender_id = m.message_sender_id
 AND sender_box.private_message_id = m.private_message_id
 AND sender_box.owner_user_id = m.message_sender_id
 AND sender_box.outgoing
 AND NOT sender_box.deleted
WHERE m.owner_user_id = sqlc.arg(owner_user_id)::bigint
  AND m.peer_type = sqlc.arg(peer_type)::text
  AND m.peer_id = sqlc.arg(peer_id)::bigint
  AND NOT m.outgoing
  AND NOT m.deleted
  AND m.box_id > sqlc.arg(old_read_inbox_max_id)::int
  AND m.box_id <= sqlc.arg(new_read_inbox_max_id)::int
ORDER BY m.box_id DESC
LIMIT 1;

-- name: UpdateDialogReadInbox :one
UPDATE dialogs d
SET
  read_inbox_max_id = GREATEST(d.read_inbox_max_id, sqlc.arg(read_inbox_max_id)::int),
  unread_count = (
    SELECT count(*)::int
    FROM message_boxes m
    WHERE m.owner_user_id = d.user_id
      AND m.peer_type = d.peer_type
      AND m.peer_id = d.peer_id
      AND NOT m.deleted
      AND NOT m.outgoing
      AND m.box_id > GREATEST(d.read_inbox_max_id, sqlc.arg(read_inbox_max_id)::int)
  ),
  unread_mark = false,
  unread_mentions_count = 0,
  unread_reactions_count = 0,
  updated_at = now()
WHERE d.user_id = sqlc.arg(user_id)::bigint
  AND d.peer_type = sqlc.arg(peer_type)::text
  AND d.peer_id = sqlc.arg(peer_id)::bigint
RETURNING
  d.read_inbox_max_id,
  d.unread_count;

-- name: UpdateDialogReadOutbox :one
UPDATE dialogs
SET
  read_outbox_max_id = GREATEST(read_outbox_max_id, sqlc.arg(read_outbox_max_id)::int),
  updated_at = now()
WHERE user_id = sqlc.arg(user_id)::bigint
  AND peer_type = sqlc.arg(peer_type)::text
  AND peer_id = sqlc.arg(peer_id)::bigint
  AND read_outbox_max_id < sqlc.arg(read_outbox_max_id)::int
RETURNING read_outbox_max_id;

-- name: GetOutboxMessageForReadDate :one
SELECT box_id
FROM message_boxes
WHERE owner_user_id = sqlc.arg(owner_user_id)::bigint
  AND peer_type = sqlc.arg(peer_type)::text
  AND peer_id = sqlc.arg(peer_id)::bigint
  AND box_id = sqlc.arg(box_id)::int
  AND outgoing
  AND NOT deleted
LIMIT 1;

-- name: GetOutboxReadDate :one
SELECT COALESCE(MIN(date), 0)::int AS read_date
FROM user_update_events
WHERE user_id = sqlc.arg(user_id)::bigint
  AND event_type = 'read_history_outbox'
  AND peer_type = sqlc.arg(peer_type)::text
  AND peer_id = sqlc.arg(peer_id)::bigint
  AND max_id >= sqlc.arg(message_id)::int;

-- name: DeleteMessageBoxesByIDs :many
WITH updated AS (
  UPDATE message_boxes m
  SET deleted = true
  WHERE m.owner_user_id = sqlc.arg(owner_user_id)::bigint
    AND m.box_id = ANY(sqlc.arg(box_ids)::int[])
    AND NOT m.deleted
  RETURNING
    m.owner_user_id,
    m.box_id,
    m.private_message_id,
    m.message_sender_id,
    m.peer_type,
    m.peer_id
)
SELECT
  owner_user_id,
  box_id,
  private_message_id,
  message_sender_id,
  peer_type,
  peer_id
FROM updated
ORDER BY box_id ASC;

-- name: DeleteMessageBoxesByPeer :many
WITH updated AS (
  UPDATE message_boxes m
  SET deleted = true
  WHERE m.owner_user_id = sqlc.arg(owner_user_id)::bigint
    AND m.peer_type = sqlc.arg(peer_type)::text
    AND m.peer_id = sqlc.arg(peer_id)::bigint
    AND (sqlc.arg(max_id)::int <= 0 OR m.box_id <= sqlc.arg(max_id)::int)
    AND NOT m.deleted
  RETURNING
    m.owner_user_id,
    m.box_id,
    m.private_message_id,
    m.message_sender_id,
    m.peer_type,
    m.peer_id
)
SELECT
  owner_user_id,
  box_id,
  private_message_id,
  message_sender_id,
  peer_type,
  peer_id
FROM updated
ORDER BY box_id ASC;

-- name: DeleteMessageBoxesByPeerBatch :many
WITH target AS (
  SELECT
    m.owner_user_id,
    m.box_id
  FROM message_boxes m
  WHERE m.owner_user_id = sqlc.arg(owner_user_id)::bigint
    AND m.peer_type = sqlc.arg(peer_type)::text
    AND m.peer_id = sqlc.arg(peer_id)::bigint
    AND (sqlc.arg(max_id)::int <= 0 OR m.box_id <= sqlc.arg(max_id)::int)
    AND NOT m.deleted
  ORDER BY m.box_id DESC
  LIMIT sqlc.arg(limit_count)::int
  FOR UPDATE SKIP LOCKED
),
updated AS (
  UPDATE message_boxes m
  SET deleted = true
  FROM target t
  WHERE m.owner_user_id = t.owner_user_id
    AND m.box_id = t.box_id
  RETURNING
    m.owner_user_id,
    m.box_id,
    m.private_message_id,
    m.message_sender_id,
    m.peer_type,
    m.peer_id
)
SELECT
  owner_user_id,
  box_id,
  private_message_id,
  message_sender_id,
  peer_type,
  peer_id
FROM updated
ORDER BY box_id ASC;

-- name: HasDeletableMessageBoxByPeer :one
SELECT EXISTS (
  SELECT 1
  FROM message_boxes m
  WHERE m.owner_user_id = sqlc.arg(owner_user_id)::bigint
    AND m.peer_type = sqlc.arg(peer_type)::text
    AND m.peer_id = sqlc.arg(peer_id)::bigint
    AND (sqlc.arg(max_id)::int <= 0 OR m.box_id <= sqlc.arg(max_id)::int)
    AND NOT m.deleted
  LIMIT 1
)::boolean AS more;

-- name: DeleteMessageBoxesByPrivateMessages :many
WITH requested AS (
  SELECT
    (sqlc.arg(message_sender_ids)::bigint[])[i] AS message_sender_id,
    (sqlc.arg(private_message_ids)::bigint[])[i] AS private_message_id
  FROM generate_subscripts(sqlc.arg(private_message_ids)::bigint[], 1) AS g(i)
  WHERE i <= cardinality(sqlc.arg(message_sender_ids)::bigint[])
),
deduped AS (
  SELECT DISTINCT message_sender_id, private_message_id
  FROM requested
),
updated AS (
  UPDATE message_boxes m
  SET deleted = true
  FROM deduped d
  WHERE m.message_sender_id = d.message_sender_id
    AND m.private_message_id = d.private_message_id
    AND NOT m.deleted
  RETURNING
    m.owner_user_id,
    m.box_id,
    m.private_message_id,
    m.message_sender_id,
    m.peer_type,
    m.peer_id
)
SELECT
  owner_user_id,
  box_id,
  private_message_id,
  message_sender_id,
  peer_type,
  peer_id
FROM updated
ORDER BY owner_user_id ASC, box_id ASC;

-- name: TopVisibleMessageBoxByPeer :one
SELECT
  box_id,
  message_date
FROM message_boxes
WHERE owner_user_id = $1
  AND peer_type = $2
  AND peer_id = $3
  AND NOT deleted
ORDER BY box_id DESC
LIMIT 1;
