-- name: ListDialogsByUser :many
WITH base AS (
  SELECT
    d.user_id,
    d.peer_type,
    d.peer_id,
    d.folder_id,
    d.top_message_id,
    d.top_message_date,
    d.read_inbox_max_id,
    d.read_outbox_max_id,
    d.unread_count,
    d.unread_mentions_count,
    d.unread_reactions_count,
    d.pinned,
    d.pinned_order,
    d.unread_mark,
    d.hidden_peer_settings_bar,
    COALESCE(u.id, 0)::bigint AS peer_user_id,
    COALESCE(u.access_hash, 0)::bigint AS peer_access_hash,
    COALESCE(NULLIF(c.contact_phone, ''), u.phone, '')::text AS peer_phone,
    COALESCE(NULLIF(c.contact_first_name, ''), u.first_name, '')::text AS peer_first_name,
    COALESCE(c.contact_last_name, u.last_name, '')::text AS peer_last_name,
    COALESCE(u.username, '')::text AS peer_username,
    COALESCE(u.country_code, '')::text AS peer_country_code,
    COALESCE(u.verified, false)::boolean AS peer_verified,
    COALESCE(u.support, false)::boolean AS peer_support,
    COALESCE(u.last_seen_at, 0)::bigint AS peer_last_seen_at,
    (c.contact_user_id IS NOT NULL)::boolean AS peer_contact,
    COALESCE(c.mutual, false)::boolean AS peer_mutual,
    COALESCE(m.box_id, 0)::int AS message_id,
    COALESCE(m.from_user_id, 0)::bigint AS message_from_user_id,
    COALESCE(m.message_date, 0)::int AS message_date,
    COALESCE(m.outgoing, false)::boolean AS message_outgoing,
    COALESCE(m.body, '')::text AS message_body,
    COALESCE(m.entities::text, '[]')::text AS message_entities_json
  FROM dialogs d
  LEFT JOIN users u ON d.peer_type = 'user' AND u.id = d.peer_id
  LEFT JOIN contacts c ON d.peer_type = 'user' AND c.user_id = d.user_id AND c.contact_user_id = d.peer_id
  LEFT JOIN message_boxes m ON m.owner_user_id = d.user_id AND m.box_id = d.top_message_id AND NOT m.deleted
  WHERE d.user_id = $1
    AND (
      NOT sqlc.arg(has_folder_id)::boolean
      OR (
        sqlc.arg(folder_id)::int < 2
        AND d.folder_id = sqlc.arg(folder_id)::int
      )
      OR (
        sqlc.arg(folder_id)::int >= 2
        AND NOT (sqlc.arg(folder_exclude_archived)::boolean AND d.folder_id = 1)
        AND NOT (sqlc.arg(folder_exclude_read)::boolean AND d.unread_count = 0 AND NOT d.unread_mark)
        AND NOT EXISTS (
          SELECT 1
          FROM (
            SELECT fpt.peer_type, fpi.peer_id
            FROM unnest(sqlc.arg(folder_exclude_peer_types)::text[]) WITH ORDINALITY AS fpt(peer_type, ord)
            JOIN unnest(sqlc.arg(folder_exclude_peer_ids)::bigint[]) WITH ORDINALITY AS fpi(peer_id, ord) USING (ord)
          ) fp
          WHERE fp.peer_type = d.peer_type AND fp.peer_id = d.peer_id
        )
        AND (
          EXISTS (
            SELECT 1
            FROM (
              SELECT fpt.peer_type, fpi.peer_id
              FROM unnest(sqlc.arg(folder_include_peer_types)::text[]) WITH ORDINALITY AS fpt(peer_type, ord)
              JOIN unnest(sqlc.arg(folder_include_peer_ids)::bigint[]) WITH ORDINALITY AS fpi(peer_id, ord) USING (ord)
            ) fp
            WHERE fp.peer_type = d.peer_type AND fp.peer_id = d.peer_id
          )
          OR EXISTS (
            SELECT 1
            FROM (
              SELECT fpt.peer_type, fpi.peer_id
              FROM unnest(sqlc.arg(folder_pinned_peer_types)::text[]) WITH ORDINALITY AS fpt(peer_type, ord)
              JOIN unnest(sqlc.arg(folder_pinned_peer_ids)::bigint[]) WITH ORDINALITY AS fpi(peer_id, ord) USING (ord)
            ) fp
            WHERE fp.peer_type = d.peer_type AND fp.peer_id = d.peer_id
          )
          OR (sqlc.arg(folder_contacts)::boolean AND c.contact_user_id IS NOT NULL)
          OR (sqlc.arg(folder_non_contacts)::boolean AND c.contact_user_id IS NULL)
        )
      )
    )
    AND (NOT sqlc.arg(pinned_only)::boolean OR d.pinned)
    AND (NOT sqlc.arg(exclude_pinned)::boolean OR NOT d.pinned)
),
paged AS (
  SELECT *
  FROM base
  WHERE (
    (sqlc.arg(offset_date)::int <= 0 AND sqlc.arg(offset_id)::int <= 0)
    OR (
      sqlc.arg(offset_date)::int > 0
      AND (
        top_message_date < sqlc.arg(offset_date)::int
        OR (
          top_message_date = sqlc.arg(offset_date)::int
          AND (
            sqlc.arg(offset_id)::int <= 0
            OR top_message_id < sqlc.arg(offset_id)::int
            OR (
              top_message_id = sqlc.arg(offset_id)::int
              AND sqlc.arg(has_offset_peer)::boolean
              AND peer_id < sqlc.arg(offset_peer_id)::bigint
            )
          )
        )
      )
    )
    OR (
      sqlc.arg(offset_date)::int <= 0
      AND sqlc.arg(offset_id)::int > 0
      AND (
        top_message_id < sqlc.arg(offset_id)::int
        OR (
          top_message_id = sqlc.arg(offset_id)::int
          AND sqlc.arg(has_offset_peer)::boolean
          AND peer_id < sqlc.arg(offset_peer_id)::bigint
        )
      )
    )
  )
)
SELECT
  user_id,
  peer_type::text AS peer_type,
  peer_id::bigint AS peer_id,
  folder_id,
  top_message_id,
  top_message_date,
  read_inbox_max_id,
  read_outbox_max_id,
  unread_count,
  unread_mentions_count,
  unread_reactions_count,
  pinned,
  pinned_order,
  unread_mark,
  hidden_peer_settings_bar,
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
  peer_contact,
  peer_mutual,
  message_id,
  message_from_user_id,
  message_date,
  message_outgoing,
  message_body,
  message_entities_json
FROM paged
ORDER BY
  pinned DESC,
  CASE WHEN pinned THEN COALESCE(NULLIF(pinned_order, 0), 2147483647) ELSE 2147483647 END ASC,
  top_message_date DESC,
  top_message_id DESC,
  peer_id DESC
LIMIT sqlc.arg(limit_count);

-- name: ListDialogSummaryByUser :many
SELECT
  d.peer_type,
  d.peer_id,
  d.folder_id,
  d.top_message_id,
  d.top_message_date,
  d.read_inbox_max_id,
  d.read_outbox_max_id,
  d.unread_count,
  d.unread_mentions_count,
  d.unread_reactions_count,
  d.pinned,
  d.pinned_order,
  d.unread_mark,
  d.hidden_peer_settings_bar
FROM dialogs d
LEFT JOIN contacts c ON d.peer_type = 'user' AND c.user_id = d.user_id AND c.contact_user_id = d.peer_id
WHERE d.user_id = $1
  AND (
    NOT sqlc.arg(has_folder_id)::boolean
    OR (
      sqlc.arg(folder_id)::int < 2
      AND d.folder_id = sqlc.arg(folder_id)::int
    )
    OR (
      sqlc.arg(folder_id)::int >= 2
      AND NOT (sqlc.arg(folder_exclude_archived)::boolean AND d.folder_id = 1)
      AND NOT (sqlc.arg(folder_exclude_read)::boolean AND d.unread_count = 0 AND NOT d.unread_mark)
      AND NOT EXISTS (
        SELECT 1
        FROM (
          SELECT fpt.peer_type, fpi.peer_id
          FROM unnest(sqlc.arg(folder_exclude_peer_types)::text[]) WITH ORDINALITY AS fpt(peer_type, ord)
          JOIN unnest(sqlc.arg(folder_exclude_peer_ids)::bigint[]) WITH ORDINALITY AS fpi(peer_id, ord) USING (ord)
        ) fp
        WHERE fp.peer_type = d.peer_type AND fp.peer_id = d.peer_id
      )
      AND (
        EXISTS (
          SELECT 1
          FROM (
            SELECT fpt.peer_type, fpi.peer_id
            FROM unnest(sqlc.arg(folder_include_peer_types)::text[]) WITH ORDINALITY AS fpt(peer_type, ord)
            JOIN unnest(sqlc.arg(folder_include_peer_ids)::bigint[]) WITH ORDINALITY AS fpi(peer_id, ord) USING (ord)
          ) fp
          WHERE fp.peer_type = d.peer_type AND fp.peer_id = d.peer_id
        )
        OR EXISTS (
          SELECT 1
          FROM (
            SELECT fpt.peer_type, fpi.peer_id
            FROM unnest(sqlc.arg(folder_pinned_peer_types)::text[]) WITH ORDINALITY AS fpt(peer_type, ord)
            JOIN unnest(sqlc.arg(folder_pinned_peer_ids)::bigint[]) WITH ORDINALITY AS fpi(peer_id, ord) USING (ord)
          ) fp
          WHERE fp.peer_type = d.peer_type AND fp.peer_id = d.peer_id
        )
        OR (sqlc.arg(folder_contacts)::boolean AND c.contact_user_id IS NOT NULL)
        OR (sqlc.arg(folder_non_contacts)::boolean AND c.contact_user_id IS NULL)
      )
    )
  )
  AND (NOT sqlc.arg(pinned_only)::boolean OR d.pinned)
  AND (NOT sqlc.arg(exclude_pinned)::boolean OR NOT d.pinned)
ORDER BY
  d.pinned DESC,
  CASE WHEN d.pinned THEN COALESCE(NULLIF(d.pinned_order, 0), 2147483647) ELSE 2147483647 END ASC,
  d.top_message_date DESC,
  d.top_message_id DESC,
  d.peer_id DESC;

-- name: ListDialogsByPeers :many
WITH requested AS (
  SELECT
    (sqlc.arg(peer_types)::text[])[i] AS peer_type,
    (sqlc.arg(peer_ids)::bigint[])[i] AS peer_id,
    i::int AS ord
  FROM generate_subscripts(sqlc.arg(peer_ids)::bigint[], 1) AS g(i)
  WHERE i <= cardinality(sqlc.arg(peer_types)::text[])
),
deduped AS (
  SELECT DISTINCT ON (peer_type, peer_id)
    peer_type,
    peer_id,
    ord
  FROM requested
  ORDER BY peer_type, peer_id, ord
),
base AS (
  SELECT
    sqlc.arg(user_id)::bigint AS user_id,
    r.peer_type,
    r.peer_id,
    COALESCE(d.folder_id, 0)::int AS folder_id,
    COALESCE(d.top_message_id, 0)::int AS top_message_id,
    COALESCE(d.top_message_date, 0)::int AS top_message_date,
    COALESCE(d.read_inbox_max_id, 0)::int AS read_inbox_max_id,
    COALESCE(d.read_outbox_max_id, 0)::int AS read_outbox_max_id,
    COALESCE(d.unread_count, 0)::int AS unread_count,
    COALESCE(d.unread_mentions_count, 0)::int AS unread_mentions_count,
    COALESCE(d.unread_reactions_count, 0)::int AS unread_reactions_count,
    COALESCE(d.pinned, false)::boolean AS pinned,
    COALESCE(d.pinned_order, 0)::int AS pinned_order,
    COALESCE(d.unread_mark, false)::boolean AS unread_mark,
    COALESCE(d.hidden_peer_settings_bar, false)::boolean AS hidden_peer_settings_bar,
    COALESCE(u.id, 0)::bigint AS peer_user_id,
    COALESCE(u.access_hash, 0)::bigint AS peer_access_hash,
    COALESCE(NULLIF(c.contact_phone, ''), u.phone, '')::text AS peer_phone,
    COALESCE(NULLIF(c.contact_first_name, ''), u.first_name, '')::text AS peer_first_name,
    COALESCE(c.contact_last_name, u.last_name, '')::text AS peer_last_name,
    COALESCE(u.username, '')::text AS peer_username,
    COALESCE(u.country_code, '')::text AS peer_country_code,
    COALESCE(u.verified, false)::boolean AS peer_verified,
    COALESCE(u.support, false)::boolean AS peer_support,
    COALESCE(u.last_seen_at, 0)::bigint AS peer_last_seen_at,
    (c.contact_user_id IS NOT NULL)::boolean AS peer_contact,
    COALESCE(c.mutual, false)::boolean AS peer_mutual,
    COALESCE(m.box_id, 0)::int AS message_id,
    COALESCE(m.from_user_id, 0)::bigint AS message_from_user_id,
    COALESCE(m.message_date, 0)::int AS message_date,
    COALESCE(m.outgoing, false)::boolean AS message_outgoing,
    COALESCE(m.body, '')::text AS message_body,
    COALESCE(m.entities::text, '[]')::text AS message_entities_json,
    r.ord
  FROM deduped r
  LEFT JOIN dialogs d
    ON d.user_id = sqlc.arg(user_id)::bigint
    AND d.peer_type = r.peer_type
    AND d.peer_id = r.peer_id
  LEFT JOIN users u ON r.peer_type = 'user' AND u.id = r.peer_id
  LEFT JOIN contacts c ON r.peer_type = 'user' AND c.user_id = sqlc.arg(user_id)::bigint AND c.contact_user_id = r.peer_id
  LEFT JOIN message_boxes m ON m.owner_user_id = sqlc.arg(user_id)::bigint AND m.box_id = d.top_message_id AND NOT m.deleted
)
SELECT
  user_id,
  peer_type::text AS peer_type,
  peer_id::bigint AS peer_id,
  folder_id,
  top_message_id,
  top_message_date,
  read_inbox_max_id,
  read_outbox_max_id,
  unread_count,
  unread_mentions_count,
  unread_reactions_count,
  pinned,
  pinned_order,
  unread_mark,
  hidden_peer_settings_bar,
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
  peer_contact,
  peer_mutual,
  message_id,
  message_from_user_id,
  message_date,
  message_outgoing,
  message_body,
  message_entities_json
FROM base
ORDER BY ord;

-- name: UpsertDialog :exec
INSERT INTO dialogs (
  user_id,
  peer_type,
  peer_id,
  top_message_id,
  top_message_date,
  read_inbox_max_id,
  read_outbox_max_id,
  unread_count,
  unread_mentions_count,
  unread_reactions_count,
  pinned,
  unread_mark
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
ON CONFLICT (user_id, peer_type, peer_id) DO UPDATE SET
  top_message_id = EXCLUDED.top_message_id,
  top_message_date = EXCLUDED.top_message_date,
  read_inbox_max_id = EXCLUDED.read_inbox_max_id,
  read_outbox_max_id = EXCLUDED.read_outbox_max_id,
  unread_count = EXCLUDED.unread_count,
  unread_mentions_count = EXCLUDED.unread_mentions_count,
  unread_reactions_count = EXCLUDED.unread_reactions_count,
  pinned = EXCLUDED.pinned,
  unread_mark = EXCLUDED.unread_mark,
  updated_at = now();

-- name: UpsertOutboxDialog :exec
INSERT INTO dialogs (
  user_id,
  peer_type,
  peer_id,
  top_message_id,
  top_message_date,
  unread_count
) VALUES (
  $1, $2, $3, $4, $5, 0
)
ON CONFLICT (user_id, peer_type, peer_id) DO UPDATE SET
  top_message_id = EXCLUDED.top_message_id,
  top_message_date = EXCLUDED.top_message_date,
  updated_at = now();

-- name: UpsertInboxDialog :exec
INSERT INTO dialogs (
  user_id,
  peer_type,
  peer_id,
  top_message_id,
  top_message_date,
  unread_count
) VALUES (
  $1, $2, $3, $4, $5, 1
)
ON CONFLICT (user_id, peer_type, peer_id) DO UPDATE SET
  top_message_id = EXCLUDED.top_message_id,
  top_message_date = EXCLUDED.top_message_date,
  unread_count = dialogs.unread_count + 1,
  updated_at = now();

-- name: MarkDialogRead :one
WITH target AS (
  SELECT
    d.user_id,
    d.peer_type,
    d.peer_id,
    d.top_message_id,
    d.read_inbox_max_id,
    d.unread_count
  FROM dialogs d
  WHERE d.user_id = $1
    AND d.peer_type = $2
    AND d.peer_id = $3
),
updated AS (
UPDATE dialogs d
SET
  read_inbox_max_id = GREATEST(
    d.read_inbox_max_id,
    CASE WHEN sqlc.arg(max_id)::int > 0 THEN sqlc.arg(max_id)::int ELSE d.top_message_id END
  ),
  unread_count = 0,
  unread_mark = false,
  unread_mentions_count = 0,
  unread_reactions_count = 0,
  updated_at = now()
FROM target
WHERE d.user_id = target.user_id
  AND d.peer_type = target.peer_type
  AND d.peer_id = target.peer_id
RETURNING
  d.user_id,
  d.peer_type,
  d.peer_id,
  d.read_inbox_max_id,
  d.unread_count,
  (
    target.unread_count > 0
    OR (
      CASE WHEN sqlc.arg(max_id)::int > 0 THEN sqlc.arg(max_id)::int ELSE target.top_message_id END
    ) > target.read_inbox_max_id
  )::boolean AS changed
)
SELECT
  user_id,
  peer_type,
  peer_id,
  read_inbox_max_id,
  unread_count,
  changed
FROM updated;

-- name: SetDialogPinned :one
WITH next_order AS (
  SELECT COALESCE(MAX(pinned_order), 0)::int + 1 AS value
  FROM dialogs
  WHERE user_id = sqlc.arg(user_id)::bigint
    AND pinned
),
updated AS (
  UPDATE dialogs d
  SET pinned = sqlc.arg(pinned)::boolean,
      pinned_order = CASE
        WHEN sqlc.arg(pinned)::boolean THEN
          CASE WHEN d.pinned_order > 0 THEN d.pinned_order ELSE next_order.value END
        ELSE 0
      END,
      updated_at = now()
  FROM next_order
  WHERE d.user_id = sqlc.arg(user_id)::bigint
    AND d.peer_type = sqlc.arg(peer_type)::text
    AND d.peer_id = sqlc.arg(peer_id)::bigint
  RETURNING d.user_id
)
SELECT EXISTS (SELECT 1 FROM updated)::boolean AS changed;

-- name: SetDialogUnreadMark :one
WITH updated AS (
  UPDATE dialogs d
  SET unread_mark = sqlc.arg(unread)::boolean,
      updated_at = now()
  WHERE d.user_id = sqlc.arg(user_id)::bigint
    AND d.peer_type = sqlc.arg(peer_type)::text
    AND d.peer_id = sqlc.arg(peer_id)::bigint
  RETURNING d.user_id
)
SELECT EXISTS (SELECT 1 FROM updated)::boolean AS changed;

-- name: ListDialogUnreadMarks :many
SELECT
  peer_type,
  peer_id
FROM dialogs
WHERE user_id = $1
  AND unread_mark
ORDER BY top_message_date DESC, top_message_id DESC, peer_id DESC;

-- name: SetPeerSettingsBarHidden :one
WITH updated AS (
  UPDATE dialogs d
  SET hidden_peer_settings_bar = true,
      updated_at = now()
  WHERE d.user_id = sqlc.arg(user_id)::bigint
    AND d.peer_type = sqlc.arg(peer_type)::text
    AND d.peer_id = sqlc.arg(peer_id)::bigint
  RETURNING d.user_id
)
SELECT EXISTS (SELECT 1 FROM updated)::boolean AS changed;

-- name: GetPeerSettingsBarHidden :one
SELECT hidden_peer_settings_bar
FROM dialogs
WHERE user_id = $1
  AND peer_type = $2
  AND peer_id = $3;

-- name: ReorderPinnedDialogs :exec
WITH requested AS (
  SELECT
    (sqlc.arg(peer_types)::text[])[i] AS peer_type,
    (sqlc.arg(peer_ids)::bigint[])[i] AS peer_id,
    i::int AS pos
  FROM generate_subscripts(sqlc.arg(peer_ids)::bigint[], 1) AS g(i)
  WHERE i <= cardinality(sqlc.arg(peer_types)::text[])
),
deduped AS (
  SELECT DISTINCT ON (peer_type, peer_id)
    peer_type,
    peer_id,
    pos::int AS ord
  FROM requested
  ORDER BY peer_type, peer_id, pos
)
UPDATE dialogs d
SET pinned = true,
    pinned_order = deduped.ord,
    updated_at = now()
FROM deduped
WHERE d.user_id = sqlc.arg(user_id)::bigint
  AND d.peer_type = deduped.peer_type
  AND d.peer_id = deduped.peer_id;

-- name: EditDialogPeerFolders :exec
WITH requested AS (
  SELECT
    (sqlc.arg(peer_types)::text[])[i] AS peer_type,
    (sqlc.arg(peer_ids)::bigint[])[i] AS peer_id,
    (sqlc.arg(folder_ids)::int[])[i] AS folder_id
  FROM generate_subscripts(sqlc.arg(peer_ids)::bigint[], 1) AS g(i)
  WHERE i <= cardinality(sqlc.arg(peer_types)::text[])
    AND i <= cardinality(sqlc.arg(folder_ids)::int[])
),
deduped AS (
  SELECT DISTINCT ON (peer_type, peer_id)
    peer_type,
    peer_id,
    folder_id
  FROM requested
  WHERE folder_id IN (0, 1)
  ORDER BY peer_type, peer_id
)
UPDATE dialogs d
SET folder_id = deduped.folder_id,
    updated_at = now()
FROM deduped
WHERE d.user_id = sqlc.arg(user_id)::bigint
  AND d.peer_type = deduped.peer_type
  AND d.peer_id = deduped.peer_id;

-- name: ClearPinnedDialogsNotInOrder :exec
WITH requested AS (
  SELECT
    (sqlc.arg(peer_types)::text[])[i] AS peer_type,
    (sqlc.arg(peer_ids)::bigint[])[i] AS peer_id
  FROM generate_subscripts(sqlc.arg(peer_ids)::bigint[], 1) AS g(i)
  WHERE i <= cardinality(sqlc.arg(peer_types)::text[])
)
UPDATE dialogs d
SET pinned = false,
    pinned_order = 0,
    updated_at = now()
WHERE d.user_id = sqlc.arg(user_id)::bigint
  AND d.pinned
  AND NOT EXISTS (
    SELECT 1
    FROM requested r
    WHERE r.peer_type = d.peer_type
      AND r.peer_id = d.peer_id
  );

-- name: RefreshDialogAfterMessageDelete :exec
UPDATE dialogs d
SET
  top_message_id = sqlc.arg(top_message_id)::int,
  top_message_date = sqlc.arg(top_message_date)::int,
  unread_count = (
    SELECT COUNT(*)::int
    FROM message_boxes m
    WHERE m.owner_user_id = d.user_id
      AND m.peer_type = d.peer_type
      AND m.peer_id = d.peer_id
      AND NOT m.deleted
      AND NOT m.outgoing
      AND m.box_id > d.read_inbox_max_id
  ),
  unread_mentions_count = 0,
  unread_reactions_count = 0,
  updated_at = now()
WHERE d.user_id = sqlc.arg(user_id)::bigint
  AND d.peer_type = sqlc.arg(peer_type)::text
  AND d.peer_id = sqlc.arg(peer_id)::bigint;

-- name: ClearDialogAfterHistoryDelete :exec
UPDATE dialogs d
SET
  top_message_id = 0,
  top_message_date = 0,
  read_inbox_max_id = GREATEST(d.read_inbox_max_id, d.top_message_id),
  read_outbox_max_id = GREATEST(d.read_outbox_max_id, d.top_message_id),
  unread_count = 0,
  unread_mark = false,
  unread_mentions_count = 0,
  unread_reactions_count = 0,
  updated_at = now()
WHERE d.user_id = sqlc.arg(user_id)::bigint
  AND d.peer_type = sqlc.arg(peer_type)::text
  AND d.peer_id = sqlc.arg(peer_id)::bigint;

-- name: DeleteDialogByPeer :exec
DELETE FROM dialogs
WHERE user_id = $1
  AND peer_type = $2
  AND peer_id = $3;

-- name: ListDialogFolders :many
SELECT
  filter_id,
  is_chatlist,
  filter::text AS filter_json
FROM dialog_filters
WHERE user_id = $1
ORDER BY order_value ASC, filter_id ASC;

-- name: GetDialogFolder :one
SELECT
  filter_id,
  is_chatlist,
  filter::text AS filter_json
FROM dialog_filters
WHERE user_id = $1
  AND filter_id = $2;

-- name: UpsertDialogFolder :exec
INSERT INTO dialog_filters (
  user_id,
  filter_id,
  is_chatlist,
  filter,
  order_value
) VALUES (
  $1,
  $2,
  $3,
  sqlc.arg(filter_json)::jsonb,
  COALESCE(
    (SELECT order_value FROM dialog_filters WHERE user_id = $1 AND filter_id = $2),
    (SELECT COALESCE(MAX(order_value), 0) + 1 FROM dialog_filters WHERE user_id = $1)
  )
)
ON CONFLICT (user_id, filter_id) DO UPDATE SET
  is_chatlist = EXCLUDED.is_chatlist,
  filter = EXCLUDED.filter,
  updated_at = now();

-- name: DeleteDialogFolder :exec
DELETE FROM dialog_filters
WHERE user_id = $1
  AND filter_id = $2;

-- name: ReorderDialogFolders :exec
WITH requested AS (
  SELECT filter_id, ord::int AS order_value
  FROM unnest(sqlc.arg(filter_ids)::int[]) WITH ORDINALITY AS t(filter_id, ord)
),
deduped AS (
  SELECT DISTINCT ON (filter_id)
    filter_id,
    order_value
  FROM requested
  WHERE filter_id >= 2
  ORDER BY filter_id, order_value
)
UPDATE dialog_filters f
SET order_value = deduped.order_value,
    updated_at = now()
FROM deduped
WHERE f.user_id = sqlc.arg(user_id)::bigint
  AND f.filter_id = deduped.filter_id;

-- name: GetDialogFolderTags :one
SELECT tags_enabled
FROM dialog_filter_settings
WHERE user_id = $1;

-- name: SetDialogFolderTags :exec
INSERT INTO dialog_filter_settings (
  user_id,
  tags_enabled
) VALUES (
  $1,
  $2
)
ON CONFLICT (user_id) DO UPDATE SET
  tags_enabled = EXCLUDED.tags_enabled,
  updated_at = now();

-- name: UpsertDialogDraft :exec
INSERT INTO dialog_drafts (
  user_id,
  peer_type,
  peer_id,
  top_message_id,
  date,
  draft
) VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  sqlc.arg(draft_json)::jsonb
)
ON CONFLICT (user_id, peer_type, peer_id, top_message_id) DO UPDATE SET
  date = EXCLUDED.date,
  draft = EXCLUDED.draft,
  updated_at = now();

-- name: DeleteDialogDraft :one
WITH deleted AS (
  DELETE FROM dialog_drafts
  WHERE user_id = $1
    AND peer_type = $2
    AND peer_id = $3
    AND top_message_id = $4
  RETURNING user_id
)
SELECT EXISTS (SELECT 1 FROM deleted)::boolean AS changed;

-- name: ListDialogDrafts :many
SELECT draft::text AS draft_json
FROM dialog_drafts
WHERE user_id = $1
ORDER BY date DESC, peer_type ASC, peer_id DESC, top_message_id DESC
LIMIT sqlc.arg(limit_count);

-- name: ClearDialogDrafts :many
WITH doomed AS (
  SELECT d.user_id, d.peer_type, d.peer_id, d.top_message_id
  FROM dialog_drafts d
  WHERE d.user_id = $1
  ORDER BY d.date DESC, d.peer_type ASC, d.peer_id DESC, d.top_message_id DESC
  LIMIT sqlc.arg(limit_count)
),
deleted AS (
  DELETE FROM dialog_drafts d
  USING doomed
  WHERE d.user_id = doomed.user_id
    AND d.peer_type = doomed.peer_type
    AND d.peer_id = doomed.peer_id
    AND d.top_message_id = doomed.top_message_id
  RETURNING d.draft::text AS draft_json
)
SELECT draft_json
FROM deleted;
