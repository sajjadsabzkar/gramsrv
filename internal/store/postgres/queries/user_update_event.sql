-- name: AppendUserUpdateEvent :exec
INSERT INTO user_update_events (
  user_id,
  pts,
  pts_count,
  date,
  event_type,
  event_bool,
  event_peers,
  peer_settings,
  message_ids,
  dialog_filter,
  filter_order,
  folder_peers,
  message_box_id,
  peer_type,
  peer_id,
  filter_id,
  max_id,
  still_unread_count,
  tags_enabled
) VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  sqlc.arg(event_bool)::boolean,
  sqlc.arg(event_peers)::jsonb,
  sqlc.arg(peer_settings)::jsonb,
  sqlc.arg(message_ids)::jsonb,
  sqlc.arg(dialog_filter)::jsonb,
  sqlc.arg(filter_order)::jsonb,
  sqlc.arg(folder_peers)::jsonb,
  sqlc.narg(message_box_id),
  sqlc.narg(peer_type)::text,
  sqlc.narg(peer_id)::bigint,
  sqlc.arg(filter_id)::int,
  sqlc.arg(max_id)::int,
  sqlc.arg(still_unread_count)::int,
  sqlc.arg(tags_enabled)::boolean
)
ON CONFLICT (user_id, pts) DO NOTHING;

-- name: ListUserUpdateEventsAfter :many
SELECT
  e.user_id,
  e.pts,
  e.pts_count,
  e.date,
  e.event_type,
  e.event_bool,
  COALESCE(e.event_peers::text, '[]')::text AS event_peers_json,
  COALESCE(e.peer_settings::text, '{}')::text AS peer_settings_json,
  COALESCE(e.message_ids::text, '[]')::text AS message_ids_json,
  COALESCE(e.dialog_filter::text, '{}')::text AS dialog_filter_json,
  COALESCE(e.filter_order::text, '[]')::text AS filter_order_json,
  COALESCE(e.folder_peers::text, '[]')::text AS folder_peers_json,
  COALESCE(e.peer_type, '')::text AS event_peer_type,
  COALESCE(e.peer_id, 0)::bigint AS event_peer_id,
  e.filter_id,
  e.max_id,
  e.still_unread_count,
  e.tags_enabled,
  COALESCE(m.box_id, 0)::int AS message_id,
  COALESCE(m.private_message_id, 0)::bigint AS private_message_id,
  COALESCE(m.owner_user_id, 0)::bigint AS owner_user_id,
  COALESCE(m.peer_type, '')::text AS peer_type,
  COALESCE(m.peer_id, 0)::bigint AS peer_id,
  COALESCE(m.from_user_id, 0)::bigint AS from_user_id,
  COALESCE(m.message_date, 0)::int AS message_date,
  COALESCE(m.edit_date, 0)::int AS edit_date,
  COALESCE(m.outgoing, false)::boolean AS outgoing,
  COALESCE(m.body, '')::text AS body,
  COALESCE(m.entities::text, '[]')::text AS message_entities_json,
  COALESCE(m.silent, false)::boolean AS silent,
  COALESCE(m.noforwards, false)::boolean AS noforwards,
  COALESCE(m.reply_to_msg_id, 0)::int AS reply_to_msg_id,
  COALESCE(m.reply_to_peer_type, '')::text AS reply_to_peer_type,
  COALESCE(m.reply_to_peer_id, 0)::bigint AS reply_to_peer_id,
  COALESCE(m.reply_to_top_id, 0)::int AS reply_to_top_id,
  COALESCE(m.quote_text, '')::text AS quote_text,
  COALESCE(m.quote_entities::text, '[]')::text AS quote_entities_json,
  COALESCE(m.quote_offset, 0)::int AS quote_offset,
  COALESCE(m.fwd_from_peer_type, '')::text AS fwd_from_peer_type,
  COALESCE(m.fwd_from_peer_id, 0)::bigint AS fwd_from_peer_id,
  COALESCE(m.fwd_from_name, '')::text AS fwd_from_name,
  COALESCE(m.fwd_date, 0)::int AS fwd_date,
  COALESCE(m.media::text, '{}')::text AS media_json,
  COALESCE(m.media_unread, false)::boolean AS media_unread,
  COALESCE(m.reaction_unread, false)::boolean AS reaction_unread,
  COALESCE(peer_u.id, 0)::bigint AS peer_user_id,
  COALESCE(peer_u.access_hash, 0)::bigint AS peer_access_hash,
  COALESCE(peer_u.phone, '')::text AS peer_phone,
  COALESCE(peer_u.first_name, '')::text AS peer_first_name,
  COALESCE(peer_u.last_name, '')::text AS peer_last_name,
  COALESCE(peer_u.username, '')::text AS peer_username,
  COALESCE(peer_u.country_code, '')::text AS peer_country_code,
  COALESCE(peer_u.verified, false)::boolean AS peer_verified,
  COALESCE(peer_u.support, false)::boolean AS peer_support,
  COALESCE(from_u.id, 0)::bigint AS from_user_user_id,
  COALESCE(from_u.access_hash, 0)::bigint AS from_user_access_hash,
  COALESCE(from_u.phone, '')::text AS from_user_phone,
  COALESCE(from_u.first_name, '')::text AS from_user_first_name,
  COALESCE(from_u.last_name, '')::text AS from_user_last_name,
  COALESCE(from_u.username, '')::text AS from_user_username,
  COALESCE(from_u.country_code, '')::text AS from_user_country_code,
  COALESCE(from_u.verified, false)::boolean AS from_user_verified,
  COALESCE(from_u.support, false)::boolean AS from_user_support,
  COALESCE(fwd_u.id, 0)::bigint AS fwd_user_id,
  COALESCE(fwd_u.access_hash, 0)::bigint AS fwd_user_access_hash,
  COALESCE(fwd_u.phone, '')::text AS fwd_user_phone,
  COALESCE(fwd_u.first_name, '')::text AS fwd_user_first_name,
  COALESCE(fwd_u.last_name, '')::text AS fwd_user_last_name,
  COALESCE(fwd_u.username, '')::text AS fwd_user_username,
  COALESCE(fwd_u.country_code, '')::text AS fwd_user_country_code,
  COALESCE(fwd_u.verified, false)::boolean AS fwd_user_verified,
  COALESCE(fwd_u.support, false)::boolean AS fwd_user_support,
  COALESCE(reply_u.id, 0)::bigint AS reply_user_id,
  COALESCE(reply_u.access_hash, 0)::bigint AS reply_user_access_hash,
  COALESCE(reply_u.phone, '')::text AS reply_user_phone,
  COALESCE(reply_u.first_name, '')::text AS reply_user_first_name,
  COALESCE(reply_u.last_name, '')::text AS reply_user_last_name,
  COALESCE(reply_u.username, '')::text AS reply_user_username,
  COALESCE(reply_u.country_code, '')::text AS reply_user_country_code,
  COALESCE(reply_u.verified, false)::boolean AS reply_user_verified,
  COALESCE(reply_u.support, false)::boolean AS reply_user_support,
  COALESCE(fwd_ch.id, 0)::bigint AS fwd_channel_id,
  COALESCE(fwd_ch.access_hash, 0)::bigint AS fwd_channel_access_hash,
  COALESCE(fwd_ch.creator_user_id, 0)::bigint AS fwd_channel_creator_user_id,
  COALESCE(fwd_ch.title, '')::text AS fwd_channel_title,
  COALESCE(fwd_ch.about, '')::text AS fwd_channel_about,
  COALESCE(fwd_ch.username, '')::text AS fwd_channel_username,
  COALESCE(fwd_ch.broadcast, false)::boolean AS fwd_channel_broadcast,
  COALESCE(fwd_ch.megagroup, false)::boolean AS fwd_channel_megagroup,
  COALESCE(fwd_ch.forum, false)::boolean AS fwd_channel_forum,
  COALESCE(fwd_ch.noforwards, false)::boolean AS fwd_channel_noforwards,
  COALESCE(fwd_ch.signatures, false)::boolean AS fwd_channel_signatures,
  COALESCE(fwd_ch.pre_history_hidden, false)::boolean AS fwd_channel_pre_history_hidden,
  COALESCE(fwd_ch.slowmode_seconds, 0)::int AS fwd_channel_slowmode_seconds,
  COALESCE(fwd_ch.default_banned_rights::text, '{}')::text AS fwd_channel_default_banned_rights,
  COALESCE(fwd_ch.participants_count, 0)::int AS fwd_channel_participants_count,
  COALESCE(fwd_ch.admins_count, 0)::int AS fwd_channel_admins_count,
  COALESCE(fwd_ch.kicked_count, 0)::int AS fwd_channel_kicked_count,
  COALESCE(fwd_ch.banned_count, 0)::int AS fwd_channel_banned_count,
  COALESCE(fwd_ch.top_message_id, 0)::int AS fwd_channel_top_message_id,
  COALESCE(fwd_ch.pinned_message_id, 0)::int AS fwd_channel_pinned_message_id,
  COALESCE(fwd_ch.pts, 0)::int AS fwd_channel_pts,
  COALESCE(fwd_ch.ttl_period, 0)::int AS fwd_channel_ttl_period,
  COALESCE(fwd_ch.date, 0)::int AS fwd_channel_date,
  COALESCE(fwd_ch.deleted, false)::boolean AS fwd_channel_deleted,
  COALESCE(reply_ch.id, 0)::bigint AS reply_channel_id,
  COALESCE(reply_ch.access_hash, 0)::bigint AS reply_channel_access_hash,
  COALESCE(reply_ch.creator_user_id, 0)::bigint AS reply_channel_creator_user_id,
  COALESCE(reply_ch.title, '')::text AS reply_channel_title,
  COALESCE(reply_ch.about, '')::text AS reply_channel_about,
  COALESCE(reply_ch.username, '')::text AS reply_channel_username,
  COALESCE(reply_ch.broadcast, false)::boolean AS reply_channel_broadcast,
  COALESCE(reply_ch.megagroup, false)::boolean AS reply_channel_megagroup,
  COALESCE(reply_ch.forum, false)::boolean AS reply_channel_forum,
  COALESCE(reply_ch.noforwards, false)::boolean AS reply_channel_noforwards,
  COALESCE(reply_ch.signatures, false)::boolean AS reply_channel_signatures,
  COALESCE(reply_ch.pre_history_hidden, false)::boolean AS reply_channel_pre_history_hidden,
  COALESCE(reply_ch.slowmode_seconds, 0)::int AS reply_channel_slowmode_seconds,
  COALESCE(reply_ch.default_banned_rights::text, '{}')::text AS reply_channel_default_banned_rights,
  COALESCE(reply_ch.participants_count, 0)::int AS reply_channel_participants_count,
  COALESCE(reply_ch.admins_count, 0)::int AS reply_channel_admins_count,
  COALESCE(reply_ch.kicked_count, 0)::int AS reply_channel_kicked_count,
  COALESCE(reply_ch.banned_count, 0)::int AS reply_channel_banned_count,
  COALESCE(reply_ch.top_message_id, 0)::int AS reply_channel_top_message_id,
  COALESCE(reply_ch.pinned_message_id, 0)::int AS reply_channel_pinned_message_id,
  COALESCE(reply_ch.pts, 0)::int AS reply_channel_pts,
  COALESCE(reply_ch.ttl_period, 0)::int AS reply_channel_ttl_period,
  COALESCE(reply_ch.date, 0)::int AS reply_channel_date,
  COALESCE(reply_ch.deleted, false)::boolean AS reply_channel_deleted
FROM user_update_events e
LEFT JOIN message_boxes m ON m.owner_user_id = e.user_id AND m.box_id = e.message_box_id
LEFT JOIN users peer_u ON m.peer_type = 'user' AND peer_u.id = m.peer_id
LEFT JOIN users from_u ON from_u.id = m.from_user_id
LEFT JOIN users fwd_u ON m.fwd_from_peer_type = 'user' AND fwd_u.id = m.fwd_from_peer_id
LEFT JOIN users reply_u ON m.reply_to_peer_type = 'user' AND reply_u.id = m.reply_to_peer_id
LEFT JOIN channels fwd_ch ON m.fwd_from_peer_type = 'channel' AND fwd_ch.id = m.fwd_from_peer_id
LEFT JOIN channels reply_ch ON m.reply_to_peer_type = 'channel' AND reply_ch.id = m.reply_to_peer_id
WHERE e.user_id = $1
  AND e.pts > $2
ORDER BY e.pts ASC
LIMIT sqlc.arg(limit_count);

-- name: MaxUserPts :one
SELECT COALESCE(MAX(pts), 0)::int AS max_pts
FROM user_update_events
WHERE user_id = $1;

-- name: RecentUserPts :many
-- 取某 user 最近的一段 pts（降序），供计算「最大连续已提交 pts」用。
-- 只看顶部窗口：瞬时空洞只可能出现在最近在途事务区，窗口足够大即可覆盖其下方连续。
SELECT pts, pts_count
FROM user_update_events
WHERE user_id = $1
ORDER BY pts DESC
LIMIT sqlc.arg(window_size);

-- name: EnsureUserUpdateWatermark :exec
INSERT INTO user_update_watermarks (user_id, contiguous_pts)
VALUES ($1, 0)
ON CONFLICT (user_id) DO NOTHING;

-- name: GetUserUpdateWatermark :one
SELECT contiguous_pts
FROM user_update_watermarks
WHERE user_id = $1;

-- name: LockUserUpdateWatermark :one
SELECT contiguous_pts
FROM user_update_watermarks
WHERE user_id = $1
FOR UPDATE;

-- name: NextUserPtsAfter :many
SELECT pts, pts_count
FROM user_update_events
WHERE user_id = $1
  AND pts > $2
ORDER BY pts ASC
LIMIT sqlc.arg(limit_count);

-- name: SaveUserUpdateWatermark :exec
INSERT INTO user_update_watermarks (user_id, contiguous_pts, updated_at)
VALUES ($1, $2, now())
ON CONFLICT (user_id) DO UPDATE SET
  contiguous_pts = GREATEST(user_update_watermarks.contiguous_pts, EXCLUDED.contiguous_pts),
  updated_at = now();

-- name: EnqueueDispatch :exec
INSERT INTO dispatch_outbox (
  target_user_id,
  pts,
  event_type,
  exclude_auth_key_id,
  exclude_session_id
) VALUES (
  $1, $2, $3, $4, $5
)
ON CONFLICT DO NOTHING;

-- name: ClaimDispatchOutbox :many
WITH picked AS (
  SELECT target_user_id, id
  FROM dispatch_outbox
  WHERE (
      status = 'pending'
      AND next_attempt_at <= now()
    )
    OR (
      status = 'dispatching'
      AND updated_at < now() - make_interval(secs => sqlc.arg(lease_seconds)::int)
    )
  ORDER BY next_attempt_at ASC, target_user_id ASC, id ASC
  LIMIT sqlc.arg(limit_count)
  FOR UPDATE SKIP LOCKED
)
UPDATE dispatch_outbox d
SET
  status = 'dispatching',
  attempts = d.attempts + 1,
  updated_at = now()
FROM picked p
WHERE d.target_user_id = p.target_user_id
  AND d.id = p.id
RETURNING
  d.id,
  d.target_user_id,
  d.pts,
  d.event_type,
  d.exclude_auth_key_id,
  d.exclude_session_id,
  d.attempts;

-- name: MarkDispatchDelivered :exec
-- 方案 A：投递成功即删除。outbox 是任务队列，delivered 行无保留价值
-- （消息在 message_boxes、离线补偿在 user_update_events），删除让表维持「未完成任务」小稳态。
DELETE FROM dispatch_outbox
WHERE target_user_id = $1
  AND id = $2;

-- name: MarkDispatchFailed :exec
UPDATE dispatch_outbox
SET
  status = CASE WHEN attempts >= 5 THEN 'failed' ELSE 'pending' END,
  next_attempt_at = CASE
    WHEN attempts >= 5 THEN next_attempt_at
    ELSE now() + make_interval(secs => LEAST(60, attempts * attempts))
  END,
  last_error = $3,
  updated_at = now()
WHERE target_user_id = $1
  AND id = $2;

-- name: BatchListDispatchEvents :many
-- 按 (user_id, pts) 精确批量取账号事件，供 outbox worker 一次性加载一批 claim 的事件详情，
-- 取代逐条 ListUserUpdateEventsAfter。列与 ListUserUpdateEventsAfter 完全一致以复用转换逻辑。
SELECT
  e.user_id,
  e.pts,
  e.pts_count,
  e.date,
  e.event_type,
  e.event_bool,
  COALESCE(e.event_peers::text, '[]')::text AS event_peers_json,
  COALESCE(e.peer_settings::text, '{}')::text AS peer_settings_json,
  COALESCE(e.message_ids::text, '[]')::text AS message_ids_json,
  COALESCE(e.dialog_filter::text, '{}')::text AS dialog_filter_json,
  COALESCE(e.filter_order::text, '[]')::text AS filter_order_json,
  COALESCE(e.folder_peers::text, '[]')::text AS folder_peers_json,
  COALESCE(e.peer_type, '')::text AS event_peer_type,
  COALESCE(e.peer_id, 0)::bigint AS event_peer_id,
  e.filter_id,
  e.max_id,
  e.still_unread_count,
  e.tags_enabled,
  COALESCE(m.box_id, 0)::int AS message_id,
  COALESCE(m.private_message_id, 0)::bigint AS private_message_id,
  COALESCE(m.owner_user_id, 0)::bigint AS owner_user_id,
  COALESCE(m.peer_type, '')::text AS peer_type,
  COALESCE(m.peer_id, 0)::bigint AS peer_id,
  COALESCE(m.from_user_id, 0)::bigint AS from_user_id,
  COALESCE(m.message_date, 0)::int AS message_date,
  COALESCE(m.edit_date, 0)::int AS edit_date,
  COALESCE(m.outgoing, false)::boolean AS outgoing,
  COALESCE(m.body, '')::text AS body,
  COALESCE(m.entities::text, '[]')::text AS message_entities_json,
  COALESCE(m.silent, false)::boolean AS silent,
  COALESCE(m.noforwards, false)::boolean AS noforwards,
  COALESCE(m.reply_to_msg_id, 0)::int AS reply_to_msg_id,
  COALESCE(m.reply_to_peer_type, '')::text AS reply_to_peer_type,
  COALESCE(m.reply_to_peer_id, 0)::bigint AS reply_to_peer_id,
  COALESCE(m.reply_to_top_id, 0)::int AS reply_to_top_id,
  COALESCE(m.quote_text, '')::text AS quote_text,
  COALESCE(m.quote_entities::text, '[]')::text AS quote_entities_json,
  COALESCE(m.quote_offset, 0)::int AS quote_offset,
  COALESCE(m.fwd_from_peer_type, '')::text AS fwd_from_peer_type,
  COALESCE(m.fwd_from_peer_id, 0)::bigint AS fwd_from_peer_id,
  COALESCE(m.fwd_from_name, '')::text AS fwd_from_name,
  COALESCE(m.fwd_date, 0)::int AS fwd_date,
  COALESCE(m.media::text, '{}')::text AS media_json,
  COALESCE(m.media_unread, false)::boolean AS media_unread,
  COALESCE(m.reaction_unread, false)::boolean AS reaction_unread,
  COALESCE(peer_u.id, 0)::bigint AS peer_user_id,
  COALESCE(peer_u.access_hash, 0)::bigint AS peer_access_hash,
  COALESCE(peer_u.phone, '')::text AS peer_phone,
  COALESCE(peer_u.first_name, '')::text AS peer_first_name,
  COALESCE(peer_u.last_name, '')::text AS peer_last_name,
  COALESCE(peer_u.username, '')::text AS peer_username,
  COALESCE(peer_u.country_code, '')::text AS peer_country_code,
  COALESCE(peer_u.verified, false)::boolean AS peer_verified,
  COALESCE(peer_u.support, false)::boolean AS peer_support,
  COALESCE(from_u.id, 0)::bigint AS from_user_user_id,
  COALESCE(from_u.access_hash, 0)::bigint AS from_user_access_hash,
  COALESCE(from_u.phone, '')::text AS from_user_phone,
  COALESCE(from_u.first_name, '')::text AS from_user_first_name,
  COALESCE(from_u.last_name, '')::text AS from_user_last_name,
  COALESCE(from_u.username, '')::text AS from_user_username,
  COALESCE(from_u.country_code, '')::text AS from_user_country_code,
  COALESCE(from_u.verified, false)::boolean AS from_user_verified,
  COALESCE(from_u.support, false)::boolean AS from_user_support,
  COALESCE(fwd_u.id, 0)::bigint AS fwd_user_id,
  COALESCE(fwd_u.access_hash, 0)::bigint AS fwd_user_access_hash,
  COALESCE(fwd_u.phone, '')::text AS fwd_user_phone,
  COALESCE(fwd_u.first_name, '')::text AS fwd_user_first_name,
  COALESCE(fwd_u.last_name, '')::text AS fwd_user_last_name,
  COALESCE(fwd_u.username, '')::text AS fwd_user_username,
  COALESCE(fwd_u.country_code, '')::text AS fwd_user_country_code,
  COALESCE(fwd_u.verified, false)::boolean AS fwd_user_verified,
  COALESCE(fwd_u.support, false)::boolean AS fwd_user_support,
  COALESCE(reply_u.id, 0)::bigint AS reply_user_id,
  COALESCE(reply_u.access_hash, 0)::bigint AS reply_user_access_hash,
  COALESCE(reply_u.phone, '')::text AS reply_user_phone,
  COALESCE(reply_u.first_name, '')::text AS reply_user_first_name,
  COALESCE(reply_u.last_name, '')::text AS reply_user_last_name,
  COALESCE(reply_u.username, '')::text AS reply_user_username,
  COALESCE(reply_u.country_code, '')::text AS reply_user_country_code,
  COALESCE(reply_u.verified, false)::boolean AS reply_user_verified,
  COALESCE(reply_u.support, false)::boolean AS reply_user_support,
  COALESCE(fwd_ch.id, 0)::bigint AS fwd_channel_id,
  COALESCE(fwd_ch.access_hash, 0)::bigint AS fwd_channel_access_hash,
  COALESCE(fwd_ch.creator_user_id, 0)::bigint AS fwd_channel_creator_user_id,
  COALESCE(fwd_ch.title, '')::text AS fwd_channel_title,
  COALESCE(fwd_ch.about, '')::text AS fwd_channel_about,
  COALESCE(fwd_ch.username, '')::text AS fwd_channel_username,
  COALESCE(fwd_ch.broadcast, false)::boolean AS fwd_channel_broadcast,
  COALESCE(fwd_ch.megagroup, false)::boolean AS fwd_channel_megagroup,
  COALESCE(fwd_ch.forum, false)::boolean AS fwd_channel_forum,
  COALESCE(fwd_ch.noforwards, false)::boolean AS fwd_channel_noforwards,
  COALESCE(fwd_ch.signatures, false)::boolean AS fwd_channel_signatures,
  COALESCE(fwd_ch.pre_history_hidden, false)::boolean AS fwd_channel_pre_history_hidden,
  COALESCE(fwd_ch.slowmode_seconds, 0)::int AS fwd_channel_slowmode_seconds,
  COALESCE(fwd_ch.default_banned_rights::text, '{}')::text AS fwd_channel_default_banned_rights,
  COALESCE(fwd_ch.participants_count, 0)::int AS fwd_channel_participants_count,
  COALESCE(fwd_ch.admins_count, 0)::int AS fwd_channel_admins_count,
  COALESCE(fwd_ch.kicked_count, 0)::int AS fwd_channel_kicked_count,
  COALESCE(fwd_ch.banned_count, 0)::int AS fwd_channel_banned_count,
  COALESCE(fwd_ch.top_message_id, 0)::int AS fwd_channel_top_message_id,
  COALESCE(fwd_ch.pinned_message_id, 0)::int AS fwd_channel_pinned_message_id,
  COALESCE(fwd_ch.pts, 0)::int AS fwd_channel_pts,
  COALESCE(fwd_ch.ttl_period, 0)::int AS fwd_channel_ttl_period,
  COALESCE(fwd_ch.date, 0)::int AS fwd_channel_date,
  COALESCE(fwd_ch.deleted, false)::boolean AS fwd_channel_deleted,
  COALESCE(reply_ch.id, 0)::bigint AS reply_channel_id,
  COALESCE(reply_ch.access_hash, 0)::bigint AS reply_channel_access_hash,
  COALESCE(reply_ch.creator_user_id, 0)::bigint AS reply_channel_creator_user_id,
  COALESCE(reply_ch.title, '')::text AS reply_channel_title,
  COALESCE(reply_ch.about, '')::text AS reply_channel_about,
  COALESCE(reply_ch.username, '')::text AS reply_channel_username,
  COALESCE(reply_ch.broadcast, false)::boolean AS reply_channel_broadcast,
  COALESCE(reply_ch.megagroup, false)::boolean AS reply_channel_megagroup,
  COALESCE(reply_ch.forum, false)::boolean AS reply_channel_forum,
  COALESCE(reply_ch.noforwards, false)::boolean AS reply_channel_noforwards,
  COALESCE(reply_ch.signatures, false)::boolean AS reply_channel_signatures,
  COALESCE(reply_ch.pre_history_hidden, false)::boolean AS reply_channel_pre_history_hidden,
  COALESCE(reply_ch.slowmode_seconds, 0)::int AS reply_channel_slowmode_seconds,
  COALESCE(reply_ch.default_banned_rights::text, '{}')::text AS reply_channel_default_banned_rights,
  COALESCE(reply_ch.participants_count, 0)::int AS reply_channel_participants_count,
  COALESCE(reply_ch.admins_count, 0)::int AS reply_channel_admins_count,
  COALESCE(reply_ch.kicked_count, 0)::int AS reply_channel_kicked_count,
  COALESCE(reply_ch.banned_count, 0)::int AS reply_channel_banned_count,
  COALESCE(reply_ch.top_message_id, 0)::int AS reply_channel_top_message_id,
  COALESCE(reply_ch.pinned_message_id, 0)::int AS reply_channel_pinned_message_id,
  COALESCE(reply_ch.pts, 0)::int AS reply_channel_pts,
  COALESCE(reply_ch.ttl_period, 0)::int AS reply_channel_ttl_period,
  COALESCE(reply_ch.date, 0)::int AS reply_channel_date,
  COALESCE(reply_ch.deleted, false)::boolean AS reply_channel_deleted
FROM unnest(@user_ids::bigint[]) WITH ORDINALITY AS u(user_id, ord)
JOIN unnest(@pts_list::int[]) WITH ORDINALITY AS p(pts, ord) USING (ord)
JOIN user_update_events e ON e.user_id = u.user_id AND e.pts = p.pts
LEFT JOIN message_boxes m ON m.owner_user_id = e.user_id AND m.box_id = e.message_box_id
LEFT JOIN users peer_u ON m.peer_type = 'user' AND peer_u.id = m.peer_id
LEFT JOIN users from_u ON from_u.id = m.from_user_id
LEFT JOIN users fwd_u ON m.fwd_from_peer_type = 'user' AND fwd_u.id = m.fwd_from_peer_id
LEFT JOIN users reply_u ON m.reply_to_peer_type = 'user' AND reply_u.id = m.reply_to_peer_id
LEFT JOIN channels fwd_ch ON m.fwd_from_peer_type = 'channel' AND fwd_ch.id = m.fwd_from_peer_id
LEFT JOIN channels reply_ch ON m.reply_to_peer_type = 'channel' AND reply_ch.id = m.reply_to_peer_id;

-- name: MarkDispatchDeliveredBatch :exec
-- 批量删除一批已投递的 (target_user_id, id)；target_user_id 入 WHERE 保证分区裁剪。
DELETE FROM dispatch_outbox d
USING unnest(@target_user_ids::bigint[]) WITH ORDINALITY AS tu(target_user_id, ord)
JOIN unnest(@ids::bigint[]) WITH ORDINALITY AS di(id, ord) USING (ord)
WHERE d.target_user_id = tu.target_user_id
  AND d.id = di.id;

-- name: DeleteFailedDispatchOutbox :one
WITH doomed AS (
  SELECT target_user_id, id
  FROM dispatch_outbox
  WHERE status = 'failed'
    AND updated_at < now() - make_interval(secs => sqlc.arg(older_than_seconds)::int)
  ORDER BY updated_at ASC, target_user_id ASC, id ASC
  LIMIT sqlc.arg(limit_count)
),
deleted AS (
  DELETE FROM dispatch_outbox d
  USING doomed x
  WHERE d.target_user_id = x.target_user_id
    AND d.id = x.id
  RETURNING d.id
)
SELECT count(*)::int AS deleted_count
FROM deleted;
