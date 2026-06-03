WITH fixed AS (
    SELECT
        d.user_id,
        d.channel_id,
        COUNT(msg.id)::int AS unread_count
    FROM channel_dialogs d
    LEFT JOIN channel_messages msg
      ON msg.channel_id = d.channel_id
     AND msg.id > d.read_inbox_max_id
     AND msg.id <= d.top_message_id
     AND NOT msg.deleted
     AND msg.sender_user_id <> d.user_id
    GROUP BY d.user_id, d.channel_id
)
UPDATE channel_dialogs d
SET unread_count = fixed.unread_count,
    updated_at = now()
FROM fixed
WHERE fixed.user_id = d.user_id
  AND fixed.channel_id = d.channel_id
  AND d.unread_count IS DISTINCT FROM fixed.unread_count;
