ALTER TABLE channel_messages
    ADD COLUMN IF NOT EXISTS reply_to_msg_id INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reply_to_peer_type VARCHAR(16) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS reply_to_peer_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reply_to_top_id INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS discussion_channel_id BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS discussion_message_id INT NOT NULL DEFAULT 0;

UPDATE channel_messages
SET
    reply_to_msg_id = CASE
        WHEN (reply_to ->> 'MessageID') ~ '^[0-9]+$' THEN (reply_to ->> 'MessageID')::INT
        ELSE reply_to_msg_id
    END,
    reply_to_peer_type = COALESCE(NULLIF(reply_to #>> '{Peer,Type}', ''), reply_to_peer_type),
    reply_to_peer_id = CASE
        WHEN (reply_to #>> '{Peer,ID}') ~ '^[0-9]+$' THEN (reply_to #>> '{Peer,ID}')::BIGINT
        ELSE reply_to_peer_id
    END,
    reply_to_top_id = CASE
        WHEN (reply_to ->> 'TopMessageID') ~ '^[0-9]+$' AND (reply_to ->> 'TopMessageID')::INT > 0
            THEN (reply_to ->> 'TopMessageID')::INT
        WHEN (reply_to ->> 'MessageID') ~ '^[0-9]+$'
            THEN (reply_to ->> 'MessageID')::INT
        ELSE reply_to_top_id
    END
WHERE reply_to <> '{}'::jsonb
  AND reply_to_msg_id = 0;

CREATE INDEX IF NOT EXISTS channel_messages_reply_thread_idx
    ON channel_messages (channel_id, reply_to_top_id, id DESC)
    WHERE reply_to_top_id > 0 AND NOT deleted;

CREATE INDEX IF NOT EXISTS channel_messages_discussion_ref_idx
    ON channel_messages (discussion_channel_id, discussion_message_id)
    WHERE discussion_channel_id <> 0 AND discussion_message_id <> 0 AND NOT deleted;
