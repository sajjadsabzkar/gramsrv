DROP INDEX IF EXISTS channel_messages_discussion_ref_idx;
DROP INDEX IF EXISTS channel_messages_reply_thread_idx;

ALTER TABLE channel_messages
    DROP COLUMN IF EXISTS discussion_message_id,
    DROP COLUMN IF EXISTS discussion_channel_id,
    DROP COLUMN IF EXISTS reply_to_top_id,
    DROP COLUMN IF EXISTS reply_to_peer_id,
    DROP COLUMN IF EXISTS reply_to_peer_type,
    DROP COLUMN IF EXISTS reply_to_msg_id;
