DROP INDEX IF EXISTS message_boxes_reply_lookup_idx;

ALTER TABLE message_boxes
    DROP COLUMN IF EXISTS fwd_date,
    DROP COLUMN IF EXISTS fwd_from_name,
    DROP COLUMN IF EXISTS fwd_from_peer_id,
    DROP COLUMN IF EXISTS fwd_from_peer_type,
    DROP COLUMN IF EXISTS quote_offset,
    DROP COLUMN IF EXISTS quote_entities,
    DROP COLUMN IF EXISTS quote_text,
    DROP COLUMN IF EXISTS reply_to_top_id,
    DROP COLUMN IF EXISTS reply_to_peer_id,
    DROP COLUMN IF EXISTS reply_to_peer_type,
    DROP COLUMN IF EXISTS reply_to_msg_id,
    DROP COLUMN IF EXISTS noforwards,
    DROP COLUMN IF EXISTS silent;

ALTER TABLE private_messages
    DROP COLUMN IF EXISTS fwd_date,
    DROP COLUMN IF EXISTS fwd_from_name,
    DROP COLUMN IF EXISTS fwd_from_peer_id,
    DROP COLUMN IF EXISTS fwd_from_peer_type,
    DROP COLUMN IF EXISTS quote_offset,
    DROP COLUMN IF EXISTS quote_entities,
    DROP COLUMN IF EXISTS quote_text,
    DROP COLUMN IF EXISTS reply_to_top_id,
    DROP COLUMN IF EXISTS reply_to_peer_id,
    DROP COLUMN IF EXISTS reply_to_peer_type,
    DROP COLUMN IF EXISTS reply_to_msg_id,
    DROP COLUMN IF EXISTS noforwards,
    DROP COLUMN IF EXISTS silent;
