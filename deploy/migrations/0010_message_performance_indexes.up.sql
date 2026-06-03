-- 0010_message_performance_indexes: indexes for second-stage message seek paths.

CREATE INDEX IF NOT EXISTS message_boxes_dialog_date_seek_idx
    ON message_boxes (owner_user_id, peer_type, peer_id, message_date DESC, box_id DESC)
    WHERE NOT deleted;

CREATE INDEX IF NOT EXISTS dispatch_outbox_dispatching_stale_idx
    ON dispatch_outbox (status, updated_at, target_user_id, id)
    WHERE status = 'dispatching';
