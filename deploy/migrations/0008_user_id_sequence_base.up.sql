-- 0008_user_id_sequence_base: move ordinary user ids to the agreed timestamp range.
--
-- Base: 2026-06-01 00:00:00 Asia/Shanghai => Unix seconds 1780243200.
-- Existing users keep their ids; the next generated id is at least this base.

SELECT setval(
    pg_get_serial_sequence('users', 'id'),
    GREATEST((SELECT COALESCE(MAX(id), 0) FROM users), 1780243199),
    true
);
