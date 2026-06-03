-- Revert generated ordinary user ids to the original first-phase base when possible.
-- Existing users keep their ids; the next generated id is never moved below MAX(id)+1.

SELECT setval(
    pg_get_serial_sequence('users', 'id'),
    GREATEST((SELECT COALESCE(MAX(id), 0) FROM users), 999999999),
    true
);
