-- System sticker sets are request-by-input resources for TDesktop
-- (animated emoji, dice, generic animations). They must not be persisted as
-- normal installed sticker packs, otherwise the client can abort/redo its
-- installed stickers local-cache write path.
UPDATE sticker_sets
SET installed = false,
    installed_date = 0
WHERE set_kind = 'system';
