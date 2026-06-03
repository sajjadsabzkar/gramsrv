UPDATE sticker_sets
SET installed = true,
    installed_date = 0
WHERE set_kind = 'system';
