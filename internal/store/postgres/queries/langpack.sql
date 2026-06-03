-- name: GetLangPackMeta :one
SELECT lang_pack, lang_code, version, strings_count
FROM lang_packs
WHERE lang_pack = $1 AND lang_code = $2;

-- name: UpsertLangPackMeta :exec
INSERT INTO lang_packs (lang_pack, lang_code, version, strings_count)
VALUES ($1, $2, $3, $4)
ON CONFLICT (lang_pack, lang_code) DO UPDATE SET
  version = EXCLUDED.version,
  strings_count = EXCLUDED.strings_count,
  updated_at = now();

-- name: UpsertLangPackString :exec
INSERT INTO lang_pack_strings (
  lang_pack, lang_code, key, version, pluralized, value,
  zero_value, one_value, two_value, few_value, many_value, other_value, deleted
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (lang_pack, lang_code, key) DO UPDATE SET
  version = EXCLUDED.version,
  pluralized = EXCLUDED.pluralized,
  value = EXCLUDED.value,
  zero_value = EXCLUDED.zero_value,
  one_value = EXCLUDED.one_value,
  two_value = EXCLUDED.two_value,
  few_value = EXCLUDED.few_value,
  many_value = EXCLUDED.many_value,
  other_value = EXCLUDED.other_value,
  deleted = EXCLUDED.deleted,
  updated_at = now();

-- name: ListLangPackStrings :many
SELECT
  lang_pack, lang_code, key, version, pluralized, value,
  zero_value, one_value, two_value, few_value, many_value, other_value, deleted
FROM lang_pack_strings
WHERE lang_pack = $1 AND lang_code = $2 AND NOT deleted
ORDER BY key;

-- name: GetLangPackStringsByKeys :many
SELECT
  lang_pack, lang_code, key, version, pluralized, value,
  zero_value, one_value, two_value, few_value, many_value, other_value, deleted
FROM lang_pack_strings
WHERE lang_pack = $1 AND lang_code = $2 AND key = ANY(sqlc.arg(keys)::text[]) AND NOT deleted
ORDER BY key;
