-- 0002_phase1_business: first-stage business persistence for startup RPCs.
--
-- 表结构按 telesrv domain/store 边界建模，不引入旧工程依赖。

CREATE TABLE IF NOT EXISTS update_states (
    auth_key_id BIGINT      PRIMARY KEY REFERENCES auth_keys(auth_key_id) ON DELETE CASCADE,
    pts         INT         NOT NULL DEFAULT 0,
    qts         INT         NOT NULL DEFAULT 0,
    date        INT         NOT NULL DEFAULT 0,
    seq         INT         NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contacts (
    user_id         BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    contact_user_id BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    mutual          BOOLEAN     NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, contact_user_id)
);

CREATE INDEX IF NOT EXISTS contacts_contact_user_id_idx ON contacts (contact_user_id);

CREATE TABLE IF NOT EXISTS dialogs (
    user_id                  BIGINT      NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    peer_type                VARCHAR(16) NOT NULL,
    peer_id                  BIGINT      NOT NULL,
    top_message_id           INT         NOT NULL DEFAULT 0,
    read_inbox_max_id        INT         NOT NULL DEFAULT 0,
    read_outbox_max_id       INT         NOT NULL DEFAULT 0,
    unread_count             INT         NOT NULL DEFAULT 0,
    unread_mentions_count    INT         NOT NULL DEFAULT 0,
    unread_reactions_count   INT         NOT NULL DEFAULT 0,
    pinned                   BOOLEAN     NOT NULL DEFAULT false,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, peer_type, peer_id),
    CONSTRAINT dialogs_peer_type_check CHECK (peer_type IN ('user'))
);

CREATE INDEX IF NOT EXISTS dialogs_user_updated_idx ON dialogs (user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS dialogs_user_pinned_idx ON dialogs (user_id, pinned) WHERE pinned;

CREATE TABLE IF NOT EXISTS lang_packs (
    lang_pack        VARCHAR(32)  NOT NULL,
    lang_code        VARCHAR(64)  NOT NULL,
    version          INT          NOT NULL,
    strings_count    INT          NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (lang_pack, lang_code)
);

CREATE TABLE IF NOT EXISTS lang_pack_strings (
    lang_pack      VARCHAR(32)  NOT NULL,
    lang_code      VARCHAR(64)  NOT NULL,
    key            VARCHAR(128) NOT NULL,
    version        INT          NOT NULL,
    pluralized     BOOLEAN      NOT NULL DEFAULT false,
    value          TEXT         NOT NULL DEFAULT '',
    zero_value     TEXT         NOT NULL DEFAULT '',
    one_value      TEXT         NOT NULL DEFAULT '',
    two_value      TEXT         NOT NULL DEFAULT '',
    few_value      TEXT         NOT NULL DEFAULT '',
    many_value     TEXT         NOT NULL DEFAULT '',
    other_value    TEXT         NOT NULL DEFAULT '',
    deleted        BOOLEAN      NOT NULL DEFAULT false,
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (lang_pack, lang_code, key),
    FOREIGN KEY (lang_pack, lang_code) REFERENCES lang_packs(lang_pack, lang_code) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS lang_pack_strings_pack_version_idx
    ON lang_pack_strings (lang_pack, lang_code, version);
