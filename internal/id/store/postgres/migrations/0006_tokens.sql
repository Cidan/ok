-- Tokens (docs/hosted.md, "Robots and tokens per organization"): a person's personal tokens, each
-- a random secret stored as its SHA-256 (32 random bytes, not a password), naming the repositories
-- it reaches (none: every one its holder may) and the most it allows, until it expires or is
-- revoked.
CREATE TABLE tokens (
    id            text        PRIMARY KEY,               -- tok_…
    token_hash    bytea       NOT NULL UNIQUE,
    principal_id  text        NOT NULL REFERENCES principals (id),
    kind          text        NOT NULL,                  -- personal
    label         text        NOT NULL,
    role          text        NOT NULL,                  -- the most it allows
    repositories  bigint[]    NOT NULL DEFAULT '{}',
    created_at    timestamptz NOT NULL,
    expires_at    timestamptz NOT NULL,
    last_used_at  timestamptz,
    revoked_at    timestamptz
);
CREATE INDEX tokens_principal ON tokens (principal_id, created_at);
