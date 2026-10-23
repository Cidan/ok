-- A person's approval, with a passkey, of exactly what ok secrets asked (docs/secrets.md,
-- "Unsealing"): waiting until it is answered, or expires.
CREATE TABLE approvals (
    id           text        PRIMARY KEY,
    principal_id text        NOT NULL,
    scope        text        NOT NULL,
    description  text        NOT NULL,
    created_at   timestamptz NOT NULL,
    expires_at   timestamptz NOT NULL,
    decision     text        NOT NULL DEFAULT '' CHECK (decision IN ('', 'approved', 'declined')),
    decided_at   timestamptz
);
CREATE INDEX approvals_expires ON approvals (expires_at);
