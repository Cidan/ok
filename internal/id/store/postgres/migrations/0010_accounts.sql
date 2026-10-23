-- People's own accounts in a hosted service (docs/hosted.md, "Signing up and in, hosted"): a
-- password (argon2id), the emailed links that verify an address or reset a password, accounts
-- linked to a social provider's subjects, and the failed sign-ins that slow a guesser.
-- A person need not come from a system of record: an account made here, or one an organization's
-- identity provider provisions, has no external ID. Only people have one.
ALTER TABLE principals DROP CONSTRAINT principals_check;
ALTER TABLE principals ADD CONSTRAINT principals_external_id CHECK (kind = 'human' OR external_id IS NULL);

CREATE TABLE passwords (
    principal_id text        PRIMARY KEY REFERENCES principals (id),
    hash         text        NOT NULL,       -- argon2id, as a PHC string
    updated_at   timestamptz NOT NULL
);

CREATE TABLE email_tokens (
    token_hash   bytea       PRIMARY KEY,    -- SHA-256 of the link's token
    purpose      text        NOT NULL,       -- sign-up, or reset
    email        text        NOT NULL,
    principal_id text        REFERENCES principals (id),  -- a reset's account
    handle       text        NOT NULL DEFAULT '',          -- a sign-up's
    hash         text        NOT NULL DEFAULT '',          -- a sign-up's password, hashed
    expires_at   timestamptz NOT NULL
);
CREATE INDEX email_tokens_expiry ON email_tokens (expires_at);

CREATE TABLE login_links (
    provider     text        NOT NULL,       -- a sign-in provider's ID
    subject      text        NOT NULL,       -- its sub for the person
    principal_id text        NOT NULL REFERENCES principals (id),
    created_at   timestamptz NOT NULL,
    PRIMARY KEY (provider, subject)
);
CREATE INDEX login_links_principal ON login_links (principal_id);

CREATE TABLE sign_in_failures (
    key      text        PRIMARY KEY,        -- an account's email, lowercased
    failures integer     NOT NULL,
    since    timestamptz NOT NULL
);
