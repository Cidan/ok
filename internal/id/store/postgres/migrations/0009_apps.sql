-- Apps (docs/hosted.md, "Robots and tokens per organization"): an organization's robot with a
-- public key, which organizations install with repositories and a role; the app trades a token it
-- signs for an installation token, good for an hour.
CREATE TABLE apps (
    id              text        PRIMARY KEY,                          -- app_…
    robot_id        text        NOT NULL UNIQUE REFERENCES principals (id),
    organization_id text        NOT NULL REFERENCES principals (id),  -- its maker
    public          boolean     NOT NULL,                             -- installable by any organization
    public_key      bytea       NOT NULL,                             -- PKIX DER: ECDSA P-256 or Ed25519
    created_at      timestamptz NOT NULL
);
CREATE INDEX apps_organization ON apps (organization_id);

CREATE TABLE app_installations (
    id              text        PRIMARY KEY,                          -- ins_…
    app_id          text        NOT NULL REFERENCES apps (id),
    organization_id text        NOT NULL REFERENCES principals (id),  -- where it is installed
    role            text        NOT NULL,
    repositories    bigint[]    NOT NULL DEFAULT '{}',                -- none: every one of the organization's
    created_by      text        NOT NULL REFERENCES principals (id),
    created_at      timestamptz NOT NULL,
    UNIQUE (app_id, organization_id)
);
CREATE INDEX app_installations_organization ON app_installations (organization_id);

-- An installation token is its app's robot's, for one installation.
ALTER TABLE tokens ADD COLUMN installation_id text REFERENCES app_installations (id);
