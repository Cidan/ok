-- Enterprise organizations (docs/hosted.md, "Enterprise organizations"): an organization's own
-- identity provider — its system of record (Google Workspace, Entra ID, LDAP, or SCIM) and its single
-- sign-on (OpenID Connect) — its secrets sealed with the installation's key; the people it
-- provisions, linked by their external ID; its groups synced as the organization's teams.
CREATE TABLE organization_identity (
    organization_id text        PRIMARY KEY REFERENCES principals (id),
    source          text        NOT NULL,              -- google, entra, ldap, or scim
    settings        jsonb       NOT NULL,              -- the source's and the sign-in's, no secret
    sealed          bytea,                             -- the secrets, sealed (internal/id/seal)
    interval_secs   integer     NOT NULL,
    mirror          jsonb,                             -- the system of record as the last sync read it
    last_run        timestamptz,
    last_error      text        NOT NULL DEFAULT '',
    waiting         jsonb       NOT NULL DEFAULT '[]',
    notes           jsonb       NOT NULL DEFAULT '[]',
    updated_at      timestamptz NOT NULL
);

CREATE TABLE organization_people (
    organization_id text NOT NULL REFERENCES principals (id),
    external_id     text NOT NULL,                      -- the provider's immutable ID for them
    principal_id    text NOT NULL UNIQUE REFERENCES principals (id),
    PRIMARY KEY (organization_id, external_id)
);

-- What an organization's provider provisions over SCIM, apart from the installation's ('') and
-- every other organization's; its bearer token, by its SHA-256.
ALTER TABLE scim_users ADD COLUMN scope text NOT NULL DEFAULT '';
ALTER TABLE scim_groups ADD COLUMN scope text NOT NULL DEFAULT '';
DROP INDEX scim_users_user_name;
DROP INDEX scim_groups_display_name;
CREATE UNIQUE INDEX scim_users_user_name ON scim_users (scope, lower(user_name));
CREATE UNIQUE INDEX scim_groups_display_name ON scim_groups (scope, lower(display_name));
CREATE TABLE organization_scim_tokens (
    organization_id text        PRIMARY KEY REFERENCES principals (id),
    token_hash      bytea       NOT NULL UNIQUE,
    created_at      timestamptz NOT NULL
);

-- A team its organization's provider syncs: its group's external ID, and members only the sync
-- changes.
ALTER TABLE teams ADD COLUMN external_id text;
CREATE UNIQUE INDEX teams_external ON teams (organization_id, external_id) WHERE external_id IS NOT NULL;
