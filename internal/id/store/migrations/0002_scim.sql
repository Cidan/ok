-- What an identity provider provisioned over SCIM: the system of record, when it is SCIM's.
CREATE TABLE scim_users (
    id          text PRIMARY KEY,
    user_name   text NOT NULL,
    data        jsonb NOT NULL,                     -- the user as ok keeps it
    created_at  timestamptz NOT NULL DEFAULT now(),
    modified_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX scim_users_user_name ON scim_users (lower(user_name));

CREATE TABLE scim_groups (
    id           text PRIMARY KEY,
    display_name text NOT NULL,
    data         jsonb NOT NULL,                    -- the group, without its members
    members      text[] NOT NULL DEFAULT '{}',      -- users' and groups' IDs
    created_at   timestamptz NOT NULL DEFAULT now(),
    modified_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX scim_groups_display_name ON scim_groups (lower(display_name));
