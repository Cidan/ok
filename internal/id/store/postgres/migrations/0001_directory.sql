-- ok identity's directory: principals and the names they have had, the system of record's mirror,
-- the groups-and-ACLs tree, credentials, sessions, device logins, and passkeys.

CREATE TABLE principals (
    id           text PRIMARY KEY,                  -- prn_…: stable across renames
    name         text NOT NULL UNIQUE,              -- ada.human.<domain>
    kind         text NOT NULL CHECK (kind IN ('human', 'robot')),
    status       text NOT NULL CHECK (status IN ('active', 'suspended', 'deleted')),
    external_id  text UNIQUE,                       -- the system of record's, for a person
    email        text NOT NULL DEFAULT '',
    display_name text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'human') = (external_id IS NOT NULL))
);

-- A name no principal or set will have again: a deleted one's, a renamed one's old one.
CREATE TABLE retired_names (
    name       text PRIMARY KEY,
    retired_at timestamptz NOT NULL DEFAULT now(),
    reason     text NOT NULL
);

-- The system of record's groups, as the sync mirrors them.
CREATE TABLE synced_groups (
    name        text PRIMARY KEY,                   -- eng.group.<domain>
    external_id text NOT NULL UNIQUE,
    members     text[] NOT NULL DEFAULT '{}',       -- full names, sorted
    deleted     boolean NOT NULL DEFAULT false,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- What the sync last read, and how it went: one row.
CREATE TABLE sync_state (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    mirror     jsonb,                               -- the snapshot the directory follows
    last_run   timestamptz,
    last_error text NOT NULL DEFAULT '',
    waiting    jsonb NOT NULL DEFAULT '[]',
    notes      jsonb NOT NULL DEFAULT '[]'
);

-- Names administrators set over the system of record's, to resolve collisions.
CREATE TABLE sync_names (
    external_id text NOT NULL,
    is_group    boolean NOT NULL,
    name        text NOT NULL,                      -- a handle, or a group's label
    PRIMARY KEY (external_id, is_group)
);

-- The groups-and-ACLs tree, as last applied.
CREATE TABLE tree_sets (
    name    text PRIMARY KEY,                       -- eng.acl.<domain>
    kind    text NOT NULL CHECK (kind IN ('group', 'acl')),
    path    text NOT NULL,                          -- its folder; '' for the root
    owner   text NOT NULL,
    members text[] NOT NULL DEFAULT '{}'            -- as declared
);

CREATE TABLE tree_state (
    singleton  boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    commit     text NOT NULL,
    sequence   bigint NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);

-- Federated sign-in for robots: a token iss and sub that name one.
CREATE TABLE robot_bindings (
    issuer       text NOT NULL,
    subject      text NOT NULL,
    principal_id text NOT NULL REFERENCES principals (id),
    PRIMARY KEY (issuer, subject)
);

-- A key's enrollment as a principal's.
CREATE TABLE credentials (
    id             text PRIMARY KEY,                -- crd_…
    principal_id   text NOT NULL REFERENCES principals (id),
    label          text NOT NULL,
    public_key     bytea NOT NULL UNIQUE,           -- PKIX DER
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_issued_at timestamptz,
    not_after      timestamptz,
    revoked_at     timestamptz
);
CREATE INDEX credentials_principal ON credentials (principal_id);

-- Browser sessions; the cookie is the token, of which only the hash is kept.
CREATE TABLE sessions (
    token_hash   bytea PRIMARY KEY,
    principal_id text NOT NULL REFERENCES principals (id),
    via          text NOT NULL,                     -- how the person signed in
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL,              -- absolute
    idle_until   timestamptz NOT NULL               -- pushed on as it is used
);
CREATE INDEX sessions_principal ON sessions (principal_id);

-- OAuth device authorization grants in flight.
CREATE TABLE device_logins (
    device_code_hash bytea PRIMARY KEY,
    user_code        text NOT NULL UNIQUE,
    public_key       bytea NOT NULL,
    label            text NOT NULL,
    client_address   text NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    expires_at       timestamptz NOT NULL,
    last_poll_at     timestamptz,
    state            text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'approved', 'denied', 'done')),
    principal_id     text REFERENCES principals (id),
    credential_id    text REFERENCES credentials (id)
);

-- Passkeys (WebAuthn credentials).
CREATE TABLE passkeys (
    id           bytea PRIMARY KEY,                 -- the authenticator's credential ID
    principal_id text NOT NULL REFERENCES principals (id),
    data         jsonb NOT NULL,                    -- the library's credential record
    label        text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz
);
CREATE INDEX passkeys_principal ON passkeys (principal_id);

-- Sign-in ceremonies between a redirect and its return: OIDC states, WebAuthn challenges.
CREATE TABLE ceremonies (
    id         text PRIMARY KEY,
    kind       text NOT NULL,
    data       jsonb NOT NULL,
    expires_at timestamptz NOT NULL
);

-- What administrators and the sync did, for the record.
CREATE TABLE audit (
    id     bigserial PRIMARY KEY,
    at     timestamptz NOT NULL DEFAULT now(),
    actor  text NOT NULL,
    action text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'
);
