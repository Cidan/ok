-- ok identity's tables on Spanner (docs/hosted.md, "Spanner"), as postgres's migrations leave them on
-- PostgreSQL: principals and the names they have had, the system of record's mirror, the
-- groups-and-ACLs tree, credentials, sessions, device logins, and passkeys; tenancy — organizations,
-- members, teams, the repositories' directory and its grants and log; tokens, apps, accounts,
-- enterprise organizations, deletions, and moving in; and what SCIM provisioned. Foreign keys are
-- PostgreSQL's, but for those Spanner has no form of — a fork's parent set NULL as it goes, a
-- repository's redirects deleted with it — which the backend does itself. A key column is never
-- updated: a row whose key changes — a renamed team, a grant moved to another subject — is deleted
-- and written anew. JSON the store hands over as bytes is kept as those bytes.

-- ─── Principals and names ───

CREATE TABLE principals (
    id           STRING(MAX) NOT NULL,          -- prn_…: stable across renames
    name         STRING(MAX) NOT NULL,          -- ada.human.<domain>
    kind         STRING(MAX) NOT NULL,
    status       STRING(MAX) NOT NULL,
    external_id  STRING(MAX),                   -- the system of record's, for a person
    email        STRING(MAX) NOT NULL,
    display_name STRING(MAX) NOT NULL,
    created_at   TIMESTAMP   NOT NULL,
    updated_at   TIMESTAMP   NOT NULL,
    email_lower  STRING(MAX) AS (LOWER(email)) STORED,
    CONSTRAINT principals_kind_check CHECK (kind IN ('human', 'robot', 'org')),
    CONSTRAINT principals_status_check CHECK (status IN ('active', 'suspended', 'deleted')),
    CONSTRAINT principals_external_id_check CHECK (kind = 'human' OR external_id IS NULL),
) PRIMARY KEY (id);
CREATE UNIQUE INDEX principals_name ON principals (name);
CREATE UNIQUE NULL_FILTERED INDEX principals_external ON principals (external_id);
CREATE INDEX principals_email ON principals (email_lower);

-- A name no principal or set will have again: a deleted one's, a renamed one's old one.
CREATE TABLE retired_names (
    name       STRING(MAX) NOT NULL,
    retired_at TIMESTAMP   NOT NULL,
    reason     STRING(MAX) NOT NULL,
) PRIMARY KEY (name);

-- What administrators and the sync did, for the record.
CREATE TABLE audit (
    id     INT64       NOT NULL,
    audit_at TIMESTAMP NOT NULL,                -- (at is a reserved word in GoogleSQL)
    actor  STRING(MAX) NOT NULL,
    action STRING(MAX) NOT NULL,
    detail JSON        NOT NULL,
) PRIMARY KEY (id);

-- ─── The system of record's mirror ───

-- The system of record's groups, as the sync mirrors them, by the system's ID: a group renamed
-- keeps its row.
CREATE TABLE synced_groups (
    external_id STRING(MAX)        NOT NULL,
    name        STRING(MAX)        NOT NULL,    -- eng.group.<domain>
    members     ARRAY<STRING(MAX)> NOT NULL,    -- full names, sorted
    deleted     BOOL               NOT NULL,
    updated_at  TIMESTAMP          NOT NULL,
) PRIMARY KEY (external_id);
CREATE UNIQUE INDEX synced_groups_name ON synced_groups (name);

-- What the sync last read, and how it went: one row.
CREATE TABLE sync_state (
    singleton  BOOL        NOT NULL,
    mirror     BYTES(MAX),                      -- the snapshot the directory follows, JSON
    last_run   TIMESTAMP,
    last_error STRING(MAX) NOT NULL,
    waiting    BYTES(MAX)  NOT NULL,            -- JSON
    notes      BYTES(MAX)  NOT NULL,            -- JSON
    CONSTRAINT sync_state_one_check CHECK (singleton),
) PRIMARY KEY (singleton);

-- Names administrators set over the system of record's, to resolve collisions.
CREATE TABLE sync_names (
    external_id STRING(MAX) NOT NULL,
    is_group    BOOL        NOT NULL,
    name        STRING(MAX) NOT NULL,           -- a handle, or a group's label
) PRIMARY KEY (external_id, is_group);

-- ─── The groups-and-ACLs tree ───

CREATE TABLE tree_sets (
    name    STRING(MAX)        NOT NULL,        -- eng.acl.<domain>
    kind    STRING(MAX)        NOT NULL,
    path    STRING(MAX)        NOT NULL,        -- its folder; '' for the root
    owner   STRING(MAX)        NOT NULL,
    members ARRAY<STRING(MAX)> NOT NULL,        -- as declared
    CONSTRAINT tree_sets_kind_check CHECK (kind IN ('group', 'acl')),
) PRIMARY KEY (name);

CREATE TABLE tree_state (
    singleton  BOOL        NOT NULL,
    commit_id  STRING(MAX) NOT NULL,            -- (commit is a reserved word in GoogleSQL)
    sequence   INT64       NOT NULL,
    applied_at TIMESTAMP   NOT NULL,
    CONSTRAINT tree_state_one_check CHECK (singleton),
) PRIMARY KEY (singleton);

-- ─── Robots, credentials, sign-in ───

-- Federated sign-in for robots: a token's issuer and subject that name one, under an organization
-- ('' for the installation).
CREATE TABLE robot_bindings (
    organization_id STRING(MAX) NOT NULL,
    issuer          STRING(MAX) NOT NULL,
    subject         STRING(MAX) NOT NULL,
    principal_id    STRING(MAX) NOT NULL,
    CONSTRAINT robot_bindings_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (organization_id, issuer, subject);
CREATE INDEX robot_bindings_principal ON robot_bindings (principal_id);

-- A key's enrollment as a principal's.
CREATE TABLE credentials (
    id             STRING(MAX) NOT NULL,        -- crd_…
    principal_id   STRING(MAX) NOT NULL,
    label          STRING(MAX) NOT NULL,
    public_key     BYTES(MAX)  NOT NULL,        -- PKIX DER
    created_at     TIMESTAMP   NOT NULL,
    last_issued_at TIMESTAMP,
    not_after      TIMESTAMP,
    revoked_at     TIMESTAMP,
    CONSTRAINT credentials_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (id);
CREATE UNIQUE INDEX credentials_key ON credentials (public_key);
CREATE INDEX credentials_principal ON credentials (principal_id, created_at DESC);

-- Browser sessions; the cookie is the token, of which only the hash is kept.
CREATE TABLE sessions (
    token_hash   BYTES(MAX)  NOT NULL,
    principal_id STRING(MAX) NOT NULL,
    via          STRING(MAX) NOT NULL,          -- how the person signed in
    created_at   TIMESTAMP   NOT NULL,
    expires_at   TIMESTAMP   NOT NULL,          -- absolute
    idle_until   TIMESTAMP   NOT NULL,          -- pushed on as it is used
    CONSTRAINT sessions_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (token_hash);
CREATE INDEX sessions_principal ON sessions (principal_id);
CREATE INDEX sessions_expiry ON sessions (expires_at);

-- OAuth device authorization grants in flight.
CREATE TABLE device_logins (
    device_code_hash BYTES(MAX)  NOT NULL,
    user_code        STRING(MAX) NOT NULL,
    public_key       BYTES(MAX)  NOT NULL,
    label            STRING(MAX) NOT NULL,
    client_address   STRING(MAX) NOT NULL,
    created_at       TIMESTAMP   NOT NULL,
    expires_at       TIMESTAMP   NOT NULL,
    last_poll_at     TIMESTAMP,
    state            STRING(MAX) NOT NULL,
    principal_id     STRING(MAX),
    credential_id    STRING(MAX),
    CONSTRAINT device_logins_state_check CHECK (state IN ('pending', 'approved', 'denied', 'done')),
    CONSTRAINT device_logins_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
    CONSTRAINT device_logins_credential_fk FOREIGN KEY (credential_id) REFERENCES credentials (id),
) PRIMARY KEY (device_code_hash);
CREATE UNIQUE INDEX device_logins_user_code ON device_logins (user_code);

-- Passkeys (WebAuthn credentials).
CREATE TABLE passkeys (
    id           BYTES(MAX)  NOT NULL,          -- the authenticator's credential ID
    principal_id STRING(MAX) NOT NULL,
    data         BYTES(MAX)  NOT NULL,          -- the library's credential record, JSON
    label        STRING(MAX) NOT NULL,
    created_at   TIMESTAMP   NOT NULL,
    last_used_at TIMESTAMP,
    CONSTRAINT passkeys_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (id);
CREATE INDEX passkeys_principal ON passkeys (principal_id, created_at);

-- Sign-in ceremonies between a redirect and its return: OIDC states, WebAuthn challenges.
CREATE TABLE ceremonies (
    id         STRING(MAX) NOT NULL,
    kind       STRING(MAX) NOT NULL,
    data       BYTES(MAX)  NOT NULL,            -- JSON
    expires_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (id);
CREATE INDEX ceremonies_expiry ON ceremonies (expires_at);

-- ─── Tenancy ───

CREATE TABLE organizations (
    principal_id   STRING(MAX) NOT NULL,
    base_role      STRING(MAX) NOT NULL,
    members_create BOOL        NOT NULL,
    private_forks  BOOL        NOT NULL,
    enterprise     STRING(MAX) NOT NULL,
    CONSTRAINT organizations_base_role_check CHECK (base_role IN ('none', 'read', 'triage', 'write', 'maintain', 'admin')),
    CONSTRAINT organizations_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (principal_id);

CREATE TABLE memberships (
    organization_id STRING(MAX) NOT NULL,
    principal_id    STRING(MAX) NOT NULL,
    role            STRING(MAX) NOT NULL,
    CONSTRAINT memberships_role_check CHECK (role IN ('owner', 'member')),
    CONSTRAINT memberships_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
    CONSTRAINT memberships_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (organization_id, principal_id);
CREATE INDEX memberships_principal ON memberships (principal_id);

CREATE TABLE teams (
    name            STRING(MAX)        NOT NULL, -- <team>.<org>.team.<domain>
    organization_id STRING(MAX)        NOT NULL,
    parent          STRING(MAX),
    members         ARRAY<STRING(MAX)> NOT NULL, -- principals' IDs, sorted
    external_id     STRING(MAX),                 -- a synced team's, its provider's
    CONSTRAINT teams_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
    CONSTRAINT teams_parent_fk FOREIGN KEY (parent) REFERENCES teams (name),
) PRIMARY KEY (name);
CREATE INDEX teams_organization ON teams (organization_id);
CREATE UNIQUE NULL_FILTERED INDEX teams_external ON teams (organization_id, external_id);

-- The repositories' directory: each repository's owner, name, and standing.
CREATE TABLE repositories (
    repository_id INT64       NOT NULL,
    owner_id      STRING(MAX) NOT NULL,
    name          STRING(MAX) NOT NULL,
    visibility    STRING(MAX) NOT NULL,
    propose       STRING(MAX) NOT NULL,
    parent_id     INT64,                         -- set NULL as its parent goes (DeleteRepository)
    network_id    INT64       NOT NULL,
    cell          STRING(MAX) NOT NULL,
    state         STRING(MAX) NOT NULL,
    state_at      TIMESTAMP   NOT NULL,
    created_by    STRING(MAX) NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
    CONSTRAINT repositories_visibility_check CHECK (visibility IN ('private', 'internal', 'public')),
    CONSTRAINT repositories_propose_check CHECK (propose IN ('read', 'write')),
    CONSTRAINT repositories_state_check CHECK (state IN ('creating', 'active', 'deleted')),
    CONSTRAINT repositories_owner_fk FOREIGN KEY (owner_id) REFERENCES principals (id),
    CONSTRAINT repositories_creator_fk FOREIGN KEY (created_by) REFERENCES principals (id),
) PRIMARY KEY (repository_id);
CREATE UNIQUE INDEX repositories_name ON repositories (owner_id, name);
CREATE INDEX repositories_parent ON repositories (parent_id);
CREATE INDEX repositories_network ON repositories (network_id);
CREATE INDEX repositories_state ON repositories (state, state_at);

-- A name an owner's repository had, pointing at it; deleted with it (DeleteRepository).
CREATE TABLE redirects (
    owner_id      STRING(MAX) NOT NULL,
    name          STRING(MAX) NOT NULL,
    repository_id INT64       NOT NULL,
    CONSTRAINT redirects_owner_fk FOREIGN KEY (owner_id) REFERENCES principals (id),
    CONSTRAINT redirects_repository_fk FOREIGN KEY (repository_id) REFERENCES repositories (repository_id),
) PRIMARY KEY (owner_id, name);
CREATE INDEX redirects_repository ON redirects (repository_id);

-- A subject's role on a repository; deleted with it.
CREATE TABLE grants (
    repository_id INT64       NOT NULL,
    subject       STRING(MAX) NOT NULL,
    role          STRING(MAX) NOT NULL,
    CONSTRAINT grants_role_check CHECK (role IN ('read', 'triage', 'write', 'maintain', 'admin')),
) PRIMARY KEY (repository_id, subject), INTERLEAVE IN PARENT repositories ON DELETE CASCADE;
CREATE INDEX grants_subject ON grants (subject);

-- The directory's log: each repository's events, in order, pending until the relay publishes
-- them. A relay holds the pending events it took for a lease (holder, lease_until), publishing
-- outside any transaction.
CREATE TABLE directory_event (
    repository_id INT64      NOT NULL,
    seq           INT64      NOT NULL,
    event_at      TIMESTAMP  NOT NULL,
    body          BYTES(MAX) NOT NULL,           -- an ok.identity.v1.DirectoryEvent
) PRIMARY KEY (repository_id, seq);

CREATE TABLE directory_event_head (
    repository_id INT64 NOT NULL,
    seq           INT64 NOT NULL,
) PRIMARY KEY (repository_id);

CREATE TABLE directory_event_pending (
    repository_id INT64       NOT NULL,
    seq           INT64       NOT NULL,
    holder        STRING(MAX),
    lease_until   TIMESTAMP,
) PRIMARY KEY (repository_id, seq);

-- ─── Organizations' robots, apps, tokens ───

CREATE TABLE organization_robots (
    principal_id    STRING(MAX) NOT NULL,
    organization_id STRING(MAX) NOT NULL,
    CONSTRAINT organization_robots_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
    CONSTRAINT organization_robots_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
) PRIMARY KEY (principal_id);
CREATE INDEX organization_robots_organization ON organization_robots (organization_id);

CREATE TABLE organization_issuers (
    organization_id STRING(MAX) NOT NULL,
    issuer          STRING(MAX) NOT NULL,
    created_at      TIMESTAMP   NOT NULL,
    CONSTRAINT organization_issuers_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
) PRIMARY KEY (organization_id, issuer);

CREATE TABLE apps (
    id              STRING(MAX) NOT NULL,        -- app_…
    robot_id        STRING(MAX) NOT NULL,
    organization_id STRING(MAX) NOT NULL,        -- its maker
    public          BOOL        NOT NULL,        -- installable by any organization
    public_key      BYTES(MAX)  NOT NULL,        -- PKIX DER: ECDSA P-256 or Ed25519
    created_at      TIMESTAMP   NOT NULL,
    CONSTRAINT apps_robot_fk FOREIGN KEY (robot_id) REFERENCES principals (id),
    CONSTRAINT apps_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
) PRIMARY KEY (id);
CREATE UNIQUE INDEX apps_robot ON apps (robot_id);
CREATE INDEX apps_organization ON apps (organization_id, created_at);

CREATE TABLE app_installations (
    id              STRING(MAX)  NOT NULL,       -- ins_…
    app_id          STRING(MAX)  NOT NULL,
    organization_id STRING(MAX)  NOT NULL,       -- where it is installed
    role            STRING(MAX)  NOT NULL,
    repositories    ARRAY<INT64> NOT NULL,       -- none: every one of the organization's
    created_by      STRING(MAX)  NOT NULL,
    created_at      TIMESTAMP    NOT NULL,
    CONSTRAINT app_installations_app_fk FOREIGN KEY (app_id) REFERENCES apps (id),
    CONSTRAINT app_installations_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
    CONSTRAINT app_installations_creator_fk FOREIGN KEY (created_by) REFERENCES principals (id),
) PRIMARY KEY (id);
CREATE UNIQUE INDEX app_installations_app ON app_installations (app_id, organization_id);
CREATE INDEX app_installations_organization ON app_installations (organization_id, created_at);

CREATE TABLE tokens (
    id              STRING(MAX)  NOT NULL,       -- tok_…
    token_hash      BYTES(MAX)   NOT NULL,
    principal_id    STRING(MAX)  NOT NULL,
    kind            STRING(MAX)  NOT NULL,       -- personal, or installation
    label           STRING(MAX)  NOT NULL,
    role            STRING(MAX)  NOT NULL,       -- the most it allows
    repositories    ARRAY<INT64> NOT NULL,
    created_at      TIMESTAMP    NOT NULL,
    expires_at      TIMESTAMP    NOT NULL,
    last_used_at    TIMESTAMP,
    revoked_at      TIMESTAMP,
    installation_id STRING(MAX),
    CONSTRAINT tokens_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
    CONSTRAINT tokens_installation_fk FOREIGN KEY (installation_id) REFERENCES app_installations (id),
) PRIMARY KEY (id);
CREATE UNIQUE INDEX tokens_hash ON tokens (token_hash);
CREATE INDEX tokens_principal ON tokens (principal_id, created_at DESC);
CREATE INDEX tokens_installation ON tokens (installation_id);

-- ─── Accounts ───

CREATE TABLE passwords (
    principal_id STRING(MAX) NOT NULL,
    password_hash STRING(MAX) NOT NULL,          -- argon2id, as a PHC string (hash is reserved in GoogleSQL)
    updated_at   TIMESTAMP   NOT NULL,
    CONSTRAINT passwords_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (principal_id);

CREATE TABLE email_tokens (
    token_hash   BYTES(MAX)  NOT NULL,           -- SHA-256 of the link's token
    purpose      STRING(MAX) NOT NULL,           -- sign-up, or reset
    email        STRING(MAX) NOT NULL,
    principal_id STRING(MAX),                    -- a reset's account
    handle       STRING(MAX) NOT NULL,           -- a sign-up's
    password_hash STRING(MAX) NOT NULL,          -- a sign-up's password, hashed
    expires_at   TIMESTAMP   NOT NULL,
    CONSTRAINT email_tokens_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (token_hash);
CREATE INDEX email_tokens_expiry ON email_tokens (expires_at);
CREATE INDEX email_tokens_principal ON email_tokens (principal_id);

CREATE TABLE login_links (
    provider     STRING(MAX) NOT NULL,           -- a sign-in provider's ID
    subject      STRING(MAX) NOT NULL,           -- its sub for the person
    principal_id STRING(MAX) NOT NULL,
    created_at   TIMESTAMP   NOT NULL,
    CONSTRAINT login_links_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (provider, subject);
CREATE INDEX login_links_principal ON login_links (principal_id, provider);

CREATE TABLE sign_in_failures (
    key      STRING(MAX) NOT NULL,               -- an account's email, lowercased
    failures INT64       NOT NULL,
    since    TIMESTAMP   NOT NULL,
) PRIMARY KEY (key);

-- ─── Enterprise organizations ───

CREATE TABLE organization_identity (
    organization_id STRING(MAX) NOT NULL,
    source          STRING(MAX) NOT NULL,        -- google, entra, ldap, or scim
    settings        BYTES(MAX)  NOT NULL,        -- the source's and the sign-in's, no secret, JSON
    sealed          BYTES(MAX),                  -- the secrets, sealed (internal/id/seal)
    interval_secs   INT64       NOT NULL,
    mirror          BYTES(MAX),                  -- the system of record as the last sync read it, JSON
    last_run        TIMESTAMP,
    last_error      STRING(MAX) NOT NULL,
    waiting         BYTES(MAX)  NOT NULL,        -- JSON
    notes           BYTES(MAX)  NOT NULL,        -- JSON
    updated_at      TIMESTAMP   NOT NULL,
    plain_handles   BOOL        NOT NULL,
    CONSTRAINT organization_identity_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
) PRIMARY KEY (organization_id);

CREATE TABLE organization_people (
    organization_id STRING(MAX) NOT NULL,
    external_id     STRING(MAX) NOT NULL,        -- the provider's immutable ID for them
    principal_id    STRING(MAX) NOT NULL,
    CONSTRAINT organization_people_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
    CONSTRAINT organization_people_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
) PRIMARY KEY (organization_id, external_id);
CREATE UNIQUE INDEX organization_people_principal ON organization_people (principal_id);

CREATE TABLE organization_scim_tokens (
    organization_id STRING(MAX) NOT NULL,
    token_hash      BYTES(MAX)  NOT NULL,
    created_at      TIMESTAMP   NOT NULL,
    CONSTRAINT organization_scim_tokens_organization_fk FOREIGN KEY (organization_id) REFERENCES principals (id),
) PRIMARY KEY (organization_id);
CREATE UNIQUE INDEX organization_scim_tokens_hash ON organization_scim_tokens (token_hash);

-- ─── Deletions and moving in ───

CREATE TABLE deletions (
    principal_id STRING(MAX) NOT NULL,
    requested_at TIMESTAMP   NOT NULL,
    requested_by STRING(MAX) NOT NULL,
    CONSTRAINT deletions_principal_fk FOREIGN KEY (principal_id) REFERENCES principals (id),
    CONSTRAINT deletions_requester_fk FOREIGN KEY (requested_by) REFERENCES principals (id),
) PRIMARY KEY (principal_id);
CREATE INDEX deletions_due ON deletions (requested_at);

CREATE TABLE tenancy_state (
    singleton BOOL        NOT NULL,
    mode      STRING(MAX) NOT NULL,
    moved_in  STRING(MAX),
    moved_at  TIMESTAMP,
    CONSTRAINT tenancy_state_one_check CHECK (singleton),
    CONSTRAINT tenancy_state_mode_check CHECK (mode IN ('installation', 'hosted')),
    CONSTRAINT tenancy_state_moved_in_fk FOREIGN KEY (moved_in) REFERENCES principals (id),
) PRIMARY KEY (singleton);

CREATE TABLE name_aliases (
    old_name STRING(MAX) NOT NULL,
    new_name STRING(MAX) NOT NULL,
) PRIMARY KEY (old_name);

-- ─── SCIM ───

-- What an identity provider provisioned, by scope: the installation's (''), or an organization's.
-- A user's name and a group's are unique in their scope, case-insensitively.
CREATE TABLE scim_users (
    id              STRING(MAX) NOT NULL,
    user_name       STRING(MAX) NOT NULL,
    data            BYTES(MAX)  NOT NULL,        -- the user as ok keeps it, JSON
    scope           STRING(MAX) NOT NULL,
    created_at      TIMESTAMP   NOT NULL,
    modified_at     TIMESTAMP   NOT NULL,
    user_name_lower STRING(MAX) AS (LOWER(user_name)) STORED,
) PRIMARY KEY (id);
CREATE UNIQUE INDEX scim_users_user_name ON scim_users (scope, user_name_lower);

CREATE TABLE scim_groups (
    id                 STRING(MAX)        NOT NULL,
    display_name       STRING(MAX)        NOT NULL,
    data               BYTES(MAX)         NOT NULL, -- the group, without its members, JSON
    members            ARRAY<STRING(MAX)> NOT NULL, -- users' and groups' IDs
    scope              STRING(MAX)        NOT NULL,
    created_at         TIMESTAMP          NOT NULL,
    modified_at        TIMESTAMP          NOT NULL,
    display_name_lower STRING(MAX) AS (LOWER(display_name)) STORED,
) PRIMARY KEY (id);
CREATE UNIQUE INDEX scim_groups_display_name ON scim_groups (scope, display_name_lower);
CREATE INDEX scim_groups_scope ON scim_groups (scope);
