-- ok secrets' tables (docs/secrets.md, "ok secrets"). Nothing here opens without a root outside
-- the database: values are sealed under data keys, data keys wrapped by repository keys, and
-- repository keys by a root in a key management service or a keyring file.

-- Each version of a repository's key, wrapped by the root it names.
CREATE TABLE repository_key (
    repository_id bigint      NOT NULL,
    version       bigint      NOT NULL,
    root_id       text        NOT NULL,         -- keyring:<file>, gcp-kms:<key>, aws-kms:<ARN>
    wrapped       bytea,                        -- NULL once shredded: the repository is being purged
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, version)
);

-- Each version of each secret: its opening and value sealed under a data key of its own, the data
-- key wrapped by a repository key version — which the version's row keeps from being deleted.
CREATE TABLE secret_version (
    repository_id   bigint      NOT NULL,
    path            text        NOT NULL,       -- secret/prod/db-password
    digest          bytea       NOT NULL,       -- the pointer's commitment
    key_version     bigint      NOT NULL,
    wrapped_key     bytea,                      -- the data key; NULL once destroyed
    sealed          bytea,                      -- NULL in an external store, or once destroyed
    store           text        NOT NULL,       -- default, or an external store's name
    written_by      text        NOT NULL,       -- the writer's stable ID
    written_at      timestamptz NOT NULL DEFAULT now(),
    unreferenced_at timestamptz,                -- when collection first found nothing referencing it
    destroyed_at    timestamptz,
    destroyed_by    text        NOT NULL DEFAULT '',
    PRIMARY KEY (repository_id, path, digest),
    CONSTRAINT secret_version_key_fk FOREIGN KEY (repository_id, key_version) REFERENCES repository_key (repository_id, version)
);
CREATE INDEX secret_version_key ON secret_version (repository_id, key_version);

-- A person's unseal: one read of a path, until it expires.
CREATE TABLE unseal (
    repository_id bigint      NOT NULL,
    id            bigint      NOT NULL,
    path          text        NOT NULL,
    principal_id  text        NOT NULL,
    expires_at    timestamptz NOT NULL,
    used_at       timestamptz,
    PRIMARY KEY (repository_id, id)
);
CREATE INDEX unseal_of ON unseal (repository_id, principal_id, path);

-- An approval (a passkey's) whose unseals were granted: once, however often it is asked.
CREATE TABLE approval_grant (
    repository_id bigint      NOT NULL,
    approval_id   text        NOT NULL,
    granted_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, approval_id)
);

-- Every decision ok secrets made about a repository's secrets.
CREATE TABLE audit (
    repository_id bigint      NOT NULL,
    id            bigint      NOT NULL,
    decided_at    timestamptz NOT NULL DEFAULT now(),
    actor_id      text        NOT NULL,         -- the caller
    for_id        text        NOT NULL DEFAULT '', -- whom it acted for: a person, a deploy, a post job
    op            text        NOT NULL,
    path          text        NOT NULL DEFAULT '',
    digest        text        NOT NULL DEFAULT '', -- the version's digest's prefix
    outcome       text        NOT NULL,
    PRIMARY KEY (repository_id, id)
);
CREATE INDEX audit_order ON audit (repository_id, decided_at, id);

-- ok secrets' keyed work (internal/work; pgdata.WorkSchema): each repository's daily collection.
CREATE TABLE work (
    kind        text        NOT NULL,
    key         bigint      NOT NULL,
    dirty       boolean     NOT NULL DEFAULT false,
    due_at      timestamptz,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (kind, key)
);
CREATE INDEX work_due ON work (kind, key) WHERE dirty OR holder IS NOT NULL;

-- A repository's move gate (docs/hosted.md, "Cells, as built"; move.GateDDL).
CREATE TABLE move_gate (
    repository_id bigint PRIMARY KEY,
    move_state    text NOT NULL DEFAULT '' CHECK (move_state IN ('', 'frozen', 'moved')),
    moved_to      text NOT NULL DEFAULT '',
    moved_at      timestamptz
);
