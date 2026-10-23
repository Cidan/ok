-- ok secrets' tables on Spanner (docs/secrets.md, "ok secrets"), named as on PostgreSQL so a move
-- carries rows between the two as they are. Nothing here opens without a root outside the
-- database.

-- Each version of a repository's key, wrapped by the root it names; wrapped is NULL once shredded.
CREATE TABLE repository_key (
    repository_id INT64       NOT NULL,
    version       INT64       NOT NULL,
    root_id       STRING(MAX) NOT NULL,
    wrapped       BYTES(MAX),
    created_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, version);

-- Each version of each secret: its opening and value sealed under a data key of its own, the data
-- key wrapped by a repository key version — which the version's row keeps from being deleted.
CREATE TABLE secret_version (
    repository_id   INT64       NOT NULL,
    path            STRING(MAX) NOT NULL,
    digest          BYTES(32)   NOT NULL,
    key_version     INT64       NOT NULL,
    wrapped_key     BYTES(MAX),
    sealed          BYTES(MAX),
    store           STRING(MAX) NOT NULL,
    written_by      STRING(MAX) NOT NULL,
    written_at      TIMESTAMP   NOT NULL,
    unreferenced_at TIMESTAMP,
    destroyed_at    TIMESTAMP,
    destroyed_by    STRING(MAX) NOT NULL,
    CONSTRAINT secret_version_key_fk FOREIGN KEY (repository_id, key_version) REFERENCES repository_key (repository_id, version),
) PRIMARY KEY (repository_id, path, digest);

CREATE INDEX secret_version_key ON secret_version (repository_id, key_version);

-- A person's unseal: one read of a path, until it expires.
CREATE TABLE unseal (
    repository_id INT64       NOT NULL,
    id            INT64       NOT NULL,
    path          STRING(MAX) NOT NULL,
    principal_id  STRING(MAX) NOT NULL,
    expires_at    TIMESTAMP   NOT NULL,
    used_at       TIMESTAMP,
) PRIMARY KEY (repository_id, id);

CREATE INDEX unseal_of ON unseal (repository_id, principal_id, path);

-- An approval (a passkey's) whose unseals were granted.
CREATE TABLE approval_grant (
    repository_id INT64       NOT NULL,
    approval_id   STRING(MAX) NOT NULL,
    granted_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, approval_id);

-- Every decision ok secrets made about a repository's secrets.
CREATE TABLE audit (
    repository_id INT64       NOT NULL,
    id            INT64       NOT NULL,
    decided_at    TIMESTAMP   NOT NULL,
    actor_id      STRING(MAX) NOT NULL,
    for_id        STRING(MAX) NOT NULL,
    op            STRING(MAX) NOT NULL,
    path          STRING(MAX) NOT NULL,
    digest        STRING(MAX) NOT NULL,
    outcome       STRING(MAX) NOT NULL,
) PRIMARY KEY (repository_id, id);

CREATE INDEX audit_order ON audit (repository_id, decided_at, id);

-- ok secrets' keyed work (internal/work): each repository's daily collection.
CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key);

-- A repository's move gate (docs/hosted.md, "Cells, as built"; move.SpannerGateDDL).
CREATE TABLE move_gate (
    repository_id INT64       NOT NULL,
    move_state    STRING(MAX) NOT NULL,
    moved_to      STRING(MAX) NOT NULL,
    moved_at      TIMESTAMP,
) PRIMARY KEY (repository_id);
