-- The SCM's tables on Spanner (docs/hosted.md, "The SCM on Spanner, as built"), as postgres's
-- migrations leave them on PostgreSQL, under the same names, so the store's tests read both alike.
-- A repository's rows are interleaved in it, and a network's in it; where PostgreSQL's foreign key
-- deletes a repository's rows with it, its interleaving is ON DELETE NO ACTION instead: a row needs
-- its repository, as PostgreSQL's key has it, and the backend deletes them before it, which a
-- repository of any size needs (one commit holds 80,000 mutations). What hangs off one row —
-- an operation log's operations, a CL's patchsets, a commit's parent edges — goes with it.
-- PostgreSQL's identity columns and sequence are counters (a pack's number, a content log row's,
-- a dictionary's, a segment's age), taken in a transaction of their own as a sequence's are.

CREATE TABLE network (
    network_id INT64     NOT NULL,                -- random, as a repository's
    created_at TIMESTAMP NOT NULL,
    trainer_id INT64,                             -- the repository that trains its dictionaries
) PRIMARY KEY (network_id);

CREATE TABLE repository (
    repository_id INT64       NOT NULL,
    network_id    INT64       NOT NULL,
    parent_id     INT64,                          -- a fork's upstream
    parent_seq    INT64,                          -- the place in its parent's main it follows up to
    head          BYTES(MAX)  NOT NULL,           -- main; the root commit at creation
    head_seq      INT64       NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
    created_by    STRING(MAX) NOT NULL,           -- the principal that created it
    CONSTRAINT repository_network_fk FOREIGN KEY (network_id) REFERENCES network (network_id),
    CONSTRAINT repository_parent_fk FOREIGN KEY (parent_id) REFERENCES repository (repository_id),
) PRIMARY KEY (repository_id);
CREATE INDEX repository_network ON repository (network_id, created_at);
CREATE INDEX repository_parent ON repository (parent_id);

-- Every move of main, one per landed commit.
CREATE TABLE head_log (
    repository_id INT64       NOT NULL,
    seq           INT64       NOT NULL,
    old_head      BYTES(MAX)  NOT NULL,
    new_head      BYTES(MAX)  NOT NULL,
    cl_number     INT64       NOT NULL,
    patchset      INT64       NOT NULL,
    landed_by     STRING(MAX) NOT NULL,
    landed_at     TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, seq), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE INDEX head_log_new_head ON head_log (repository_id, new_head);
CREATE INDEX head_log_cl ON head_log (repository_id, cl_number);

CREATE TABLE object (
    repository_id INT64      NOT NULL,
    id            BYTES(MAX) NOT NULL,
    kind          INT64      NOT NULL,            -- 2 symlink, 3 tree, 4 copy history
    body          BYTES(MAX) NOT NULL,
    pinned        BOOL       NOT NULL,
    touched_at    DATE       NOT NULL,            -- freshness day (GC), in UTC
) PRIMARY KEY (repository_id, id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE INDEX object_unpinned ON object (repository_id, pinned, touched_at);

-- A tree's sizes record, kept while the tree is in the tail or once it moved into a segment.
CREATE TABLE tree_sizes (
    repository_id INT64      NOT NULL,
    tree_id       BYTES(MAX) NOT NULL,
    body          BYTES(MAX) NOT NULL,
) PRIMARY KEY (repository_id, tree_id), INTERLEAVE IN repository;

CREATE TABLE commit (
    repository_id INT64       NOT NULL,
    id            BYTES(MAX)  NOT NULL,
    change_id     BYTES(MAX)  NOT NULL,
    body          BYTES(MAX)  NOT NULL,
    generation    INT64       NOT NULL,           -- 1 + the highest parent's; the root is 0
    conflicted    BOOL        NOT NULL,
    committed_at  TIMESTAMP   NOT NULL,
    written_by    STRING(MAX) NOT NULL,           -- the publisher, or the submitter for landings and candidates
    pinned        BOOL        NOT NULL,
    touched_at    DATE        NOT NULL,           -- freshness day (GC), in UTC
) PRIMARY KEY (repository_id, id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE INDEX commit_unpinned ON commit (repository_id, pinned, touched_at);

-- A commit's parents, in order; the virtual root has no commit row.
CREATE TABLE commit_parent (
    repository_id INT64      NOT NULL,
    commit_id     BYTES(MAX) NOT NULL,
    ord           INT64      NOT NULL,
    parent_id     BYTES(MAX) NOT NULL,
    CONSTRAINT commit_parent_commit_fk FOREIGN KEY (repository_id, commit_id) REFERENCES commit (repository_id, id) ON DELETE CASCADE,
) PRIMARY KEY (repository_id, commit_id, ord), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

-- Copy histories' parent edges, reversed, for GetRelatedCopies.
CREATE TABLE copy_parent (
    repository_id INT64      NOT NULL,
    copy_id       BYTES(MAX) NOT NULL,
    parent_id     BYTES(MAX) NOT NULL,
) PRIMARY KEY (repository_id, copy_id, parent_id), INTERLEAVE IN repository;
CREATE INDEX copy_parent_parent ON copy_parent (repository_id, parent_id);

-- The paths each commit on main changed, and their directories. Spanner orders strings by their
-- bytes, as PostgreSQL's COLLATE "C" does.
CREATE TABLE commit_path (
    repository_id INT64       NOT NULL,
    path          STRING(MAX) NOT NULL,
    generation    INT64       NOT NULL,
    commit_id     BYTES(MAX)  NOT NULL,
) PRIMARY KEY (repository_id, path, generation, commit_id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

-- A network's packs: what its content index names by number.
CREATE TABLE pack (
    network_id    INT64      NOT NULL,
    id            BYTES(MAX) NOT NULL,            -- BLAKE3 of the pack
    size          INT64      NOT NULL,
    created_at    TIMESTAMP  NOT NULL,
    retired_at    TIMESTAMP,                      -- compacted away; deleted from the bucket an hour later
    dictionary_id INT64,                          -- the dictionary compaction considered it with, if any
    num           INT64      NOT NULL,            -- what the content index names it by: the network's counter
) PRIMARY KEY (network_id, id), INTERLEAVE IN PARENT network ON DELETE CASCADE;
CREATE NULL_FILTERED INDEX pack_retired ON pack (retired_at);

-- A content index — a repository's (scope 0) or a network's (1) — its blocks, and its log.
CREATE TABLE content_index (
    scope      INT64 NOT NULL,
    owner_id   INT64 NOT NULL,
    generation INT64 NOT NULL,                    -- the blocks readers load
) PRIMARY KEY (scope, owner_id);

CREATE TABLE index_block (
    scope      INT64      NOT NULL,
    owner_id   INT64      NOT NULL,
    generation INT64      NOT NULL,
    first_id   BYTES(MAX) NOT NULL,               -- its first entry's ID
    body       BYTES(MAX) NOT NULL,               -- contentindex.EncodeBlock: entries in ID order
) PRIMARY KEY (scope, owner_id, generation, first_id), INTERLEAVE IN content_index;

CREATE TABLE content_log (
    scope      INT64      NOT NULL,
    owner_id   INT64      NOT NULL,
    id         INT64      NOT NULL,               -- the content index's counter
    created_at TIMESTAMP  NOT NULL,
    body       BYTES(MAX) NOT NULL,               -- contentindex.EncodeLog: one transaction's changes
) PRIMARY KEY (scope, owner_id, id), INTERLEAVE IN content_index;

-- A network's zstd dictionaries. A frame names one as 32768 + id, so ids are counted, and small.
-- A network's dictionaries, numbered by its counter: a frame in its packs names one by its ID, which
-- the network keeps wherever it moves.
CREATE TABLE dictionary (
    network_id INT64      NOT NULL,
    id         INT64      NOT NULL,
    trainer_id INT64,
    body       BYTES(MAX) NOT NULL,               -- empty while it is trained
    samples    INT64      NOT NULL,               -- the files it was trained on
    created_at TIMESTAMP  NOT NULL,
) PRIMARY KEY (network_id, id), INTERLEAVE IN PARENT network ON DELETE CASCADE;
CREATE INDEX dictionary_trainer ON dictionary (trainer_id, id);

-- A workspace's operation log, and its operations, views, heads, and tombstones.
CREATE TABLE op_log (
    op_log_id     INT64       NOT NULL,
    repository_id INT64       NOT NULL,
    principal     STRING(MAX) NOT NULL,
    workspace     STRING(MAX) NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
    CONSTRAINT op_log_repository_fk FOREIGN KEY (repository_id) REFERENCES repository (repository_id),
) PRIMARY KEY (op_log_id);
CREATE UNIQUE INDEX op_log_workspace ON op_log (repository_id, principal, workspace);

CREATE TABLE operation (
    op_log_id  INT64      NOT NULL,
    id         BYTES(MAX) NOT NULL,
    view_id    BYTES(MAX) NOT NULL,
    body       BYTES(MAX) NOT NULL,
    touched_at TIMESTAMP  NOT NULL,               -- server time; bumped on re-publish
) PRIMARY KEY (op_log_id, id), INTERLEAVE IN PARENT op_log ON DELETE CASCADE;
CREATE INDEX operation_view ON operation (op_log_id, view_id);

CREATE TABLE view (
    op_log_id  INT64      NOT NULL,
    id         BYTES(MAX) NOT NULL,
    body       BYTES(MAX) NOT NULL,
    touched_at TIMESTAMP  NOT NULL,               -- server time; bumped on re-publish
) PRIMARY KEY (op_log_id, id), INTERLEAVE IN PARENT op_log ON DELETE CASCADE;

-- The root op (32 zero bytes) until the first publish.
CREATE TABLE op_head (
    op_log_id INT64      NOT NULL,
    op_id     BYTES(MAX) NOT NULL,
) PRIMARY KEY (op_log_id, op_id), INTERLEAVE IN PARENT op_log ON DELETE CASCADE;

CREATE TABLE op_tombstone (
    op_log_id INT64      NOT NULL,
    kind      INT64      NOT NULL,                -- 6 operation, 7 view
    id        BYTES(MAX) NOT NULL,
) PRIMARY KEY (op_log_id, kind, id), INTERLEAVE IN PARENT op_log ON DELETE CASCADE;

CREATE TABLE changelist (
    repository_id    INT64              NOT NULL,
    number           INT64              NOT NULL,
    change_id        BYTES(MAX)         NOT NULL,
    author           STRING(MAX)        NOT NULL,
    status           INT64              NOT NULL, -- 1 draft, 2 open, 3 submitted, 4 abandoned
    abandoned_from   INT64,
    parent_number    INT64,
    current_patchset INT64              NOT NULL,
    landed_commit    BYTES(MAX),
    submitted_by     STRING(MAX),
    description      STRING(MAX)        NOT NULL, -- the current patchset's
    created_at       TIMESTAMP          NOT NULL,
    updated_at       TIMESTAMP          NOT NULL,
    rolls_back       ARRAY<INT64>       NOT NULL,
    rollback_paths   ARRAY<STRING(MAX)> NOT NULL,
    rollback_left    ARRAY<STRING(MAX)> NOT NULL,
) PRIMARY KEY (repository_id, number), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE UNIQUE INDEX changelist_change ON changelist (repository_id, change_id);
CREATE INDEX changelist_status ON changelist (repository_id, status, number);
CREATE INDEX changelist_author ON changelist (repository_id, author, number);

CREATE TABLE patchset (
    repository_id  INT64       NOT NULL,
    cl_number      INT64       NOT NULL,
    number         INT64       NOT NULL,
    commit_id      BYTES(MAX)  NOT NULL,
    base_commit_id BYTES(MAX)  NOT NULL,
    parent_number  INT64,                         -- the CL it was stacked on when uploaded
    uploaded_by    STRING(MAX) NOT NULL,
    uploaded_at    TIMESTAMP   NOT NULL,
    CONSTRAINT patchset_changelist_fk FOREIGN KEY (repository_id, cl_number) REFERENCES changelist (repository_id, number) ON DELETE CASCADE,
) PRIMARY KEY (repository_id, cl_number, number), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE INDEX patchset_commit ON patchset (repository_id, commit_id);
CREATE INDEX patchset_parent ON patchset (repository_id, parent_number);

CREATE TABLE candidate (
    repository_id INT64       NOT NULL,
    commit_id     BYTES(MAX)  NOT NULL,
    cl_number     INT64       NOT NULL,
    patchset      INT64       NOT NULL,
    base          BYTES(MAX)  NOT NULL,
    prepared_by   STRING(MAX) NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, commit_id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE INDEX candidate_cl ON candidate (repository_id, cl_number);

CREATE TABLE changelist_counter (
    repository_id INT64 NOT NULL,
    last          INT64 NOT NULL,
) PRIMARY KEY (repository_id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

-- A repository's log of events, each pending until the relay publishes it. A relay holds the
-- pending events it took for a lease (holder, lease_until), publishing outside any transaction.
CREATE TABLE repository_event_counter (
    repository_id INT64 NOT NULL,
    last          INT64 NOT NULL,
) PRIMARY KEY (repository_id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

CREATE TABLE event (
    repository_id  INT64      NOT NULL,
    repository_seq INT64      NOT NULL,
    event_at       TIMESTAMP  NOT NULL,           -- (at is a reserved word in GoogleSQL)
    body           BYTES(MAX) NOT NULL,           -- an ok.scm.v1.Event
) PRIMARY KEY (repository_id, repository_seq), INTERLEAVE IN repository;

CREATE TABLE event_pending (
    repository_id  INT64       NOT NULL,
    repository_seq INT64       NOT NULL,
    holder         STRING(MAX),
    lease_until    TIMESTAMP,
) PRIMARY KEY (repository_id, repository_seq), INTERLEAVE IN repository;

CREATE TABLE gc_pass (
    repository_id       INT64       NOT NULL,
    started_at          TIMESTAMP   NOT NULL,
    finished_at         TIMESTAMP   NOT NULL,
    operations_deleted  INT64       NOT NULL,
    views_deleted       INT64       NOT NULL,
    marked              INT64       NOT NULL,     -- stale objects kept as still reachable
    commits_swept       INT64       NOT NULL,
    objects_swept       INT64       NOT NULL,
    files_swept         INT64       NOT NULL,
    chunks_swept        INT64       NOT NULL,
    chunk_bytes_swept   INT64       NOT NULL,
    packs_written       INT64       NOT NULL,
    packs_retired       INT64       NOT NULL,
    bytes_read          INT64       NOT NULL,
    bytes_reclaimed     INT64       NOT NULL,
    packs_deleted       INT64       NOT NULL,
    orphan_packs        INT64       NOT NULL,
    error               STRING(MAX),              -- the pass failed here; counts are what it did first
    checkpoints_deleted INT64       NOT NULL,
    op_logs_deleted     INT64       NOT NULL,
    workspaces_deleted  INT64       NOT NULL,
) PRIMARY KEY (repository_id, started_at), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

CREATE TABLE workspace (
    repository_id INT64       NOT NULL,
    principal     STRING(MAX) NOT NULL,
    name          STRING(MAX) NOT NULL,
    holder        STRING(MAX),                    -- the machine holding the lease; NULL: none
    epoch         INT64       NOT NULL,
    expires_at    TIMESTAMP,                      -- the lease's end, unless renewed
    takeover_by   STRING(MAX),                    -- a machine waiting for the holder to hand over
    created_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, principal, name), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

-- A workspace's checkpoints; a child's parent is set NULL as its parent goes (the backend does it).
CREATE TABLE checkpoint (
    repository_id  INT64             NOT NULL,
    id             INT64             NOT NULL,
    principal      STRING(MAX)       NOT NULL,
    workspace      STRING(MAX)       NOT NULL,
    machine        STRING(MAX)       NOT NULL,
    parent_id      INT64,
    tree_ids       ARRAY<BYTES(MAX)> NOT NULL,    -- the working copy's tree: a merge's terms
    base_commit_id BYTES(MAX)        NOT NULL,    -- the working-copy commit it was taken on
    fork           BOOL              NOT NULL,
    created_at     TIMESTAMP         NOT NULL,
    CONSTRAINT checkpoint_workspace_fk FOREIGN KEY (repository_id, principal, workspace)
        REFERENCES workspace (repository_id, principal, name) ON DELETE CASCADE,
) PRIMARY KEY (repository_id, id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
CREATE INDEX checkpoint_workspace ON checkpoint (repository_id, principal, workspace, created_at, id);
CREATE INDEX checkpoint_parent ON checkpoint (repository_id, parent_id);

CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key);

CREATE TABLE segment (
    scope      INT64      NOT NULL,               -- 0 a repository's, 1 a network's
    owner_id   INT64      NOT NULL,
    kind       INT64      NOT NULL,
    id         BYTES(MAX) NOT NULL,               -- the BLAKE3 of its bytes
    network_id INT64      NOT NULL,               -- whose object it names
    level      INT64      NOT NULL,               -- 0 written from the tail; n+1 merged from n
    records    INT64      NOT NULL,
    size       INT64      NOT NULL,
    created_at TIMESTAMP  NOT NULL,
    retired_at TIMESTAMP,                         -- merged into another; deleted a day later
    age        INT64      NOT NULL,               -- the network's counter
    inherited  BOOL       NOT NULL,
    bound      INT64,
) PRIMARY KEY (scope, owner_id, kind, id);
CREATE INDEX segment_object ON segment (network_id, kind, id);
CREATE NULL_FILTERED INDEX segment_retired ON segment (retired_at);

CREATE TABLE chunk_release (
    network_id  INT64      NOT NULL,
    chunk_id    BYTES(MAX) NOT NULL,
    released_at TIMESTAMP  NOT NULL,
) PRIMARY KEY (network_id, chunk_id), INTERLEAVE IN PARENT network ON DELETE CASCADE;

CREATE TABLE dictionary_grant (
    repository_id INT64 NOT NULL,
    trainer_id    INT64 NOT NULL,
    up_to         INT64 NOT NULL,                 -- the newest of the trainer's it may read
) PRIMARY KEY (repository_id, trainer_id), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;

CREATE TABLE repository_deletion (
    repository_id INT64     NOT NULL,
    network_id    INT64     NOT NULL,
    deleted_at    TIMESTAMP NOT NULL,
    CONSTRAINT repository_deletion_network_fk FOREIGN KEY (network_id) REFERENCES network (network_id) ON DELETE CASCADE,
) PRIMARY KEY (repository_id);
CREATE INDEX repository_deletion_due ON repository_deletion (deleted_at);

-- What PostgreSQL's identity columns and sequence counted: by name, and the network, content index,
-- or nothing (0) they are counted in.
CREATE TABLE counter (
    name     STRING(MAX) NOT NULL,
    scope_id INT64       NOT NULL,
    last     INT64       NOT NULL,
) PRIMARY KEY (name, scope_id);
