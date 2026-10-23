-- The SCM's tables (docs/scm.md, "Schema"): the repository registry, every object but file
-- contents, the operation logs, ChangeLists, the change feed, garbage collection's passes, and
-- okfs's workspaces. File contents are packs in the SCM's bucket.

-- Each repository, and main: its head, and head_log, every move of it.
CREATE TABLE repository (
    id         bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        NOT NULL UNIQUE,
    head       bytea       NOT NULL,                  -- main; the root commit at creation
    head_seq   bigint      NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text        NOT NULL                   -- the principal that created it
);

CREATE TABLE head_log (                               -- every move of main, one per landed commit
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    seq           bigint      NOT NULL,
    old_head      bytea       NOT NULL,
    new_head      bytea       NOT NULL,
    cl_number     bigint      NOT NULL,
    patchset      int         NOT NULL,
    landed_by     text        NOT NULL,
    landed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, seq)
);
-- main is linear: the root commit and every landed commit, each some row's new head. "Is
-- this commit on main" is one probe.
CREATE INDEX head_log_new_head ON head_log (repository_id, new_head);

-- A repository's content (docs/scm.md, "Objects and identity", "Storage"): trees,
-- symlinks, and copy histories as canonical bodies; commits with their parent edges; the
-- paths each commit on main changed; and file contents as chunks in packs on S3, which the
-- content index records. Objects and commits carry GC's pinned flag and freshness day (files
-- and chunks theirs in the index); the *_unpinned indexes are what garbage collection lists its
-- candidates from (most rows end pinned).

-- Trees, symlinks, and copy histories, as canonical bodies.
CREATE TABLE object (
    repository_id bigint   NOT NULL REFERENCES repository ON DELETE CASCADE,
    id            bytea    NOT NULL,
    kind          smallint NOT NULL,                  -- 2 symlink, 3 tree, 4 copy history
    body          bytea    NOT NULL,
    pinned        boolean  NOT NULL DEFAULT false,
    touched_at    date     NOT NULL DEFAULT current_date,  -- freshness day (GC)
    PRIMARY KEY (repository_id, id)
);
CREATE INDEX object_unpinned ON object (repository_id, touched_at) WHERE NOT pinned;

-- Each tree's sizes (docs/scm.md, "Tree sizes"): a record derived from the tree and what
-- it names (encoding.EncodeTreeSizes), kept the first time it is read, and gone with its tree.
CREATE TABLE tree_sizes (
    repository_id bigint NOT NULL,
    tree_id       bytea  NOT NULL,
    body          bytea  NOT NULL,
    PRIMARY KEY (repository_id, tree_id),
    FOREIGN KEY (repository_id, tree_id) REFERENCES object ON DELETE CASCADE
);

CREATE TABLE commit (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    id            bytea       NOT NULL,
    change_id     bytea       NOT NULL,
    body          bytea       NOT NULL,
    generation    bigint      NOT NULL,               -- 1 + the highest parent's; the root is 0
    conflicted    boolean     NOT NULL,
    committed_at  timestamptz NOT NULL,
    written_by    text        NOT NULL,               -- the publisher, or the submitter for landings and candidates
    pinned        boolean     NOT NULL DEFAULT false,
    touched_at    date        NOT NULL DEFAULT current_date,  -- freshness day (GC)
    PRIMARY KEY (repository_id, id)
);
CREATE INDEX commit_change ON commit (repository_id, change_id);
CREATE INDEX commit_unpinned ON commit (repository_id, touched_at) WHERE NOT pinned;

CREATE TABLE commit_parent (                          -- the virtual root has no commit row
    repository_id bigint   NOT NULL,
    commit_id     bytea    NOT NULL,
    ord           smallint NOT NULL,
    parent_id     bytea    NOT NULL,
    PRIMARY KEY (repository_id, commit_id, ord),
    FOREIGN KEY (repository_id, commit_id) REFERENCES commit ON DELETE CASCADE
);
CREATE INDEX commit_parent_parent ON commit_parent (repository_id, parent_id);

CREATE TABLE copy_parent (                            -- reverse index for GetRelatedCopies
    repository_id bigint NOT NULL,
    copy_id       bytea  NOT NULL,
    parent_id     bytea  NOT NULL,
    PRIMARY KEY (repository_id, copy_id, parent_id),
    FOREIGN KEY (repository_id, copy_id) REFERENCES object ON DELETE CASCADE
);
CREATE INDEX copy_parent_parent ON copy_parent (repository_id, parent_id);

CREATE TABLE commit_path (                            -- paths each commit on main changed, and their directories
    repository_id bigint NOT NULL REFERENCES repository ON DELETE CASCADE,
    path          text   NOT NULL COLLATE "C",
    generation    bigint NOT NULL,
    commit_id     bytea  NOT NULL,
    PRIMARY KEY (repository_id, path, generation, commit_id)
);

CREATE TABLE pack (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    id            bytea       NOT NULL,               -- BLAKE3 of the pack
    size          bigint      NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    retired_at    timestamptz,                        -- compacted away; deleted from S3 an hour later
    dictionary_id bigint,                             -- the dictionary compaction wrote it with, if any
    num           bigint      GENERATED ALWAYS AS IDENTITY,  -- what the content index names it by
    PRIMARY KEY (repository_id, id)
);
CREATE INDEX pack_retired ON pack (retired_at) WHERE retired_at IS NOT NULL;

-- The content index (docs/scm.md, "The content index"): each file's size and chunks and
-- each chunk's location, with whether each is pinned and the last day anything referenced it.
-- A merge writes a generation's blocks whole; the log holds each transaction's changes since.
CREATE TABLE content_index (
    repository_id bigint PRIMARY KEY REFERENCES repository ON DELETE CASCADE,
    generation    bigint NOT NULL                     -- the blocks readers load
);

CREATE TABLE index_block (
    repository_id bigint NOT NULL REFERENCES repository ON DELETE CASCADE,
    generation    bigint NOT NULL,
    first_id      bytea  NOT NULL,                    -- its first entry's ID
    body          bytea  NOT NULL,                    -- contentindex.EncodeBlock: entries in ID order
    PRIMARY KEY (repository_id, generation, first_id)
);

CREATE TABLE content_log (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    id            bigint      GENERATED ALWAYS AS IDENTITY,
    created_at    timestamptz NOT NULL DEFAULT now(),
    body          bytea       NOT NULL,               -- contentindex.EncodeLog: one transaction's changes
    PRIMARY KEY (repository_id, id)
);

CREATE TABLE dictionary (                             -- a repository's zstd dictionaries; never deleted
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- a frame names it as 32768 + id
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    body          bytea       NOT NULL,               -- empty while it is trained
    samples       int         NOT NULL,               -- the files it was trained on
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX dictionary_repository ON dictionary (repository_id, id);

-- The operation logs (docs/scm.md, "Decisions", 3): one per workspace — (repository,
-- principal, workspace) — each with its operations, views, and operation heads, so no workspace
-- sees or changes another's. Operations and views carry the server time they were last
-- published, for collection. A log of no workspace (the empty name) is the shared log of a
-- principal's workspaces from before each had its own: no request reaches it, and garbage
-- collection deletes it.

CREATE TABLE op_log (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    principal     text        NOT NULL,
    workspace     text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (repository_id, principal, workspace)
);

CREATE TABLE operation (
    op_log_id  bigint      NOT NULL REFERENCES op_log ON DELETE CASCADE,
    id         bytea       NOT NULL,
    view_id    bytea       NOT NULL,
    body       bytea       NOT NULL,
    touched_at timestamptz NOT NULL DEFAULT now(),    -- server time; bumped on re-publish
    PRIMARY KEY (op_log_id, id)
);
CREATE INDEX operation_view ON operation (op_log_id, view_id);

CREATE TABLE view (
    op_log_id  bigint      NOT NULL REFERENCES op_log ON DELETE CASCADE,
    id         bytea       NOT NULL,
    body       bytea       NOT NULL,
    touched_at timestamptz NOT NULL DEFAULT now(),    -- server time; bumped on re-publish
    PRIMARY KEY (op_log_id, id)
);

CREATE TABLE op_head (                                -- the root op (32 zero bytes) until the first publish
    op_log_id bigint NOT NULL REFERENCES op_log ON DELETE CASCADE,
    op_id     bytea  NOT NULL,
    PRIMARY KEY (op_log_id, op_id)
);

-- ChangeLists (docs/scm.md, "ChangeLists in the SCM"): each CL, its patchsets, the
-- candidates prepared for post-submit automation, and the change feed.

CREATE TABLE changelist (
    repository_id    bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    number           bigint      NOT NULL,
    change_id        bytea       NOT NULL,
    author           text        NOT NULL,
    status           smallint    NOT NULL,            -- 1 draft, 2 open, 3 submitted, 4 abandoned
    abandoned_from   smallint,
    parent_number    bigint,
    current_patchset int         NOT NULL,
    landed_commit    bytea,
    submitted_by     text,
    description      text        NOT NULL,            -- the current patchset's
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    -- A rollback CL's (docs/scm.md, "Rollbacks"): the CLs it rolls back, in landing
    -- order; the paths its first patchset rolled back; and the paths it left, which main
    -- changed after the CLs landed. Empty for any other CL.
    rolls_back       bigint[]    NOT NULL DEFAULT '{}',
    rollback_paths   text[]      NOT NULL DEFAULT '{}',
    rollback_left    text[]      NOT NULL DEFAULT '{}',
    PRIMARY KEY (repository_id, number),
    UNIQUE (repository_id, change_id)
);
CREATE INDEX changelist_status ON changelist (repository_id, status, number);
CREATE INDEX changelist_author ON changelist (repository_id, author, number);

CREATE TABLE patchset (
    repository_id  bigint      NOT NULL,
    cl_number      bigint      NOT NULL,
    number         int         NOT NULL,
    commit_id      bytea       NOT NULL,
    base_commit_id bytea       NOT NULL,
    parent_number  bigint,                            -- the CL it was stacked on when uploaded
    uploaded_by    text        NOT NULL,
    uploaded_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, cl_number, number),
    FOREIGN KEY (repository_id, cl_number) REFERENCES changelist ON DELETE CASCADE
);
CREATE INDEX patchset_commit ON patchset (repository_id, commit_id);

CREATE TABLE candidate (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    commit_id     bytea       NOT NULL,
    cl_number     bigint      NOT NULL,
    patchset      int         NOT NULL,
    base          bytea       NOT NULL,
    prepared_by   text        NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, commit_id)            -- preparing the same candidate again is a no-op
);
CREATE INDEX candidate_cl ON candidate (repository_id, cl_number);

-- A repository's CL numbers come from their own row rather than the repository row. An
-- upload takes it just before the event counter, after its CL row locks, while a landing
-- locks the repository row first and CL rows after: with the counter on the repository
-- row, the two could wait on each other. The row is made by the repository's first CL.
CREATE TABLE changelist_counter (
    repository_id bigint PRIMARY KEY REFERENCES repository ON DELETE CASCADE,
    last          bigint NOT NULL
);

-- The change feed. One row, made by the first event; its lock orders events by commit.
CREATE TABLE event_counter (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    last bigint  NOT NULL
);

CREATE TABLE event (
    seq           bigint      PRIMARY KEY,
    repository_id bigint      NOT NULL,
    at            timestamptz NOT NULL DEFAULT now(),
    body          bytea       NOT NULL                -- an ok.scm.v1.Event
);

-- Garbage collection's passes (docs/scm.md, "Garbage collection and compaction"):
-- one row per pass over a repository — what it collected, swept, compacted, and deleted,
-- or why it failed. A repository is due a pass when its last one began before the latest
-- scheduled time; a failed one is tried again an hour later.
CREATE TABLE gc_pass (
    repository_id      bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    started_at         timestamptz NOT NULL,
    finished_at        timestamptz NOT NULL,
    operations_deleted bigint      NOT NULL,
    views_deleted      bigint      NOT NULL,
    marked             bigint      NOT NULL,          -- stale objects kept as still reachable
    commits_swept      bigint      NOT NULL,
    objects_swept      bigint      NOT NULL,
    files_swept        bigint      NOT NULL,
    chunks_swept       bigint      NOT NULL,
    chunk_bytes_swept  bigint      NOT NULL,
    packs_written      int         NOT NULL,
    packs_retired      int         NOT NULL,
    bytes_read         bigint      NOT NULL,
    bytes_reclaimed    bigint      NOT NULL,
    packs_deleted      int         NOT NULL,
    orphan_packs       int         NOT NULL,
    error              text,                          -- the pass failed here; counts are what it did first
    checkpoints_deleted bigint     NOT NULL DEFAULT 0, -- okfs checkpoints retention no longer keeps
    op_logs_deleted    bigint      NOT NULL DEFAULT 0, -- operation logs of no workspace, which no request reaches
    workspaces_deleted bigint      NOT NULL DEFAULT 0, -- workspaces with no operation log, which no clone can use
    PRIMARY KEY (repository_id, started_at)
);

-- okfs's workspaces (docs/okfs.md, "Checkpoints" and "Attaching a workspace on another
-- machine"), keyed by (repository, principal, workspace name) as the operation log is.

-- A workspace's attachment lease: the machine holding it, until when, at which epoch (every
-- grant bumps it), and a machine that has asked to take over.
CREATE TABLE workspace (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    principal     text        NOT NULL,
    name          text        NOT NULL,
    holder        text,                             -- the machine holding the lease; NULL: none
    epoch         bigint      NOT NULL DEFAULT 0,
    expires_at    timestamptz,                      -- the lease's end, unless renewed
    takeover_by   text,                             -- a machine waiting for the holder to hand over
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, principal, name)
);

-- A checkpoint: what a workspace's working copy held at a moment, on a machine. The latest is the
-- newest that is not a fork; a fork was recorded without the current lease, or off the latest.
CREATE TABLE checkpoint (
    id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id  bigint      NOT NULL,
    principal      text        NOT NULL,
    workspace      text        NOT NULL,
    machine        text        NOT NULL,
    parent_id      bigint      REFERENCES checkpoint ON DELETE SET NULL,
    tree_ids       bytea[]     NOT NULL,            -- the working copy's tree: a merge's terms
    base_commit_id bytea       NOT NULL,            -- the working-copy commit it was taken on
    fork           boolean     NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (repository_id, principal, workspace) REFERENCES workspace ON DELETE CASCADE
);
CREATE INDEX checkpoint_workspace ON checkpoint (repository_id, principal, workspace, id);
CREATE INDEX checkpoint_parent ON checkpoint (parent_id);

-- No statistics on the tenant keys (docs/scm.md, "Schema"). With every repository
-- (or operation log) in a column's most-common-values list, a value ANALYZE has not yet
-- seen — a new repository, a fresh import, a new principal's log — is estimated at one
-- row, so the planner costs "scan the whole tenant through any index led by the key" the
-- same as a unique-key lookup and can pick the scan, per probe. Without statistics the
-- key's estimate is the fixed default, and a full-key lookup always wins.
ALTER TABLE head_log           ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE object             ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE tree_sizes         ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE commit             ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE commit_parent      ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE copy_parent        ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE commit_path        ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE pack               ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE dictionary         ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE index_block        ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE content_log        ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE op_log             ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE changelist         ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE patchset           ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE candidate          ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE changelist_counter ALTER COLUMN repository_id SET STATISTICS 0;
ALTER TABLE operation          ALTER COLUMN op_log_id     SET STATISTICS 0;
ALTER TABLE view               ALTER COLUMN op_log_id     SET STATISTICS 0;
ALTER TABLE op_head            ALTER COLUMN op_log_id     SET STATISTICS 0;
ALTER TABLE gc_pass            ALTER COLUMN repository_id SET STATISTICS 0;
