-- ok control's tables on Spanner (docs/hosted.md, "Spanner"), as postgres's migrations leave them on
-- PostgreSQL: the projection of the SCM's repositories and CLs, review — reviewers, label grants,
-- comments and threads, each CL's history — and each person's favorite repositories. A CL is
-- recorded whether or not its repository is (the SCM's events and a request may each come first), so
-- CLs are a table of their own; what review keeps of a CL is interleaved in it. An interleaved
-- table's key begins with its parent's, by name.

-- The SCM's repositories, as its events and the directory's describe them: what the repository
-- finder lists, searches, and orders — favorites first, then most recently updated.
CREATE TABLE repository (
    repository_id INT64       NOT NULL,
    name          STRING(MAX),                  -- its full name, as the directory's events say; NULL before they do
    deleted       BOOL        NOT NULL DEFAULT (FALSE),
    created_by    STRING(MAX) NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
    head_seq      INT64       NOT NULL DEFAULT (0), -- commits on main
    updated_at    TIMESTAMP   NOT NULL,         -- when main last moved (created_at before)
) PRIMARY KEY (repository_id);
CREATE INDEX repository_updated ON repository (updated_at DESC);
CREATE INDEX repository_name ON repository (name);

-- The SCM's CLs, as its events describe them, plus approved: the review evaluation's stored
-- projection, which lists and filters read and nothing trusts.
CREATE TABLE changelist (
    repository_id    INT64        NOT NULL,
    number           INT64        NOT NULL,
    change_id        BYTES(MAX)   NOT NULL,
    author           STRING(MAX)  NOT NULL,     -- a principal
    status           INT64        NOT NULL,     -- 1 draft, 2 open, 3 submitted, 4 abandoned
    parent_number    INT64,                     -- the stack parent
    current_patchset INT64        NOT NULL,
    description      STRING(MAX)  NOT NULL,     -- the current patchset's
    landed_commit    BYTES(MAX),
    submitted_by     STRING(MAX),
    approved         BOOL         NOT NULL DEFAULT (FALSE),
    created_at       TIMESTAMP    NOT NULL,
    updated_at       TIMESTAMP    NOT NULL,
    rolls_back       ARRAY<INT64> NOT NULL,     -- a rollback CL's: the CLs it rolls back
) PRIMARY KEY (repository_id, number);
CREATE INDEX changelist_status ON changelist (repository_id, status, number);
CREATE INDEX changelist_author ON changelist (author, status);
CREATE INDEX changelist_landed ON changelist (repository_id, landed_commit);
CREATE INDEX changelist_parent ON changelist (repository_id, parent_number);
CREATE INDEX changelist_updated ON changelist (repository_id, updated_at DESC);

-- A CL's reviewers: assigned, or joined by granting a label. Never the author.
CREATE TABLE reviewer (
    repository_id INT64       NOT NULL,
    number        INT64       NOT NULL,
    principal     STRING(MAX) NOT NULL,
    added_by      STRING(MAX) NOT NULL,
    added_at      TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, number, principal), INTERLEAVE IN PARENT changelist ON DELETE CASCADE;
CREATE INDEX reviewer_principal ON reviewer (principal);

-- Label grants, one row per CL, reviewer, and label: its current state, and the patchset it was
-- granted or revoked on. A grant stays in force across patchsets.
CREATE TABLE label_grant (
    repository_id INT64       NOT NULL,
    number        INT64       NOT NULL,
    principal     STRING(MAX) NOT NULL,
    label         INT64       NOT NULL,         -- 1 LGTM, 2 Approval
    granted       BOOL        NOT NULL,
    patchset      INT64       NOT NULL,
    updated_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, number, principal, label), INTERLEAVE IN PARENT changelist ON DELETE CASCADE;

-- Comments: CL-level (no file), file-level (a file, no line), or inline (a file, a line, and a side
-- of the diff it was written on), threaded by parent: a thread is its root and the replies under it,
-- the root carrying its resolution. Deleting a comment deletes its replies (DeleteComment).
CREATE TABLE comment (
    repository_id INT64       NOT NULL,
    number        INT64       NOT NULL,
    comment_id    INT64       NOT NULL,
    patchset      INT64       NOT NULL,         -- the patchset it was written on
    author        STRING(MAX) NOT NULL,
    parent_id     INT64,
    file_path     STRING(MAX),
    line          INT64,                        -- 1-based
    side          INT64,                        -- 1 base, 2 patchset
    body          STRING(MAX) NOT NULL,
    resolved      BOOL        NOT NULL DEFAULT (FALSE),
    created_at    TIMESTAMP   NOT NULL,
    updated_at    TIMESTAMP   NOT NULL,
    CONSTRAINT comment_anchor CHECK (line IS NULL OR (file_path IS NOT NULL AND line > 0 AND side IN (1, 2))),
) PRIMARY KEY (repository_id, number, comment_id), INTERLEAVE IN PARENT changelist ON DELETE CASCADE;
CREATE INDEX comment_parent ON comment (repository_id, number, parent_id), INTERLEAVE IN changelist;

-- Each CL's history: the Change Log. An empty actor is ok itself. Its time is the event's, and
-- recorded_at orders lines of one time. (at is a reserved word in GoogleSQL.)
CREATE TABLE event (
    repository_id INT64       NOT NULL,
    number        INT64       NOT NULL,
    event_id      INT64       NOT NULL,
    event_at      TIMESTAMP   NOT NULL,
    recorded_at   TIMESTAMP   NOT NULL,
    actor         STRING(MAX) NOT NULL,
    kind          INT64       NOT NULL,         -- see internal/control/review
    from_status   INT64       NOT NULL,
    to_status     INT64       NOT NULL,
    subject       STRING(MAX) NOT NULL,         -- the reviewer an assignment concerns
    label         INT64       NOT NULL,
    patchset      INT64       NOT NULL,
    commit_id     BYTES(MAX),                   -- the commit a CL landed as
    message       STRING(MAX) NOT NULL,         -- an abandon reason
    related       INT64       NOT NULL,         -- the other CL it names: the rollback of a CL rolled back
) PRIMARY KEY (repository_id, number, event_id), INTERLEAVE IN PARENT changelist ON DELETE CASCADE;

-- A person's favorite repositories, which the repository finder lists first.
CREATE TABLE favorite (
    principal     STRING(MAX) NOT NULL,
    repository_id INT64       NOT NULL,
    added_at      TIMESTAMP   NOT NULL,
) PRIMARY KEY (principal, repository_id);
CREATE INDEX favorite_repository ON favorite (repository_id);

-- ok control's place in each repository's log on the bus's stream (spannerdata.CursorsDDL).
CREATE TABLE follow_cursor (
    topic STRING(MAX) NOT NULL,
    key   INT64       NOT NULL,
    seq   INT64       NOT NULL,
) PRIMARY KEY (topic, key);
