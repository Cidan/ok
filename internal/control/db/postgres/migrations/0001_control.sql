-- ok control's tables: review — reviewers, label grants, comments and threads, each CL's change
-- log — a projection of the SCM's repositories and CLs kept current from the SCM's change feed,
-- and each person's favorite repositories. Code, CLs' lifecycle, and main live in the SCM; these
-- hold only what review and the web add.

-- The SCM's repositories, as its change feed describes them: what the repository finder
-- lists, searches, and orders — favorites first, then most recently updated.
CREATE TABLE repository (
    id         bigint      PRIMARY KEY,
    name       text        NOT NULL UNIQUE COLLATE "C",
    created_by text        NOT NULL,
    created_at timestamptz NOT NULL,
    head_seq   bigint      NOT NULL DEFAULT 0,        -- commits on main
    updated_at timestamptz NOT NULL                   -- when main last moved (created_at before)
);
CREATE INDEX repository_updated ON repository (updated_at DESC, id);

-- The SCM's CLs, as its change feed describes them (Events.Watch), plus approved: the
-- review evaluation's stored projection, which lists and filters read and nothing trusts
-- (submit and the CL page evaluate live).
CREATE TABLE changelist (
    repository_id    bigint      NOT NULL,
    number           bigint      NOT NULL,
    change_id        bytea       NOT NULL,
    author           text        NOT NULL,        -- a principal
    status           smallint    NOT NULL,        -- 1 draft, 2 open, 3 submitted, 4 abandoned
    parent_number    bigint,                      -- the stack parent
    current_patchset int         NOT NULL,
    description      text        NOT NULL,        -- the current patchset's
    landed_commit    bytea,
    submitted_by     text,
    approved         boolean     NOT NULL DEFAULT false,
    created_at       timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL,
    rolls_back       bigint[]    NOT NULL DEFAULT '{}', -- a rollback CL's: the CLs it rolls back
    PRIMARY KEY (repository_id, number)
);
CREATE INDEX changelist_status ON changelist (repository_id, status, number);
CREATE INDEX changelist_author ON changelist (author, status);
CREATE INDEX changelist_landed ON changelist (repository_id, landed_commit);
CREATE INDEX changelist_rolls_back ON changelist USING gin (rolls_back);

-- Where the projection has read the SCM's change feed to: its last event's sequence.
CREATE TABLE feed_cursor (
    id  boolean PRIMARY KEY DEFAULT true CHECK (id),
    seq bigint  NOT NULL
);

-- Review (docs/changelists.md, "Lifecycle"): who reviews a CL, their binary label
-- grants, the comments and threads, and the CL's history.

-- A CL's reviewers: assigned (Request Review, Add Reviewers, jj cl --reviewer), or joined
-- by granting a label. Never the author.
CREATE TABLE reviewer (
    repository_id bigint      NOT NULL,
    number        bigint      NOT NULL,
    principal     text        NOT NULL,
    added_by      text        NOT NULL,
    added_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, number, principal),
    FOREIGN KEY (repository_id, number) REFERENCES changelist ON DELETE CASCADE
);
CREATE INDEX reviewer_principal ON reviewer (principal);

-- Label grants, one row per CL, reviewer, and label: its current state, and the patchset
-- it was granted or revoked on. A grant stays in force across patchsets.
CREATE TABLE label_grant (
    repository_id bigint      NOT NULL,
    number        bigint      NOT NULL,
    principal     text        NOT NULL,
    label         smallint    NOT NULL,           -- 1 LGTM, 2 Approval
    granted       boolean     NOT NULL,
    patchset      int         NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, number, principal, label),
    FOREIGN KEY (repository_id, number) REFERENCES changelist ON DELETE CASCADE
);

-- Comments: CL-level (no file), file-level (a file, no line), or inline (a file, a line,
-- and a side of the diff it was written on), threaded by parent. A thread is its root
-- comment and the replies under it; the root carries the thread's resolution.
CREATE TABLE comment (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint      NOT NULL,
    number        bigint      NOT NULL,
    patchset      int         NOT NULL,           -- the patchset it was written on
    author        text        NOT NULL,
    parent_id     bigint      REFERENCES comment ON DELETE CASCADE,
    file_path     text,
    line          int,                            -- 1-based
    side          smallint,                       -- 1 base, 2 patchset
    body          text        NOT NULL,
    resolved      boolean     NOT NULL DEFAULT false,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (repository_id, number) REFERENCES changelist ON DELETE CASCADE,
    CHECK (line IS NULL OR (file_path IS NOT NULL AND line > 0 AND side IN (1, 2)))
);
CREATE INDEX comment_cl ON comment (repository_id, number, created_at);
CREATE INDEX comment_parent ON comment (parent_id);

-- Each CL's history: the Change Log, and the feed a notifier will read. An empty actor is
-- ok itself.
CREATE TABLE event (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint      NOT NULL,
    number        bigint      NOT NULL,
    at            timestamptz NOT NULL DEFAULT now(),
    actor         text        NOT NULL DEFAULT '',
    kind          smallint    NOT NULL,           -- see internal/control/review
    from_status   smallint    NOT NULL DEFAULT 0,
    to_status     smallint    NOT NULL DEFAULT 0,
    subject       text        NOT NULL DEFAULT '', -- the reviewer an assignment concerns
    label         smallint    NOT NULL DEFAULT 0,
    patchset      int         NOT NULL DEFAULT 0,
    commit_id     bytea,                          -- the commit a CL landed as
    message       text        NOT NULL DEFAULT '', -- an abandon reason
    related       bigint      NOT NULL DEFAULT 0,  -- the other CL it names: the rollback of a CL rolled back
    FOREIGN KEY (repository_id, number) REFERENCES changelist ON DELETE CASCADE
);
CREATE INDEX event_cl ON event (repository_id, number, id);

-- A person's favorite repositories, which the repository finder lists first.
CREATE TABLE favorite (
    principal     text        NOT NULL,
    repository_id bigint      NOT NULL,
    added_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (principal, repository_id)
);
