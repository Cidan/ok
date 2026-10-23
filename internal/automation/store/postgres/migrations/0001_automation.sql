-- ok automation's tables (docs/automation.md, "Execution"): every run of a job — what it runs,
-- why, its lease while a worker runs it, its outcome — the log of a run in progress, which moves to
-- the logs' bucket once the run finishes, the plan of each patchset's pre, the landing queue —
-- submissions, their attempts and post plans, and main's history as automation has seen it — and
-- where automation has read the SCM's change feed to.

-- Plans (docs/automation.md, "What runs"): what a patchset's pre runs, or post on one of
-- a submission's candidates. automation follows the SCM's change feed, and for each patchset
-- uploaded records a pre plan and queues its planning run; the landing queue does the same for
-- each candidate it prepares. The run's worker computes the affected set and reads the workflow
-- files; the jobs it selects are queued, each a run of the plan — pre's as the planning run
-- finishes, post's once the queue releases the plan's attempt.
CREATE TABLE plan (
    id               bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id    bigint      NOT NULL,
    cl_number        bigint      NOT NULL,
    patchset         int         NOT NULL,
    phase            smallint    NOT NULL,           -- 1 pre, 2 post
    commit_id        bytea       NOT NULL,           -- the patchset's commit, or post's candidate
    workflows_commit bytea       NOT NULL,           -- main's head, whose workflow files select jobs
    context          jsonb       NOT NULL,           -- the change (the job context but its files, which the worker reads)
    touched          jsonb       NOT NULL DEFAULT '[]', -- a post plan's footprint: every path its change touches
    planned          boolean     NOT NULL DEFAULT false, -- its planning run passed
    jobs             jsonb       NOT NULL DEFAULT '[]', -- the jobs it selected (post's wait for their attempt)
    skipped          jsonb       NOT NULL DEFAULT '[]', -- the workflow jobs it leaves to main
    affected         jsonb       NOT NULL DEFAULT '[]', -- the targets the change affects, by label
    upstream         jsonb       NOT NULL DEFAULT '[]', -- what they build on
    created_at       timestamptz NOT NULL DEFAULT now(),
    pre_commit       bytea                               -- post's: the commit the CL's pre ran on, which its jobs' inputs are compared with
);
-- A patchset has one pre plan.
CREATE UNIQUE INDEX plan_pre ON plan (repository_id, cl_number, patchset) WHERE phase = 1;

-- Where automation has read the SCM's change feed to: its last event's sequence.
CREATE TABLE feed_cursor (
    id  smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    seq bigint   NOT NULL
);

-- Runs: one job — a Bazel invocation — on one commit.
CREATE TABLE run (
    id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id   bigint      NOT NULL,
    commit_id       bytea       NOT NULL,
    kind            smallint    NOT NULL,           -- 1 build, 2 test, 3 run, 4 lint, 5 plan, 7 push
    targets         text[]      NOT NULL,
    args            text[]      NOT NULL,           -- as the workflow file writes them; the worker expands them
    phase           smallint    NOT NULL,           -- 1 pre, 2 post, 3 manual, 4 deploy
    cl_number       bigint,
    patchset        int,
    job             text        NOT NULL,           -- the job's identity, '' for none
    priority        smallint    NOT NULL DEFAULT 0, -- workers take higher first: post's and deploys' 1, the rest 0
    plan_id         bigint      REFERENCES plan,    -- the plan it runs for: its planning run, or a job it selected
    why             text        NOT NULL DEFAULT '', -- what selected a planned job
    requested_by    text        NOT NULL,           -- a principal
    rerun_of        bigint      REFERENCES run,
    status          smallint    NOT NULL,           -- 1 queued, 2 running, 3 passed, 4 failed, 5 canceled, 6 error
    attempt         int         NOT NULL DEFAULT 1,
    timeout_seconds int         NOT NULL,
    worker          text,                           -- the lease's holder while running
    lease_expires   timestamptz,
    stop_reason     text,                           -- set on a running run its worker must stop
    exit_code       int,
    reason          text        NOT NULL DEFAULT '',
    log_size        bigint      NOT NULL DEFAULT 0,
    log_stored      boolean     NOT NULL DEFAULT false, -- the log is the bucket's object runs/<id>
    queued_at       timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,
    finished_at     timestamptz,
    carried_from    bigint      REFERENCES run      -- a post run that never ran: the pre run whose pass it carries
);
-- What a push run that passed pushed (docs/deploy.md, "Push").
CREATE TABLE pushed_image (
    run_id     bigint NOT NULL REFERENCES run ON DELETE CASCADE,
    position   int    NOT NULL,
    target     text   NOT NULL,                    -- the oci_push
    registry   text   NOT NULL,
    repository text   NOT NULL,
    digest     text   NOT NULL,                    -- sha256:<hex>, of the manifest
    tags       text[] NOT NULL,
    PRIMARY KEY (run_id, position)
);

-- The queue: highest priority first, then oldest.
CREATE INDEX run_queued ON run (priority DESC, id) WHERE status = 1;
-- Leases, for the sweep of the lapsed.
CREATE INDEX run_leased ON run (lease_expires) WHERE status = 2;
-- A CL's runs and a commit's, newest first.
CREATE INDEX run_cl ON run (repository_id, cl_number, patchset, id);
CREATE INDEX run_commit ON run (repository_id, commit_id, id);
-- A plan's runs.
CREATE INDEX run_plan ON run (plan_id, id);
-- Finished runs whose logs have not moved to the bucket yet.
CREATE INDEX run_log_pending ON run (id) WHERE status >= 3 AND NOT log_stored;

-- A running run's log, as its worker appends it: each chunk at its byte offset. The chunks
-- become one object in the bucket when the run finishes, and are deleted.
CREATE TABLE log_chunk (
    run_id bigint NOT NULL REFERENCES run ON DELETE CASCADE,
    at     bigint NOT NULL,
    data   bytea  NOT NULL,
    PRIMARY KEY (run_id, at)
);

-- The landing queue (docs/automation.md, "The landing queue"): what ok control hands
-- automation to land — one CL, or a stack of them — on behalf of the person who submitted it.
-- Each submission is tested by attempts: its candidates, prepared on main's head and — when it
-- speculates — on the candidates of the earlier submissions it conflicts with, each with a
-- post plan a worker runs. The queue lands a submission once an attempt that still holds for
-- main has passed.
CREATE TABLE submission (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint      NOT NULL,
    submitted_by  text        NOT NULL,             -- the person ok control authorized
    status        smallint    NOT NULL,             -- 1 active, 2 landed, 3 failed, 4 canceled
    reason        text        NOT NULL DEFAULT '',  -- why it failed or was canceled
    note          text        NOT NULL DEFAULT '',  -- what the queue is waiting for, while active
    created_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz
);
-- Each repository's queue: its active submissions, in order.
CREATE INDEX submission_active ON submission (repository_id, id) WHERE status = 1;

-- A submission's CLs, bottom first.
CREATE TABLE submission_cl (
    submission_id bigint NOT NULL REFERENCES submission ON DELETE CASCADE,
    position      int    NOT NULL,
    repository_id bigint NOT NULL,
    cl_number     bigint NOT NULL,
    patchset      int    NOT NULL,
    landed_commit bytea,                            -- the commit it landed as
    PRIMARY KEY (submission_id, position)
);
CREATE INDEX submission_cl_cl ON submission_cl (repository_id, cl_number, submission_id);

-- An attempt: a submission's candidates on one arrangement of main — its head when the attempt
-- was prepared, and the earlier submissions it speculates land first (its chain) — with their
-- post plans.
CREATE TABLE attempt (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    submission_id bigint      NOT NULL REFERENCES submission ON DELETE CASCADE,
    base          bytea       NOT NULL,             -- main's head it was prepared on
    base_seq      bigint      NOT NULL,             -- that head's place in main's history
    chain         bigint[]    NOT NULL,             -- the earlier submissions it is tested on, in order
    status        smallint    NOT NULL,             -- 1 active, 2 discarded, 3 landed, 4 failed
    released      boolean     NOT NULL DEFAULT false, -- its plans' jobs are queued
    prepare_error text        NOT NULL DEFAULT '',  -- the SCM refused one of its candidates
    reason        text        NOT NULL DEFAULT '',  -- why it was discarded
    created_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz
);
CREATE INDEX attempt_submission ON attempt (submission_id, id);

-- An attempt's candidates, in landing order: its chain's CLs, prepared so its own build on them,
-- then its own CLs', each with its post plan.
CREATE TABLE attempt_candidate (
    attempt_id    bigint NOT NULL REFERENCES attempt ON DELETE CASCADE,
    position      int    NOT NULL,
    repository_id bigint NOT NULL,
    submission_id bigint NOT NULL,                  -- whose CL it is
    cl_number     bigint NOT NULL,
    patchset      int    NOT NULL,
    commit_id     bytea  NOT NULL,                  -- the candidate
    base          bytea  NOT NULL,                  -- what it was prepared on
    plan_id       bigint REFERENCES plan,           -- the attempt's own CLs': their post plans
    PRIMARY KEY (attempt_id, position)
);
-- A candidate already prepared on the same base is used again.
CREATE INDEX attempt_candidate_reuse ON attempt_candidate (repository_id, base, cl_number, patchset);

-- Main's history, as automation has seen it: each commit main moved to (the SCM's head_moved
-- events, and the queue's own landings), with the submission that landed it when the queue
-- did. The queue checks what landed since an attempt's base against it.
CREATE TABLE main_commit (
    repository_id bigint NOT NULL,
    seq           bigint NOT NULL,                  -- its place in main's history (head_seq)
    commit_id     bytea  NOT NULL,
    submission_id bigint REFERENCES submission,     -- the submission that landed it, when the queue did
    PRIMARY KEY (repository_id, seq)
);
CREATE INDEX main_commit_commit ON main_commit (repository_id, commit_id);

-- The CL each landed commit is (the SCM's landed events).
CREATE TABLE landed_change (
    repository_id bigint NOT NULL,
    commit_id     bytea  NOT NULL,
    cl_number     bigint NOT NULL,
    PRIMARY KEY (repository_id, commit_id)
);
CREATE INDEX landed_change_cl ON landed_change (repository_id, cl_number);
