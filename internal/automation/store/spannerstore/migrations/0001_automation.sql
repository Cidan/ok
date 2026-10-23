-- ok automation's tables on Spanner (docs/hosted.md, "Spanner"), as postgres's migrations leave
-- them on PostgreSQL: plans, runs, the landing queue's submissions and attempts, and main's history
-- as automation has seen it. Plans, runs, submissions, and attempts are reached by their random IDs:
-- tables of their own, what each holds interleaved in it. An interleaved table's key begins with
-- its parent's, by name. A plan's JSON is kept as the text the store wrote: nothing reads into it.

-- Plans: what a planning run decides for a CL's patchset (pre) or a candidate (post).
CREATE TABLE plan (
    plan_id          INT64       NOT NULL,
    repository_id    INT64       NOT NULL,
    cl_number        INT64       NOT NULL,
    patchset         INT64       NOT NULL,
    phase            INT64       NOT NULL,      -- 1 pre, 2 post
    commit_id        BYTES(MAX)  NOT NULL,      -- the patchset's commit, or post's candidate
    workflows_commit BYTES(MAX)  NOT NULL,      -- main's head, whose workflow files select jobs
    context          STRING(MAX) NOT NULL,      -- the change, JSON
    touched          STRING(MAX) NOT NULL,      -- a post plan's footprint, JSON
    planned          BOOL        NOT NULL,      -- its planning run passed
    jobs             STRING(MAX) NOT NULL,      -- the jobs it selected, JSON
    skipped          STRING(MAX) NOT NULL,      -- the workflow jobs it leaves to main, JSON
    affected         STRING(MAX) NOT NULL,      -- the targets the change affects, JSON
    upstream         STRING(MAX) NOT NULL,      -- what they build on, JSON
    created_at       TIMESTAMP   NOT NULL,
    pre_commit       BYTES(MAX),                -- post's: the commit the CL's pre ran on
    -- A patchset has one pre plan: its patchset here, NULL for a post plan.
    pre_patchset     INT64 AS (IF(phase = 1, patchset, NULL)) STORED,
) PRIMARY KEY (plan_id);
CREATE UNIQUE NULL_FILTERED INDEX plan_pre ON plan (repository_id, cl_number, pre_patchset);
CREATE INDEX plan_cl ON plan (repository_id, cl_number, phase, patchset);
CREATE INDEX plan_repository ON plan (repository_id);

-- Runs: one job — a Bazel invocation — on one commit.
CREATE TABLE run (
    run_id          INT64              NOT NULL,
    repository_id   INT64              NOT NULL,
    commit_id       BYTES(MAX)         NOT NULL,
    kind            INT64              NOT NULL, -- 1 build, 2 test, 3 run, 4 lint, 5 plan, 7 push
    targets         ARRAY<STRING(MAX)> NOT NULL,
    args            ARRAY<STRING(MAX)> NOT NULL, -- as the workflow file writes them; the worker expands them
    phase           INT64              NOT NULL, -- 1 pre, 2 post, 3 manual, 4 deploy
    cl_number       INT64,
    patchset        INT64,
    job             STRING(MAX)        NOT NULL, -- the job's identity, '' for none
    priority        INT64              NOT NULL, -- workers take higher first: post's and deploys' 1, the rest 0
    plan_id         INT64,                       -- the plan it runs for
    why             STRING(MAX)        NOT NULL, -- what selected a planned job
    requested_by    STRING(MAX)        NOT NULL, -- a principal
    rerun_of        INT64,
    status          INT64              NOT NULL, -- 1 queued, 2 running, 3 passed, 4 failed, 5 canceled, 6 error
    attempt         INT64              NOT NULL,
    timeout_seconds INT64              NOT NULL,
    worker          STRING(MAX),                 -- the lease's holder while running
    lease_expires   TIMESTAMP,
    stop_reason     STRING(MAX),                 -- set on a running run its worker must stop
    exit_code       INT64,
    reason          STRING(MAX)        NOT NULL,
    log_size        INT64              NOT NULL,
    log_stored      BOOL               NOT NULL, -- the log is the bucket's object runs/<id>
    queued_at       TIMESTAMP          NOT NULL,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    carried_from    INT64,                       -- a post run that never ran: the pre run whose pass it carries
    canceled_by     STRING(MAX),                 -- who canceled it, by stable ID
) PRIMARY KEY (run_id);
-- The queue: highest priority first, then oldest.
CREATE INDEX run_queued ON run (status, priority DESC, queued_at);
-- Leases, for the sweep of the lapsed.
CREATE INDEX run_leased ON run (status, lease_expires);
-- A CL's runs and a commit's, newest first; a plan's; a repository's.
CREATE INDEX run_cl ON run (repository_id, cl_number, patchset, queued_at);
CREATE INDEX run_commit ON run (repository_id, commit_id, queued_at);
CREATE INDEX run_plan ON run (plan_id, queued_at);
CREATE INDEX run_repository ON run (repository_id);
-- Finished runs whose logs have not moved to the bucket yet.
CREATE INDEX run_log_pending ON run (log_stored, status);

-- What a push run that passed pushed (docs/deploy.md, "Push").
CREATE TABLE pushed_image (
    run_id     INT64              NOT NULL,
    position   INT64              NOT NULL,
    target     STRING(MAX)        NOT NULL,    -- the oci_push
    registry   STRING(MAX)        NOT NULL,
    repository STRING(MAX)        NOT NULL,
    digest     STRING(MAX)        NOT NULL,    -- sha256:<hex>, of the manifest
    tags       ARRAY<STRING(MAX)> NOT NULL,
) PRIMARY KEY (run_id, position), INTERLEAVE IN PARENT run ON DELETE CASCADE;

-- The landing queue (docs/automation.md, "The landing queue"): a submission of one CL or a stack,
-- tested by attempts.
CREATE TABLE submission (
    submission_id INT64       NOT NULL,
    repository_id INT64       NOT NULL,
    submitted_by  STRING(MAX) NOT NULL,         -- the person ok control authorized
    status        INT64       NOT NULL,         -- 1 active, 2 landed, 3 failed, 4 canceled
    reason        STRING(MAX) NOT NULL,         -- why it failed or was canceled
    note          STRING(MAX) NOT NULL,         -- what the queue is waiting for, while active
    created_at    TIMESTAMP   NOT NULL,
    finished_at   TIMESTAMP,
    canceled_by   STRING(MAX),                  -- who canceled it, by stable ID
) PRIMARY KEY (submission_id);
CREATE INDEX submission_active ON submission (repository_id, status, created_at);

-- A submission's CLs, bottom first.
CREATE TABLE submission_cl (
    submission_id INT64      NOT NULL,
    position      INT64      NOT NULL,
    repository_id INT64      NOT NULL,
    cl_number     INT64      NOT NULL,
    patchset      INT64      NOT NULL,
    landed_commit BYTES(MAX),                   -- the commit it landed as
) PRIMARY KEY (submission_id, position), INTERLEAVE IN PARENT submission ON DELETE CASCADE;
CREATE INDEX submission_cl_cl ON submission_cl (repository_id, cl_number);

-- An attempt: a submission's candidates on one arrangement of main.
CREATE TABLE attempt (
    attempt_id    INT64        NOT NULL,
    submission_id INT64        NOT NULL,
    repository_id INT64        NOT NULL,        -- its submission's: what purging deletes it by
    base          BYTES(MAX)   NOT NULL,        -- main's head it was prepared on
    base_seq      INT64        NOT NULL,        -- that head's place in main's history
    chain         ARRAY<INT64> NOT NULL,        -- the earlier submissions it is tested on, in order
    status        INT64        NOT NULL,        -- 1 active, 2 discarded, 3 landed, 4 failed
    released      BOOL         NOT NULL,        -- its plans' jobs are queued
    prepare_error STRING(MAX)  NOT NULL,        -- the SCM refused one of its candidates
    reason        STRING(MAX)  NOT NULL,        -- why it was discarded
    created_at    TIMESTAMP    NOT NULL,
    finished_at   TIMESTAMP,
) PRIMARY KEY (attempt_id);
CREATE INDEX attempt_submission ON attempt (submission_id, status, created_at);
CREATE INDEX attempt_repository ON attempt (repository_id);

-- An attempt's candidates, in landing order.
CREATE TABLE attempt_candidate (
    attempt_id    INT64      NOT NULL,
    position      INT64      NOT NULL,
    repository_id INT64      NOT NULL,
    submission_id INT64      NOT NULL,          -- whose CL it is
    cl_number     INT64      NOT NULL,
    patchset      INT64      NOT NULL,
    commit_id     BYTES(MAX) NOT NULL,          -- the candidate
    base          BYTES(MAX) NOT NULL,          -- what it was prepared on
    plan_id       INT64,                        -- the attempt's own CLs': their post plans
) PRIMARY KEY (attempt_id, position), INTERLEAVE IN PARENT attempt ON DELETE CASCADE;
CREATE INDEX attempt_candidate_reuse ON attempt_candidate (repository_id, base, cl_number, patchset);
CREATE INDEX attempt_candidate_repository ON attempt_candidate (repository_id);

-- Main's history, as automation has seen it, with the submission that landed each commit when the
-- queue did.
CREATE TABLE main_commit (
    repository_id INT64      NOT NULL,
    seq           INT64      NOT NULL,          -- its place in main's history (head_seq)
    commit_id     BYTES(MAX) NOT NULL,
    submission_id INT64,                        -- the submission that landed it, when the queue did
) PRIMARY KEY (repository_id, seq);
CREATE INDEX main_commit_commit ON main_commit (repository_id, commit_id);

-- The CL each landed commit is (the SCM's landed events).
CREATE TABLE landed_change (
    repository_id INT64      NOT NULL,
    commit_id     BYTES(MAX) NOT NULL,
    cl_number     INT64      NOT NULL,
) PRIMARY KEY (repository_id, commit_id);
CREATE INDEX landed_change_cl ON landed_change (repository_id, cl_number);

-- ok automation's place in each repository's log on the bus's stream (spannerdata.CursorsDDL).
CREATE TABLE follow_cursor (
    topic STRING(MAX) NOT NULL,
    key   INT64       NOT NULL,
    seq   INT64       NOT NULL,
) PRIMARY KEY (topic, key);

-- ok automation's keyed work: each repository's landing queue (spannerdata.WorkDDL).
CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key);
