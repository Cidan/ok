-- ok's deployer's tables on Spanner (docs/deploy.md; docs/hosted.md, "Spanner"), as postgres's
-- migrations leave them on PostgreSQL. What is reached through its repository — main's history, the
-- plans — is interleaved in it; deployments and services, reached by their random IDs, are tables of
-- their own, what each holds interleaved in it. Deleting a repository deletes its deployments and
-- services with it (DropRepository). An interleaved table's key begins with its parent's, by name:
-- each table's own key column is named for it (repository_id, deployment_id, service_id).

-- Each repository, main's head as the SCM's events last said, and the head the latest plan covered.
CREATE TABLE repository (
    repository_id INT64      NOT NULL,          -- the SCM's
    name         STRING(MAX) NOT NULL,
    head         BYTES(MAX),
    head_seq     INT64       NOT NULL DEFAULT (0),
    planned_head BYTES(MAX),                    -- NULL until it is planned
    planned_seq  INT64       NOT NULL DEFAULT (0),
) PRIMARY KEY (repository_id);

-- main's history: each commit main moved to, by its place in it.
CREATE TABLE main_commit (
    repository_id INT64      NOT NULL,
    seq           INT64      NOT NULL,
    commit_id     BYTES(MAX) NOT NULL,
) PRIMARY KEY (repository_id, seq), INTERLEAVE IN PARENT repository ON DELETE CASCADE;
CREATE INDEX main_commit_id ON main_commit (repository_id, commit_id), INTERLEAVE IN repository;

-- A plan: the deployments due at one head of main, for the commits since the previous plan's.
CREATE TABLE plan (
    repository_id INT64              NOT NULL,
    id            INT64              NOT NULL,
    head          BYTES(MAX)         NOT NULL,
    head_seq      INT64              NOT NULL,
    base_seq      INT64              NOT NULL,  -- the previous plan's head_seq; 0 for the first
    notes         ARRAY<STRING(MAX)> NOT NULL,  -- what planning found wrong: a workflow file that does not parse
    created_at    TIMESTAMP          NOT NULL,
) PRIMARY KEY (repository_id, id), INTERLEAVE IN PARENT repository ON DELETE CASCADE;

-- A service: a deploy that rolls a workload, as main's latest plan of its repository declared it —
-- or, no longer declared, as it last was.
CREATE TABLE service (
    service_id    INT64       NOT NULL,
    repository_id INT64       NOT NULL,
    deploy        STRING(MAX) NOT NULL,         -- its deploy's identity, //<file>:deploy:<name>
    name          STRING(MAX) NOT NULL,
    namespace     STRING(MAX) NOT NULL,
    kind          STRING(MAX) NOT NULL,         -- deployment or statefulset
    workload      STRING(MAX) NOT NULL,
    declared      BOOL        NOT NULL,         -- main's latest plan declares it
    declared_seq  INT64       NOT NULL,         -- the head of main that last declared it
    group_name    STRING(MAX) NOT NULL,         -- its deploy's group, on the Services pages; '': a group of its own
) PRIMARY KEY (service_id);
CREATE UNIQUE INDEX service_deploy ON service (repository_id, deploy);

-- What the cluster runs of each service, as the deployer's observer last read it.
CREATE TABLE service_observation (
    service_id INT64       NOT NULL,
    error      STRING(MAX) NOT NULL,            -- the cluster could not be read: the rest is empty
    found      BOOL        NOT NULL,            -- the workload exists
    live       STRING(MAX) NOT NULL,            -- a Deployment's live copy, blue or green; '' for a StatefulSet
    replicas   INT64       NOT NULL,            -- the live copy's wanted replicas
    ready      INT64       NOT NULL,            -- its pods Ready and running its template's images
    images     JSON        NOT NULL,            -- the live copy's template: container → image
    pods       JSON        NOT NULL,            -- every pod: its copy, readiness, restarts, and images
) PRIMARY KEY (service_id), INTERLEAVE IN PARENT service ON DELETE CASCADE;

-- A deployment: one deploy, at one commit of main.
CREATE TABLE deployment (
    deployment_id INT64        NOT NULL,
    repository_id INT64        NOT NULL,
    plan_id       INT64,                        -- NULL for a retry
    retry_of      INT64,
    deploy        STRING(MAX)  NOT NULL,        -- the deploy's identity, //<file>:deploy:<name>
    name          STRING(MAX)  NOT NULL,
    commit_id     BYTES(MAX)   NOT NULL,
    from_seq      INT64        NOT NULL,        -- it covers main's commits after from_seq,
    to_seq        INT64        NOT NULL,        -- up to to_seq
    why           STRING(MAX)  NOT NULL,
    wave          INT64        NOT NULL,
    -- The places of earlier plans it took, replacing their waiting deployments of its deploy: it
    -- starts only once each of those plans' waves before gate_waves[i] succeeded, and a failure of
    -- one of them stops it too.
    gate_plans    ARRAY<INT64> NOT NULL,
    gate_waves    ARRAY<INT64> NOT NULL,
    requested_by  STRING(MAX)  NOT NULL,        -- the deployer, or the person who retried it
    namespace     STRING(MAX),                  -- the workload; NULL for a deploy that only migrates
    kind          STRING(MAX),
    workload      STRING(MAX),
    status        INT64        NOT NULL,        -- 1 waiting, 2 running, 3 succeeded, 4 failed, 5 halted, 6 canceled
    stage         INT64        NOT NULL,        -- 1 queued, 2 push, 3 migrate, 4 hazard, 5 roll, 6 done
    reason        STRING(MAX)  NOT NULL,
    roll          JSON,                         -- the roll's state, as it resumes
    self          BOOL         NOT NULL,        -- it rolls the deployer itself
    driver        STRING(MAX),                  -- a self deployment's: the copy of the deployer driving it
    halt_by       STRING(MAX),                  -- who asked to halt it, by stable ID
    halt_reason   STRING(MAX),                  -- why ok asked to halt it
    created_at    TIMESTAMP    NOT NULL,
    started_at    TIMESTAMP,
    stage_at      TIMESTAMP,
    finished_at   TIMESTAMP,
    alone         BOOL         NOT NULL,        -- it runs while no other deployment does
) PRIMARY KEY (deployment_id);
CREATE INDEX deployment_open ON deployment (status, created_at);
CREATE INDEX deployment_range ON deployment (repository_id, to_seq, from_seq);
CREATE INDEX deployment_deploy ON deployment (repository_id, deploy, created_at);
CREATE INDEX deployment_recent ON deployment (repository_id, created_at);
CREATE INDEX deployment_plan ON deployment (plan_id);

-- Each container a deployment rolls: the oci_push that makes its image, the push run, and what it
-- pushed.
CREATE TABLE deployment_image (
    deployment_id INT64       NOT NULL,
    container     STRING(MAX) NOT NULL,
    target        STRING(MAX) NOT NULL,
    run_id        INT64       NOT NULL,         -- 0 until its push starts
    reference     STRING(MAX) NOT NULL,         -- registry/repository@sha256:… once pushed
) PRIMARY KEY (deployment_id, container), INTERLEAVE IN PARENT deployment ON DELETE CASCADE;

-- Each migration a deployment applies, in order: its outcome, the plan Bazel reported, the gated
-- hazard classes it refused, and those a person allowed.
CREATE TABLE deployment_migration (
    deployment_id INT64              NOT NULL,
    position      INT64              NOT NULL,
    target        STRING(MAX)        NOT NULL,
    status        INT64              NOT NULL,  -- 1 pending, 2 applied, 3 waiting for an allowance, 4 failed
    output        STRING(MAX)        NOT NULL,  -- what Bazel printed
    report        JSON,                         -- Bazel's report of the plan
    refused       ARRAY<STRING(MAX)> NOT NULL,
    allowed       ARRAY<STRING(MAX)> NOT NULL,
    allowed_by    STRING(MAX),
) PRIMARY KEY (deployment_id, position), INTERLEAVE IN PARENT deployment ON DELETE CASCADE;

-- What a deployment did, in order: its log. (at is a reserved word in GoogleSQL.)
CREATE TABLE deployment_event (
    deployment_id INT64       NOT NULL,
    id            INT64       NOT NULL,
    event_at      TIMESTAMP   NOT NULL,
    message       STRING(MAX) NOT NULL,
) PRIMARY KEY (deployment_id, id), INTERLEAVE IN PARENT deployment ON DELETE CASCADE;

-- The deployer's place in each repository's log on the bus's stream (spannerdata.CursorsDDL).
CREATE TABLE follow_cursor (
    topic STRING(MAX) NOT NULL,
    key   INT64       NOT NULL,
    seq   INT64       NOT NULL,
) PRIMARY KEY (topic, key);

-- The deployer's keyed work: each repository's planning, each service's observation
-- (spannerdata.WorkDDL).
CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key);
