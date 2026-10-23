-- ok's deployer's tables (docs/deploy.md): each repository's main as the SCM's change feed told
-- it, the plans made at its heads, and every deployment — what it deploys, where it stands, what it
-- pushed and migrated, its roll's resumable state, and what it did.

-- Where the deployer has read the SCM's change feed to: its last event's sequence.
CREATE TABLE feed_cursor (
    seq bigint NOT NULL
);

-- Each repository, main's head as the feed last said, and the head the latest plan covered.
CREATE TABLE repository (
    id           bigint PRIMARY KEY,              -- the SCM's
    name         text   NOT NULL,
    head         bytea,
    head_seq     bigint NOT NULL DEFAULT 0,
    planned_head bytea,                           -- NULL until it is planned
    planned_seq  bigint NOT NULL DEFAULT 0
);

-- main's history: each commit main moved to, by its place in it.
CREATE TABLE main_commit (
    repository_id bigint NOT NULL REFERENCES repository ON DELETE CASCADE,
    seq           bigint NOT NULL,
    commit_id     bytea  NOT NULL,
    PRIMARY KEY (repository_id, seq)
);
CREATE INDEX main_commit_id ON main_commit (repository_id, commit_id);

-- A plan: the deployments due at one head of main, for the commits since the previous plan's.
CREATE TABLE plan (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    head          bytea       NOT NULL,
    head_seq      bigint      NOT NULL,
    base_seq      bigint      NOT NULL,           -- the previous plan's head_seq; 0 for the first
    notes         text[]      NOT NULL DEFAULT '{}', -- what planning found wrong: a workflow file that does not parse
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- A service: a deploy that rolls a workload, as main's latest plan of its repository declared
-- it — or, no longer declared, as it last was.
CREATE TABLE service (
    id            bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint  NOT NULL REFERENCES repository ON DELETE CASCADE,
    deploy        text    NOT NULL,               -- its deploy's identity, //<file>:deploy:<name>
    name          text    NOT NULL,
    namespace     text    NOT NULL,
    kind          text    NOT NULL,               -- deployment or statefulset
    workload      text    NOT NULL,
    declared      boolean NOT NULL,               -- main's latest plan declares it
    declared_seq  bigint  NOT NULL,               -- the head of main that last declared it
    group_name    text    NOT NULL DEFAULT '',    -- its deploy's group, on the Services pages; '': a group of its own
    UNIQUE (repository_id, deploy)
);

-- What the cluster runs of each service, as the deployer's observer last read it: it reads every
-- service's workload ten seconds after its last round ended, and the Services pages read this,
-- never the cluster.
CREATE TABLE service_observation (
    service_id  bigint      PRIMARY KEY REFERENCES service ON DELETE CASCADE,
    error       text        NOT NULL DEFAULT '', -- the cluster could not be read: the rest is empty
    found       boolean     NOT NULL,            -- the workload exists
    live        text        NOT NULL,            -- a Deployment's live copy, blue or green; '' for a StatefulSet
    replicas    int         NOT NULL,            -- the live copy's wanted replicas
    ready       int         NOT NULL,            -- its pods Ready and running its template's images
    images      jsonb       NOT NULL,            -- the live copy's template: container → image
    pods        jsonb       NOT NULL             -- every pod: its copy, readiness, restarts, and images
);

-- A deployment: one deploy, at one commit of main.
CREATE TABLE deployment (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    plan_id       bigint      REFERENCES plan,    -- NULL for a retry
    retry_of      bigint      REFERENCES deployment,
    deploy        text        NOT NULL,           -- the deploy's identity, //<file>:deploy:<name>
    name          text        NOT NULL,
    commit_id     bytea       NOT NULL,
    from_seq      bigint      NOT NULL,           -- it covers main's commits after from_seq,
    to_seq        bigint      NOT NULL,           -- up to to_seq
    why           text        NOT NULL,
    wave          int         NOT NULL,
    -- The places of earlier plans it took, replacing their waiting deployments of its deploy:
    -- it starts only once each of those plans' waves before gate_waves[i] succeeded, and a
    -- failure of one of them stops it too.
    gate_plans    bigint[]    NOT NULL DEFAULT '{}',
    gate_waves    int[]       NOT NULL DEFAULT '{}',
    requested_by  text        NOT NULL,           -- the deployer, or the person who retried it
    namespace     text,                           -- the workload; NULL for a deploy that only migrates
    kind          text,
    workload      text,
    status        smallint    NOT NULL,           -- 1 waiting, 2 running, 3 succeeded, 4 failed, 5 halted, 6 canceled
    stage         smallint    NOT NULL,           -- 1 queued, 2 push, 3 migrate, 4 hazard, 5 roll, 6 done
    reason        text        NOT NULL DEFAULT '',
    roll          jsonb,                          -- the roll's state, as it resumes
    self          boolean     NOT NULL DEFAULT false, -- it rolls the deployer itself
    driver        text,                           -- a self deployment's: the copy of the deployer driving it
    halt_by       text,                           -- who asked to halt it
    created_at    timestamptz NOT NULL DEFAULT now(),
    started_at    timestamptz,
    stage_at      timestamptz,
    finished_at   timestamptz,
    alone         boolean     NOT NULL DEFAULT false  -- it runs while no other deployment does
);
CREATE INDEX deployment_open ON deployment (repository_id, id) WHERE status IN (1, 2);
CREATE INDEX deployment_range ON deployment (repository_id, to_seq, from_seq);
CREATE INDEX deployment_deploy ON deployment (repository_id, deploy, id);

-- Each container a deployment rolls: the oci_push that makes its image, the push run, and what
-- it pushed.
CREATE TABLE deployment_image (
    deployment_id bigint NOT NULL REFERENCES deployment ON DELETE CASCADE,
    container     text   NOT NULL,
    target        text   NOT NULL,
    run_id        bigint NOT NULL DEFAULT 0,      -- 0 until its push starts
    reference     text   NOT NULL DEFAULT '',     -- registry/repository@sha256:… once pushed
    PRIMARY KEY (deployment_id, container)
);

-- Each migration a deployment applies, in order: its outcome, the plan Bazel reported, the
-- gated hazard classes it refused, and those a person allowed.
CREATE TABLE deployment_migration (
    deployment_id bigint   NOT NULL REFERENCES deployment ON DELETE CASCADE,
    position      int      NOT NULL,
    target        text     NOT NULL,
    status        smallint NOT NULL DEFAULT 1,    -- 1 pending, 2 applied, 3 waiting for an allowance, 4 failed
    output        text     NOT NULL DEFAULT '',   -- what Bazel printed
    report        jsonb,                          -- Bazel's report of the plan
    refused       text[]   NOT NULL DEFAULT '{}',
    allowed       text[]   NOT NULL DEFAULT '{}',
    allowed_by    text,
    PRIMARY KEY (deployment_id, position)
);

-- What a deployment did, in order: its log.
CREATE TABLE deployment_event (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    deployment_id bigint      NOT NULL REFERENCES deployment ON DELETE CASCADE,
    at            timestamptz NOT NULL DEFAULT now(),
    message       text        NOT NULL
);
CREATE INDEX deployment_event_of ON deployment_event (deployment_id, id);
