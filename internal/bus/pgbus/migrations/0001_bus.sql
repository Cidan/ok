-- The PostgreSQL bus (internal/bus/pgbus): a stream of events and queues of jobs, for an
-- installation that runs on PostgreSQL alone.

-- Every event's number, taken under this row's lock until the publishing transaction commits, so
-- the order of numbers is the order of commits and a reader that has seen n has seen every event
-- before it. One counter suits the installations this backend serves (docs/hosted.md, "Backends").
CREATE TABLE stream_counter (
    id   boolean PRIMARY KEY DEFAULT true CHECK (id),
    last bigint  NOT NULL
);
INSERT INTO stream_counter (last) VALUES (0);

-- Events, by topic and partition (a key's partition is its remainder by 16).
CREATE TABLE stream_event (
    topic text        NOT NULL,
    part  smallint    NOT NULL,
    id    bigint      NOT NULL,
    key   bigint      NOT NULL,
    seq   bigint      NOT NULL,
    body  bytea       NOT NULL,
    at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, part, id)
);
CREATE INDEX stream_event_at ON stream_event (topic, at);

-- A topic's groups.
CREATE TABLE stream_group (
    topic      text        NOT NULL,
    name       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, name)
);

-- Each group's partitions: the last event delivered in each, and the member delivering it.
CREATE TABLE stream_part (
    topic       text        NOT NULL,
    name        text        NOT NULL,
    part        smallint    NOT NULL,
    position    bigint      NOT NULL,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (topic, name, part),
    FOREIGN KEY (topic, name) REFERENCES stream_group ON DELETE CASCADE
);

-- A group's live members, so each takes its share of the partitions.
CREATE TABLE stream_member (
    topic   text        NOT NULL,
    name    text        NOT NULL,
    member  text        NOT NULL,
    seen_at timestamptz NOT NULL,
    PRIMARY KEY (topic, name, member)
);

-- Events a member's handler failed, delivered again after a pause that grows with each failure.
CREATE TABLE stream_retry (
    topic      text        NOT NULL,
    name       text        NOT NULL,
    id         bigint      NOT NULL,
    part       smallint    NOT NULL,
    key        bigint      NOT NULL,
    seq        bigint      NOT NULL,
    body       bytea       NOT NULL,
    attempts   int         NOT NULL,
    not_before timestamptz NOT NULL,
    PRIMARY KEY (topic, name, id)
);
CREATE INDEX stream_retry_due ON stream_retry (topic, name, part, not_before);

-- Jobs, by queue. A job is ready when not_before has passed: a lease moves not_before to the
-- lease's end and sets its token, so an expired lease makes the job ready again, and only the
-- token's holder may acknowledge, extend, or return it.
CREATE TABLE queue_job (
    queue      text        NOT NULL,
    id         text        NOT NULL,
    key        bigint      NOT NULL,
    body       bytea       NOT NULL,
    not_before timestamptz NOT NULL,
    attempts   int         NOT NULL DEFAULT 0,
    token      bigint,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (queue, id)
);
CREATE INDEX queue_job_ready ON queue_job (queue, not_before);

-- Jobs past their queue's attempts.
CREATE TABLE queue_dead (
    queue    text        NOT NULL,
    id       text        NOT NULL,
    key      bigint      NOT NULL,
    body     bytea       NOT NULL,
    attempts int         NOT NULL,
    died_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX queue_dead_queue ON queue_dead (queue, died_at);
