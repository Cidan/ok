-- The directory's log of repositories' entries (docs/hosted.md, "Tenancy, as built"): each
-- repository's changes, numbered by their place in its log, which ok's roles follow on the bus's
-- topic directory.repositories. A change and its event are written in one transaction; the relay
-- publishes what is pending.
CREATE TABLE directory_event (
    repository_id bigint      NOT NULL,
    seq           bigint      NOT NULL,
    at            timestamptz NOT NULL,
    body          bytea       NOT NULL,    -- an ok.identity.v1.DirectoryEvent
    PRIMARY KEY (repository_id, seq)
);

-- How far each repository's log reaches: the next event's place is one past it.
CREATE TABLE directory_event_head (
    repository_id bigint PRIMARY KEY,
    seq           bigint NOT NULL
);

-- Events not yet published.
CREATE TABLE directory_event_pending (
    repository_id bigint NOT NULL,
    seq           bigint NOT NULL,
    PRIMARY KEY (repository_id, seq)
);
