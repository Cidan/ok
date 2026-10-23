-- The deployer's keyed work (internal/work; pgdata.WorkSchema), in place of its leader: each
-- repository's planning, leased to one replica at a time as its main moves, and each service's
-- observation, due ten seconds after the last.
CREATE TABLE work (
    kind        text        NOT NULL,
    key         bigint      NOT NULL,
    dirty       boolean     NOT NULL DEFAULT false,
    due_at      timestamptz,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (kind, key)
);
CREATE INDEX work_due ON work (kind, key) WHERE dirty OR holder IS NOT NULL;
