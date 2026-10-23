-- The SCM's keyed work (internal/work; pgdata.WorkSchema): each repository's garbage collection,
-- leased to one replica at a time, in place of one replica collecting every repository.
CREATE TABLE work (
    kind        text        NOT NULL,
    key         bigint      NOT NULL,
    dirty       boolean     NOT NULL DEFAULT false,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (kind, key)
);
CREATE INDEX work_due ON work (kind, key) WHERE dirty OR holder IS NOT NULL;
