-- A fork network's move between cells (docs/hosted.md, "Cells, as built"), the latest of each: the
-- directory runs it phase by phase, on whichever replica takes its lock, each step safe to repeat;
-- next_at is when it is next tried, after a step failed.
CREATE TABLE network_moves (
    network_id  bigint      PRIMARY KEY,
    source      text        NOT NULL,
    destination text        NOT NULL,
    phase       text        NOT NULL CHECK (phase IN ('copying', 'frozen', 'imported', 'named', 'done', 'abandoning', 'abandoned')),
    asked_by    text        NOT NULL REFERENCES principals (id),
    asked_at    timestamptz NOT NULL,
    frozen_at   timestamptz,
    updated_at  timestamptz NOT NULL,
    next_at     timestamptz NOT NULL,
    error       text        NOT NULL DEFAULT ''
);
CREATE INDEX network_moves_due ON network_moves (next_at) WHERE phase NOT IN ('done', 'abandoned');
