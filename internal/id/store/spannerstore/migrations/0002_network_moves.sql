-- A fork network's move between cells (docs/hosted.md, "Cells, as built"), the latest of each: the
-- directory runs it phase by phase, on whichever replica takes its lock, each step safe to repeat;
-- next_at is when it is next tried, after a step failed.
CREATE TABLE network_moves (
    network_id  INT64       NOT NULL,
    source      STRING(MAX) NOT NULL,
    destination STRING(MAX) NOT NULL,
    phase       STRING(MAX) NOT NULL,
    asked_by    STRING(MAX) NOT NULL,
    asked_at    TIMESTAMP   NOT NULL,
    frozen_at   TIMESTAMP,
    updated_at  TIMESTAMP   NOT NULL,
    next_at     TIMESTAMP   NOT NULL,
    error       STRING(MAX) NOT NULL,
) PRIMARY KEY (network_id);
CREATE INDEX network_moves_due ON network_moves (next_at);
