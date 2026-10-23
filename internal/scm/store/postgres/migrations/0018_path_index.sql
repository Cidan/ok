-- Landings whose changed paths are more than one transaction indexes (docs/hosted.md, "The SCM on
-- Spanner, as built"): main's path history of each is written after main moves, a bounded
-- transaction at a time, and read from its trees until it is.
CREATE TABLE path_index (
    repository_id bigint NOT NULL REFERENCES repository ON DELETE CASCADE,
    generation    bigint NOT NULL,                   -- its place on main
    commit_id     bytea  NOT NULL,
    parent_id     bytea  NOT NULL,
    PRIMARY KEY (repository_id, generation)
);
