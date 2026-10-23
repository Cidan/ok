-- Landings whose changed paths are more than one transaction indexes (docs/hosted.md, "The SCM on
-- Spanner, as built"): main's path history of each is written after main moves, a bounded
-- transaction at a time, and read from its trees until it is.
CREATE TABLE path_index (
    repository_id INT64      NOT NULL,
    generation    INT64      NOT NULL,           -- its place on main
    commit_id     BYTES(MAX) NOT NULL,
    parent_id     BYTES(MAX) NOT NULL,
) PRIMARY KEY (repository_id, generation), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
