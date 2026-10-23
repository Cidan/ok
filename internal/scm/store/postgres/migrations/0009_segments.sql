-- Segments (docs/hosted.md, "Segments, as built"): what a repository holds pinned, moved out of
-- the tables below into immutable sorted files in the bucket, repos/<repository>/segments/<kind>/<id>.
-- The tail — the tables' rows — keeps what is unpinned, and what was pinned since compaction last
-- ran.
CREATE TABLE segment (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    kind          smallint    NOT NULL,              -- 1 objects, 2 tree sizes, 3 copies' reverse edges
    id            bytea       NOT NULL,              -- the BLAKE3 of its bytes
    level         smallint    NOT NULL,              -- 0 written from the tail; n+1 merged from n
    records       bigint      NOT NULL,
    size          bigint      NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    retired_at    timestamptz,                       -- merged into another; deleted a day later
    PRIMARY KEY (repository_id, kind, id)
);
CREATE INDEX segment_retired ON segment (retired_at) WHERE retired_at IS NOT NULL;

-- What compaction moves, in key order.
CREATE INDEX object_pinned ON object (repository_id, id) WHERE pinned;

-- A tree's sizes and a copy's edges outlive its row once it moves into a segment; a sweep deletes
-- them with what it sweeps.
ALTER TABLE tree_sizes DROP CONSTRAINT tree_sizes_repository_id_tree_id_fkey;
ALTER TABLE copy_parent DROP CONSTRAINT copy_parent_repository_id_copy_id_fkey;
