-- A repository's move gate (docs/hosted.md, "Cells, as built"): every write transaction about a
-- repository reads it first, in share mode; a move's freeze writes it, so it waits for the writes
-- under way and none starts after; and a repository moved away is refused, naming its cell, until
-- its rows here are deleted.
CREATE TABLE move_gate (
    repository_id bigint PRIMARY KEY,
    move_state    text NOT NULL DEFAULT '' CHECK (move_state IN ('', 'frozen', 'moved')),
    moved_to      text NOT NULL DEFAULT '',
    moved_at      timestamptz
);
