-- A repository's move gate (docs/hosted.md, "Cells, as built"): every write transaction about a
-- repository reads it first, which locks its key; a move's freeze writes it, so it waits for the
-- writes under way and none starts after; and a repository moved away is refused, naming its cell,
-- until its rows here are deleted.
CREATE TABLE move_gate (
    repository_id INT64       NOT NULL,
    move_state    STRING(MAX) NOT NULL,
    moved_to      STRING(MAX) NOT NULL,
    moved_at      TIMESTAMP,
) PRIMARY KEY (repository_id);
