-- Fork networks (docs/hosted.md, "Fork networks, as built"). A network holds what its repositories
-- share — packs, dictionaries, and where each chunk is — and a repository what it holds: its
-- content index of files and chunks, and everything else it had. Every repository is in one
-- network; one with no forks is a network of one. ok carries no installation's data across this
-- (it has none), so a repository's content index here keeps its chunk locations until its first
-- fold, and nothing reads them.
CREATE TABLE network (
    id         bigint      PRIMARY KEY,                -- random, as a repository's
    created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO network (id) SELECT id FROM repository;
ALTER TABLE repository ADD COLUMN network_id bigint REFERENCES network;
UPDATE repository SET network_id = id;
ALTER TABLE repository ALTER COLUMN network_id SET NOT NULL;
CREATE INDEX repository_network ON repository (network_id);

ALTER TABLE pack DROP CONSTRAINT pack_repository_id_fkey;
ALTER TABLE pack RENAME COLUMN repository_id TO network_id;
ALTER TABLE pack ADD FOREIGN KEY (network_id) REFERENCES network ON DELETE CASCADE;

ALTER TABLE dictionary DROP CONSTRAINT dictionary_repository_id_fkey;
ALTER TABLE dictionary RENAME COLUMN repository_id TO network_id;
ALTER TABLE dictionary ADD FOREIGN KEY (network_id) REFERENCES network ON DELETE CASCADE;

-- A content index, and a segment, is a repository's (scope 0) or a network's (scope 1): a
-- network's content index holds where its chunks are, and its segments that index's folds.
ALTER TABLE content_index DROP CONSTRAINT content_index_repository_id_fkey;
ALTER TABLE content_index DROP CONSTRAINT content_index_pkey;
ALTER TABLE content_index RENAME COLUMN repository_id TO owner_id;
ALTER TABLE content_index ADD COLUMN scope smallint NOT NULL DEFAULT 0;
ALTER TABLE content_index ADD PRIMARY KEY (scope, owner_id);

ALTER TABLE index_block DROP CONSTRAINT index_block_repository_id_fkey;
ALTER TABLE index_block DROP CONSTRAINT index_block_pkey;
ALTER TABLE index_block RENAME COLUMN repository_id TO owner_id;
ALTER TABLE index_block ADD COLUMN scope smallint NOT NULL DEFAULT 0;
ALTER TABLE index_block ADD PRIMARY KEY (scope, owner_id, generation, first_id);

ALTER TABLE content_log DROP CONSTRAINT content_log_repository_id_fkey;
ALTER TABLE content_log DROP CONSTRAINT content_log_pkey;
ALTER TABLE content_log RENAME COLUMN repository_id TO owner_id;
ALTER TABLE content_log ADD COLUMN scope smallint NOT NULL DEFAULT 0;
ALTER TABLE content_log ADD PRIMARY KEY (scope, owner_id, id);

ALTER TABLE segment DROP CONSTRAINT segment_repository_id_fkey;
ALTER TABLE segment DROP CONSTRAINT segment_pkey;
ALTER TABLE segment RENAME COLUMN repository_id TO owner_id;
ALTER TABLE segment ADD COLUMN scope smallint NOT NULL DEFAULT 0;
ALTER TABLE segment ADD PRIMARY KEY (scope, owner_id, kind, id);

INSERT INTO content_index (scope, owner_id, generation) SELECT 1, id, 1 FROM network;
