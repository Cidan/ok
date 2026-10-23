-- Forks (docs/hosted.md, "Fork networks, as built"). A fork is a repository in its parent's network
-- that names its parent's segments in its manifest, inherited; nothing a parent does changes them.
-- Its main's history follows its parent's up to parent_seq, a place in the parent's main history:
-- where it forked, or last synced.
ALTER TABLE repository ADD COLUMN parent_id bigint REFERENCES repository;
ALTER TABLE repository ADD COLUMN parent_seq bigint;

-- Segment objects are their network's (nets/<network>/segments/…), and a segment row is a
-- manifest's reference to one: a repository's own, or inherited from its parent. An inherited one
-- of a kind read along main (path history, main's log) is read only up to bound, the place in
-- main's history the fork took it at. An object leaves the bucket only when no row in its network
-- names it.
ALTER TABLE segment ADD COLUMN network_id bigint;
UPDATE segment SET network_id = owner_id WHERE scope = 1;
UPDATE segment g SET network_id = r.network_id FROM repository r WHERE g.scope = 0 AND r.id = g.owner_id;
ALTER TABLE segment ALTER COLUMN network_id SET NOT NULL;
ALTER TABLE segment ADD COLUMN inherited boolean NOT NULL DEFAULT false;
ALTER TABLE segment ADD COLUMN bound bigint;
CREATE INDEX segment_object ON segment (network_id, kind, id);

-- A repository's sweep releases a chunk it no longer holds; its network's collection drops the
-- chunk's location once no repository in the network holds it.
CREATE TABLE chunk_release (
    network_id  bigint      NOT NULL REFERENCES network ON DELETE CASCADE,
    chunk_id    bytea       NOT NULL,
    released_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (network_id, chunk_id)
);

-- Dictionaries hold pieces of the files they were trained on. One repository of a network trains
-- its dictionaries, from what it has pinned; a repository reads those its trainer trained, if it
-- is the trainer, and those up to what its source could read when it last forked or synced.
ALTER TABLE network ADD COLUMN trainer_id bigint;
UPDATE network n SET trainer_id = (SELECT min(r.id) FROM repository r WHERE r.network_id = n.id);
ALTER TABLE dictionary ADD COLUMN trainer_id bigint;
UPDATE dictionary d SET trainer_id = n.trainer_id FROM network n WHERE n.id = d.network_id;
CREATE TABLE dictionary_grant (
    repository_id bigint NOT NULL REFERENCES repository ON DELETE CASCADE,
    trainer_id    bigint NOT NULL,
    up_to         bigint NOT NULL,                     -- the newest of the trainer's it may read
    PRIMARY KEY (repository_id, trainer_id)
);
