-- Deleting a repository (docs/hosted.md, "Fork networks, as built", "Deleting"): its row and its
-- tail go at once; its content index and segment references stay, recorded here, until every chunk
-- it held is released to its network and its references retired — then, when it was the network's
-- last, the network goes whole.
CREATE TABLE repository_deletion (
    repository_id bigint      PRIMARY KEY,
    network_id    bigint      NOT NULL REFERENCES network ON DELETE CASCADE,
    deleted_at    timestamptz NOT NULL DEFAULT now()
);
