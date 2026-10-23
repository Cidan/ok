-- Deploy agents (docs/hosted.md, "Deploy agents, as built"): each repository's owner, whose agent
-- its deployments go through; and each agent's connection to this cell, held by the replica it
-- reached, at the address the other replicas relay to it through, while its lease lasts.
ALTER TABLE repository ADD COLUMN owner_id text NOT NULL DEFAULT '';

CREATE TABLE agent_session (
    account_id   text        PRIMARY KEY,
    holder       text        NOT NULL,
    address      text        NOT NULL,
    lease_until  timestamptz NOT NULL,
    connected_at timestamptz NOT NULL
);
