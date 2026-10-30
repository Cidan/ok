-- The live requests waiting on ok control's subjects — an action waiting for the change feed to
-- apply what it did, a CL's watchers — each its inbox, until it lapses (internal/wait;
-- pgdata.WaitsSchema).
CREATE TABLE waiter (
    subject   text        NOT NULL,
    inbox     text        NOT NULL,
    lapses_at timestamptz NOT NULL,
    PRIMARY KEY (subject, inbox)
);
CREATE INDEX waiter_inbox ON waiter (inbox);
