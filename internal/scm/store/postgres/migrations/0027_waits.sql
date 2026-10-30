-- The live requests waiting on the SCM's subjects, each its inbox, until it lapses (internal/wait;
-- pgdata.WaitsSchema).
CREATE TABLE waiter (
    subject   text        NOT NULL,
    inbox     text        NOT NULL,
    lapses_at timestamptz NOT NULL,
    PRIMARY KEY (subject, inbox)
);
CREATE INDEX waiter_inbox ON waiter (inbox);
