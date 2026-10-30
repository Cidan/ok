-- The live requests waiting on automation's subjects — a run's log as it grows, the run as it ends —
-- each its inbox, until it lapses (internal/wait; pgdata.WaitsSchema).
CREATE TABLE waiter (
    subject   text        NOT NULL,
    inbox     text        NOT NULL,
    lapses_at timestamptz NOT NULL,
    PRIMARY KEY (subject, inbox)
);
CREATE INDEX waiter_inbox ON waiter (inbox);
