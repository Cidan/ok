-- The live requests waiting on automation's subjects — a run's log as it grows, the run as it ends —
-- each its inbox, until it lapses (internal/wait; spannerdata.WaitsDDL).
CREATE TABLE waiter (
    subject   STRING(MAX) NOT NULL,
    inbox     STRING(MAX) NOT NULL,
    lapses_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (subject, inbox);
CREATE INDEX waiter_inbox ON waiter (inbox);
