-- The live requests waiting on ok control's subjects — an action waiting for the change feed to
-- apply what it did, a CL's watchers — each its inbox, until it lapses (internal/wait;
-- spannerdata.WaitsDDL).
CREATE TABLE waiter (
    subject   STRING(MAX) NOT NULL,
    inbox     STRING(MAX) NOT NULL,
    lapses_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (subject, inbox);
CREATE INDEX waiter_inbox ON waiter (inbox);
