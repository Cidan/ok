-- The live requests waiting on the deployer's subjects — ok control's watchers of a landed CL, on
-- its commit's deployments — each its inbox, until it lapses (internal/wait; spannerdata.WaitsDDL).
CREATE TABLE waiter (
    subject   STRING(MAX) NOT NULL,
    inbox     STRING(MAX) NOT NULL,
    lapses_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (subject, inbox);
CREATE INDEX waiter_inbox ON waiter (inbox);
