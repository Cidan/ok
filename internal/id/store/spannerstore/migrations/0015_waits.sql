-- The live requests waiting on identity's subjects — a sync asked for while another runs, waiting
-- for its claim — each its inbox, until it lapses (internal/wait; spannerdata.WaitsDDL).
CREATE TABLE waiter (
    subject   STRING(MAX) NOT NULL,
    inbox     STRING(MAX) NOT NULL,
    lapses_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (subject, inbox);
CREATE INDEX waiter_inbox ON waiter (inbox);
