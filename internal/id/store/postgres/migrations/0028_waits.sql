-- The live requests waiting on identity's subjects — a sync asked for while another runs, waiting
-- for its claim — each its inbox, until it lapses (internal/wait; pgdata.WaitsSchema).
CREATE TABLE waiter (
    subject   text        NOT NULL,
    inbox     text        NOT NULL,
    lapses_at timestamptz NOT NULL,
    PRIMARY KEY (subject, inbox)
);
CREATE INDEX waiter_inbox ON waiter (inbox);
