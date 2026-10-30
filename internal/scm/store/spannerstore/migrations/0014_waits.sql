-- The live requests waiting on the SCM's subjects, each its inbox, until it lapses (internal/wait;
-- spannerdata.WaitsDDL): a writer waiting for its repository's sequencing pass among them.
CREATE TABLE waiter (
    subject   STRING(MAX) NOT NULL,
    inbox     STRING(MAX) NOT NULL,
    lapses_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (subject, inbox);
CREATE INDEX waiter_inbox ON waiter (inbox);

-- When the latest sequencing pass of each repository that finished began, by its replica's clock:
-- what a writer waiting for one that began after it committed reads (docs/scm.md, "Sequencing").
CREATE TABLE sequence_pass (
    repository_id INT64     NOT NULL,
    began_at      TIMESTAMP NOT NULL,
) PRIMARY KEY (repository_id);
