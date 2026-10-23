-- Fair share at the claim (docs/hosted.md, "Hosted automation, as built"): a run's pool of
-- workers; each repository's account, from the directory's events; each account's run seconds a
-- month (UTC), charged as each run that ran finishes or is given back, and its claims, which a
-- claim's write of the row serializes on.
ALTER TABLE run ADD COLUMN pool text NOT NULL DEFAULT 'default';
DROP INDEX run_queued;
CREATE INDEX run_queued ON run (pool, priority DESC, queued_at, id) WHERE status = 1;
CREATE INDEX run_running ON run (repository_id) WHERE status = 2;

-- Priority by phase: post's runs and deploys' 2, pre's 1, manual runs 0.
UPDATE run SET priority = CASE phase WHEN 2 THEN 2 WHEN 4 THEN 2 WHEN 1 THEN 1 ELSE 0 END WHERE status = 1;

CREATE TABLE repository_owner (
    repository_id bigint PRIMARY KEY,
    account_id    text   NOT NULL
);
CREATE INDEX repository_owner_account ON repository_owner (account_id);

CREATE TABLE account_usage (
    account_id text   NOT NULL,
    month      text   NOT NULL, -- YYYY-MM, UTC
    seconds    bigint NOT NULL DEFAULT 0,
    claims     bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, month)
);
