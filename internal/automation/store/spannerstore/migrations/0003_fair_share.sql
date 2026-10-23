-- Fair share at the claim (docs/hosted.md, "Hosted automation, as built"): a run's pool of
-- workers; each repository's account, from the directory's events; each account's run seconds a
-- month (UTC), charged as each run that ran finishes or is given back, and its claims, which a
-- claim's write of the row serializes on. Priority is by phase from here on: post's runs and
-- deploys' 2, pre's 1, manual runs 0.
ALTER TABLE run ADD COLUMN pool STRING(MAX) NOT NULL DEFAULT ('default');
CREATE INDEX run_pool_queued ON run (status, pool, priority DESC, queued_at);

CREATE TABLE repository_owner (
    repository_id INT64       NOT NULL,
    account_id    STRING(MAX) NOT NULL,
) PRIMARY KEY (repository_id);
CREATE INDEX repository_owner_account ON repository_owner (account_id);

CREATE TABLE account_usage (
    account_id STRING(MAX) NOT NULL,
    month      STRING(MAX) NOT NULL,
    seconds    INT64       NOT NULL,
    claims     INT64       NOT NULL,
) PRIMARY KEY (account_id, month);
