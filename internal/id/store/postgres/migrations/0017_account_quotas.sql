-- An account's quota of CI (docs/hosted.md, "Hosted automation, as built"): a person's or an
-- organization's concurrent runs and run minutes in a calendar month (zero for unlimited), and its
-- weight in fair share; an account with none has identity.default_quota.
CREATE TABLE account_quotas (
    principal_id text   PRIMARY KEY REFERENCES principals (id) ON DELETE CASCADE,
    concurrent   int    NOT NULL CHECK (concurrent >= 0),
    minutes      bigint NOT NULL CHECK (minutes >= 0),
    weight       int    NOT NULL CHECK (weight > 0)
);

-- Each cell's use this month by each account (ReportUsage): its run seconds, and its running runs
-- when it reported, which the other cells' admission checks quotas against.
CREATE TABLE account_usage (
    account_id  text        NOT NULL,
    month       text        NOT NULL, -- YYYY-MM, UTC
    cell        text        NOT NULL,
    seconds     bigint      NOT NULL,
    running     bigint      NOT NULL,
    reported_at timestamptz NOT NULL,
    PRIMARY KEY (account_id, month, cell)
);
