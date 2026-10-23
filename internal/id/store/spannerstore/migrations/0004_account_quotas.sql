-- An account's quota of CI (docs/hosted.md, "Hosted automation, as built"): a person's or an
-- organization's concurrent runs and run minutes in a calendar month (zero for unlimited), and its
-- weight in fair share; an account with none has identity.default_quota.
CREATE TABLE account_quotas (
    principal_id STRING(MAX) NOT NULL,
    concurrent   INT64       NOT NULL,
    minutes      INT64       NOT NULL,
    weight       INT64       NOT NULL,
) PRIMARY KEY (principal_id);

-- Each cell's use this month by each account (ReportUsage): its run seconds, and its running runs
-- when it reported, which the other cells' admission checks quotas against.
CREATE TABLE account_usage (
    account_id  STRING(MAX) NOT NULL,
    month       STRING(MAX) NOT NULL,
    cell        STRING(MAX) NOT NULL,
    seconds     INT64       NOT NULL,
    running     INT64       NOT NULL,
    reported_at TIMESTAMP   NOT NULL,
) PRIMARY KEY (account_id, month, cell);
