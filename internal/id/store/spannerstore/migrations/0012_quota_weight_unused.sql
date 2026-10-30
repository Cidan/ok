-- Fair share is gone (docs/hosted.md, "Hosted automation, as built"): automation dispatches runs
-- to workers in the order they are queued, and nothing reads an account's weight. Its column stays,
-- defaulted, for a release that still reads it.
ALTER TABLE account_quotas ALTER COLUMN weight SET DEFAULT (1);
