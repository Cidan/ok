-- Claims at scale (docs/hosted.md, "Hosted automation, as built"): a claim reads what it chooses
-- from outside its transaction, which locks only the run it takes. An account's run seconds a month
-- are kept in sixteen rows, a run charged to the one its ID picks, so runs ending at once write
-- apart; the single row every claim wrote, to run one at a time, goes. An account with a
-- concurrent quota runs each run in a slot of its own, at most its quota of them: a run holds its
-- slot while it runs, and its row names the slot.
DROP TABLE account_usage;

CREATE TABLE account_seconds (
    account_id STRING(MAX) NOT NULL,
    month      STRING(MAX) NOT NULL,
    shard      INT64       NOT NULL,
    seconds    INT64       NOT NULL,
) PRIMARY KEY (account_id, month, shard);

CREATE TABLE account_slot (
    account_id    STRING(MAX) NOT NULL,
    slot          INT64       NOT NULL,
    run_id        INT64       NOT NULL,
    repository_id INT64       NOT NULL,
) PRIMARY KEY (account_id, slot);
CREATE INDEX account_slot_repository ON account_slot (repository_id);

ALTER TABLE run ADD COLUMN slot_account STRING(MAX);
ALTER TABLE run ADD COLUMN slot INT64;
