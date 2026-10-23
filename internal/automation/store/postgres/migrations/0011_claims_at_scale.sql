-- Claims at scale (docs/hosted.md, "Hosted automation, as built"): a claim locks only the run it
-- takes, no longer its account's usage row, whose claims column goes. An account with a concurrent
-- quota runs each run in a slot of its own, at most its quota of them: a run holds its slot while it
-- runs, and its row names the slot.
ALTER TABLE account_usage DROP COLUMN claims;

CREATE TABLE account_slot (
    account_id    text   NOT NULL,
    slot          bigint NOT NULL,
    run_id        bigint NOT NULL,
    repository_id bigint NOT NULL,
    PRIMARY KEY (account_id, slot)
);
CREATE INDEX account_slot_repository ON account_slot (repository_id);

ALTER TABLE run ADD COLUMN slot_account text, ADD COLUMN slot bigint;
