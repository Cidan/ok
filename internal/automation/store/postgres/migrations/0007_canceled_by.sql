-- Who canceled a run or a submission, by stable ID (docs/hosted.md, "Tenancy, as built"): a
-- reason never names a person, so a rename shows wherever it is read. NULL: no one, or not
-- canceled.
ALTER TABLE run ADD COLUMN canceled_by text;
ALTER TABLE submission ADD COLUMN canceled_by text;
