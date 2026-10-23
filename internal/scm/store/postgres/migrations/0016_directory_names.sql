-- The directory names repositories (docs/hosted.md, "Tenancy, as built"): the SCM keeps each one's
-- ID and content, and no name.
ALTER TABLE repository DROP COLUMN name;
