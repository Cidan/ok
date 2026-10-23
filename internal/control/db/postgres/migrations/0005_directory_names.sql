-- The directory names repositories (docs/hosted.md, "Tenancy, as built"): ok control keeps each
-- one's full name as the directory's events say — which may come before or after the SCM's
-- creation — and whether it is deleted. Two repositories' names may cross as each one's events
-- are applied apart, so a name is no longer unique here.
ALTER TABLE repository DROP CONSTRAINT repository_name_key;
ALTER TABLE repository ALTER COLUMN name DROP NOT NULL;
ALTER TABLE repository ADD COLUMN deleted boolean NOT NULL DEFAULT false;
CREATE INDEX repository_name ON repository (name);
