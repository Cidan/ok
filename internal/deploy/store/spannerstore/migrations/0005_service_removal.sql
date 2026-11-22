-- A service main no longer declares that a person removed (docs/deploy.md, "Services"): who, by
-- stable ID (docs/hosted.md, "Tenancy, as built"), and when. The Services pages and the observer
-- pass over it until a plan declares it again, which clears both.
ALTER TABLE service ADD COLUMN removed_by STRING(MAX);
ALTER TABLE service ADD COLUMN removed_at TIMESTAMP;
