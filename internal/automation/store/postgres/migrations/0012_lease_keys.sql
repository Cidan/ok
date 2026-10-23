-- A run's lease key (docs/secrets.md, "Readers that are not people"): the SHA-256 of a random key
-- each claim mints and gives its worker alone, which proves to ok secrets that a request for a
-- post run's secrets is that lease's.
ALTER TABLE run ADD COLUMN lease_key_hash bytea;
