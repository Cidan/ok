-- The feed that numbered every repository's events together, and its counter, are gone: the SCM's
-- followers follow the bus's stream and each repository's log, keyed by its place in that log. seq
-- keeps the old feed's number of each event written while there was one — what a follower leaving
-- the feed seeds its cursors from (Events.Heads) — and is null for every event since.
ALTER TABLE event DROP CONSTRAINT event_pkey;
ALTER TABLE event ALTER COLUMN seq DROP NOT NULL;
CREATE UNIQUE INDEX event_feed_seq ON event (seq);
ALTER TABLE event ADD PRIMARY KEY USING INDEX event_repository_seq;
DROP TABLE event_counter;
