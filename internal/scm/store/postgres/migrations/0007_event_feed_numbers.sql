-- The numbers the feed across every repository gave its events (event.seq) go: no follower leaves
-- that feed any more.
ALTER TABLE event DROP COLUMN seq;
