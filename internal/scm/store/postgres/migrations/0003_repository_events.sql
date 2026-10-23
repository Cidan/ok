-- Each repository's own event order (docs/hosted.md, "The per-repository log"): an event's
-- repository_seq is its place in its repository's log, numbered by that repository's counter, so
-- two repositories' writes never wait on each other. A transaction takes its repository's counter
-- last but for the global one, which still orders Events.Watch's feed until every follower reads
-- its repositories' logs instead.
CREATE TABLE repository_event_counter (
    repository_id bigint PRIMARY KEY REFERENCES repository ON DELETE CASCADE,
    last          bigint NOT NULL
);

ALTER TABLE event ADD COLUMN repository_seq bigint;
UPDATE event e SET repository_seq = n.s
    FROM (SELECT seq, row_number() OVER (PARTITION BY repository_id ORDER BY seq) AS s FROM event) n
    WHERE e.seq = n.seq;
ALTER TABLE event ALTER COLUMN repository_seq SET NOT NULL;
CREATE UNIQUE INDEX event_repository_seq ON event (repository_id, repository_seq);
INSERT INTO repository_event_counter (repository_id, last)
    SELECT repository_id, max(repository_seq) FROM event
    WHERE repository_id IN (SELECT id FROM repository) GROUP BY repository_id;

-- The events the relay has yet to publish to the bus's stream, which it publishes and then deletes:
-- every event written from here on, in the transaction that writes it. Those before are read from
-- the log.
CREATE TABLE event_pending (
    repository_id  bigint NOT NULL,
    repository_seq bigint NOT NULL,
    PRIMARY KEY (repository_id, repository_seq)
);
