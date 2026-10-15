-- The locks of package lock: the deployer leads, follows the feed, observes, and steps each
-- deployment on one replica at a time.
CREATE TABLE locks (
    bucket       text NOT NULL,
    id           text NOT NULL,
    owner        text NOT NULL,
    refresh_id   text NOT NULL,
    last_refresh timestamptz NOT NULL,
    PRIMARY KEY (bucket, id)
);
