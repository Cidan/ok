-- The locks of package lock: garbage collection runs on one replica at a time.
CREATE TABLE IF NOT EXISTS locks (
    bucket       text NOT NULL,
    id           text NOT NULL,
    owner        text NOT NULL,
    refresh_id   text NOT NULL,
    last_refresh timestamptz NOT NULL,
    PRIMARY KEY (bucket, id)
);
