-- The roles that follow this service's logs (docs/state.md, "Events between services"): its relay
-- enqueues, on each one's queue, a job naming each key whose log grew (pgdata.FollowersSchema).
CREATE TABLE follower (
    topic       text        NOT NULL,
    name        text        NOT NULL,
    followed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, name)
);
