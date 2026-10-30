-- The roles that follow this service's logs (docs/state.md, "Events between services"): its relay
-- enqueues, on each one's queue, a job naming each key whose log grew (spannerdata.FollowersDDL).
CREATE TABLE follower (
    topic       STRING(MAX) NOT NULL,
    name        STRING(MAX) NOT NULL,
    followed_at TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (topic, name);
