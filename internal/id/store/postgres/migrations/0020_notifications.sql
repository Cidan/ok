-- People's notifications (docs/hosted.md, "ok control at scale, as built"): each once by recipient
-- and source; which kinds each person is mailed; and the mail waiting to be sent, keyed work
-- (mail) retrying each message until a day has passed, then keeping it, dead.
CREATE TABLE notifications (
    principal_id  text        NOT NULL,
    source        text        NOT NULL,
    kind          smallint    NOT NULL,
    repository_id bigint      NOT NULL,
    number        bigint      NOT NULL,
    repository    text        NOT NULL,
    title         text        NOT NULL,
    actor_id      text        NOT NULL,
    text          text        NOT NULL,
    link          text        NOT NULL,
    notified_at   timestamptz NOT NULL,
    read          boolean     NOT NULL DEFAULT false,
    PRIMARY KEY (principal_id, source)
);
CREATE INDEX notifications_latest ON notifications (principal_id, notified_at DESC);
CREATE INDEX notifications_repository ON notifications (repository_id);

CREATE TABLE notification_prefs (
    principal_id text   PRIMARY KEY,
    mailed       bigint NOT NULL -- a bit for each kind mailed
);

CREATE TABLE mail_outbox (
    mail_id      bigint      PRIMARY KEY,
    principal_id text        NOT NULL,
    to_address   text        NOT NULL,
    subject      text        NOT NULL,
    body         text        NOT NULL,
    state        smallint    NOT NULL, -- 0 waiting, 1 sent, 2 dead
    attempts     int         NOT NULL DEFAULT 0,
    next_try     timestamptz NOT NULL,
    last_error   text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL
);
CREATE INDEX mail_outbox_due ON mail_outbox (next_try) WHERE state = 0;

-- identity's keyed work (internal/work; pgdata.WorkSchema).
CREATE TABLE work (
    kind        text        NOT NULL,
    key         bigint      NOT NULL,
    dirty       boolean     NOT NULL DEFAULT false,
    due_at      timestamptz,
    holder      text,
    lease_until timestamptz,
    PRIMARY KEY (kind, key)
);
CREATE INDEX work_due ON work (kind, key) WHERE dirty OR holder IS NOT NULL;
