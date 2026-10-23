-- People's notifications (docs/hosted.md, "ok control at scale, as built"): each once by recipient
-- and source; which kinds each person is mailed; and the mail waiting to be sent, keyed work
-- (mail) retrying each message until a day has passed, then keeping it, dead.
CREATE TABLE notifications (
    principal_id  STRING(MAX) NOT NULL,
    source        STRING(MAX) NOT NULL,
    kind          INT64       NOT NULL,
    repository_id INT64       NOT NULL,
    number        INT64       NOT NULL,
    repository    STRING(MAX) NOT NULL,
    title         STRING(MAX) NOT NULL,
    actor_id      STRING(MAX) NOT NULL,
    text          STRING(MAX) NOT NULL,
    link          STRING(MAX) NOT NULL,
    notified_at   TIMESTAMP   NOT NULL,
    read          BOOL        NOT NULL,
) PRIMARY KEY (principal_id, source);
CREATE INDEX notifications_latest ON notifications (principal_id, notified_at DESC);
CREATE INDEX notifications_repository ON notifications (repository_id);

CREATE TABLE notification_prefs (
    principal_id STRING(MAX) NOT NULL,
    mailed       INT64       NOT NULL,
) PRIMARY KEY (principal_id);

CREATE TABLE mail_outbox (
    mail_id      INT64       NOT NULL,
    principal_id STRING(MAX) NOT NULL,
    to_address   STRING(MAX) NOT NULL,
    subject      STRING(MAX) NOT NULL,
    body         STRING(MAX) NOT NULL,
    state        INT64       NOT NULL,
    attempts     INT64       NOT NULL,
    next_try     TIMESTAMP   NOT NULL,
    last_error   STRING(MAX) NOT NULL,
    created_at   TIMESTAMP   NOT NULL,
) PRIMARY KEY (mail_id);
CREATE INDEX mail_outbox_due ON mail_outbox (state, next_try);

-- identity's keyed work (internal/work; spannerdata.WorkDDL).
CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key);
