-- ok control's outbox (docs/hosted.md, "ok control at scale, as built"): each event recorded in a
-- CL's history, and each comment, waits here until what it causes — the directory's inboxes, its
-- notifications, the repository's webhooks — is done, its repository's keyed work (outbox)
-- draining it oldest first. It moves with its repository.
CREATE TABLE outbox (
    repository_id INT64     NOT NULL,
    entry_id      INT64     NOT NULL,
    number        INT64     NOT NULL,
    queued_at     TIMESTAMP NOT NULL,
    event_id      INT64     NOT NULL,
    comment_id    INT64     NOT NULL,
) PRIMARY KEY (repository_id, entry_id);
CREATE INDEX outbox_order ON outbox (repository_id, queued_at, entry_id);

-- ok control's keyed work (internal/work; spannerdata.WorkDDL).
CREATE TABLE work (
    kind        STRING(MAX) NOT NULL,
    key         INT64       NOT NULL,
    dirty       BOOL        NOT NULL,
    due_at      TIMESTAMP,
    holder      STRING(MAX),
    lease_until TIMESTAMP,
) PRIMARY KEY (kind, key);
