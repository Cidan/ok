-- ok control's outbox (docs/hosted.md, "ok control at scale, as built"): each event recorded in a
-- CL's history, and each comment, waits here until what it causes — the directory's inboxes, its
-- notifications, the repository's webhooks — is done, its repository's keyed work (outbox)
-- draining it oldest first. It moves with its repository.
CREATE TABLE outbox (
    repository_id bigint      NOT NULL,
    entry_id      bigint      NOT NULL,
    number        bigint      NOT NULL,
    queued_at     timestamptz NOT NULL,
    event_id      bigint      NOT NULL DEFAULT 0, -- an event's, or 0
    comment_id    bigint      NOT NULL DEFAULT 0, -- a comment's, or 0
    PRIMARY KEY (repository_id, entry_id)
);
CREATE INDEX outbox_order ON outbox (repository_id, queued_at, entry_id);

-- ok control's keyed work (internal/work; pgdata.WorkSchema).
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
