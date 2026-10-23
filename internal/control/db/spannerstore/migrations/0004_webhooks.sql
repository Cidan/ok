-- Repositories' webhooks (docs/hosted.md, "ok control at scale, as built"): each an endpoint, the
-- events it wants, and its secret, sealed (control.sealing_key_file); and their deliveries, sent
-- by keyed work (webhooks) by repository, the last hundred of each kept. Both move with their
-- repository. Main's moves wait in the outbox too, for push webhooks: an entry of no CL (number 0)
-- naming the head.
CREATE TABLE webhook (
    repository_id INT64       NOT NULL,
    webhook_id    INT64       NOT NULL,
    url           STRING(MAX) NOT NULL,
    events        INT64       NOT NULL,
    secret        BYTES(MAX)  NOT NULL,
    created_by    STRING(MAX) NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, webhook_id);

CREATE TABLE webhook_delivery (
    repository_id INT64       NOT NULL,
    delivery_id   INT64       NOT NULL,
    webhook_id    INT64       NOT NULL,
    event         STRING(MAX) NOT NULL,
    payload       BYTES(MAX)  NOT NULL,
    state         INT64       NOT NULL,
    attempts      INT64       NOT NULL,
    next_try      TIMESTAMP   NOT NULL,
    last_status   INT64       NOT NULL,
    last_error    STRING(MAX) NOT NULL,
    created_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, delivery_id);
CREATE INDEX webhook_delivery_due ON webhook_delivery (repository_id, state, next_try);
CREATE INDEX webhook_delivery_recent ON webhook_delivery (repository_id, webhook_id, created_at DESC);

ALTER TABLE outbox ADD COLUMN head_seq INT64 NOT NULL DEFAULT (0);
ALTER TABLE outbox ADD COLUMN head BYTES(MAX);
