-- Repositories' webhooks (docs/hosted.md, "ok control at scale, as built"): each an endpoint, the
-- events it wants, and its secret, sealed (control.sealing_key_file); and their deliveries, sent
-- by keyed work (webhooks) by repository, the last hundred of each kept. Both move with their
-- repository. Main's moves wait in the outbox too, for push webhooks: an entry of no CL (number 0)
-- naming the head.
CREATE TABLE webhook (
    repository_id bigint      NOT NULL,
    webhook_id    bigint      NOT NULL,
    url           text        NOT NULL,
    events        bigint      NOT NULL, -- a bit for each: 1 cl, 2 push
    secret        bytea       NOT NULL, -- sealed
    created_by    text        NOT NULL,
    created_at    timestamptz NOT NULL,
    PRIMARY KEY (repository_id, webhook_id)
);

CREATE TABLE webhook_delivery (
    repository_id bigint      NOT NULL,
    delivery_id   bigint      NOT NULL,
    webhook_id    bigint      NOT NULL,
    event         text        NOT NULL, -- cl, push, ping
    payload       bytea       NOT NULL, -- JSON, as sent
    state         smallint    NOT NULL, -- 0 waiting, 1 delivered, 2 dead
    attempts      int         NOT NULL,
    next_try      timestamptz NOT NULL,
    last_status   int         NOT NULL, -- the endpoint's last answer's, 0 for none
    last_error    text        NOT NULL,
    created_at    timestamptz NOT NULL,
    PRIMARY KEY (repository_id, delivery_id)
);
CREATE INDEX webhook_delivery_due ON webhook_delivery (repository_id, next_try) WHERE state = 0;
CREATE INDEX webhook_delivery_recent ON webhook_delivery (repository_id, webhook_id, created_at DESC);

ALTER TABLE outbox ADD COLUMN head_seq bigint NOT NULL DEFAULT 0;
ALTER TABLE outbox ADD COLUMN head bytea;
