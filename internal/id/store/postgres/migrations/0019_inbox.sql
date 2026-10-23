-- People's inboxes (docs/hosted.md, "ok control at scale, as built"): each CL's entries for its
-- author and reviewers, as the cell that holds it last set them (inbox_version), kept here so a
-- person's inbox spans every cell. A repository's purge removes them.
CREATE TABLE inbox (
    principal_id  text        NOT NULL,
    repository_id bigint      NOT NULL,
    number        bigint      NOT NULL,
    relation      smallint    NOT NULL, -- 1 authored, 2 reviewing
    attention     boolean     NOT NULL,
    repository    text        NOT NULL,
    title         text        NOT NULL,
    status        text        NOT NULL,
    updated_at    timestamptz NOT NULL,
    PRIMARY KEY (principal_id, repository_id, number, relation)
);
CREATE INDEX inbox_cl ON inbox (repository_id, number);
CREATE INDEX inbox_list ON inbox (principal_id, attention DESC, updated_at DESC);

CREATE TABLE inbox_version (
    repository_id bigint NOT NULL,
    number        bigint NOT NULL,
    version       bigint NOT NULL,
    PRIMARY KEY (repository_id, number)
);
