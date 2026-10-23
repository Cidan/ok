-- People's inboxes (docs/hosted.md, "ok control at scale, as built"): each CL's entries for its
-- author and reviewers, as the cell that holds it last set them (inbox_version), kept here so a
-- person's inbox spans every cell. A repository's purge removes them.
CREATE TABLE inbox (
    principal_id  STRING(MAX) NOT NULL,
    repository_id INT64       NOT NULL,
    number        INT64       NOT NULL,
    relation      INT64       NOT NULL,
    attention     BOOL        NOT NULL,
    repository    STRING(MAX) NOT NULL,
    title         STRING(MAX) NOT NULL,
    status        STRING(MAX) NOT NULL,
    updated_at    TIMESTAMP   NOT NULL,
) PRIMARY KEY (principal_id, repository_id, number, relation);
CREATE INDEX inbox_cl ON inbox (repository_id, number);

CREATE TABLE inbox_version (
    repository_id INT64 NOT NULL,
    number        INT64 NOT NULL,
    version       INT64 NOT NULL,
) PRIMARY KEY (repository_id, number);
