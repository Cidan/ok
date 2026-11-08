-- Locks of paths (docs/scm.md, "Locks"): a person's, of a path of a repository, live for 18 hours
-- from taken_at — judged as it is read, not ended by a timer — unless they unlock it or a landing of
-- theirs that changes it releases it. holder_name is their name as of when they took it.
CREATE TABLE file_lock (
    repository_id INT64       NOT NULL,
    path          STRING(MAX) NOT NULL,
    holder        STRING(MAX) NOT NULL,
    holder_name   STRING(MAX) NOT NULL,
    taken_at      TIMESTAMP   NOT NULL,
) PRIMARY KEY (repository_id, path), INTERLEAVE IN PARENT repository ON DELETE NO ACTION;
