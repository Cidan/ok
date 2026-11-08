-- Locks of paths (docs/scm.md, "Locks"): a person's, of a path of a repository, live for 18 hours
-- from taken_at — judged as it is read, not ended by a timer — unless they unlock it or a landing of
-- theirs that changes it releases it. holder_name is their name as of when they took it.
CREATE TABLE file_lock (
    repository_id bigint      NOT NULL REFERENCES repository ON DELETE CASCADE,
    path          text        NOT NULL,
    holder        text        NOT NULL,
    holder_name   text        NOT NULL,
    taken_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (repository_id, path)
);
