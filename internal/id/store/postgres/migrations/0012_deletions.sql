-- Accounts and organizations deleted (docs/hosted.md, "Deleting, renaming, transferring"): soft
-- for the retention period — suspended, their repositories deleted, restorable — then for good:
-- their handles retired, their memberships, credentials, and tokens gone.
CREATE TABLE deletions (
    principal_id text        PRIMARY KEY REFERENCES principals (id),
    requested_at timestamptz NOT NULL,
    requested_by text        NOT NULL REFERENCES principals (id)
);
CREATE INDEX deletions_due ON deletions (requested_at);
