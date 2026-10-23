-- A person's approval, with a passkey, of exactly what ok secrets asked (docs/secrets.md,
-- "Unsealing"): waiting until it is answered, or expires.
CREATE TABLE approvals (
    id           STRING(MAX) NOT NULL,
    principal_id STRING(MAX) NOT NULL,
    scope        STRING(MAX) NOT NULL,
    description  STRING(MAX) NOT NULL,
    created_at   TIMESTAMP   NOT NULL,
    expires_at   TIMESTAMP   NOT NULL,
    decision     STRING(MAX) NOT NULL,
    decided_at   TIMESTAMP,
) PRIMARY KEY (id);

CREATE INDEX approvals_expires ON approvals (expires_at);
