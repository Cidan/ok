-- An organization's own root for its repositories' secrets (docs/secrets.md, "Keys"): a key in its
-- own KMS, which ok's workload is granted to use.
CREATE TABLE organization_secrets_roots (
    organization_id text        PRIMARY KEY,
    root            text        NOT NULL,
    set_by          text        NOT NULL,
    set_at          timestamptz NOT NULL DEFAULT now()
);
