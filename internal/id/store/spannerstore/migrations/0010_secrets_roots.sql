-- An organization's own root for its repositories' secrets (docs/secrets.md, "Keys"): a key in its
-- own KMS, which ok's workload is granted to use.
CREATE TABLE organization_secrets_roots (
    organization_id STRING(MAX) NOT NULL,
    root            STRING(MAX) NOT NULL,
    set_by          STRING(MAX) NOT NULL,
    set_at          TIMESTAMP   NOT NULL,
) PRIMARY KEY (organization_id);
