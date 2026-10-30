-- An organization's sync is keyed work (internal/work), keyed by its provider's sync_key: each sync
-- books the next. A provider of a release before has none until the first replica of this one
-- gives it one (enterprise.Syncer).
ALTER TABLE organization_identity ADD COLUMN sync_key bigint;
CREATE UNIQUE INDEX organization_identity_key ON organization_identity (sync_key);
