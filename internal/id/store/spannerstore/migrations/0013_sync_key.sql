-- An organization's sync is keyed work (internal/work), keyed by its provider's sync_key: each sync
-- books the next. A provider of a release before has none until the first replica of this one
-- gives it one (enterprise.Syncer).
ALTER TABLE organization_identity ADD COLUMN sync_key INT64;
CREATE UNIQUE NULL_FILTERED INDEX organization_identity_key ON organization_identity (sync_key);
