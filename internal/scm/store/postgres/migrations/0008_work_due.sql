-- Keyed work marked for a time (pgdata.MarkWorkAt), as every role's work table has.
ALTER TABLE work ADD COLUMN due_at timestamptz;
