-- Keyed work marked for a time (pgdata.MarkWorkAt): the landing queue of a repository whose candidates
-- are too young to discard is due when the youngest comes of age.
ALTER TABLE work ADD COLUMN due_at timestamptz;
