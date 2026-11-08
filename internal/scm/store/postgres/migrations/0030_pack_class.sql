-- Storage tiering (docs/scm.md, "Tiering"): the object store's class each pack is kept in — 0, the
-- store's standard class, or the cold class tiering moved it to once nothing in it is read often.
ALTER TABLE pack ADD COLUMN class smallint NOT NULL DEFAULT 0;
