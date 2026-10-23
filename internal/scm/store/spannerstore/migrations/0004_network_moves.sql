-- A network moving between cells (docs/hosted.md, "Cells, as built"): frozen, every write about it
-- refused from the commit that froze it — each write transaction about it reads this row first,
-- which locks it, and freezing writes it — or moved to another cell, whose rows here go a day later.
ALTER TABLE network ADD COLUMN move_state STRING(MAX) NOT NULL DEFAULT ('');
ALTER TABLE network ADD COLUMN moved_to STRING(MAX) NOT NULL DEFAULT ('');
ALTER TABLE network ADD COLUMN moved_at TIMESTAMP;
