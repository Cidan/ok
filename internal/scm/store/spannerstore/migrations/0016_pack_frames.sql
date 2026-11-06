-- Packs of frames (pack format version 3, docs/scm.md "Packs"). Compaction no longer recompresses a
-- pack's small chunks alone with the newest dictionary — a frame names the dictionary it was
-- compressed with in its own header — so no pack records which dictionary last considered it. What
-- compaction looks for instead is a pack it did not write: a publish's, each record a frame of its
-- own at the level a waiting client is given, which it rewrites into frames.
ALTER TABLE pack DROP COLUMN dictionary_id;
ALTER TABLE pack ADD COLUMN compacted BOOL NOT NULL DEFAULT (false);
