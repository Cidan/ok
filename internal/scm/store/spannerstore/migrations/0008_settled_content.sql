-- What garbage collection need not walk again (docs/scm.md, "Garbage collection and compaction"):
-- the content a settled operation or view reaches is pinned as it moves into a segment, and a
-- repository whose earlier settled operations' content a pass has pinned is marked so; an
-- operation log is collected only once its heads moved other than forward since it last was; and a
-- checkpoint kept past its first week has its trees pinned.
ALTER TABLE repository ADD COLUMN op_content_pinned BOOL NOT NULL DEFAULT (FALSE);
-- An operation log is collected only once its heads moved other than forward (diverged_at) since
-- a collection that took everything that left unreached (collected_through); a log made before is
-- taken as diverged now, and collected so once.
ALTER TABLE op_log ADD COLUMN diverged_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP());
ALTER TABLE op_log ALTER COLUMN diverged_at DROP DEFAULT;
ALTER TABLE op_log ADD COLUMN collected_through TIMESTAMP;
ALTER TABLE checkpoint ADD COLUMN trees_pinned BOOL NOT NULL DEFAULT (FALSE);
CREATE INDEX checkpoint_unpinned ON checkpoint (repository_id, trees_pinned, created_at);
