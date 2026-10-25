-- A content index is its log and its runs (docs/scm.md, "The content index"): the generation's
-- blocks go. The index keeps the day its last complete garbage collection swept to (swept_before),
-- days since 1970-01-01; a segment, the day the fold that wrote it committed (a repository's
-- unpinned index's runs').
DROP TABLE index_block;
ALTER TABLE content_index ADD COLUMN swept_before INT64;
ALTER TABLE segment ADD COLUMN day INT64;
