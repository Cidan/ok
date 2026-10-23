-- Commits leave the tail for segments once pinned (docs/hosted.md, "Segments, as built"): the
-- commit graph — each commit's generation and parents — and their bodies, segment kinds 4 and 5.
CREATE INDEX commit_pinned ON commit (repository_id, id) WHERE pinned;
-- Read by nothing: a commit's change ID is read from its body, and its children never are.
DROP INDEX commit_change;
DROP INDEX commit_parent_parent;
