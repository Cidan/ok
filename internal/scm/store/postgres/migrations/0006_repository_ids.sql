-- A repository's ID is random (internal/data.NewID), chosen by the SCM as it creates the
-- repository, so it is unique across every installation and cell a repository may move between
-- (docs/hosted.md, "Shared primitives"). The repositories made before keep theirs.
ALTER TABLE repository ALTER COLUMN id DROP IDENTITY;
