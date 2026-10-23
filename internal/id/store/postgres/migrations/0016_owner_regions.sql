-- The region an owner's new repositories are placed in (docs/hosted.md, "Cells, as built"): a
-- person's own, or an organization's, set by its owners; none for the directory's default.
CREATE TABLE owner_regions (
    principal_id text PRIMARY KEY REFERENCES principals (id) ON DELETE CASCADE,
    region       text NOT NULL
);
