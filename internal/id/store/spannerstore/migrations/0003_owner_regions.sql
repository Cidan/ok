-- The region an owner's new repositories are placed in (docs/hosted.md, "Cells, as built"): a
-- person's own, or an organization's, set by its owners; none for the directory's default.
CREATE TABLE owner_regions (
    principal_id STRING(MAX) NOT NULL,
    region       STRING(MAX) NOT NULL,
) PRIMARY KEY (principal_id);
