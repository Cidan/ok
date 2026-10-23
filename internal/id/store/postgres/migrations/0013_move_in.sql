-- Moving an installation in (docs/hosted.md, "Moving an installation in"): the mode the directory
-- serves, so an installation's identity refuses a directory moved in and a hosted service's one
-- that never was; the organization that was the installation's, which an installation's bare
-- repository names resolve to; and the names a move gave up, each an alias of the name that took
-- its place, so what an installation wrote keeps naming what it named.
CREATE TABLE tenancy_state (
    singleton    boolean     PRIMARY KEY DEFAULT true CHECK (singleton),
    mode         text        NOT NULL CHECK (mode IN ('installation', 'hosted')),
    moved_in     text        REFERENCES principals (id),
    moved_at     timestamptz
);

CREATE TABLE name_aliases (
    old_name text PRIMARY KEY,
    new_name text NOT NULL
);

-- An organization moved in names the people its identity provider makes by their handles alone, as
-- the installation did, where any other's are <handle>-<org>.
ALTER TABLE organization_identity ADD COLUMN plain_handles boolean NOT NULL DEFAULT false;
