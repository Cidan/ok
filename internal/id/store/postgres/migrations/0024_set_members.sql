-- Which sets hold a member (docs/identity.md, "Sets, by their members"): each set's members
-- indexed by their elements, so a caller's sets are a walk up from the caller — a statement a level
-- of nesting — rather than every set read and walked in memory. GIN indexes the arrays for &&.
CREATE INDEX tree_sets_members ON tree_sets USING gin (members);
CREATE INDEX synced_groups_members ON synced_groups USING gin (members);
CREATE INDEX teams_members ON teams USING gin (members);
-- A team's parent holds it; an alias stands for its team.
CREATE INDEX teams_parent ON teams (parent);
CREATE INDEX name_aliases_new ON name_aliases (new_name);
