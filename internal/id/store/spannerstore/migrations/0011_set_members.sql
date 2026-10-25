-- Which sets hold a member (docs/identity.md, "Sets, by their members"): each set's members
-- indexed by their elements, so a caller's sets are a walk up from the caller — a statement a level
-- of nesting — rather than every set read and walked in memory. A search index over the array's
-- tokens accelerates ARRAY_INCLUDES_ANY; Spanner uses it outside read-write transactions alone.
ALTER TABLE tree_sets ADD COLUMN members_tokens TOKENLIST AS (TOKEN(members)) HIDDEN;
CREATE SEARCH INDEX tree_sets_members ON tree_sets (members_tokens);
ALTER TABLE synced_groups ADD COLUMN members_tokens TOKENLIST AS (TOKEN(members)) HIDDEN;
CREATE SEARCH INDEX synced_groups_members ON synced_groups (members_tokens);
ALTER TABLE teams ADD COLUMN members_tokens TOKENLIST AS (TOKEN(members)) HIDDEN;
CREATE SEARCH INDEX teams_members ON teams (members_tokens);
-- A team's parent holds it; an alias stands for its team.
CREATE INDEX teams_parent ON teams (parent);
CREATE INDEX name_aliases_new ON name_aliases (new_name);
