-- Tenancy (docs/hosted.md, "Tenancy, as built"): organizations — principals whose handles share the
-- namespace people's are in — their members and teams, every repository's entry, the names
-- repositories had, and the roles granted on each.

ALTER TABLE principals DROP CONSTRAINT principals_kind_check;
ALTER TABLE principals ADD CONSTRAINT principals_kind_check CHECK (kind IN ('human', 'robot', 'org'));

-- An organization's settings.
CREATE TABLE organizations (
    principal_id   text PRIMARY KEY REFERENCES principals (id),
    -- The role every member has on every repository the organization owns.
    base_role      text NOT NULL CHECK (base_role IN ('none', 'read', 'triage', 'write', 'maintain', 'admin')),
    -- Whether members may make repositories; owners always may.
    members_create boolean NOT NULL,
    -- Whether its private repositories may be forked.
    private_forks  boolean NOT NULL,
    -- Organizations sharing an enterprise read each other's internal repositories; '' for none.
    enterprise     text NOT NULL DEFAULT ''
);

CREATE TABLE memberships (
    organization_id text NOT NULL REFERENCES principals (id),
    principal_id    text NOT NULL REFERENCES principals (id),
    role            text NOT NULL CHECK (role IN ('owner', 'member')),
    PRIMARY KEY (organization_id, principal_id)
);
CREATE INDEX memberships_principal ON memberships (principal_id);

-- An organization's own teams, nested by parent: a team's members are its own and every team's
-- below it. A team is a set: its name (<team>.<org>.team.<domain>) is given once, and retired when
-- it is deleted.
CREATE TABLE teams (
    name            text PRIMARY KEY,
    organization_id text NOT NULL REFERENCES principals (id),
    parent          text REFERENCES teams (name),
    members         text[] NOT NULL DEFAULT '{}'    -- principals' IDs, sorted
);
CREATE INDEX teams_organization ON teams (organization_id);

-- Every repository's entry: its stable ID, its owner and name, who reads it, its fork network,
-- and where it lives. A name is unique per owner while its repository stands, deleted or not.
CREATE TABLE repositories (
    id          bigint PRIMARY KEY,
    owner_id    text NOT NULL REFERENCES principals (id),
    name        text NOT NULL,
    visibility  text NOT NULL CHECK (visibility IN ('private', 'internal', 'public')),
    -- The role proposing a CL needs: read (anyone who reads it) or write.
    propose     text NOT NULL CHECK (propose IN ('read', 'write')),
    parent_id   bigint REFERENCES repositories (id) ON DELETE SET NULL,
    network_id  bigint NOT NULL,
    cell        text NOT NULL,
    -- creating: its cell has not made its storage; deleted: restorable until purged.
    state       text NOT NULL CHECK (state IN ('creating', 'active', 'deleted')),
    state_at    timestamptz NOT NULL,
    created_by  text NOT NULL REFERENCES principals (id),
    created_at  timestamptz NOT NULL,
    UNIQUE (owner_id, name)
);
CREATE INDEX repositories_parent ON repositories (parent_id);
CREATE INDEX repositories_network ON repositories (network_id);

-- A name a repository had — renamed, transferred, or its owner renamed — answering for it until
-- that name is taken again.
CREATE TABLE redirects (
    owner_id      text NOT NULL REFERENCES principals (id),
    name          text NOT NULL,
    repository_id bigint NOT NULL REFERENCES repositories (id) ON DELETE CASCADE,
    PRIMARY KEY (owner_id, name)
);

-- A role on one repository, granted to a principal (by ID: an outside collaborator, a member) or a
-- set (by name: a team, a group).
CREATE TABLE grants (
    repository_id bigint NOT NULL REFERENCES repositories (id) ON DELETE CASCADE,
    subject       text NOT NULL,
    role          text NOT NULL CHECK (role IN ('read', 'triage', 'write', 'maintain', 'admin')),
    PRIMARY KEY (repository_id, subject)
);
CREATE INDEX grants_subject ON grants (subject);
