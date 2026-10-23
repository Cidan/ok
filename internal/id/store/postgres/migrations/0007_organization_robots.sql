-- An organization's robots (docs/hosted.md, "Robots and tokens per organization"): CI identities,
-- deploy agents, apps — principals named under it (<name>.<org>.robot.<domain>), reaching its
-- repositories through their grants alone.
CREATE TABLE organization_robots (
    principal_id    text PRIMARY KEY REFERENCES principals (id),
    organization_id text NOT NULL REFERENCES principals (id)
);
CREATE INDEX organization_robots_organization ON organization_robots (organization_id);
