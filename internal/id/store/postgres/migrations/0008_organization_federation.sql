-- Federation per organization (docs/hosted.md, "Robots and tokens per organization"): the OpenID
-- Connect issuers an organization trusts to sign its robots in. A token for an organization carries
-- the organization's ID as its audience, so one minted for one organization never signs in another's
-- robot.
CREATE TABLE organization_issuers (
    organization_id text        NOT NULL REFERENCES principals (id),
    issuer          text        NOT NULL,
    created_at      timestamptz NOT NULL,
    PRIMARY KEY (organization_id, issuer)
);

-- A binding is the installation's ('': an issuer the configuration names, for its robots) or an
-- organization's: the same issuer's subject may name a robot in each organization.
ALTER TABLE robot_bindings ADD COLUMN organization_id text NOT NULL DEFAULT '';
ALTER TABLE robot_bindings DROP CONSTRAINT robot_bindings_pkey;
ALTER TABLE robot_bindings ADD PRIMARY KEY (organization_id, issuer, subject);
