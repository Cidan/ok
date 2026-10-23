-- Organizations' deploy agents (docs/hosted.md, "Deploy agents, as built"): the robot of each,
-- of its own, that the deployer sends its deployments to.
CREATE TABLE deploy_agents (
    organization_id text        PRIMARY KEY REFERENCES principals (id),
    robot_id        text        NOT NULL UNIQUE REFERENCES principals (id),
    set_by          text        NOT NULL,
    set_at          timestamptz NOT NULL
);
