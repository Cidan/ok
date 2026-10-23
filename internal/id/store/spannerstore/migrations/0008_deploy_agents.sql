-- Organizations' deploy agents (docs/hosted.md, "Deploy agents, as built"): the robot of each,
-- of its own, that the deployer sends its deployments to.
CREATE TABLE deploy_agents (
    organization_id STRING(MAX) NOT NULL,
    robot_id        STRING(MAX) NOT NULL,
    set_by          STRING(MAX) NOT NULL,
    set_at          TIMESTAMP   NOT NULL,
) PRIMARY KEY (organization_id);
CREATE UNIQUE INDEX deploy_agents_robot ON deploy_agents (robot_id);
