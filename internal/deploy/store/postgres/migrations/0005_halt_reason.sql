-- Why ok asked a deployment to halt — another deployment of its plan failed — apart from who asked,
-- which halt_by holds by stable ID (docs/hosted.md, "Tenancy, as built"). A deployment is asked to
-- halt when either is set.
ALTER TABLE deployment ADD COLUMN halt_reason text;
