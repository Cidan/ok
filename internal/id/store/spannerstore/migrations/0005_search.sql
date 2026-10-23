-- The directory's search of repositories, people, and organizations by part of their names (docs/
-- hosted.md, "ok control at scale, as built"): substring search indexes.
ALTER TABLE repositories ADD COLUMN name_tokens TOKENLIST AS (TOKENIZE_SUBSTRING(LOWER(name))) HIDDEN;
ALTER TABLE principals ADD COLUMN handle_tokens TOKENLIST AS (TOKENIZE_SUBSTRING(LOWER(REGEXP_EXTRACT(name, r'^([^.]*)')))) HIDDEN;
ALTER TABLE principals ADD COLUMN display_tokens TOKENLIST AS (TOKENIZE_SUBSTRING(LOWER(display_name))) HIDDEN;
CREATE SEARCH INDEX repositories_search ON repositories (name_tokens);
CREATE SEARCH INDEX principals_search ON principals (handle_tokens, display_tokens);
