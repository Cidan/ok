-- The directory's search of repositories, people, and organizations by part of their names (docs/
-- hosted.md, "ok control at scale, as built"): trigram indexes where the database lets this role
-- make pg_trgm; without them, the search scans, as a small installation's may.
DO $$
DECLARE
    s text;
BEGIN
    BEGIN
        CREATE EXTENSION IF NOT EXISTS pg_trgm;
    EXCEPTION WHEN insufficient_privilege OR feature_not_supported OR undefined_file THEN
        RETURN;
    END;
    SELECT extnamespace::regnamespace::text INTO s FROM pg_extension WHERE extname = 'pg_trgm';
    EXECUTE format('CREATE INDEX repositories_name_trgm ON repositories USING gin (lower(name) %s.gin_trgm_ops)', s);
    EXECUTE format('CREATE INDEX principals_handle_trgm ON principals USING gin (lower(split_part(name, ''.'', 1)) %s.gin_trgm_ops)', s);
    EXECUTE format('CREATE INDEX principals_display_trgm ON principals USING gin (lower(display_name) %s.gin_trgm_ops)', s);
END
$$;
