\set ON_ERROR_STOP on
BEGIN;
SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '20s';
SELECT pg_advisory_xact_lock(1643376077, 1);

SELECT to_regnamespace('work_identity') IS NULL AS clean_install \gset
\if :clean_install
\if :{?identity_owner}
CREATE SCHEMA work_identity AUTHORIZATION :"identity_owner";
\else
CREATE SCHEMA work_identity;
\endif
\endif
\if :{?identity_owner}
SET LOCAL ROLE :"identity_owner";
DO $owner$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = current_user AND (
            rolcanlogin OR rolinherit OR rolsuper OR rolcreaterole
            OR rolcreatedb OR rolreplication OR rolbypassrls
        )
    ) THEN
        RAISE EXCEPTION 'work identity schema owner has incompatible role attributes';
    END IF;
END
$owner$;
\endif

-- Compare the installed catalog to the shape recorded in this transaction.
CREATE FUNCTION pg_temp.identity_fingerprint() RETURNS text
LANGUAGE sql SET search_path = pg_catalog, pg_temp AS $$
    SELECT md5(coalesce(string_agg(row_shape::text, E'\n' ORDER BY row_shape::text), ''))
    FROM (
        SELECT jsonb_build_array('relation', c.relname, c.relkind, pg_get_userbyid(c.relowner),
                                 c.relpersistence, c.relrowsecurity, c.relforcerowsecurity) AS row_shape
        FROM pg_class c WHERE c.relnamespace = 'work_identity'::regnamespace
        UNION ALL
        SELECT jsonb_build_array('column', c.relname, a.attnum, a.attname,
                                 a.atttypid, a.atttypmod, a.attnotnull,
                                 a.attidentity, a.attgenerated, pg_get_expr(d.adbin, d.adrelid))
        FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE c.relnamespace = 'work_identity'::regnamespace AND a.attnum > 0
              AND NOT a.attisdropped
        UNION ALL
        SELECT jsonb_build_array('constraint', c.relname, k.conname, k.convalidated,
                                 pg_get_constraintdef(k.oid))
        FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid
        WHERE c.relnamespace = 'work_identity'::regnamespace
        UNION ALL
        SELECT jsonb_build_array('index', c.relname, i.indisvalid, pg_get_indexdef(c.oid))
        FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relnamespace = 'work_identity'::regnamespace
        UNION ALL
        SELECT jsonb_build_array('trigger', c.relname, t.tgname, t.tgenabled, pg_get_triggerdef(t.oid))
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        WHERE c.relnamespace = 'work_identity'::regnamespace AND NOT t.tgisinternal
    ) shapes
$$;

\if :clean_install
REVOKE ALL ON SCHEMA work_identity FROM PUBLIC;
\ir 001-relations.sql
CREATE TABLE work_identity.schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    fingerprint text NOT NULL
);
INSERT INTO work_identity.schema_migrations VALUES (1, pg_temp.identity_fingerprint());
\else
DO $verify$
BEGIN
    IF (SELECT nspowner FROM pg_namespace WHERE nspname = 'work_identity')
        <> (SELECT oid FROM pg_roles WHERE rolname = current_user)
    THEN
        RAISE EXCEPTION 'work identity schema has a different owner';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_class
        WHERE relnamespace = 'work_identity'::regnamespace
          AND relname = 'schema_migrations' AND relkind = 'r'
    ) THEN
        RAISE EXCEPTION 'work identity schema has no migration ledger';
    END IF;
    IF (SELECT count(*) FROM work_identity.schema_migrations) <> 1 OR NOT EXISTS (
        SELECT 1 FROM work_identity.schema_migrations
        WHERE version = 1 AND fingerprint = pg_temp.identity_fingerprint()
    ) THEN
        RAISE EXCEPTION 'incompatible work identity schema or migration version';
    END IF;
END
$verify$;
\endif
DROP FUNCTION pg_temp.identity_fingerprint();
COMMIT;
