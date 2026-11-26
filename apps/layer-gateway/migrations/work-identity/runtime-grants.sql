\set ON_ERROR_STOP on
BEGIN;
SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '20s';
SELECT pg_advisory_xact_lock(1643376077, 1);

-- Run as the identity schema owner. The login must already exist.
SELECT set_config('work_identity.runtime_role', :'runtime_role', true) AS runtime_checked \gset
DO $verify$
DECLARE
    runtime pg_roles%ROWTYPE;
BEGIN
    SELECT * INTO STRICT runtime FROM pg_roles
    WHERE rolname = current_setting('work_identity.runtime_role');
    IF NOT runtime.rolcanlogin OR runtime.rolinherit OR runtime.rolsuper
        OR runtime.rolcreaterole OR runtime.rolcreatedb OR runtime.rolreplication
        OR runtime.rolbypassrls OR EXISTS (
            SELECT 1 FROM pg_auth_members WHERE member = runtime.oid
        )
    THEN
        RAISE EXCEPTION 'work identity runtime role has incompatible attributes or memberships';
    END IF;
END
$verify$;

CREATE TEMP TABLE identity_column_grants (
    relation_name name,
    privilege text,
    columns name[]
) ON COMMIT DROP;
INSERT INTO pg_temp.identity_column_grants VALUES
    ('schema_migrations', 'SELECT', '{version}'),
    ('work_identity_workspace', 'SELECT', '{id,label,next_ordinal,next_change}'),
    ('work_identity_workspace', 'INSERT', '{id,label}'),
    ('work_identity_workspace', 'UPDATE', '{next_ordinal,next_change}'),
    ('work_identity_ask', 'SELECT', '{workspace,id,ordinal,body}'),
    ('work_identity_ask', 'INSERT', '{workspace,id,ordinal,body}'),
    ('work_identity_ask', 'UPDATE', '{body}'),
    ('work_identity_address', 'SELECT', '{workspace,address,id}'),
    ('work_identity_address', 'INSERT', '{workspace,address,id}'),
    ('work_identity_address', 'UPDATE', '{id}'),
    ('work_identity_create', 'SELECT', '{workspace,key,payload,id}'),
    ('work_identity_create', 'INSERT', '{workspace,key,payload,id}'),
    ('work_identity_change', 'SELECT', '{workspace,ordinal,id,event,body}'),
    ('work_identity_change', 'INSERT', '{workspace,ordinal,id,event,body}');

GRANT USAGE ON SCHEMA work_identity TO :"runtime_role";
SELECT format('GRANT %s (%s) ON work_identity.%I TO %I', privilege,
              (SELECT string_agg(quote_ident(column_name), ', ')
               FROM unnest(columns) AS column_name), relation_name, :'runtime_role')
FROM pg_temp.identity_column_grants
\gexec

DO $audit$
DECLARE
    runtime text := current_setting('work_identity.runtime_role');
BEGIN
    IF NOT has_database_privilege(runtime, current_database(), 'CONNECT')
        OR has_database_privilege(runtime, current_database(), 'CREATE')
        OR EXISTS (
            SELECT 1 FROM pg_namespace n
            -- PostgreSQL reports the current session's temporary schema
            -- privileges via the database TEMP grant.
            WHERE n.oid <> pg_my_temp_schema()
              AND has_schema_privilege(runtime, n.oid, 'CREATE')
        )
        OR EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname NOT LIKE 'pg_temp_%' AND n.nspname NOT LIKE 'pg_toast%'
              AND has_schema_privilege(runtime, n.oid, 'USAGE')
              AND (
                  (c.relkind IN ('r', 'p', 'v', 'm', 'f') AND (
                      has_table_privilege(runtime, c.oid, 'DELETE, TRUNCATE, TRIGGER')
                      OR has_any_column_privilege(runtime, c.oid, 'REFERENCES')
                      OR (n.nspname <> 'work_identity' AND
                          has_any_column_privilege(runtime, c.oid, 'SELECT, INSERT, UPDATE'))
                      OR (n.nspname = 'work_identity' AND
                          has_table_privilege(runtime, c.oid, 'SELECT, INSERT, UPDATE'))
                  ))
                  OR (c.relkind = 'S' AND has_sequence_privilege(runtime, c.oid, 'USAGE, SELECT, UPDATE'))
              )
        )
        OR has_schema_privilege(runtime, 'work_identity', 'USAGE WITH GRANT OPTION')
        OR EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
            CROSS JOIN (VALUES ('SELECT'), ('INSERT'), ('UPDATE')) AS p(privilege)
            WHERE c.relnamespace = 'work_identity'::regnamespace
              AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND a.attnum > 0 AND NOT a.attisdropped
              AND (
                  has_column_privilege(runtime, c.oid, a.attnum, p.privilege || ' WITH GRANT OPTION')
                  OR has_column_privilege(runtime, c.oid, a.attnum, p.privilege) <> EXISTS (
                      SELECT 1 FROM pg_temp.identity_column_grants allowed
                      WHERE allowed.relation_name = c.relname AND allowed.privilege = p.privilege
                        AND a.attname = ANY (allowed.columns)
                  )
              )
        )
        OR EXISTS (
            SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND p.prosecdef AND has_schema_privilege(runtime, n.oid, 'USAGE')
              AND has_function_privilege(runtime, p.oid, 'EXECUTE')
        )
    THEN
        RAISE EXCEPTION 'work identity runtime role has unexpected effective privileges';
    END IF;
END
$audit$;
COMMIT;
