\set ON_ERROR_STOP on
BEGIN;
SET LOCAL search_path = pg_catalog, pg_temp;
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '20s';
-- The operator verifies the UUID and exact namespace entitlement before use.
INSERT INTO work_identity.work_identity_workspace (id, label)
VALUES (:'workspace_id'::uuid, :'workspace_label')
ON CONFLICT (id) DO NOTHING;
SELECT true AS seed_matches FROM work_identity.work_identity_workspace
WHERE id = :'workspace_id'::uuid AND label = :'workspace_label' \gset
-- No returned row is a psql error and rolls back the transaction.
COMMIT;
