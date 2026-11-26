\set ON_ERROR_STOP on
-- Reuse the owner, locking, and fingerprint checks from the initial migration.
\set work_identity_upgrade_v2 true
\ir 001.sql
\unset work_identity_upgrade_v2
