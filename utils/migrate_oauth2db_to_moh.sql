\echo
\echo '=== Starting migration: oauth2-db -> moh, role oauth2-db -> project_tracking_api ==='

-- Stop on first error
\set ON_ERROR_STOP on

-- -----------------------------------------------------------------------------
-- Parameters
-- -----------------------------------------------------------------------------
\set old_db      'oauth2-db'
\set new_db      'moh'
\set old_role    'oauth2-db'
\set app_role    project_tracking_api

-- Always start from a safe control DB (not the one we might rename)
\c postgres

\echo '--- Step 1: Rename role old_role -> app_role (rename only; no creation, no password changes)'

DO $do$
DECLARE
  has_old boolean;
  has_new boolean;
BEGIN
  SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = :'old_role') INTO has_old;
  SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = :'app_role') INTO has_new;

  IF has_old AND NOT has_new THEN
    RAISE NOTICE 'Renaming role % -> %', :'old_role', :'app_role';
    EXECUTE format('ALTER ROLE %I RENAME TO %I', :'old_role', :'app_role');
  ELSIF has_new AND NOT has_old THEN
    RAISE NOTICE 'Role % already present; nothing to rename', :'app_role';
  ELSIF has_old AND has_new THEN
    RAISE EXCEPTION 'Both roles % and % exist; refusing to continue', :'old_role', :'app_role';
  ELSE
    RAISE NOTICE 'Neither role % nor % exists; nothing to rename', :'old_role', :'app_role';
  END IF;
END
$do$;

\echo '--- Step 2: Rename DB old_db -> new_db (rename only; terminate active connections first)'

DO $do$
DECLARE
  has_old_db boolean;
  has_new_db boolean;
BEGIN
  SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = :'old_db') INTO has_old_db;
  SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = :'new_db') INTO has_new_db;

  IF has_old_db AND NOT has_new_db THEN
    RAISE NOTICE 'Terminating connections to %', :'old_db';
    PERFORM pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = :'old_db'
      AND pid <> pg_backend_pid();

    RAISE NOTICE 'Renaming database % -> %', :'old_db', :'new_db';
    EXECUTE format('ALTER DATABASE %I RENAME TO %I', :'old_db', :'new_db');
  ELSIF has_new_db AND NOT has_old_db THEN
    RAISE NOTICE 'Database % already present; nothing to rename', :'new_db';
  ELSIF has_old_db AND has_new_db THEN
    RAISE EXCEPTION 'Both databases % and % exist; refusing to continue', :'old_db', :'new_db';
  ELSE
    RAISE NOTICE 'Neither % nor % exist; nothing to rename', :'old_db', :'new_db';
  END IF;
END
$do$;

\echo '--- Step 3: Sanity checks'

SELECT rolname
FROM pg_roles
WHERE rolname IN (:'old_role', :'app_role')
ORDER BY rolname;

SELECT datname
FROM pg_database
WHERE datname IN (:'old_db', :'new_db')
ORDER BY datname;

\echo '=== Migration complete. Point your app at: postgresql://project_tracking_api:********@<host>:<port>/moh ==='
\echo