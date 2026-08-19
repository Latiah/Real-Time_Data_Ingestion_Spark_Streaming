-- One-shot database setup: creates the `realtime_events` database and the
-- `events` table inside it.
--
-- Run once, connected as a superuser, from the repository root:
--     psql -h <host> -p <port> -U postgres -f sql/postgres_setup.sql
--
-- Under Docker this script is not needed: the postgres service creates the
-- database from POSTGRES_DB, and the db-init service applies create_tables.sql.
--
-- Safe to re-run: the CREATE DATABASE is guarded, and create_tables.sql uses
-- CREATE TABLE / CREATE INDEX ... IF NOT EXISTS throughout.

-- CREATE DATABASE cannot appear in an IF NOT EXISTS form, and it cannot run
-- inside a transaction or a DO block. The standard workaround is to have the
-- server generate the statement only when the database is absent, then let
-- psql execute that result via \gexec.
SELECT 'CREATE DATABASE realtime_events'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'realtime_events'
)\gexec

-- Reconnect to the new database; the table must be created inside it, not in
-- the maintenance database this script started from.
\connect realtime_events

-- \ir resolves relative to this script's own directory, so the include works
-- regardless of the working directory psql was invoked from.
\ir create_tables.sql
