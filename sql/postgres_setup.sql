-- Run this once, connected as a superuser (e.g. `psql -U postgres`),
-- to create the database used by the pipeline.
-- The database name here must match POSTGRES_DB in your .env file.

CREATE DATABASE realtime_events;

-- After this, connect to the new database (\c realtime_events in psql)
-- and run create_tables.sql.
