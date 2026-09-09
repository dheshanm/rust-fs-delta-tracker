-- This SQL script initializes the database schema for the filesystem module.
-- It creates the necessary tables and schema for tracking file changes, scans, and staging files.
-- Ensure filesystem schema exists
-- If the schema already exists, it will not be recreated.
CREATE SCHEMA IF NOT EXISTS filesystem;

-- Drop existing tables to ensure a clean slate
DROP TABLE IF EXISTS filesystem.file_changes CASCADE;

DROP TABLE IF EXISTS filesystem.files CASCADE;

DROP TABLE IF EXISTS filesystem.scan_runs CASCADE;

DROP TABLE IF EXISTS filesystem.staging_files CASCADE;

-- Drop legacy function if it exists
DROP FUNCTION IF EXISTS filesystem.text_to_ltree(TEXT);

-- Ensure the ltree extension is available
-- This extension is used for hierarchical data representation, which is useful for file paths.
CREATE EXTENSION IF NOT EXISTS ltree;

-- Create function to convert text paths to ltree format.
-- For directories (is_dir = TRUE): keep ALL path segments.
-- For files/links (is_dir = FALSE): drop the last segment (file name).
CREATE
OR REPLACE FUNCTION filesystem.entry_to_ltree(path TEXT, is_dir BOOLEAN) RETURNS ltree LANGUAGE sql IMMUTABLE AS $$
SELECT
    array_to_string(
        ARRAY(
            SELECT
                regexp_replace(seg, '[^A-Za-z0-9_]', '', 'g')
            FROM
                unnest(
                    regexp_split_to_array(btrim(path, '/'), '/')
                ) WITH ORDINALITY AS t(seg, idx)
            WHERE
                -- For directories: keep all segments
                -- For files/links: drop the last segment (file name)
                is_dir OR idx < (
                    SELECT
                        max(idx)
                    FROM
                        unnest(regexp_split_to_array(btrim(path, '/'), '/')) WITH ORDINALITY AS t2(_, idx)
                )
        ),
        '.'
    ) :: ltree $$;

-- Create the tables (and indices) for the filesystem schema
CREATE TABLE IF NOT EXISTS filesystem.scan_runs (
    scan_id SERIAL PRIMARY KEY,
    scan_root TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ NULL,
    total_paths_count BIGINT NULL,
    added_files_count BIGINT NULL,
    modified_files_count BIGINT NULL,
    removed_files_count BIGINT NULL,
    new_data_mb FLOAT NULL,
    modified_data_mb FLOAT NULL,
    deleted_data_mb FLOAT NULL,
    scan_metadata JSONB NULL
);

CREATE TABLE IF NOT EXISTS filesystem.files (
    file_name TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    file_extension TEXT NULL,
    file_size_bytes BIGINT NOT NULL,
    file_path TEXT PRIMARY KEY,
    file_mtime TIMESTAMPTZ NOT NULL,
    file_fingerprint TEXT NULL,
    uid INTEGER NULL,
    gid INTEGER NULL,
    permissions INTEGER NULL,
    last_seen_scan INT NOT NULL REFERENCES filesystem.scan_runs(scan_id) ON UPDATE CASCADE ON DELETE CASCADE,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    path_ltree ltree GENERATED ALWAYS AS (
        filesystem.entry_to_ltree(file_path, entry_type = 'D')
    ) STORED,
    CONSTRAINT file_path_unique UNIQUE (file_path)
);

CREATE INDEX ON filesystem.files (last_seen_scan);

CREATE INDEX ON filesystem.files USING GIST (path_ltree);

CREATE TABLE IF NOT EXISTS filesystem.file_changes (
    scan_id INT NOT NULL REFERENCES filesystem.scan_runs(scan_id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    change_type TEXT NOT NULL,
    old_size_bytes BIGINT NULL,
    new_size_bytes BIGINT NULL,
    old_mtime TIMESTAMPTZ NULL,
    new_mtime TIMESTAMPTZ NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    entry_type TEXT NULL,
    path_ltree ltree GENERATED ALWAYS AS (
        filesystem.entry_to_ltree(file_path, COALESCE(entry_type, 'F') = 'D')
    ) STORED,
    PRIMARY KEY (scan_id, file_path)
);

CREATE INDEX ON filesystem.file_changes (change_type);
CREATE INDEX ON filesystem.file_changes (scan_id, change_type);

CREATE UNLOGGED TABLE filesystem.staging_files (
    scan_id INT NOT NULL REFERENCES filesystem.scan_runs(scan_id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    entry_type TEXT NOT NULL,
    file_extension TEXT NULL,
    file_size_bytes BIGINT NOT NULL,
    file_mtime TIMESTAMPTZ NOT NULL,
    file_fingerprint TEXT NULL,
    uid INTEGER NULL,
    gid INTEGER NULL,
    permissions INTEGER NULL,
    PRIMARY KEY (scan_id, file_path)
);

CREATE INDEX ON filesystem.staging_files (scan_id, file_path);
