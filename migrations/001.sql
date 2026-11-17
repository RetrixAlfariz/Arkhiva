CREATE TABLE papers (
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    abstract TEXT,
    categories_json TEXT NOT NULL CHECK (json_valid(categories_json)),
    first_submitted_on TEXT,
    last_revised_on TEXT,
    metadata_updated_on TEXT,
    oai_datestamp TEXT,
    is_deleted INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted IN (0, 1)),
    PRIMARY KEY (provider, external_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE snapshot_imports (
    provider TEXT PRIMARY KEY,
    release TEXT,
    sha256 TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    next_offset INTEGER NOT NULL DEFAULT 0,
    lines INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    rejected INTEGER NOT NULL DEFAULT 0,
    complete INTEGER NOT NULL DEFAULT 0 CHECK (complete IN (0, 1)),
    validation_json TEXT
) STRICT;

CREATE TABLE snapshot_rejects (
    provider TEXT NOT NULL REFERENCES snapshot_imports(provider),
    byte_offset INTEGER NOT NULL,
    line INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (provider, byte_offset)
) STRICT, WITHOUT ROWID;

CREATE TABLE oai_sync_state (
    feed TEXT PRIMARY KEY,
    bootstrap_from TEXT NOT NULL,
    bootstrap_basis TEXT NOT NULL,
    next_from TEXT NOT NULL,
    active_from TEXT,
    first_response TEXT,
    token TEXT,
    completed_harvests INTEGER NOT NULL DEFAULT 0,
    committed_pages INTEGER NOT NULL DEFAULT 0,
    committed_changes INTEGER NOT NULL DEFAULT 0
) STRICT;
