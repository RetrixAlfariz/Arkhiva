CREATE TABLE parquet_files (
    file_id INTEGER PRIMARY KEY,
    rows INTEGER NOT NULL CHECK (rows >= 0),
    bytes INTEGER NOT NULL CHECK (bytes >= 0)
) STRICT;

CREATE TABLE paper_index (
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    file_id INTEGER NOT NULL REFERENCES parquet_files(file_id),
    row_no INTEGER NOT NULL CHECK (row_no >= 0),
    PRIMARY KEY (provider, external_id)
) STRICT, WITHOUT ROWID;
