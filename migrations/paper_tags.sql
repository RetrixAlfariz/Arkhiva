CREATE TABLE paper_tags (
    tag TEXT NOT NULL,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    PRIMARY KEY (tag, provider, external_id),
    FOREIGN KEY (provider, external_id) REFERENCES paper_index(provider, external_id)
) STRICT, WITHOUT ROWID;
