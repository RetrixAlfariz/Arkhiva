CREATE TABLE IF NOT EXISTS search_topics (
    name TEXT PRIMARY KEY,
    prompt TEXT NOT NULL,
    plan_json TEXT NOT NULL CHECK (json_valid(plan_json))
) STRICT, WITHOUT ROWID;
