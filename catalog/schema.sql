CREATE TABLE IF NOT EXISTS entries (
	action_id TEXT PRIMARY KEY,
	output_id TEXT NOT NULL,
	size INTEGER NOT NULL,
	compressed_size INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	accessed_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS entries_output_id ON entries(output_id);
CREATE INDEX IF NOT EXISTS entries_accessed_at ON entries(accessed_at);

CREATE TABLE IF NOT EXISTS runs (
	run_id TEXT PRIMARY KEY,
	path TEXT NOT NULL,
	lock_path TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
