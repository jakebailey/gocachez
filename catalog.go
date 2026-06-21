package main

import (
	"context"
	"database/sql"
)

type catalogDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type catalog struct {
	db catalogDB
}

type catalogRun struct {
	runID    string
	path     string
	lockPath string
}

type catalogOutput struct {
	outputID       string
	size           int64
	compressedSize int64
}

func newCatalog(db catalogDB) *catalog {
	return &catalog{db: db}
}

func (c *catalog) withTx(tx *sql.Tx) *catalog {
	return &catalog{db: tx}
}

func (c *catalog) registerRun(ctx context.Context, runID, path, lockPath string, createdAt int64) error {
	_, err := c.db.ExecContext(ctx, `
INSERT OR REPLACE INTO runs(run_id, path, lock_path, created_at)
VALUES (?, ?, ?, ?)`,
		runID, path, lockPath, createdAt,
	)
	return err
}

func (c *catalog) listOtherRuns(ctx context.Context, runID string) ([]catalogRun, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT run_id, path, lock_path
FROM runs
WHERE run_id != ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var runs []catalogRun
	for rows.Next() {
		var run catalogRun
		if err := rows.Scan(&run.runID, &run.path, &run.lockPath); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return runs, nil
}

func (c *catalog) countRuns(ctx context.Context) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&count)
	return count, err
}

func (c *catalog) deleteRun(ctx context.Context, runID string) error {
	_, err := c.db.ExecContext(ctx, `
DELETE FROM runs
WHERE run_id = ?`, runID)
	return err
}

func (c *catalog) upsertEntry(ctx context.Context, ent entry) error {
	_, err := c.db.ExecContext(ctx, `
INSERT INTO entries(action_id, output_id, size, compressed_size, created_at, accessed_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(action_id) DO UPDATE SET
	output_id = excluded.output_id,
	size = excluded.size,
	compressed_size = excluded.compressed_size,
	created_at = excluded.created_at,
	accessed_at = excluded.accessed_at`,
		ent.ActionID,
		ent.OutputID,
		ent.Size,
		ent.CompressedSize,
		unixMillis(ent.CreatedAt),
		unixMillis(ent.AccessedAt),
	)
	return err
}

func (c *catalog) lookupEntry(ctx context.Context, actionID string) (entry, error) {
	var ent entry
	var createdAt, accessedAt int64
	err := c.db.QueryRowContext(ctx, `
SELECT action_id, output_id, size, compressed_size, created_at, accessed_at
FROM entries
WHERE action_id = ?`, actionID).Scan(
		&ent.ActionID,
		&ent.OutputID,
		&ent.Size,
		&ent.CompressedSize,
		&createdAt,
		&accessedAt,
	)
	if err != nil {
		return entry{}, err
	}
	ent.CreatedAt = millisTime(createdAt)
	ent.AccessedAt = millisTime(accessedAt)
	return ent, nil
}

func (c *catalog) touchEntry(ctx context.Context, actionID string, accessedAt int64) error {
	_, err := c.db.ExecContext(ctx, `
UPDATE entries
SET accessed_at = ?
WHERE action_id = ?`, accessedAt, actionID)
	return err
}

func (c *catalog) deleteEntriesByOutputID(ctx context.Context, outputID string) error {
	_, err := c.db.ExecContext(ctx, `
DELETE FROM entries
WHERE output_id = ?`, outputID)
	return err
}

func (c *catalog) compressedSize(ctx context.Context) (int64, error) {
	var size int64
	err := c.db.QueryRowContext(ctx, `
SELECT CAST(COALESCE(SUM(compressed_size), 0) AS INTEGER)
FROM (
	SELECT output_id, MAX(compressed_size) AS compressed_size
	FROM entries
	GROUP BY output_id
)`).Scan(&size)
	return size, err
}

func catalogSize(ctx context.Context, db catalogDB) (int64, error) {
	var size int64
	err := db.QueryRowContext(ctx, `
SELECT CAST(COALESCE(SUM(size), 0) AS INTEGER)
FROM (
	SELECT output_id, MAX(size) AS size
	FROM entries
	GROUP BY output_id
)`).Scan(&size)
	return size, err
}

func (c *catalog) listOutputs(ctx context.Context) ([]catalogOutput, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT output_id, CAST(MAX(size) AS INTEGER), CAST(MAX(compressed_size) AS INTEGER)
FROM entries
GROUP BY output_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var outputs []catalogOutput
	for rows.Next() {
		var output catalogOutput
		if err := rows.Scan(&output.outputID, &output.size, &output.compressedSize); err != nil {
			return nil, err
		}
		outputs = append(outputs, output)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (c *catalog) pruneCandidates(ctx context.Context) ([]pruneCandidate, error) {
	rows, err := c.db.QueryContext(ctx, `
SELECT e.output_id, CAST(MAX(e.compressed_size) AS INTEGER)
FROM entries AS e
GROUP BY e.output_id
ORDER BY MAX(e.accessed_at)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var candidates []pruneCandidate
	for rows.Next() {
		var candidate pruneCandidate
		if err := rows.Scan(&candidate.outputID, &candidate.size); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (c *catalog) countEntriesByOutputID(ctx context.Context, outputID string) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM entries
WHERE output_id = ?`, outputID).Scan(&count)
	return count, err
}
