package main

import (
	"context"
	"fmt"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// lookupEntrySQL and upsertEntrySQL are the per-request hot-path queries.
// sqlite.Conn caches their prepared statements independently on each pooled
// connection.
const lookupEntrySQL = `
SELECT action_id, output_id, size, compressed_size, created_at, accessed_at
FROM entries
WHERE action_id = ?`

const upsertEntrySQL = `
INSERT INTO entries(action_id, output_id, size, compressed_size, created_at, accessed_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(action_id) DO UPDATE SET
	output_id = excluded.output_id,
	size = excluded.size,
	compressed_size = excluded.compressed_size,
	created_at = excluded.created_at,
	accessed_at = excluded.accessed_at,
	blob_type = CASE WHEN entries.output_id = excluded.output_id THEN entries.blob_type END,
	blob_type_version = CASE WHEN entries.output_id = excluded.output_id THEN entries.blob_type_version END,
	retained_type = CASE WHEN entries.output_id = excluded.output_id THEN entries.retained_type END,
	retained_type_version = CASE WHEN entries.output_id = excluded.output_id THEN entries.retained_type_version END`

const touchEntrySQL = `
UPDATE entries
SET accessed_at = ?
WHERE action_id = ?`

type catalog struct {
	db *sqliteDB
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
	blobType       optionalInt64
	retainedType   optionalInt64
}

type optionalInt64 struct {
	value int64
	ok    bool
}

func newCatalog(db *sqliteDB) *catalog {
	return &catalog{db: db}
}

func (c *catalog) useConn(ctx context.Context, fn func(*sqlite.Conn) error) error {
	return c.db.withConn(ctx, fn)
}

func (c *catalog) registerRun(ctx context.Context, runID, path, lockPath string, createdAt int64) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		return execute(conn, `
INSERT OR REPLACE INTO runs(run_id, path, lock_path, created_at)
VALUES (?, ?, ?, ?)`,
			runID, path, lockPath, createdAt,
		)
	})
}

func (c *catalog) listOtherRuns(ctx context.Context, runID string) ([]catalogRun, error) {
	var runs []catalogRun
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn, `
SELECT run_id, path, lock_path
FROM runs
WHERE run_id != ?`, &sqlitex.ExecOptions{
			Args: []any{runID},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				runs = append(runs, catalogRun{
					runID:    stmt.ColumnText(0),
					path:     stmt.ColumnText(1),
					lockPath: stmt.ColumnText(2),
				})
				return nil
			},
		})
	})
	return runs, err
}

func (c *catalog) countRuns(ctx context.Context) (int64, error) {
	var count int64
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		var err error
		count, err = queryInt64(conn, `SELECT COUNT(*) FROM runs`, nil)
		return err
	})
	return count, err
}

func (c *catalog) deleteRun(ctx context.Context, runID string) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		return execute(conn, `
DELETE FROM runs
WHERE run_id = ?`, runID)
	})
}

func (c *catalog) upsertEntry(ctx context.Context, ent entry) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		return execPrepared(conn, upsertEntrySQL, func(stmt *sqlite.Stmt) {
			stmt.BindText(1, ent.ActionID)
			stmt.BindText(2, ent.OutputID)
			stmt.BindInt64(3, ent.Size)
			stmt.BindInt64(4, ent.CompressedSize)
			stmt.BindInt64(5, unixMillis(ent.CreatedAt))
			stmt.BindInt64(6, unixMillis(ent.AccessedAt))
		})
	})
}

func (c *catalog) lookupEntry(ctx context.Context, actionID string) (entry, bool, error) {
	var ent entry
	var createdAt, accessedAt int64
	var found bool
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		var err error
		found, err = queryPrepared(conn, lookupEntrySQL, func(stmt *sqlite.Stmt) {
			stmt.BindText(1, actionID)
		}, func(stmt *sqlite.Stmt) {
			ent.ActionID = stmt.ColumnText(0)
			ent.OutputID = stmt.ColumnText(1)
			ent.Size = stmt.ColumnInt64(2)
			ent.CompressedSize = stmt.ColumnInt64(3)
			createdAt = stmt.ColumnInt64(4)
			accessedAt = stmt.ColumnInt64(5)
		})
		return err
	})
	if err != nil {
		return entry{}, false, err
	}
	if !found {
		return entry{}, false, nil
	}
	ent.CreatedAt = millisTime(createdAt)
	ent.AccessedAt = millisTime(accessedAt)
	return ent, true, nil
}

func touchEntries(conn *sqlite.Conn, accessed map[string]int64) error {
	for actionID, accessedAt := range accessed {
		if err := execPrepared(conn, touchEntrySQL, func(stmt *sqlite.Stmt) {
			stmt.BindInt64(1, accessedAt)
			stmt.BindText(2, actionID)
		}); err != nil {
			return fmt.Errorf("touch entry: %w", err)
		}
	}
	return nil
}

func (c *catalog) deleteEntriesByOutputID(ctx context.Context, outputID string) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		return execute(conn, `
DELETE FROM entries
WHERE output_id = ?`, outputID)
	})
}

func (c *catalog) deleteEntriesAccessedBefore(ctx context.Context, cutoff int64) (int64, error) {
	var removed int64
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		if err := execute(conn, `
DELETE FROM entries
WHERE accessed_at < ?`, cutoff); err != nil {
			return err
		}
		removed = int64(conn.Changes())
		return nil
	})
	return removed, err
}

func (c *catalog) compressedSize(ctx context.Context) (int64, error) {
	var size int64
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		var err error
		size, err = queryInt64(conn, `
SELECT CAST(COALESCE(SUM(compressed_size), 0) AS INTEGER)
FROM (
	SELECT output_id, MAX(compressed_size) AS compressed_size
	FROM entries
	GROUP BY output_id
)`, nil)
		return err
	})
	return size, err
}

// listOutputs returns one row per output with its uncompressed and compressed
// size. When includeBlobType is set it also returns the cached blob
// classification (blobType), but only for entries classified at
// classifierVersion; classifications from older versions are treated as absent
// so they get recomputed. blobType is only available on migrated caches.
func (c *catalog) listOutputs(
	ctx context.Context,
	includeBlobType bool,
	blobClassifierVersion int64,
	includeRetainedType bool,
	retainedClassifierVersion int64,
) ([]catalogOutput, error) {
	columns := "output_id, CAST(MAX(size) AS INTEGER), CAST(MAX(compressed_size) AS INTEGER)"
	args := []any(nil)
	if includeBlobType {
		columns += ", MAX(CASE WHEN blob_type_version = ? THEN blob_type END)"
		args = append(args, blobClassifierVersion)
	}
	if includeRetainedType {
		columns += ", MAX(CASE WHEN retained_type_version = ? THEN retained_type END)"
		args = append(args, retainedClassifierVersion)
	}
	var outputs []catalogOutput
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn, "SELECT "+columns+" FROM entries GROUP BY output_id", &sqlitex.ExecOptions{
			Args: args,
			ResultFunc: func(stmt *sqlite.Stmt) error {
				output := catalogOutput{
					outputID:       stmt.ColumnText(0),
					size:           stmt.ColumnInt64(1),
					compressedSize: stmt.ColumnInt64(2),
				}
				column := 3
				if includeBlobType {
					if !stmt.ColumnIsNull(column) {
						output.blobType = optionalInt64{value: stmt.ColumnInt64(column), ok: true}
					}
					column++
				}
				if includeRetainedType && !stmt.ColumnIsNull(column) {
					output.retainedType = optionalInt64{value: stmt.ColumnInt64(column), ok: true}
				}
				outputs = append(outputs, output)
				return nil
			},
		})
	})
	return outputs, err
}

func (c *catalog) referencedOutputIDs(ctx context.Context, lower, upper string, outputIDs map[string]struct{}) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		clear(outputIDs)
		return sqlitex.Execute(conn, `
SELECT DISTINCT output_id
FROM entries
WHERE output_id >= ? AND output_id < ?`, &sqlitex.ExecOptions{
			Args: []any{lower, upper},
			ResultFunc: func(stmt *sqlite.Stmt) error {
				outputIDs[stmt.ColumnText(0)] = struct{}{}
				return nil
			},
		})
	})
}

func (c *catalog) updateBlobType(ctx context.Context, outputID string, kind blobTypeKind, classifierVersion int64) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		return updateBlobType(conn, outputID, kind, classifierVersion)
	})
}

func updateBlobType(conn *sqlite.Conn, outputID string, kind blobTypeKind, classifierVersion int64) error {
	return execute(conn, `
UPDATE entries
SET blob_type = ?, blob_type_version = ?
WHERE output_id = ?`, int64(kind), classifierVersion, outputID)
}

func (c *catalog) updateRetainedType(ctx context.Context, outputID string, kind retainedTypeKind) error {
	return c.useConn(ctx, func(conn *sqlite.Conn) error {
		return updateRetainedType(conn, outputID, kind)
	})
}

func updateRetainedType(conn *sqlite.Conn, outputID string, kind retainedTypeKind) error {
	return execute(conn, `
UPDATE entries
SET retained_type = ?, retained_type_version = ?
WHERE output_id = ?`, int64(kind), retainedClassifierVersion, outputID)
}

func entriesHasColumn(conn *sqlite.Conn, column string) (bool, error) {
	found := false
	err := sqlitex.Execute(conn, `SELECT name FROM pragma_table_info('entries')`, &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			if stmt.ColumnText(0) == column {
				found = true
			}
			return nil
		},
	})
	return found, err
}

func (c *catalog) pruneCandidates(ctx context.Context) ([]pruneCandidate, error) {
	var candidates []pruneCandidate
	err := c.useConn(ctx, func(conn *sqlite.Conn) error {
		return sqlitex.Execute(conn, `
SELECT e.output_id, CAST(MAX(e.compressed_size) AS INTEGER)
FROM entries AS e
GROUP BY e.output_id
ORDER BY MAX(e.accessed_at)`, &sqlitex.ExecOptions{
			ResultFunc: func(stmt *sqlite.Stmt) error {
				candidates = append(candidates, pruneCandidate{
					outputID: stmt.ColumnText(0),
					size:     stmt.ColumnInt64(1),
				})
				return nil
			},
		})
	})
	return candidates, err
}
