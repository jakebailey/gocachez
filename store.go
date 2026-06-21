package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

const cacheSchemaVersion = 1

const catalogSchema = `
CREATE TABLE IF NOT EXISTS entries (
	action_id TEXT PRIMARY KEY,
	output_id TEXT NOT NULL,
	size INTEGER NOT NULL,
	compressed_size INTEGER NOT NULL,
	executable_name TEXT NOT NULL DEFAULT '',
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
`

type entry struct {
	ActionID       string
	OutputID       string
	Size           int64
	CompressedSize int64
	ExecutableName string
	CreatedAt      time.Time
	AccessedAt     time.Time
}

type store struct {
	config

	db                *sql.DB
	q                 *catalog
	versionDir        string
	blobsDir          string
	liveRoot          string
	lifecycleLockPath string
	runID             string
	runDir            string
	runLock           *flock.Flock
	mu                sync.Mutex
	encoderPool       sync.Pool
	decoderPool       sync.Pool
	materialized      map[string]string
	accessed          map[string]int64
}

const retainedDirName = "retained"

func newStore(cfg config) (*store, error) {
	versionDir, blobsDir, liveRoot, lifecycleLockPath := cachePaths(cfg)
	if err := os.MkdirAll(versionDir, 0o777); err != nil {
		return nil, fmt.Errorf("create version dir: %w", err)
	}

	var st *store
	err := withFileLock(lifecycleLockPath, func() error {
		var err error
		st, err = newStoreLocked(cfg, versionDir, blobsDir, liveRoot, lifecycleLockPath)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	return st, nil
}

func cachePaths(cfg config) (string, string, string, string) {
	versionDir := filepath.Join(cfg.dir, fmt.Sprintf("v%d", cacheSchemaVersion))
	return versionDir,
		filepath.Join(versionDir, "blobs"),
		filepath.Join(versionDir, "live"),
		filepath.Join(versionDir, "lifecycle.lock")
}

func retainedRoot(versionDir string) string {
	return filepath.Join(versionDir, retainedDirName)
}

func newStoreLocked(cfg config, versionDir, blobsDir, liveRoot, lifecycleLockPath string) (*store, error) {
	if err := os.MkdirAll(blobsDir, 0o777); err != nil {
		return nil, fmt.Errorf("create blobs dir: %w", err)
	}
	if err := os.MkdirAll(liveRoot, 0o777); err != nil {
		return nil, fmt.Errorf("create live dir: %w", err)
	}
	runDir, err := os.MkdirTemp(liveRoot, "run-")
	if err != nil {
		return nil, fmt.Errorf("create live run dir: %w", err)
	}
	runID := filepath.Base(runDir)
	runLock := flock.New(filepath.Join(runDir, "run.lock"))
	if err := runLock.Lock(); err != nil {
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, fmt.Errorf("lock live run: %w", err)
	}

	db, err := openDB(filepath.Join(versionDir, "cache.db"))
	if err != nil {
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	st := &store{
		config:            cfg,
		db:                db,
		q:                 newCatalog(db),
		versionDir:        versionDir,
		blobsDir:          blobsDir,
		liveRoot:          liveRoot,
		lifecycleLockPath: lifecycleLockPath,
		runID:             runID,
		runDir:            runDir,
		runLock:           runLock,
		materialized:      make(map[string]string),
		accessed:          make(map[string]int64),
	}
	if err := st.registerRun(); err != nil {
		_ = db.Close()
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	return st, nil
}

func (st *store) withLifecycleLock(fn func() error) error {
	return withFileLock(st.lifecycleLockPath, fn)
}

func withFileLock(path string, fn func() error) error {
	lock := flock.New(path)
	if err := lock.Lock(); err != nil {
		_ = lock.Close()
		return fmt.Errorf("lock cache lifecycle: %w", err)
	}

	var err error
	err = errors.Join(err, fn())
	if unlockErr := lock.Unlock(); unlockErr != nil {
		err = errors.Join(err, fmt.Errorf("unlock cache lifecycle: %w", unlockErr))
	}
	if closeErr := lock.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close cache lifecycle lock: %w", closeErr))
	}
	return err
}

func openDB(path string) (*sql.DB, error) {
	dsn := "file:" + url.PathEscape(filepath.ToSlash(path)) + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	conns := min(max(runtime.GOMAXPROCS(0), 1), 8)
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := initDB(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openExistingDB(path string) (*sql.DB, error) {
	dsn := "file:" + url.PathEscape(filepath.ToSlash(path)) + "?mode=ro&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("read catalog version: %w", err)
	}
	if version != cacheSchemaVersion {
		_ = db.Close()
		return nil, fmt.Errorf("unsupported catalog version %d, want %d", version, cacheSchemaVersion)
	}
	return db, nil
}

func initDB(db *sql.DB) error {
	ctx := context.Background()
	var version int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read catalog version: %w", err)
	}
	if version != 0 && version != cacheSchemaVersion {
		return fmt.Errorf("unsupported catalog version %d, want %d", version, cacheSchemaVersion)
	}
	if _, err := db.ExecContext(ctx, catalogSchema); err != nil {
		return fmt.Errorf("initialize catalog: %w", err)
	}
	if err := ensureCatalogColumns(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, cacheSchemaVersion)); err != nil {
		return fmt.Errorf("write catalog version: %w", err)
	}
	return nil
}

func ensureCatalogColumns(ctx context.Context, db *sql.DB) error {
	hasExecutableName, err := columnExists(ctx, db, "entries", "executable_name")
	if err != nil {
		return err
	}
	if !hasExecutableName {
		if _, err := db.ExecContext(ctx, `ALTER TABLE entries ADD COLUMN executable_name TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add executable name column: %w", err)
		}
	}
	return nil
}

func columnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, fmt.Errorf("read table info: %w", err)
	}
	defer rows.Close() //nolint:errcheck

	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var defaultValue sql.NullString
		var primaryKey int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, fmt.Errorf("scan table info: %w", err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read table info: %w", err)
	}
	return false, nil
}

func (st *store) close() {
	if err := st.flushAccessTimes(); err != nil && st.verbose {
		log.Printf("gocachez: flush access times failed: %v", err)
	}
	if err := st.unregisterRun(); err != nil && st.verbose {
		log.Printf("gocachez: unregister run failed: %v", err)
	}
	if err := st.prune(); err != nil && st.verbose {
		log.Printf("gocachez: prune failed: %v", err)
	}
	if err := st.db.Close(); err != nil && st.verbose {
		log.Printf("gocachez: close catalog failed: %v", err)
	}
}

func (st *store) registerRun() error {
	now := unixMillis(time.Now())
	if err := st.q.registerRun(context.Background(), st.runID, st.runDir, st.runLock.Path(), now); err != nil {
		return fmt.Errorf("register run: %w", err)
	}
	return nil
}

func (st *store) unregisterRun() error {
	var err error
	if deleteErr := st.q.deleteRun(context.Background(), st.runID); deleteErr != nil {
		err = errors.Join(err, fmt.Errorf("delete run record: %w", deleteErr))
	}
	retainedLiveFiles, prepareErr := st.prepareLiveRunForClose()
	if prepareErr != nil {
		err = errors.Join(err, prepareErr)
	}
	if unlockErr := st.runLock.Unlock(); unlockErr != nil {
		err = errors.Join(err, fmt.Errorf("unlock live run: %w", unlockErr))
	}
	if closeErr := st.runLock.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("close live run lock: %w", closeErr))
	}
	if !retainedLiveFiles {
		if removeErr := os.RemoveAll(st.runDir); removeErr != nil {
			err = errors.Join(err, fmt.Errorf("remove live run dir: %w", removeErr))
		}
	}
	return err
}

func (st *store) prepareLiveRunForClose() (bool, error) {
	entries, err := os.ReadDir(st.runDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read live run dir: %w", err)
	}

	retained := false
	for _, entry := range entries {
		if entry.Name() == "run.lock" {
			continue
		}
		path := filepath.Join(st.runDir, entry.Name())
		if !entry.Type().IsRegular() {
			if err := os.RemoveAll(path); err != nil {
				return false, fmt.Errorf("remove live path: %w", err)
			}
			continue
		}
		stripped, err := st.stripLivePackageArchiveToExport(path)
		if err != nil {
			return false, err
		}
		if stripped {
			retained = true
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, fmt.Errorf("remove live file: %w", err)
		}
	}
	return retained, nil
}

func (st *store) stripLivePackageArchiveToExport(path string) (bool, error) {
	outputID := liveOutputID(path)
	if outputID == "" {
		return stripPackageArchiveToExport(path, "")
	}
	retained, err := stripPackageArchiveToExport(path, st.retainedPath(outputID, ".a"))
	if err != nil || retained {
		return retained, err
	}
	return retainGeneratedCgoSource(path, st.retainedPath(outputID, ".go"))
}

func liveOutputID(path string) string {
	base := filepath.Base(path)
	outputID, _, ok := strings.Cut(base, "-")
	if !ok {
		return ""
	}
	return outputID
}

func (st *store) retainedDir(outputHex string) string {
	shard := "xx"
	if len(outputHex) >= 2 {
		shard = outputHex[:2]
	}
	return filepath.Join(retainedRoot(st.versionDir), shard)
}

func (st *store) retainedPath(outputHex, ext string) string {
	return filepath.Join(st.retainedDir(outputHex), outputHex+ext)
}

func (st *store) cleanupAbandonedRuns() error {
	runs, err := st.q.listOtherRuns(context.Background(), st.runID)
	if err != nil {
		return fmt.Errorf("query runs: %w", err)
	}

	for _, run := range runs {
		reclaimed, err := st.tryReclaimRun(run.runID, run.path, run.lockPath)
		if err != nil {
			return err
		}
		if reclaimed && st.verbose {
			log.Printf("gocachez: reclaimed abandoned live run %s", run.runID)
		}
	}
	return nil
}

func (st *store) tryReclaimRun(runID, runDir, lockPath string) (bool, error) {
	if _, err := os.Stat(runDir); errors.Is(err, os.ErrNotExist) {
		if err := st.q.deleteRun(context.Background(), runID); err != nil {
			return false, fmt.Errorf("delete missing-run record: %w", err)
		}
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("stat live run dir: %w", err)
	}

	runLock := flock.New(lockPath)
	locked, err := runLock.TryLock()
	if err != nil {
		_ = runLock.Close()
		return false, fmt.Errorf("try lock live run %s: %w", runID, err)
	}
	if !locked {
		_ = runLock.Close()
		return false, nil
	}

	if err := st.q.deleteRun(context.Background(), runID); err != nil {
		_ = runLock.Unlock()
		_ = runLock.Close()
		return false, fmt.Errorf("delete abandoned run record: %w", err)
	}
	if err := runLock.Unlock(); err != nil {
		_ = runLock.Close()
		return false, fmt.Errorf("unlock abandoned live run: %w", err)
	}
	if err := runLock.Close(); err != nil {
		return false, fmt.Errorf("close abandoned live run lock: %w", err)
	}
	if err := os.RemoveAll(runDir); err != nil {
		return false, fmt.Errorf("remove abandoned live run dir: %w", err)
	}
	return true, nil
}
