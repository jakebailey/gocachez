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
	"sync"
	"time"

	"github.com/gofrs/flock"
	_ "modernc.org/sqlite"
)

const cacheSchemaVersion = 1

type entry struct {
	ActionID       string
	OutputID       string
	Size           int64
	CompressedSize int64
	CreatedAt      time.Time
	AccessedAt     time.Time
}

type store struct {
	config

	db           *sql.DB
	q            *catalog
	versionDir   string
	blobsDir     string
	liveRoot     string
	runID        string
	runDir       string
	runLock      *flock.Flock
	mu           sync.Mutex
	encoderPool  sync.Pool
	decoderPool  sync.Pool
	materialized map[string]string
	accessed     map[string]int64
}

func newStore(cfg config) (*store, error) {
	versionDir := filepath.Join(cfg.dir, fmt.Sprintf("v%d", cacheSchemaVersion))
	blobsDir := filepath.Join(versionDir, "blobs")
	liveRoot := filepath.Join(versionDir, "live")
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
		config:       cfg,
		db:           db,
		q:            newCatalog(db),
		versionDir:   versionDir,
		blobsDir:     blobsDir,
		liveRoot:     liveRoot,
		runID:        runID,
		runDir:       runDir,
		runLock:      runLock,
		materialized: make(map[string]string),
		accessed:     make(map[string]int64),
	}
	if err := st.registerRun(); err != nil {
		_ = db.Close()
		_ = runLock.Unlock()
		_ = runLock.Close()
		_ = os.RemoveAll(runDir)
		return nil, err
	}
	if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	return st, nil
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
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, cacheSchemaVersion)); err != nil {
		return fmt.Errorf("write catalog version: %w", err)
	}
	return nil
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
	if err := st.q.deleteRun(context.Background(), st.runID); err != nil {
		return fmt.Errorf("delete run record: %w", err)
	}
	if err := st.runLock.Unlock(); err != nil {
		return fmt.Errorf("unlock live run: %w", err)
	}
	if err := st.runLock.Close(); err != nil {
		return fmt.Errorf("close live run lock: %w", err)
	}
	if err := os.RemoveAll(st.runDir); err != nil {
		return fmt.Errorf("remove live run dir: %w", err)
	}
	return nil
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
