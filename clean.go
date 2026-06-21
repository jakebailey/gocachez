package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

func (st *store) cleanProtocolCache() error {
	return st.withLifecycleLock(func() error {
		if err := st.cleanupAbandonedRuns(); err != nil {
			return err
		}
		activeRuns, err := st.q.countRuns(context.Background())
		if err != nil {
			return fmt.Errorf("count active runs: %w", err)
		}
		if activeRuns > 1 {
			return nil
		}
		if err := os.RemoveAll(st.blobsDir); err != nil {
			return fmt.Errorf("remove blobs dir: %w", err)
		}
		if err := os.RemoveAll(retainedRoot(st.versionDir)); err != nil {
			return fmt.Errorf("remove retained dir: %w", err)
		}
		if _, err := st.db.ExecContext(context.Background(), `DELETE FROM entries`); err != nil {
			return fmt.Errorf("delete catalog entries: %w", err)
		}
		st.mu.Lock()
		st.materialized = make(map[string]string)
		st.mu.Unlock()
		return nil
	})
}

func cleanCache(cfg config) error {
	versionDir, blobsDir, liveRoot, lifecycleLockPath := cachePaths(cfg)
	if _, err := os.Stat(versionDir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat cache version dir: %w", err)
	}

	st := &store{
		config:            cfg,
		versionDir:        versionDir,
		blobsDir:          blobsDir,
		liveRoot:          liveRoot,
		lifecycleLockPath: lifecycleLockPath,
	}
	return st.withLifecycleLock(st.cleanLocked)
}

func (st *store) cleanLocked() error {
	dbPath := filepath.Join(st.versionDir, "cache.db")
	if regularFile(dbPath) {
		db, err := openDB(dbPath)
		if err != nil {
			return err
		}
		st.db = db
		st.q = newCatalog(db)

		if err := st.cleanupAbandonedRuns(); err != nil {
			_ = db.Close()
			return err
		}
	}

	activeLive, err := st.removeUnusedLiveDirs()
	if err != nil {
		if st.db != nil {
			_ = st.db.Close()
		}
		return err
	}
	activeRuns := int64(0)
	if st.q != nil {
		if err := st.removeOrphanRetainedFiles(); err != nil {
			_ = st.db.Close()
			return err
		}
		activeRuns, err = st.q.countRuns(context.Background())
		if err != nil {
			_ = st.db.Close()
			return fmt.Errorf("count active runs: %w", err)
		}
	}
	if activeLive || activeRuns > 0 {
		if st.db != nil {
			if err := st.db.Close(); err != nil {
				return fmt.Errorf("close catalog: %w", err)
			}
		}
		return nil
	}

	if st.db != nil {
		if err := st.db.Close(); err != nil {
			return fmt.Errorf("close catalog: %w", err)
		}
		st.db = nil
	}
	if err := os.RemoveAll(st.blobsDir); err != nil {
		return fmt.Errorf("remove blobs dir: %w", err)
	}
	if err := os.RemoveAll(st.liveRoot); err != nil {
		return fmt.Errorf("remove live dir: %w", err)
	}
	if err := os.RemoveAll(retainedRoot(st.versionDir)); err != nil {
		return fmt.Errorf("remove retained dir: %w", err)
	}
	for _, path := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove catalog file: %w", err)
		}
	}
	return nil
}

func (st *store) removeUnusedLiveDirs() (bool, error) {
	entries, err := os.ReadDir(st.liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read live dir: %w", err)
	}

	active := false
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runDir := filepath.Join(st.liveRoot, entry.Name())
		runLock := flock.New(filepath.Join(runDir, "run.lock"))
		locked, err := runLock.TryLock()
		if err != nil {
			_ = runLock.Close()
			return false, fmt.Errorf("try lock live run %s: %w", entry.Name(), err)
		}
		if !locked {
			_ = runLock.Close()
			active = true
			continue
		}
		if err := runLock.Unlock(); err != nil {
			_ = runLock.Close()
			return false, fmt.Errorf("unlock unused live run: %w", err)
		}
		if err := runLock.Close(); err != nil {
			return false, fmt.Errorf("close unused live run lock: %w", err)
		}
		if err := os.RemoveAll(runDir); err != nil {
			return false, fmt.Errorf("remove unused live run dir: %w", err)
		}
	}
	return active, nil
}
