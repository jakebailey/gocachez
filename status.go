package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofrs/flock"
)

type cacheStatus struct {
	cacheDir              string
	maxSize               int64
	verbose               bool
	versionDir            string
	catalogExists         bool
	entries               int64
	outputs               int64
	catalogCompressedSize int64
	catalogRuns           int64
	blobFiles             int64
	blobSize              int64
	activeLiveRuns        int64
	inactiveLiveRuns      int64
}

func writeStatus(cfg config, w io.Writer) error {
	status, err := readStatus(cfg)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Cache directory: %s\n", status.cacheDir); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Version directory: %s\n", status.versionDir); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Max size: %s (%d bytes)\n", formatSize(status.maxSize), status.maxSize); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Verbose: %t\n", status.verbose); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if !status.catalogExists {
		if _, err := fmt.Fprintln(w, "Catalog: missing"); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
	} else {
		if _, err := fmt.Fprintln(w, "Catalog: present"); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
	}
	if _, err := fmt.Fprintf(w, "Entries: %d\n", status.entries); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Outputs: %d\n", status.outputs); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Catalog compressed size: %s (%d bytes)\n", formatSize(status.catalogCompressedSize), status.catalogCompressedSize); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Catalog runs: %d\n", status.catalogRuns); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Blobs: %d files, %s (%d bytes)\n", status.blobFiles, formatSize(status.blobSize), status.blobSize); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	if _, err := fmt.Fprintf(w, "Live runs: %d active, %d inactive\n", status.activeLiveRuns, status.inactiveLiveRuns); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func readStatus(cfg config) (cacheStatus, error) {
	versionDir, blobsDir, liveRoot, lifecycleLockPath := cachePaths(cfg)
	status := cacheStatus{
		cacheDir:   cfg.dir,
		maxSize:    cfg.maxSize,
		verbose:    cfg.verbose,
		versionDir: versionDir,
	}
	if _, err := os.Stat(versionDir); errors.Is(err, os.ErrNotExist) {
		return status, nil
	} else if err != nil {
		return cacheStatus{}, fmt.Errorf("stat cache version dir: %w", err)
	}

	st := &store{
		config:            cfg,
		versionDir:        versionDir,
		blobsDir:          blobsDir,
		liveRoot:          liveRoot,
		lifecycleLockPath: lifecycleLockPath,
	}
	err := st.withLifecycleLock(func() error {
		var err error
		status.catalogExists, status.entries, status.outputs, status.catalogCompressedSize, status.catalogRuns, err = readCatalogStatus(filepath.Join(versionDir, "cache.db"))
		if err != nil {
			return err
		}
		status.blobFiles, status.blobSize, err = readBlobStatus(blobsDir)
		if err != nil {
			return err
		}
		status.activeLiveRuns, status.inactiveLiveRuns, err = readLiveStatus(liveRoot)
		return err
	})
	if err != nil {
		return cacheStatus{}, err
	}
	return status, nil
}

func readCatalogStatus(dbPath string) (bool, int64, int64, int64, int64, error) {
	if !regularFile(dbPath) {
		return false, 0, 0, 0, 0, nil
	}

	db, err := openExistingDB(dbPath)
	if err != nil {
		return false, 0, 0, 0, 0, err
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()
	var entries, outputs int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries`).Scan(&entries); err != nil {
		return false, 0, 0, 0, 0, fmt.Errorf("count catalog entries: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT output_id) FROM entries`).Scan(&outputs); err != nil {
		return false, 0, 0, 0, 0, fmt.Errorf("count catalog outputs: %w", err)
	}
	q := newCatalog(db)
	compressedSize, err := q.compressedSize(ctx)
	if err != nil {
		return false, 0, 0, 0, 0, fmt.Errorf("calculate catalog compressed size: %w", err)
	}
	runs, err := q.countRuns(ctx)
	if err != nil {
		return false, 0, 0, 0, 0, fmt.Errorf("count catalog runs: %w", err)
	}
	return true, entries, outputs, compressedSize, runs, nil
}

func readBlobStatus(blobsDir string) (int64, int64, error) {
	var files, size int64
	err := filepath.WalkDir(blobsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".zst") {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat blob: %w", err)
		}
		files++
		size += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	return files, size, nil
}

func readLiveStatus(liveRoot string) (int64, int64, error) {
	entries, err := os.ReadDir(liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read live dir: %w", err)
	}

	var active, inactive int64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runLock := flock.New(filepath.Join(liveRoot, entry.Name(), "run.lock"))
		locked, err := runLock.TryLock()
		if err != nil {
			_ = runLock.Close()
			return 0, 0, fmt.Errorf("try lock live run %s: %w", entry.Name(), err)
		}
		if !locked {
			_ = runLock.Close()
			active++
			continue
		}
		if err := runLock.Unlock(); err != nil {
			_ = runLock.Close()
			return 0, 0, fmt.Errorf("unlock live run: %w", err)
		}
		if err := runLock.Close(); err != nil {
			return 0, 0, fmt.Errorf("close live run lock: %w", err)
		}
		inactive++
	}
	return active, inactive, nil
}
