package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gofrs/flock"
)

type cacheStatus struct {
	cacheDir         string
	maxSize          int64
	verbose          bool
	versionDir       string
	catalogExists    bool
	catalog          catalogStatus
	blobFiles        int64
	blobSize         int64
	blobTypes        []blobTypeStatus
	retainedFiles    int64
	retainedSize     int64
	activeLiveRuns   int64
	inactiveLiveRuns int64
}

type catalogStatus struct {
	entries        int64
	outputs        int64
	size           int64
	compressedSize int64
	runs           int64
}

func writeStatus(cfg config, w io.Writer) error {
	status, err := readStatus(cfg)
	if err != nil {
		return err
	}

	if err := writeTable(w, "Configuration", [][]string{
		{"Cache directory", status.cacheDir},
		{"Version directory", status.versionDir},
		{"Max size", formatBytes(status.maxSize)},
		{"Verbose", strconv.FormatBool(status.verbose)},
	}); err != nil {
		return err
	}
	catalogState := "missing"
	if status.catalogExists {
		catalogState = "present"
	}
	if err := writeTable(w, "Catalog", [][]string{
		{"State", catalogState},
		{"Entries", formatInt(status.catalog.entries)},
		{"Outputs", formatInt(status.catalog.outputs)},
		{"Uncompressed size", formatBytes(status.catalog.size)},
		{"Compressed size", formatBytes(status.catalog.compressedSize)},
		{"Savings", formatSavings(status.catalog.size, status.catalog.compressedSize)},
		{"Runs", formatInt(status.catalog.runs)},
	}); err != nil {
		return err
	}
	if err := writeTable(w, "Storage", [][]string{
		{"Kind", "Files", "Size"},
		{"Blobs", formatInt(status.blobFiles), formatBytes(status.blobSize)},
		{"Retained files", formatInt(status.retainedFiles), formatBytes(status.retainedSize)},
	}); err != nil {
		return err
	}
	if err := writeBlobTypeStatus(w, status.blobTypes); err != nil {
		return err
	}
	return writeTable(w, "Live runs", [][]string{
		{"State", "Count"},
		{"Active", formatInt(status.activeLiveRuns)},
		{"Inactive", formatInt(status.inactiveLiveRuns)},
	})
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
		status.catalogExists, status.catalog, err = readCatalogStatus(filepath.Join(versionDir, "cache.db"))
		if err != nil {
			return err
		}
		status.blobFiles, status.blobSize, err = readBlobStatus(blobsDir)
		if err != nil {
			return err
		}
		status.blobTypes, err = readBlobTypeStatus(filepath.Join(versionDir, "cache.db"), blobsDir)
		if err != nil {
			return err
		}
		status.retainedFiles, status.retainedSize, err = readRetainedStatus(retainedRoot(versionDir))
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

func readCatalogStatus(dbPath string) (bool, catalogStatus, error) {
	if !regularFile(dbPath) {
		return false, catalogStatus{}, nil
	}

	db, err := openExistingDB(dbPath)
	if err != nil {
		return false, catalogStatus{}, err
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()
	var status catalogStatus
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries`).Scan(&status.entries); err != nil {
		return false, catalogStatus{}, fmt.Errorf("count catalog entries: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT output_id) FROM entries`).Scan(&status.outputs); err != nil {
		return false, catalogStatus{}, fmt.Errorf("count catalog outputs: %w", err)
	}
	status.size, err = catalogSize(ctx, db)
	if err != nil {
		return false, catalogStatus{}, err
	}
	q := newCatalog(db)
	status.compressedSize, err = q.compressedSize(ctx)
	if err != nil {
		return false, catalogStatus{}, fmt.Errorf("calculate catalog compressed size: %w", err)
	}
	status.runs, err = q.countRuns(ctx)
	if err != nil {
		return false, catalogStatus{}, fmt.Errorf("count catalog runs: %w", err)
	}
	return true, status, nil
}

func readBlobStatus(blobsDir string) (int64, int64, error) {
	return readSuffixedFileStatus(blobsDir, ".zst")
}

func writeBlobTypeStatus(w io.Writer, statuses []blobTypeStatus) error {
	if len(statuses) == 0 {
		return writeTable(w, "Blob types (best effort)", [][]string{
			{"Type", "Files", "Uncompressed", "Compressed", "Extra"},
			{"None", "0", formatBytes(0), formatBytes(0), ""},
		})
	}
	rows := [][]string{
		{"Type", "Files", "Uncompressed", "Compressed", "Extra"},
	}
	for _, status := range statuses {
		extra := ""
		if status.exportDataSize > 0 {
			extra = "export data: " + formatBytes(status.exportDataSize)
		}
		rows = append(rows, []string{
			status.kind.label(),
			formatInt(status.count),
			formatBytes(status.size),
			formatBytes(status.compressedSize),
			extra,
		})
	}
	return writeTable(w, "Blob types (best effort)", rows)
}

func writeTable(w io.Writer, title string, rows [][]string) error {
	if _, err := fmt.Fprintf(w, "%s:\n", title); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	widths := tableWidths(rows)
	for _, row := range rows {
		if _, err := fmt.Fprint(w, "  "); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
		last := len(row) - 1
		for last > 0 && row[last] == "" {
			last--
		}
		for i, cell := range row[:last+1] {
			if i > 0 {
				if _, err := fmt.Fprint(w, "  "); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
			if _, err := fmt.Fprint(w, cell); err != nil {
				return fmt.Errorf("write status: %w", err)
			}
			if i < last {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", widths[i]-len(cell))); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return fmt.Errorf("write status: %w", err)
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

func tableWidths(rows [][]string) []int {
	var widths []int
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			widths[i] = max(widths[i], len(cell))
		}
	}
	return widths
}

func formatBytes(size int64) string {
	return fmt.Sprintf("%s (%d bytes)", formatSize(size), size)
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func readRetainedStatus(root string) (int64, int64, error) {
	return readSuffixedFileStatus(root, "")
}

func readSuffixedFileStatus(root, suffix string) (int64, int64, error) {
	var files, size int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, suffix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat file: %w", err)
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
