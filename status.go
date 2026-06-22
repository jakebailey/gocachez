package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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
	retainedTypes    []retainedTypeStatus
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

type retainedTypeKind int

const (
	retainedTypeExportArchive retainedTypeKind = iota
	retainedTypeGeneratedCgoSource
	retainedTypeGeneratedTestmain
	retainedTypeOther
)

type retainedTypeStatus struct {
	kind  retainedTypeKind
	count int64
	size  int64
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
	if err := writeRightAlignedTable(w, "Storage", [][]string{
		{"Kind", "Files", "Size"},
		{"Blobs", formatInt(status.blobFiles), formatBytes(status.blobSize)},
		{"Retained files", formatInt(status.retainedFiles), formatBytes(status.retainedSize)},
	}); err != nil {
		return err
	}
	if err := writeBlobTypeStatus(w, status.blobTypes); err != nil {
		return err
	}
	if err := writeRetainedTypeStatus(w, status.retainedTypes); err != nil {
		return err
	}
	return writeRightAlignedTable(w, "Live runs", [][]string{
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
		status.retainedFiles, status.retainedSize, status.retainedTypes, err = readRetainedStatus(retainedRoot(versionDir))
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
		return writeRightAlignedTable(w, "Blob types (best effort)", [][]string{
			{"Type", "Files", "Uncompressed", "Compressed"},
			{"None", "0", formatBytes(0), formatBytes(0)},
		})
	}
	rows := [][]string{
		{"Type", "Files", "Uncompressed", "Compressed"},
	}
	for _, status := range statuses {
		rows = append(rows, []string{
			status.kind.label(),
			formatInt(status.count),
			formatBytes(status.size),
			formatBytes(status.compressedSize),
		})
	}
	return writeRightAlignedTable(w, "Blob types (best effort)", rows)
}

func writeRetainedTypeStatus(w io.Writer, statuses []retainedTypeStatus) error {
	if len(statuses) == 0 {
		return writeRightAlignedTable(w, "Retained file types", [][]string{
			{"Type", "Files", "Size"},
			{"None", "0", formatBytes(0)},
		})
	}
	rows := [][]string{
		{"Type", "Files", "Size"},
	}
	for _, status := range statuses {
		rows = append(rows, []string{
			status.kind.label(),
			formatInt(status.count),
			formatBytes(status.size),
		})
	}
	return writeRightAlignedTable(w, "Retained file types", rows)
}

func writeTable(w io.Writer, title string, rows [][]string) error {
	return writeTableAligned(w, title, rows, false)
}

func writeRightAlignedTable(w io.Writer, title string, rows [][]string) error {
	return writeTableAligned(w, title, rows, true)
}

func writeTableAligned(w io.Writer, title string, rows [][]string, rightAlignColumns bool) error {
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
			padding := widths[i] - len(cell)
			if rightAlignColumns && i > 0 {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", padding)); err != nil {
					return fmt.Errorf("write status: %w", err)
				}
			}
			if _, err := fmt.Fprint(w, cell); err != nil {
				return fmt.Errorf("write status: %w", err)
			}
			if i < last && (!rightAlignColumns || i == 0) {
				if _, err := fmt.Fprint(w, strings.Repeat(" ", padding)); err != nil {
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
	return formatSize(size)
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func readRetainedStatus(root string) (int64, int64, []retainedTypeStatus, error) {
	byKind := make(map[retainedTypeKind]*retainedTypeStatus)
	var files, size int64
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat retained file: %w", err)
		}
		kind := retainedFileKind(path)
		status := byKind[kind]
		if status == nil {
			status = &retainedTypeStatus{
				kind: kind,
			}
			byKind[kind] = status
		}
		status.count++
		status.size += info.Size()
		files++
		size += info.Size()
		return nil
	})
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}

	statuses := make([]retainedTypeStatus, 0, len(byKind))
	for _, status := range byKind {
		statuses = append(statuses, *status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].size != statuses[j].size {
			return statuses[i].size > statuses[j].size
		}
		return statuses[i].kind.label() < statuses[j].kind.label()
	})
	return files, size, statuses, nil
}

func retainedFileKind(path string) retainedTypeKind {
	switch filepath.Ext(path) {
	case ".a":
		return retainedTypeExportArchive
	case ".go":
		data, err := os.ReadFile(path)
		if err != nil {
			return retainedTypeOther
		}
		if isGeneratedCgoSource(data) {
			return retainedTypeGeneratedCgoSource
		}
		if isGeneratedTestmainSource(data) {
			return retainedTypeGeneratedTestmain
		}
	}
	return retainedTypeOther
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

func (kind retainedTypeKind) label() string {
	switch kind {
	case retainedTypeExportArchive:
		return "Export archives"
	case retainedTypeGeneratedCgoSource:
		return "Generated cgo sources"
	case retainedTypeGeneratedTestmain:
		return "Generated test mains"
	default:
		return "Other retained files"
	}
}
