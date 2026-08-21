package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/klauspost/compress/zstd"
	"zombiezen.com/go/sqlite"
)

var errInvalidCacheEntry = errors.New("invalid cache entry")

const (
	// mtimeInterval mirrors cmd/go's DiskCache: retained-file mtimes are updated
	// at most once per interval to avoid churn. The age cutoff itself is the
	// configurable maxAge (see config); the default matches GOCACHE's 5 days.
	mtimeInterval          = time.Hour
	fullPruneInterval      = time.Hour
	sizePruneTargetPercent = 90
	lastFullPruneStateKey  = "last-full-prune"
	accessFlushInterval    = 30 * time.Second
	accessFlushThreshold   = 256
	accessFlushMinInterval = time.Second
)

var decoderOptions = []zstd.DOption{
	zstd.WithDecoderConcurrency(1),
	zstd.WithDecoderLowmem(true),
}

func (st *store) put(req request, br *bufio.Reader) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	outputHex, err := idHex("OutputID", req.OutputID)
	if err != nil {
		return response{}, err
	}
	body, err := bodyReader(br, req.BodySize)
	if err != nil {
		return response{}, err
	}
	bodyDrained := false
	defer func() {
		if !bodyDrained {
			_ = drainBody(body)
		}
	}()

	blobDir := st.blobDir(outputHex)
	if err := os.MkdirAll(blobDir, 0o777); err != nil {
		return response{}, fmt.Errorf("create blob dir: %w", err)
	}
	blobPath := st.blobPath(outputHex)
	compressedSize, blobExists := existingFileSize(blobPath)

	bodyPath, err := st.createLiveFile(outputHex)
	if err != nil {
		return response{}, err
	}
	keepBody := false
	defer func() {
		if !keepBody {
			_ = os.Remove(bodyPath)
		}
	}()

	bodyFile, err := os.Create(bodyPath)
	if err != nil {
		return response{}, fmt.Errorf("create live file: %w", err)
	}

	var written int64
	if blobExists {
		written, err = io.Copy(bodyFile, body)
	} else {
		written, compressedSize, err = st.writeNewBlob(bodyFile, body, blobDir, outputHex)
	}
	bodyCloseErr := bodyFile.Close()
	if err != nil {
		return response{}, fmt.Errorf("read put body: %w", err)
	}
	bodyDrained = true
	if bodyCloseErr != nil {
		return response{}, fmt.Errorf("close live file: %w", bodyCloseErr)
	}
	if written != req.BodySize {
		return response{}, fmt.Errorf("put body size mismatch: got %d bytes, expected %d", written, req.BodySize)
	}

	if blobExists {
		if size, ok := existingFileSize(blobPath); ok {
			compressedSize = size
		} else {
			compressedSize, err = st.compressLiveFile(bodyPath, blobDir, outputHex)
			if err != nil {
				return response{}, err
			}
		}
	}

	now := time.Now()
	ent := entry{
		ActionID:       actionHex,
		OutputID:       outputHex,
		Size:           written,
		CompressedSize: compressedSize,
		CreatedAt:      now,
		AccessedAt:     now,
	}
	if err := st.upsertEntry(ent); err != nil {
		return response{}, err
	}
	keepBody = true
	st.setMaterialized(outputHex, bodyPath)

	return response{
		ID:       req.ID,
		DiskPath: bodyPath,
	}, nil
}

func (st *store) writeNewBlob(bodyFile *os.File, body io.Reader, blobDir, outputHex string) (int64, int64, error) {
	blobTmp, err := os.CreateTemp(blobDir, outputHex+"-pending-*.zst")
	if err != nil {
		return 0, 0, fmt.Errorf("create compressed file: %w", err)
	}
	blobTmpPath := blobTmp.Name()
	defer os.Remove(blobTmpPath) //nolint:errcheck

	zw, err := st.getEncoder(blobTmp)
	if err != nil {
		_ = blobTmp.Close()
		return 0, 0, fmt.Errorf("create zstd encoder: %w", err)
	}
	written, copyErr := io.Copy(io.MultiWriter(bodyFile, zw), body)
	encoderCloseErr := zw.Close()
	st.putEncoder(zw)
	blobCloseErr := blobTmp.Close()
	if copyErr != nil {
		return 0, 0, copyErr
	}
	if encoderCloseErr != nil {
		return 0, 0, fmt.Errorf("finish zstd stream: %w", encoderCloseErr)
	}
	if blobCloseErr != nil {
		return 0, 0, fmt.Errorf("close compressed file: %w", blobCloseErr)
	}
	compressedSize, err := st.installBlob(blobTmpPath, outputHex)
	return written, compressedSize, err
}

func (st *store) compressLiveFile(bodyPath, blobDir, outputHex string) (int64, error) {
	body, err := os.Open(bodyPath)
	if err != nil {
		return 0, fmt.Errorf("open live file for compression: %w", err)
	}
	defer body.Close() //nolint:errcheck

	blobTmp, err := os.CreateTemp(blobDir, outputHex+"-pending-*.zst")
	if err != nil {
		return 0, fmt.Errorf("create compressed file: %w", err)
	}
	blobTmpPath := blobTmp.Name()
	defer os.Remove(blobTmpPath) //nolint:errcheck

	zw, err := st.getEncoder(blobTmp)
	if err != nil {
		_ = blobTmp.Close()
		return 0, fmt.Errorf("create zstd encoder: %w", err)
	}
	_, copyErr := io.Copy(zw, body)
	encoderCloseErr := zw.Close()
	st.putEncoder(zw)
	blobCloseErr := blobTmp.Close()
	if copyErr != nil {
		return 0, fmt.Errorf("compress live file: %w", copyErr)
	}
	if encoderCloseErr != nil {
		return 0, fmt.Errorf("finish zstd stream: %w", encoderCloseErr)
	}
	if blobCloseErr != nil {
		return 0, fmt.Errorf("close compressed file: %w", blobCloseErr)
	}
	return st.installBlob(blobTmpPath, outputHex)
}

func existingFileSize(path string) (int64, bool) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return 0, false
	}
	return info.Size(), true
}

func (st *store) get(req request) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	ent, found, err := st.lookupEntry(actionHex)
	if !found && err == nil {
		return response{ID: req.ID, Miss: true}, nil
	}
	if err != nil {
		return response{}, err
	}

	path := st.getMaterialized(ent.OutputID)
	if path == "" || !regularFile(path) {
		path, err = st.materialize(ent)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, errInvalidCacheEntry) {
				if deleteErr := st.deleteOutput(ent.OutputID); deleteErr != nil && st.verbose {
					log.Printf("gocachez: delete bad cache output failed: %v", deleteErr)
				}
				return response{ID: req.ID, Miss: true}, nil
			}
			return response{}, err
		}
		st.setMaterialized(ent.OutputID, path)
	}

	st.markEntryAccess(actionHex)

	outputID, err := hex.DecodeString(ent.OutputID)
	if err != nil {
		return response{}, fmt.Errorf("decode output ID: %w", err)
	}
	return response{
		ID:       req.ID,
		OutputID: outputID,
		Size:     ent.Size,
		Time:     &ent.CreatedAt,
		DiskPath: path,
	}, nil
}

func (st *store) getMaterialized(outputID string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.materialized[outputID]
}

func (st *store) setMaterialized(outputID, path string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.materialized[outputID] = path
}

func (st *store) deleteMaterialized(outputID string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.materialized, outputID)
}

func (st *store) markEntryAccess(actionID string) {
	if err := st.markEntryAccessAt(actionID, time.Now()); err != nil && st.verbose {
		log.Printf("gocachez: flush access times failed: %v", err)
	}
}

func (st *store) markEntryAccessAt(actionID string, now time.Time) error {
	st.mu.Lock()
	st.accessed[actionID] = unixMillis(now)
	elapsed := now.Sub(st.lastAccessFlush)
	shouldFlush := elapsed >= accessFlushInterval ||
		(len(st.accessed) >= accessFlushThreshold && elapsed >= accessFlushMinInterval)
	if !shouldFlush {
		st.mu.Unlock()
		return nil
	}
	accessed := st.accessed
	st.accessed = make(map[string]int64)
	st.lastAccessFlush = now
	st.mu.Unlock()
	return st.writeAccessTimes(accessed)
}

func (st *store) flushAccessTimes() error {
	return st.flushAccessTimesAt(time.Now())
}

func (st *store) flushAccessTimesAt(now time.Time) error {
	st.mu.Lock()
	accessed := st.accessed
	st.accessed = make(map[string]int64)
	st.lastAccessFlush = now
	st.mu.Unlock()
	if len(accessed) == 0 {
		return nil
	}
	return st.writeAccessTimes(accessed)
}

func (st *store) writeAccessTimes(accessed map[string]int64) error {
	err := st.db.withTx(context.Background(), func(conn *sqlite.Conn) error {
		return touchEntries(conn, accessed)
	})
	if err == nil {
		return nil
	}

	st.mu.Lock()
	for actionID, accessedAt := range accessed {
		st.accessed[actionID] = max(st.accessed[actionID], accessedAt)
	}
	st.mu.Unlock()
	return err
}

func (st *store) materialize(ent entry) (string, error) {
	bodyPath, err := st.createLiveFile(ent.OutputID)
	if err != nil {
		return "", err
	}
	keepBody := false
	defer func() {
		if !keepBody {
			_ = os.Remove(bodyPath)
		}
	}()

	blob, err := os.Open(st.blobPath(ent.OutputID))
	if err != nil {
		return "", err
	}
	defer blob.Close() //nolint:errcheck

	zr, err := st.getDecoder(blob)
	if err != nil {
		return "", fmt.Errorf("%w: create zstd decoder: %w", errInvalidCacheEntry, err)
	}
	defer st.putDecoder(zr)

	bodyFile, err := os.Create(bodyPath)
	if err != nil {
		return "", fmt.Errorf("create live file: %w", err)
	}

	written, copyErr := io.Copy(bodyFile, zr)
	closeErr := bodyFile.Close()
	if copyErr != nil {
		return "", fmt.Errorf("%w: decompress cache entry: %w", errInvalidCacheEntry, copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close live file: %w", closeErr)
	}
	if written != ent.Size {
		return "", fmt.Errorf("%w: decompressed size mismatch: got %d bytes, expected %d", errInvalidCacheEntry, written, ent.Size)
	}

	keepBody = true
	return bodyPath, nil
}

func (st *store) getEncoder(w io.Writer) (*zstd.Encoder, error) {
	if enc, ok := st.encoderPool.Get().(*zstd.Encoder); ok {
		enc.Reset(w)
		return enc, nil
	}
	return zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(true))
}

func (st *store) putEncoder(enc *zstd.Encoder) {
	enc.Reset(io.Discard)
	st.encoderPool.Put(enc)
}

func (st *store) getDecoder(r io.Reader) (*zstd.Decoder, error) {
	if dec, ok := st.decoderPool.Get().(*zstd.Decoder); ok {
		if err := dec.Reset(r); err != nil {
			dec.Close()
			return nil, err
		}
		return dec, nil
	}
	return zstd.NewReader(r, decoderOptions...)
}

func (st *store) putDecoder(dec *zstd.Decoder) {
	_ = dec.Reset(bytes.NewReader(nil))
	st.decoderPool.Put(dec)
}

func (st *store) createLiveFile(outputHex string) (string, error) {
	file, err := os.CreateTemp(st.runDir, outputHex+"-*")
	if err != nil {
		return "", fmt.Errorf("create live file: %w", err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close live file placeholder: %w", err)
	}
	return path, nil
}

func (st *store) installBlob(tmpPath, outputHex string) (int64, error) {
	dst := st.blobPath(outputHex)
	if regularFile(dst) {
		return fileSize(dst)
	}
	err := os.Rename(tmpPath, dst)
	if err == nil {
		return fileSize(dst)
	}
	if regularFile(dst) {
		return fileSize(dst)
	}
	return 0, fmt.Errorf("install compressed file: %w", err)
}

func (st *store) upsertEntry(ent entry) error {
	if err := st.q.upsertEntry(context.Background(), ent); err != nil {
		return fmt.Errorf("upsert entry: %w", err)
	}
	return nil
}

func (st *store) lookupEntry(actionID string) (entry, bool, error) {
	return st.q.lookupEntry(context.Background(), actionID)
}

func (st *store) deleteOutput(outputID string) error {
	if err := os.Remove(st.blobPath(outputID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove bad blob: %w", err)
	}
	if err := st.q.deleteEntriesByOutputID(context.Background(), outputID); err != nil {
		return fmt.Errorf("delete bad output entries: %w", err)
	}
	st.deleteMaterialized(outputID)
	return nil
}

func (st *store) prune() error {
	return st.withLifecycleLock(func() error {
		activeRuns, err := st.prepareToPruneLocked()
		if err != nil || activeRuns > 0 {
			return err
		}
		if err := st.pruneOldDataLocked(time.Now()); err != nil {
			return err
		}
		if _, err := st.q.reconcileCompressedSize(context.Background()); err != nil {
			return fmt.Errorf("reconcile compressed size: %w", err)
		}
		if err := st.pruneSizeLocked(st.maxSize); err != nil {
			return err
		}
		return st.removeOrphanOutputFiles(true)
	})
}

func (st *store) pruneAutomatically(now time.Time) error {
	return st.withLifecycleLock(func() error {
		activeRuns, err := st.prepareToPruneLocked()
		if err != nil {
			return err
		}

		if activeRuns > 0 {
			return st.pruneSizeWithHysteresis()
		}

		fullPruneDue, err := st.fullPruneDue(now)
		if err != nil {
			return err
		}
		if !fullPruneDue {
			return st.pruneSizeWithHysteresis()
		}
		if err := st.pruneOldDataLocked(now); err != nil {
			return err
		}
		if _, err := st.q.reconcileCompressedSize(context.Background()); err != nil {
			return fmt.Errorf("reconcile compressed size: %w", err)
		}
		if err := st.pruneSizeWithHysteresis(); err != nil {
			return err
		}
		if err := st.removeOrphanOutputFiles(true); err != nil {
			return err
		}
		return st.q.setState(context.Background(), lastFullPruneStateKey, unixMillis(now))
	})
}

func (st *store) pruneSizeWithHysteresis() error {
	if st.maxSize <= 0 {
		return nil
	}
	target := st.maxSize * sizePruneTargetPercent / 100
	return st.pruneSizeLocked(target)
}

func (st *store) prepareToPruneLocked() (int64, error) {
	if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	activeRuns, err := st.q.countRuns(context.Background())
	if err != nil {
		return 0, fmt.Errorf("count active runs: %w", err)
	}
	return activeRuns, nil
}

func (st *store) pruneOldDataLocked(now time.Time) error {
	if err := st.pruneOldRetainedFiles(now); err != nil {
		return err
	}
	if err := st.pruneOldRetainedLiveDirs(now); err != nil {
		return err
	}
	return st.pruneOldEntries(now)
}

func (st *store) pruneSizeLocked(target int64) error {
	if st.maxSize <= 0 {
		return nil
	}
	total, err := st.compressedSize()
	if err != nil {
		return err
	}
	if total <= st.maxSize {
		return nil
	}

	candidates, err := st.pruneCandidates()
	if err != nil {
		return err
	}
	removed := 0
	for _, candidate := range candidates {
		if total <= target {
			break
		}
		if err := os.Remove(st.blobPath(candidate.outputID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove compressed entry: %w", err)
		}
		if err := st.q.deleteEntriesByOutputID(context.Background(), candidate.outputID); err != nil {
			return fmt.Errorf("delete pruned entries: %w", err)
		}
		total -= candidate.size
		removed++
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d blobs, compressed size now %s", removed, formatSize(total))
	}
	return nil
}

func (st *store) fullPruneDue(now time.Time) (bool, error) {
	lastMillis, found, err := st.q.state(context.Background(), lastFullPruneStateKey)
	if err != nil {
		return false, fmt.Errorf("read last full prune: %w", err)
	}
	if !found {
		return true, nil
	}
	last := millisTime(lastMillis)
	if last.After(now) {
		return true, nil
	}
	return now.Sub(last) >= fullPruneInterval, nil
}

type pruneCandidate struct {
	outputID string
	size     int64
}

func (st *store) compressedSize() (int64, error) {
	total, err := st.q.compressedSize(context.Background())
	if err != nil {
		return 0, fmt.Errorf("calculate compressed size: %w", err)
	}
	return total, nil
}

func (st *store) pruneCandidates() ([]pruneCandidate, error) {
	candidates, err := st.q.pruneCandidates(context.Background())
	if err != nil {
		return nil, fmt.Errorf("query prune candidates: %w", err)
	}
	return candidates, nil
}

func (st *store) pruneOldEntries(now time.Time) error {
	if st.maxAge <= 0 {
		return nil
	}
	cutoff := unixMillis(trimCutoff(st.maxAge, now))
	removed, err := st.q.deleteEntriesAccessedBefore(context.Background(), cutoff)
	if err != nil {
		return fmt.Errorf("prune old entries: %w", err)
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d entries not used in %s", removed, st.maxAge)
	}
	return nil
}

func (st *store) pruneOldRetainedFiles(now time.Time) error {
	if st.maxAge <= 0 {
		return nil
	}
	root := retainedRoot(st.versionDir)
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat retained root: %w", err)
	}
	cutoff := trimCutoff(st.maxAge, now)
	shards, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read retained root: %w", err)
	}
	removed := 0
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		shardPath := filepath.Join(root, shard.Name())
		files, err := os.ReadDir(shardPath)
		if err != nil {
			return fmt.Errorf("read retained shard: %w", err)
		}
		for _, file := range files {
			if file.IsDir() || (!strings.HasSuffix(file.Name(), ".a") && !strings.HasSuffix(file.Name(), ".go")) {
				continue
			}
			info, err := file.Info()
			if err != nil {
				return fmt.Errorf("stat retained file: %w", err)
			}
			if info.ModTime().Before(cutoff) {
				if err := os.Remove(filepath.Join(shardPath, file.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("remove old retained file: %w", err)
				}
				removed++
			}
		}
		removeDirIfEmpty(shardPath)
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d old retained files", removed)
	}
	removeDirIfEmpty(root)
	return nil
}

func (st *store) pruneOldRetainedLiveDirs(now time.Time) error {
	if st.maxAge <= 0 {
		return nil
	}
	entries, err := os.ReadDir(st.liveRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read live dir: %w", err)
	}
	cutoff := trimCutoff(st.maxAge, now)
	removed := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runDir := filepath.Join(st.liveRoot, entry.Name())
		runLock := flock.New(filepath.Join(runDir, "run.lock"))
		locked, err := runLock.TryLock()
		if err != nil {
			_ = runLock.Close()
			return fmt.Errorf("try lock retained live run %s: %w", entry.Name(), err)
		}
		if !locked {
			_ = runLock.Close()
			continue
		}
		expired, expireErr := retainedLiveRunExpired(runDir, cutoff)
		unlockErr := runLock.Unlock()
		closeErr := runLock.Close()
		if expireErr != nil {
			return expireErr
		}
		if unlockErr != nil {
			return fmt.Errorf("unlock retained live run: %w", unlockErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close retained live run lock: %w", closeErr)
		}
		if expired {
			if err := os.RemoveAll(runDir); err != nil {
				return fmt.Errorf("remove old retained live run: %w", err)
			}
			removed++
		}
	}
	if st.verbose && removed > 0 {
		log.Printf("gocachez: pruned %d old retained live runs", removed)
	}
	return nil
}

func retainedLiveRunExpired(runDir string, cutoff time.Time) (bool, error) {
	info, err := os.Stat(filepath.Join(runDir, "run.lock"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat retained live run: %w", err)
	}
	// Retained files are hard-linked into live runs, so their mtimes cannot
	// represent the age of a particular run. The lock file is unique to the run
	// and timestamped when that run closes.
	return info.ModTime().Before(cutoff), nil
}

func trimCutoff(maxAge time.Duration, now time.Time) time.Time {
	return now.Add(-maxAge - mtimeInterval)
}

func markRetainedFileUsed(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	now := time.Now()
	if now.Sub(info.ModTime()) < mtimeInterval {
		return nil
	}
	return os.Chtimes(path, now, now)
}

func (st *store) removeOrphanOutputFiles(includeBlobs bool) error {
	// Output files are already partitioned by the first byte of their hex ID.
	// Reconcile one shard at a time to keep memory bounded without per-file SQL.
	referenced := make(map[string]struct{})
	for shard := range 256 {
		lower := fmt.Sprintf("%02x", shard)
		upper := fmt.Sprintf("%02x", shard+1)
		if shard == 255 {
			upper = "g"
		}
		if err := st.q.referencedOutputIDs(context.Background(), lower, upper, referenced); err != nil {
			return fmt.Errorf("query referenced outputs in shard %s: %w", lower, err)
		}
		if includeBlobs {
			if err := removeOrphanFilesInDir(filepath.Join(st.blobsDir, lower), referenced, ".zst"); err != nil {
				return fmt.Errorf("remove orphan blobs in shard %s: %w", lower, err)
			}
		}
		if err := removeOrphanFilesInDir(filepath.Join(retainedRoot(st.versionDir), lower), referenced, ".a", ".go"); err != nil {
			return fmt.Errorf("remove orphan retained files in shard %s: %w", lower, err)
		}
	}
	return nil
}

func removeOrphanFilesInDir(root string, referenced map[string]struct{}, extensions ...string) error {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if !slices.Contains(extensions, ext) {
			continue
		}
		outputID := strings.TrimSuffix(entry.Name(), ext)
		if _, ok := referenced[outputID]; !ok {
			if err := os.Remove(filepath.Join(root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	removeDirIfEmpty(root)
	return nil
}

func removeDirIfEmpty(path string) {
	_ = os.Remove(path)
}

func (st *store) blobDir(outputHex string) string {
	return blobDir(st.blobsDir, outputHex)
}

func blobDir(blobsDir, outputHex string) string {
	shard := "xx"
	if len(outputHex) >= 2 {
		shard = outputHex[:2]
	}
	return filepath.Join(blobsDir, shard)
}

func (st *store) blobPath(outputHex string) string {
	return blobPath(st.blobsDir, outputHex)
}

func blobPath(blobsDir, outputHex string) string {
	return filepath.Join(blobDir(blobsDir, outputHex), outputHex+".zst")
}
