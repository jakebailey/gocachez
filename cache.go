package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
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

	"github.com/klauspost/compress/zstd"
)

var errInvalidCacheEntry = errors.New("invalid cache entry")

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

	blobTmp, err := os.CreateTemp(blobDir, outputHex+"-pending-*.zst")
	if err != nil {
		return response{}, fmt.Errorf("create compressed file: %w", err)
	}
	blobTmpPath := blobTmp.Name()
	defer func() {
		_ = os.Remove(blobTmpPath)
	}()

	bodyFile, err := os.Create(bodyPath)
	if err != nil {
		_ = blobTmp.Close()
		return response{}, fmt.Errorf("create live file: %w", err)
	}
	zw, err := st.getEncoder(blobTmp)
	if err != nil {
		_ = bodyFile.Close()
		_ = blobTmp.Close()
		return response{}, fmt.Errorf("create zstd encoder: %w", err)
	}

	written, copyErr := io.Copy(io.MultiWriter(bodyFile, zw), body)
	closeErr := zw.Close()
	st.putEncoder(zw)
	bodyCloseErr := bodyFile.Close()
	blobCloseErr := blobTmp.Close()
	if copyErr != nil {
		return response{}, fmt.Errorf("read put body: %w", copyErr)
	}
	bodyDrained = true
	if closeErr != nil {
		return response{}, fmt.Errorf("finish zstd stream: %w", closeErr)
	}
	if bodyCloseErr != nil {
		return response{}, fmt.Errorf("close live file: %w", bodyCloseErr)
	}
	if blobCloseErr != nil {
		return response{}, fmt.Errorf("close compressed file: %w", blobCloseErr)
	}
	if written != req.BodySize {
		return response{}, fmt.Errorf("put body size mismatch: got %d bytes, expected %d", written, req.BodySize)
	}

	compressedSize, err := st.installBlob(blobTmpPath, outputHex)
	if err != nil {
		return response{}, err
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

func (st *store) get(req request) (response, error) {
	actionHex, err := idHex("ActionID", req.ActionID)
	if err != nil {
		return response{}, err
	}
	ent, err := st.lookupEntry(actionHex)
	if errors.Is(err, sql.ErrNoRows) {
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
	st.mu.Lock()
	defer st.mu.Unlock()
	st.accessed[actionID] = unixMillis(time.Now())
}

func (st *store) flushAccessTimes() error {
	st.mu.Lock()
	accessed := st.accessed
	st.accessed = make(map[string]int64)
	st.mu.Unlock()
	if len(accessed) == 0 {
		return nil
	}

	ctx := context.Background()
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin access-time transaction: %w", err)
	}
	qtx := st.q.withTx(tx)
	for actionID, accessedAt := range accessed {
		if err := qtx.touchEntry(ctx, actionID, accessedAt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("touch entry: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit access-time transaction: %w", err)
	}
	return nil
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

func (st *store) lookupEntry(actionID string) (entry, error) {
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
	return st.withLifecycleLock(st.pruneLocked)
}

func (st *store) pruneLocked() error {
	if err := st.cleanupAbandonedRuns(); err != nil && st.verbose {
		log.Printf("gocachez: cleanup abandoned runs failed: %v", err)
	}
	activeRuns, err := st.q.countRuns(context.Background())
	if err != nil {
		return fmt.Errorf("count active runs: %w", err)
	}
	if activeRuns > 0 {
		return nil
	}
	if err := st.removeOrphanRetainedFiles(); err != nil {
		return err
	}
	if st.maxSize <= 0 {
		return st.removeOrphanBlobs()
	}
	total, err := st.compressedSize()
	if err != nil {
		return err
	}
	if total <= st.maxSize {
		return st.removeOrphanBlobs()
	}

	candidates, err := st.pruneCandidates()
	if err != nil {
		return err
	}
	removed := 0
	for _, candidate := range candidates {
		if total <= st.maxSize {
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
	return st.removeOrphanBlobs()
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

func (st *store) removeOrphanBlobs() error {
	err := filepath.WalkDir(st.blobsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".zst") {
			return nil
		}
		outputID := strings.TrimSuffix(filepath.Base(path), ".zst")
		entryRefs, err := st.q.countEntriesByOutputID(context.Background(), outputID)
		if err != nil {
			return fmt.Errorf("query entry blob references: %w", err)
		}
		if entryRefs == 0 {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove orphan blob: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return removeEmptyDirs(st.blobsDir)
}

func (st *store) removeOrphanRetainedFiles() error {
	return st.removeOrphanOutputFiles(retainedRoot(st.versionDir), func(path string) bool {
		return strings.HasSuffix(path, ".a") || strings.HasSuffix(path, ".go")
	})
}

func (st *store) removeOrphanOutputFiles(root string, include func(string) bool) error {
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("stat retained root: %w", err)
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !include(path) {
			return nil
		}
		outputID := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		entryRefs, err := st.q.countEntriesByOutputID(context.Background(), outputID)
		if err != nil {
			return fmt.Errorf("query retained references: %w", err)
		}
		if entryRefs == 0 {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove orphan retained file: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return removeEmptyDirs(root)
}

func removeEmptyDirs(root string) error {
	var dirs []string
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, dir := range slices.Backward(dirs) {
		if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			if entries, readErr := os.ReadDir(dir); readErr != nil || len(entries) != 0 {
				continue
			}
		}
	}
	return nil
}

func (st *store) blobDir(outputHex string) string {
	shard := "xx"
	if len(outputHex) >= 2 {
		shard = outputHex[:2]
	}
	return filepath.Join(st.blobsDir, shard)
}

func (st *store) blobPath(outputHex string) string {
	return filepath.Join(st.blobDir(outputHex), outputHex+".zst")
}
