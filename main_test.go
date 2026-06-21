package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

func TestStorePutGet(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	body := bytes.NewBufferString("hello from the cache")
	actionID := bytes.Repeat([]byte{1}, 32)
	outputID := bytes.Repeat([]byte{2}, 32)
	req := request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(body.Len()),
	}
	res, err := st.put(req, bufio.NewReader(encodedBody(body.Bytes())))
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(res.DiskPath) {
		t.Fatalf("put DiskPath is not absolute: %q", res.DiskPath)
	}
	if rel, err := filepath.Rel(st.liveRoot, res.DiskPath); err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("put DiskPath = %q, not under live root %q", res.DiskPath, st.liveRoot)
	}
	gotBody, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != "hello from the cache" {
		t.Fatalf("put body = %q", gotBody)
	}

	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss {
		t.Fatal("get missed")
	}
	if !bytes.Equal(getRes.OutputID, outputID) {
		t.Fatalf("OutputID = %x, want %x", getRes.OutputID, outputID)
	}
	if getRes.Size != int64(len(gotBody)) {
		t.Fatalf("Size = %d, want %d", getRes.Size, len(gotBody))
	}
	gotBody, err = os.ReadFile(getRes.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotBody) != "hello from the cache" {
		t.Fatalf("get body = %q", gotBody)
	}
}

func TestGetMiss(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{33}, 32)
	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.ID != 1 || !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
}

func TestGetMaterializesAfterLiveFileRemoved(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{34}, 32)
	body := []byte("materialize this")
	outputID := sha256Sum(body)
	putRes, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body)))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(putRes.DiskPath); err != nil {
		t.Fatal(err)
	}

	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss || getRes.DiskPath == putRes.DiskPath {
		t.Fatalf("get response = %+v, put path %q", getRes, putRes.DiskPath)
	}
	got, err := os.ReadFile(getRes.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("materialized body = %q, want %q", got, body)
	}
}

func TestGetRejectsInvalidCatalogOutputID(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := hexOf(bytes.Repeat([]byte{36}, 32))
	outputID := "not-hex"
	path := filepath.Join(st.runDir, "body")
	if err := os.WriteFile(path, []byte("body"), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := st.upsertEntry(entry{
		ActionID:       actionID,
		OutputID:       outputID,
		Size:           4,
		CompressedSize: 4,
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	st.setMaterialized(outputID, path)

	if _, err := st.get(request{ID: 1, Command: cmdGet, ActionID: bytes.Repeat([]byte{36}, 32)}); err == nil {
		t.Fatal("get accepted invalid catalog output ID")
	}
}

func TestInvalidMaterializedBlobIsCacheMiss(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{38}, 32)
	body := []byte("body")
	outputHex := hexOf(sha256Sum(body))
	if err := os.MkdirAll(st.blobDir(outputHex), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := writeCompressedFile(st, st.blobPath(outputHex), body); err != nil {
		t.Fatal(err)
	}
	if err := st.upsertEntry(entry{
		ActionID:       hexOf(actionID),
		OutputID:       outputHex,
		Size:           int64(len(body)) + 1,
		CompressedSize: int64(len(body)),
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
}

func TestOutputIDIsOpaque(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{39}, 32)
	outputID := bytes.Repeat([]byte{40}, 32)
	body := []byte("body")
	outputHex := hexOf(outputID)
	if err := os.MkdirAll(st.blobDir(outputHex), 0o777); err != nil {
		t.Fatal(err)
	}
	if err := writeCompressedFile(st, st.blobPath(outputHex), body); err != nil {
		t.Fatal(err)
	}
	if err := st.upsertEntry(entry{
		ActionID:       hexOf(actionID),
		OutputID:       outputHex,
		Size:           int64(len(body)),
		CompressedSize: int64(len(body)),
		CreatedAt:      time.Now(),
		AccessedAt:     time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 1, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Miss || !bytes.Equal(res.OutputID, outputID) {
		t.Fatalf("get response = %+v, want hit with opaque OutputID %x", res, outputID)
	}
	got, err := os.ReadFile(res.DiskPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestPutAcceptsZeroSizeBody(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{35}, 32)
	outputID := sha256Sum(nil)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 0,
	}, bufio.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if res.DiskPath == "" {
		t.Fatalf("put response = %+v", res)
	}
	getRes, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if getRes.Miss || getRes.Size != 0 {
		t.Fatalf("get response = %+v", getRes)
	}
}

func TestCacheRejectsInvalidRequests(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := st.put(request{ID: 1, Command: cmdPut}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted missing ActionID")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
	}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted missing OutputID")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
		OutputID: bytes.Repeat([]byte{2}, 32),
		BodySize: -1,
	}, bufio.NewReader(nil)); err == nil {
		t.Fatal("put accepted negative body size")
	}
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{1}, 32),
		OutputID: bytes.Repeat([]byte{2}, 32),
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("x")))); err == nil {
		t.Fatal("put accepted mismatched body size")
	}
	if _, err := st.get(request{ID: 2, Command: cmdGet}); err == nil {
		t.Fatal("get accepted missing ActionID")
	}
}

func TestPutDrainsBodyOnSetupError(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	notDir := filepath.Join(t.TempDir(), "not-dir")
	if err := os.WriteFile(notDir, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	st.blobsDir = notDir

	var input bytes.Buffer
	input.Write(encodedBody([]byte("body")).Bytes())
	writeJSON(t, &input, request{ID: 2, Command: cmdClose})
	br := bufio.NewReader(&input)

	_, err = st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: bytes.Repeat([]byte{41}, 32),
		OutputID: bytes.Repeat([]byte{42}, 32),
		BodySize: 4,
	}, br)
	if err == nil {
		t.Fatal("put succeeded with invalid blobs dir")
	}

	req, err := readRequest(br)
	if err != nil {
		t.Fatal(err)
	}
	if req.ID != 2 || req.Command != cmdClose {
		t.Fatalf("next request = %+v, want close request", req)
	}
}

func TestEncoderAndDecoderPools(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	var compressed bytes.Buffer
	enc, err := st.getEncoder(&compressed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write([]byte("body")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	st.putEncoder(enc)

	var compressedAgain bytes.Buffer
	enc, err = st.getEncoder(&compressedAgain)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enc.Write([]byte("again")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	st.putEncoder(enc)

	dec, err := st.getDecoder(bytes.NewReader(compressed.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	st.putDecoder(dec)
	dec, err = st.getDecoder(strings.NewReader("not zstd"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(dec); err == nil {
		t.Fatal("pooled decoder accepted invalid zstd")
	}
	dec.Close()
}

func TestVersionedLayout(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	wantVersionDir := filepath.Join(cacheDir, "v1")
	if st.versionDir != wantVersionDir {
		t.Fatalf("versionDir = %q, want %q", st.versionDir, wantVersionDir)
	}
	if st.blobsDir != filepath.Join(wantVersionDir, "blobs") {
		t.Fatalf("blobsDir = %q, want under version dir", st.blobsDir)
	}
	if st.liveRoot != filepath.Join(wantVersionDir, "live") {
		t.Fatalf("liveRoot = %q, want under version dir", st.liveRoot)
	}
	if _, err := os.Stat(filepath.Join(wantVersionDir, "cache.db")); err != nil {
		t.Fatal(err)
	}
	var version int
	ctx := context.Background()
	if err := st.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != cacheSchemaVersion {
		t.Fatalf("user_version = %d, want %d", version, cacheSchemaVersion)
	}
	var synchronous int
	if err := st.db.QueryRowContext(ctx, `PRAGMA synchronous`).Scan(&synchronous); err != nil {
		t.Fatal(err)
	}
	if synchronous != 1 {
		t.Fatalf("synchronous = %d, want 1 (NORMAL)", synchronous)
	}
}

func TestRejectsMismatchedDBVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "cache.db")
	db, err := openDB(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(context.Background(), `PRAGMA user_version = 999`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = openDB(dbPath)
	if err == nil {
		_ = db.Close()
		t.Fatal("openDB succeeded with an unsupported user_version")
	}
}

func TestReclaimsAbandonedUnlockedRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{18}, 32)
	outputID := bytes.Repeat([]byte{19}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	runID := st.runID
	runDir := st.runDir
	livePath := res.DiskPath
	abandonStore(t, st)

	st, err = newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("abandoned run dir still exists: err=%v", err)
	}
	if _, err := os.Stat(livePath); !os.IsNotExist(err) {
		t.Fatalf("abandoned live file still exists: err=%v", err)
	}
	if got := countRows(t, st.db, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, runID); got != 0 {
		t.Fatalf("abandoned run rows = %d, want 0", got)
	}
}

func TestKeepsLockedRun(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st1, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st1.close()
	actionID := bytes.Repeat([]byte{20}, 32)
	outputID := bytes.Repeat([]byte{21}, 32)
	if _, err := st1.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}

	st2, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.close()

	if _, err := os.Stat(st1.runDir); err != nil {
		t.Fatalf("locked run dir was removed: %v", err)
	}
	if got := countRows(t, st2.db, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, st1.runID); got != 1 {
		t.Fatalf("locked run rows = %d, want 1", got)
	}
}

func TestCleanupDropsMissingRunRecord(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	missingRunDir := filepath.Join(st.liveRoot, "run-missing")
	missingLock := filepath.Join(missingRunDir, "run.lock")
	if err := st.q.registerRun(context.Background(), "run-missing", missingRunDir, missingLock, unixMillis(time.Now())); err != nil {
		t.Fatal(err)
	}
	if err := st.cleanupAbandonedRuns(); err != nil {
		t.Fatal(err)
	}
	if got := countRows(t, st.db, `SELECT COUNT(*) FROM runs WHERE run_id = ?`, "run-missing"); got != 0 {
		t.Fatalf("missing run rows = %d, want 0", got)
	}
}

func TestCloseDropsLiveFiles(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{5}, 32)
	outputID := bytes.Repeat([]byte{6}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(res.DiskPath); err != nil {
		t.Fatal(err)
	}
	runDir := st.runDir
	st.close()
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("live run dir still exists: err=%v", err)
	}
}

func TestPruneKeepsLiveBlobs(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{7}, 32)
	outputID := bytes.Repeat([]byte{8}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 64,
	}, bufio.NewReader(encodedBody(bytes.Repeat([]byte("x"), 64)))); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("live entry was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(outputID))); err != nil {
		t.Fatalf("live blob was pruned: %v", err)
	}
}

func TestPruneUsesLifecycleLock(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	lock := flock.New(st.lifecycleLockPath)
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	done := make(chan error, 1)
	go func() {
		done <- st.prune()
	}()

	select {
	case err := <-done:
		t.Fatalf("prune finished while lifecycle lock was held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNewStoreUsesLifecycleLock(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cacheDir, "v1"), 0o777); err != nil {
		t.Fatal(err)
	}
	lock := flock.New(filepath.Join(cacheDir, "v1", "lifecycle.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Close() //nolint:errcheck

	type result struct {
		st  *store
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := newStore(config{dir: cacheDir})
		done <- result{st: st, err: err}
	}()

	select {
	case res := <-done:
		if res.st != nil {
			res.st.close()
		}
		t.Fatalf("newStore finished while lifecycle lock was held: %v", res.err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	res := <-done
	if res.err != nil {
		t.Fatal(res.err)
	}
	res.st.close()
}

func TestPruneRemovesUnusedBlobs(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir:     cacheDir,
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{9}, 32)
	outputID := bytes.Repeat([]byte{10}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 64,
	}, bufio.NewReader(encodedBody(bytes.Repeat([]byte("x"), 64)))); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	st.close()

	st, err = newStore(config{
		dir:     cacheDir,
		maxSize: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("entry was not pruned: %v", err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob was not pruned: %v", err)
	}
}

func TestPruneRemovesOrphanBlobsWithSizePruningDisabled(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir:     t.TempDir(),
		maxSize: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	outputID := strings.Repeat("a", 64)
	blobDir := st.blobDir(outputID)
	if err := os.MkdirAll(blobDir, 0o777); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(outputID)
	if err := os.WriteFile(blobPath, []byte("orphan"), 0o666); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("orphan blob stat err = %v, want not exist", err)
	}
}

func TestCatalogQueriesRespectCanceledContext(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := st.q.listOtherRuns(ctx, st.runID); err == nil {
		t.Fatal("listOtherRuns succeeded with canceled context")
	}
	if _, err := st.q.pruneCandidates(ctx); err == nil {
		t.Fatal("pruneCandidates succeeded with canceled context")
	}
}

func TestPruneUsesBlobLRU(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	sharedOutputID := bytes.Repeat([]byte{11}, 32)
	oldSharedActionID := bytes.Repeat([]byte{12}, 32)
	newSharedActionID := bytes.Repeat([]byte{13}, 32)
	prunedOutputID := bytes.Repeat([]byte{14}, 32)
	prunedActionID := bytes.Repeat([]byte{15}, 32)
	body := bytes.Repeat([]byte("x"), 64)

	for _, tc := range []struct {
		actionID []byte
		outputID []byte
	}{
		{oldSharedActionID, sharedOutputID},
		{newSharedActionID, sharedOutputID},
		{prunedActionID, prunedOutputID},
	} {
		if _, err := st.put(request{
			ID:       1,
			Command:  cmdPut,
			ActionID: tc.actionID,
			OutputID: tc.outputID,
			BodySize: int64(len(body)),
		}, bufio.NewReader(encodedBody(body))); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := st.db.ExecContext(
		context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(1000),
		hexOf(oldSharedActionID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(
		context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(3000),
		hexOf(newSharedActionID),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(
		context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(2000),
		hexOf(prunedActionID),
	); err != nil {
		t.Fatal(err)
	}

	total, err := st.compressedSize()
	if err != nil {
		t.Fatal(err)
	}
	prunedInfo, err := os.Stat(st.blobPath(hexOf(prunedOutputID)))
	if err != nil {
		t.Fatal(err)
	}
	st.maxSize = total - prunedInfo.Size()
	if _, err := st.db.ExecContext(context.Background(), `DELETE FROM runs WHERE run_id = ?`, st.runID); err != nil {
		t.Fatal(err)
	}
	if err := st.prune(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.lookupEntry(hexOf(prunedActionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("middle-aged blob was not pruned: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(oldSharedActionID)); err != nil {
		t.Fatalf("shared output old action was pruned: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(newSharedActionID)); err != nil {
		t.Fatalf("shared output new action was pruned: %v", err)
	}
	if _, err := os.Stat(st.blobPath(hexOf(sharedOutputID))); err != nil {
		t.Fatalf("shared output blob was pruned: %v", err)
	}
}

func TestAccessTimesFlushOnClose(t *testing.T) {
	t.Parallel()

	st, err := newStore(config{
		dir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{30}, 32)
	outputID := bytes.Repeat([]byte{31}, 32)
	body := []byte("access body")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	actionHex := hexOf(actionID)
	if _, err := st.db.ExecContext(
		context.Background(),
		`UPDATE entries SET accessed_at = ? WHERE action_id = ?`,
		int64(1000),
		actionHex,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID}); err != nil {
		t.Fatal(err)
	}
	if err := st.flushAccessTimes(); err != nil {
		t.Fatal(err)
	}
	var accessedAt int64
	if err := st.db.QueryRowContext(
		context.Background(),
		`SELECT accessed_at FROM entries WHERE action_id = ?`,
		actionHex,
	).Scan(&accessedAt); err != nil {
		t.Fatal(err)
	}
	if accessedAt <= 1000 {
		t.Fatalf("accessed_at = %d, want > 1000", accessedAt)
	}
}

func TestRunProtocol(t *testing.T) {
	t.Parallel()

	actionID := bytes.Repeat([]byte{3}, 32)
	outputID := bytes.Repeat([]byte{4}, 32)
	body := []byte("protocol body")

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	})
	stdin.WriteByte('\n')
	writeJSON(t, &stdin, base64.StdEncoding.EncodeToString(body))
	writeJSON(t, &stdin, request{
		ID:       2,
		Command:  cmdGet,
		ActionID: actionID,
	})
	writeJSON(t, &stdin, request{ID: 3, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if len(hello.KnownCommands) != 3 {
		t.Fatalf("KnownCommands = %v", hello.KnownCommands)
	}
	var putRes response
	if err := dec.Decode(&putRes); err != nil {
		t.Fatal(err)
	}
	if putRes.ID != 1 || putRes.Err != "" || putRes.DiskPath == "" {
		t.Fatalf("put response = %+v", putRes)
	}
	var getRes response
	if err := dec.Decode(&getRes); err != nil {
		t.Fatal(err)
	}
	if getRes.ID != 2 || getRes.Err != "" || getRes.Miss || !bytes.Equal(getRes.OutputID, outputID) {
		t.Fatalf("get response = %+v", getRes)
	}
	var closeRes response
	if err := dec.Decode(&closeRes); err != nil {
		t.Fatal(err)
	}
	if closeRes.ID != 3 || closeRes.Err != "" {
		t.Fatalf("close response = %+v", closeRes)
	}
}

func TestRunProtocolHandlesFinalRequestWithoutNewline(t *testing.T) {
	t.Parallel()

	var stdin bytes.Buffer
	stdin.WriteString(`{"ID":1,"Command":"close"}`)

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var closeRes response
	if err := dec.Decode(&closeRes); err != nil {
		t.Fatal(err)
	}
	if closeRes.ID != 1 || closeRes.Err != "" {
		t.Fatalf("close response = %+v", closeRes)
	}
}

func TestRunReturnsAfterEOF(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	var hello response
	if err := json.NewDecoder(&stdout).Decode(&hello); err != nil {
		t.Fatal(err)
	}
	if len(hello.KnownCommands) != 3 {
		t.Fatalf("KnownCommands = %v", hello.KnownCommands)
	}
}

func TestRunRejectsBadProfilePath(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{
		"-dir", t.TempDir(),
		"-cpuprofile", t.TempDir(),
	}, strings.NewReader(""), &stdout)
	if err == nil {
		t.Fatal("run accepted directory CPU profile path")
	}
}

func TestRunRejectsBadArgsAndCacheDir(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	if err := run([]string{"-bad"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted bad args")
	}
	if err := run([]string{"wat"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted unexpected argument")
	}
	if err := run([]string{"-dir", t.TempDir(), "status"}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted flags before subcommand")
	}

	cacheFile := filepath.Join(t.TempDir(), "cache-file")
	if err := os.WriteFile(cacheFile, nil, 0o666); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"-dir", cacheFile}, strings.NewReader(""), &stdout); err == nil {
		t.Fatal("run accepted file cache dir")
	}
}

func TestRunHelp(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "root", args: []string{"-h"}, want: "Usage:\n  gocachez [flags]\n"},
		{name: "clean", args: []string{"clean", "-h"}, want: "Usage:\n  gocachez clean [flags]\n"},
		{name: "status", args: []string{"status", "-dir", t.TempDir(), "-h"}, want: "Usage:\n  gocachez status [flags]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			if err := run(tc.args, strings.NewReader(""), &stdout); err != nil {
				t.Fatal(err)
			}
			assertContains(t, stdout.String(), tc.want)
			assertContains(t, stdout.String(), "-h")
		})
	}
}

func TestRunHelpDoesNotCreateCacheState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	if err := run([]string{"clean", "-h", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "v1")); !os.IsNotExist(err) {
		t.Fatalf("help created cache state: %v", err)
	}
}

func TestRunCleanRemovesInactiveState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{43}, 32)
	outputID := bytes.Repeat([]byte{44}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	dbPath := filepath.Join(st.versionDir, "cache.db")
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("clean stdout = %q, want empty", stdout.String())
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(res.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("live file stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("catalog stat err = %v, want not exist", err)
	}
}

func TestRunCleanKeepsActiveState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{45}, 32)
	outputID := bytes.Repeat([]byte{46}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))

	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(blobPath); err != nil {
		t.Fatalf("active blob was removed: %v", err)
	}
	if _, err := os.Stat(res.DiskPath); err != nil {
		t.Fatalf("active live file was removed: %v", err)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); err != nil {
		t.Fatalf("active entry was removed: %v", err)
	}
}

func TestRunCleanRemovesAbandonedState(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{47}, 32)
	outputID := bytes.Repeat([]byte{48}, 32)
	res, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body"))))
	if err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	runDir := st.runDir
	abandonStore(t, st)

	if err := run([]string{"clean", "-dir", cacheDir}, strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("abandoned run dir stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(res.DiskPath); !os.IsNotExist(err) {
		t.Fatalf("abandoned live file stat err = %v, want not exist", err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("abandoned blob stat err = %v, want not exist", err)
	}
}

func TestRunStatusEmptyCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Cache directory: "+cacheDir)
	assertContains(t, got, "Max size: 20.0GiB (21474836480 bytes)")
	assertContains(t, got, "Verbose: false")
	assertContains(t, got, "Catalog: missing")
	assertContains(t, got, "Entries: 0")
	assertContains(t, got, "Outputs: 0")
	assertContains(t, got, "Catalog uncompressed size: 0B (0 bytes)")
	assertContains(t, got, "Catalog compressed size: 0B (0 bytes)")
	assertContains(t, got, "Catalog savings: 0B (0.0%)")
	assertContains(t, got, "Blobs: 0 files, 0B (0 bytes)")
	assertContains(t, got, "Live runs: 0 active, 0 inactive")
}

func TestRunStatusShowsEffectiveConfig(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir, "-max-size", "1MiB", "-v"}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Cache directory: "+cacheDir)
	assertContains(t, got, "Max size: 1.0MiB (1048576 bytes)")
	assertContains(t, got, "Verbose: true")
}

func TestRunStatusInactiveCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{49}, 32)
	outputID := bytes.Repeat([]byte{50}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}
	st.close()

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Catalog: present")
	assertContains(t, got, "Entries: 1")
	assertContains(t, got, "Outputs: 1")
	assertContains(t, got, "Catalog uncompressed size: 4B (4 bytes)")
	assertContains(t, got, "Catalog compressed size: ")
	assertContains(t, got, "Catalog savings: ")
	assertContains(t, got, "Catalog runs: 0")
	assertContains(t, got, "Blobs: 1 files, ")
	assertContains(t, got, "Live runs: 0 active, 0 inactive")
}

func TestRunStatusActiveCache(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()

	actionID := bytes.Repeat([]byte{51}, 32)
	outputID := bytes.Repeat([]byte{52}, 32)
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: 4,
	}, bufio.NewReader(encodedBody([]byte("body")))); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := run([]string{"status", "-dir", cacheDir}, strings.NewReader(""), &stdout); err != nil {
		t.Fatal(err)
	}
	got := stdout.String()
	assertContains(t, got, "Catalog: present")
	assertContains(t, got, "Entries: 1")
	assertContains(t, got, "Outputs: 1")
	assertContains(t, got, "Catalog runs: 1")
	assertContains(t, got, "Blobs: 1 files, ")
	assertContains(t, got, "Live runs: 1 active, 0 inactive")
}

func TestRunReportsInitialWriteError(t *testing.T) {
	t.Parallel()

	if err := run([]string{"-dir", t.TempDir()}, strings.NewReader(""), errWriter{}); err == nil {
		t.Fatal("run accepted failing stdout")
	}
}

func TestRunWritesProfiles(t *testing.T) {
	dir := t.TempDir()
	cpuProfile := filepath.Join(dir, "cpu.pprof")
	memProfile := filepath.Join(dir, "mem.pprof")

	var stdin bytes.Buffer
	stdin.WriteString(`{"ID":1,"Command":"close"}`)

	var stdout bytes.Buffer
	if err := run([]string{
		"-dir", filepath.Join(dir, "cache"),
		"-max-size", "0",
		"-cpuprofile", cpuProfile,
		"-memprofile", memProfile,
	}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	assertNonEmptyFile(t, cpuProfile)
	assertNonEmptyFile(t, memProfile)
}

func TestRunProtocolConcurrentGets(t *testing.T) {
	t.Parallel()

	actionID := bytes.Repeat([]byte{16}, 32)
	outputID := bytes.Repeat([]byte{17}, 32)
	body := []byte("concurrent protocol body")

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	})
	stdin.WriteByte('\n')
	writeJSON(t, &stdin, base64.StdEncoding.EncodeToString(body))
	for id := int64(2); id < 42; id++ {
		writeJSON(t, &stdin, request{
			ID:       id,
			Command:  cmdGet,
			ActionID: actionID,
		})
	}

	writeJSON(t, &stdin, request{ID: 42, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var putRes response
	if err := dec.Decode(&putRes); err != nil {
		t.Fatal(err)
	}
	if putRes.ID != 1 || putRes.Err != "" {
		t.Fatalf("put response = %+v", putRes)
	}

	seen := map[int64]bool{}
	for range 41 {
		var res response
		if err := dec.Decode(&res); err != nil {
			t.Fatal(err)
		}
		if res.Err != "" {
			t.Fatalf("response %d has error: %s", res.ID, res.Err)
		}
		seen[res.ID] = true
		if res.ID >= 2 && res.ID < 42 && (res.Miss || !bytes.Equal(res.OutputID, outputID)) {
			t.Fatalf("get response = %+v", res)
		}
	}

	for id := int64(2); id <= 42; id++ {
		if !seen[id] {
			t.Fatalf("missing response ID %d", id)
		}
	}
}

func TestRunReturnsProtocolReadError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, strings.NewReader("{bad json}\n"), &stdout)
	if err == nil {
		t.Fatal("run succeeded with bad JSON request")
	}
}

func TestRunUnknownCommandResponse(t *testing.T) {
	t.Parallel()

	var stdin bytes.Buffer
	writeJSON(t, &stdin, request{ID: 1, Command: command("wat")})
	writeJSON(t, &stdin, request{ID: 2, Command: cmdClose})

	var stdout bytes.Buffer
	if err := run([]string{"-dir", t.TempDir(), "-max-size", "0"}, &stdin, &stdout); err != nil {
		t.Fatal(err)
	}

	dec := json.NewDecoder(&stdout)
	var hello response
	if err := dec.Decode(&hello); err != nil {
		t.Fatal(err)
	}
	var unknown response
	if err := dec.Decode(&unknown); err != nil {
		t.Fatal(err)
	}
	if unknown.ID != 1 || !strings.Contains(unknown.Err, "unknown command") {
		t.Fatalf("unknown command response = %+v", unknown)
	}
}

func TestProtocolHelpersRejectInvalidBodies(t *testing.T) {
	t.Parallel()

	if _, err := bodyReader(bufio.NewReader(strings.NewReader("")), -1); err == nil {
		t.Fatal("bodyReader accepted negative size")
	}
	if _, err := bodyReader(bufio.NewReader(strings.NewReader("null")), 1); err == nil {
		t.Fatal("bodyReader accepted non-string body")
	}
	r, err := bodyReader(bufio.NewReader(strings.NewReader(`"bad\n"`)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("bodyReader accepted escaped string body")
	}
}

func TestJSONStringReaderSmallReads(t *testing.T) {
	t.Parallel()

	raw, err := newJSONStringReader(bufio.NewReaderSize(strings.NewReader(`"abcdef"`), 3))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	var got []byte
	for {
		n, err := raw.Read(buf)
		got = append(got, buf[:n]...)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != "abcdef" {
		t.Fatalf("read = %q, want abcdef", got)
	}
}

func TestReadRequestSkipsBlankLinesAndReportsEOF(t *testing.T) {
	t.Parallel()

	br := bufio.NewReader(strings.NewReader("\n \n"))
	if _, err := readRequest(br); !errors.Is(err, io.EOF) {
		t.Fatalf("readRequest err = %v, want EOF", err)
	}
}

func TestBodyReaderZeroSize(t *testing.T) {
	t.Parallel()

	r, err := bodyReader(bufio.NewReader(strings.NewReader("")), 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("body = %q, want empty", got)
	}
}

func TestBodyReaderReportsEOFBeforeString(t *testing.T) {
	t.Parallel()

	if _, err := bodyReader(bufio.NewReader(strings.NewReader("")), 1); !errors.Is(err, io.EOF) {
		t.Fatalf("bodyReader err = %v, want EOF", err)
	}
}

func TestJSONStringReaderLargeStringAndZeroRead(t *testing.T) {
	t.Parallel()

	want := strings.Repeat("a", 5000)
	raw, err := newJSONStringReader(bufio.NewReaderSize(strings.NewReader(strconvQuote(want)), 16))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := raw.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero Read = %d, %v; want 0, nil", n, err)
	}
	got, err := io.ReadAll(raw)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("read len = %d, want %d", len(got), len(want))
	}
}

func TestResponseWriterKeepsFirstError(t *testing.T) {
	t.Parallel()

	rw := &responseWriter{enc: json.NewEncoder(errWriter{})}
	if err := rw.write(response{ID: 1}); err == nil {
		t.Fatal("write succeeded")
	}
	if err := rw.write(response{ID: 2}); err == nil {
		t.Fatal("second write succeeded")
	}
	if err := rw.err(); err == nil {
		t.Fatal("err returned nil")
	}
}

func TestCorruptBlobIsCacheMiss(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	st, err := newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	actionID := bytes.Repeat([]byte{28}, 32)
	outputID := bytes.Repeat([]byte{29}, 32)
	body := []byte("valid body")
	if _, err := st.put(request{
		ID:       1,
		Command:  cmdPut,
		ActionID: actionID,
		OutputID: outputID,
		BodySize: int64(len(body)),
	}, bufio.NewReader(encodedBody(body))); err != nil {
		t.Fatal(err)
	}
	blobPath := st.blobPath(hexOf(outputID))
	st.close()

	st, err = newStore(config{
		dir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.close()
	if err := os.WriteFile(blobPath, []byte("not zstd"), 0o666); err != nil {
		t.Fatal(err)
	}

	res, err := st.get(request{ID: 2, Command: cmdGet, ActionID: actionID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Miss {
		t.Fatalf("get response = %+v, want miss", res)
	}
	if _, err := st.lookupEntry(hexOf(actionID)); !errorsIs(err, sql.ErrNoRows) {
		t.Fatalf("entry = %v, want sql.ErrNoRows", err)
	}
	if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
		t.Fatalf("blob stat err = %v, want not exist", err)
	}
}

func TestParseFlagsLoadsDefaultConfigFile(t *testing.T) {
	setUserDirEnv(t)

	configPath, _ := defaultConfigPath()
	cacheDir := filepath.Join(t.TempDir(), "configured-cache")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o777); err != nil {
		t.Fatal(err)
	}
	configJSON := `{
		"cacheDir": ` + strconvQuote(cacheDir) + `,
		"maxSize": "123MiB",
		"verbose": true
	}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != cacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, cacheDir)
	}
	if cfg.maxSize != 123<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 123<<20)
	}
	if !cfg.verbose {
		t.Fatal("verbose = false, want true")
	}
}

func TestParseFlagsUsesUserCacheDirDefault(t *testing.T) {
	setUserDirEnv(t)

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(userCacheDir, "gocachez")
	if cfg.dir != want {
		t.Fatalf("dir = %q, want %q", cfg.dir, want)
	}
}

func TestParseFlagsRequiresExplicitConfig(t *testing.T) {
	if _, err := parseFlags([]string{"-config", filepath.Join(t.TempDir(), "missing.json")}); err == nil {
		t.Fatal("parseFlags succeeded with a missing explicit config")
	}
}

func TestParseFlagsOverrideConfigFile(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	configCacheDir := filepath.Join(configDir, "config-cache")
	flagCacheDir := filepath.Join(configDir, "flag-cache")
	configJSON := `{"cacheDir": ` + strconvQuote(configCacheDir) + `, "maxSize": "10MiB"}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o666); err != nil {
		t.Fatal(err)
	}

	cfg, err := parseFlags([]string{"-config", configPath, "-dir", flagCacheDir, "-max-size", "1MiB"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != flagCacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, flagCacheDir)
	}
	if cfg.maxSize != 1<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 1<<20)
	}
}

func TestParseFlagsUsesEnvironmentOverrides(t *testing.T) {
	cacheDir := t.TempDir()
	t.Setenv("GOCACHEZ_DIR", cacheDir)
	t.Setenv("GOCACHEZ_MAX_SIZE", "7MiB")
	t.Setenv("GOCACHEZ_VERBOSE", "true")
	t.Setenv("GOCACHEZ_CONFIG", "")

	cfg, err := parseFlags(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.dir != cacheDir {
		t.Fatalf("dir = %q, want %q", cfg.dir, cacheDir)
	}
	if cfg.maxSize != 7<<20 {
		t.Fatalf("maxSize = %d, want %d", cfg.maxSize, 7<<20)
	}
	if !cfg.verbose {
		t.Fatal("verbose = false, want true")
	}
}

func TestDefaultConfigPathUsesEnvironment(t *testing.T) {
	t.Setenv("GOCACHEZ_CONFIG", filepath.Join(t.TempDir(), "config.json"))

	path, required := defaultConfigPath()
	if path != os.Getenv("GOCACHEZ_CONFIG") || !required {
		t.Fatalf("defaultConfigPath = %q, %v; want env path required", path, required)
	}
}

func TestParseFlagsRejectsInvalidInputs(t *testing.T) {
	if _, err := parseFlags([]string{"-bad"}); err == nil {
		t.Fatal("parseFlags accepted unknown flag")
	}
	if _, err := parseFlags([]string{"-dir", t.TempDir(), "-max-size", "bad"}); err == nil {
		t.Fatal("parseFlags accepted bad max size")
	}

	t.Setenv("GOCACHEZ_MAX_SIZE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_MAX_SIZE")
	}
	t.Setenv("GOCACHEZ_MAX_SIZE", "")
	t.Setenv("GOCACHEZ_VERBOSE", "bad")
	if _, err := parseFlags([]string{"-dir", t.TempDir()}); err == nil {
		t.Fatal("parseFlags accepted bad GOCACHEZ_VERBOSE")
	}
}

func TestApplyConfigFileRejectsInvalidJSONAndSize(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	badJSON := filepath.Join(dir, "bad-json.json")
	if err := os.WriteFile(badJSON, []byte("{"), 0o666); err != nil {
		t.Fatal(err)
	}
	var cfg config
	if err := applyConfigFile(&cfg, badJSON, true); err == nil {
		t.Fatal("applyConfigFile accepted bad JSON")
	}

	badSize := filepath.Join(dir, "bad-size.json")
	if err := os.WriteFile(badSize, []byte(`{"maxSize":"wat"}`), 0o666); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigFile(&cfg, badSize, true); err == nil {
		t.Fatal("applyConfigFile accepted bad maxSize")
	}
}

func TestParseSize(t *testing.T) {
	t.Parallel()

	tests := map[string]int64{
		"0":      0,
		"42":     42,
		"1KiB":   1 << 10,
		"1.5MiB": 1<<20 + 1<<19,
		"2g":     2 << 30,
	}
	for input, want := range tests {
		got, err := parseSize(input)
		if err != nil {
			t.Fatalf("parseSize(%q): %v", input, err)
		}
		if got != want {
			t.Fatalf("parseSize(%q) = %d, want %d", input, got, want)
		}
	}
}

func TestParseSizeErrors(t *testing.T) {
	t.Parallel()

	for _, input := range []string{"", "MiB", "1bad", "-1", "."} {
		if _, err := parseSize(input); err == nil {
			t.Fatalf("parseSize(%q) succeeded", input)
		}
	}
}

func TestFormatSize(t *testing.T) {
	t.Parallel()

	tests := map[int64]string{
		0:       "0B",
		42:      "42B",
		1024:    "1.0KiB",
		1536:    "1.5KiB",
		1 << 20: "1.0MiB",
		1 << 30: "1.0GiB",
	}
	for input, want := range tests {
		if got := formatSize(input); got != want {
			t.Fatalf("formatSize(%d) = %q, want %q", input, got, want)
		}
	}
}

func TestFormatSavings(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		uncompressed int64
		compressed   int64
		want         string
	}{
		"empty": {
			uncompressed: 0,
			compressed:   0,
			want:         "0B (0.0%)",
		},
		"saved": {
			uncompressed: 100,
			compressed:   25,
			want:         "75B (75.0%)",
		},
		"grew": {
			uncompressed: 100,
			compressed:   125,
			want:         "-25B (-25.0%)",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			if got := formatSavings(tc.uncompressed, tc.compressed); got != tc.want {
				t.Fatalf("formatSavings(%d, %d) = %q, want %q", tc.uncompressed, tc.compressed, got, tc.want)
			}
		})
	}
}

func encodedBody(body []byte) *bytes.Buffer {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(base64.StdEncoding.EncodeToString(body)); err != nil {
		panic(err)
	}
	return &buf
}

func writeJSON(t *testing.T, buf *bytes.Buffer, v any) {
	t.Helper()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		t.Fatal(err)
	}
}

func hexOf(id []byte) string {
	return hex.EncodeToString(id)
}

func sha256Sum(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func writeCompressedFile(st *store, path string, body []byte) error {
	var buf bytes.Buffer
	enc, err := st.getEncoder(&buf)
	if err != nil {
		return err
	}
	if _, err := enc.Write(body); err != nil {
		return err
	}
	if err := enc.Close(); err != nil {
		return err
	}
	st.putEncoder(enc)
	return os.WriteFile(path, buf.Bytes(), 0o666)
}

func errorsIs(err, target error) bool {
	return err != nil && errors.Is(err, target)
}

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertNonEmptyFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatalf("%s is empty", path)
	}
}

func assertContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("output missing %q:\n%s", want, got)
	}
}

func setUserDirEnv(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	home := filepath.Join(root, "home")
	cacheDir := filepath.Join(root, "cache")
	configDir := filepath.Join(root, "config")
	for _, dir := range []string{home, cacheDir, configDir} {
		if err := os.MkdirAll(dir, 0o777); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv("XDG_CONFIG_HOME", configDir)
	t.Setenv("LOCALAPPDATA", cacheDir)
	t.Setenv("APPDATA", configDir)
	t.Setenv("GOCACHEZ_CONFIG", "")
}

func abandonStore(t *testing.T, st *store) {
	t.Helper()
	if err := st.runLock.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := st.runLock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.db.Close(); err != nil {
		t.Fatal(err)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func strconvQuote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}
