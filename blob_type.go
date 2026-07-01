package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const blobTypePrefixLimit = 64 << 10

// blobClassifierVersion identifies the behavior of classifyBlobData. Cached
// classifications are stored with the version that produced them (see
// entries.blob_type_version); bump this whenever the classification logic
// changes so status ignores and recomputes stale cached values.
const blobClassifierVersion = 1

type blobTypeKind int

// blobTypeKind values are persisted in the catalog's entries.blob_type column as
// a classification cache, so existing constants must not be renumbered; append
// new kinds at the end. Prefer bumping blobClassifierVersion when detection
// changes so old cached values are recomputed.
const (
	blobTypeGoPackageArchive blobTypeKind = iota
	blobTypeGoPackageIndex
	blobTypeGoAnalysisFacts
	blobTypeUnixArchive
	blobTypeGeneratedCgoSource
	blobTypeGoSource
	blobTypeELFBinary
	blobTypeMachOBinary
	blobTypePEBinary
	blobTypeWasmBinary
	blobTypeCCompilerID
	blobTypeGoTestOutput
	blobTypeGoTestInputLog
	blobTypeGoCoverageProfile
	blobTypeGoToolOutput
	blobTypeGoSourceFileList
	blobTypeGoToolFlagProbe
	blobTypeText
	blobTypeEmpty
	blobTypeUnknownBinary
	blobTypeUnreadable
)

type blobTypeStatus struct {
	kind           blobTypeKind
	count          int64
	size           int64
	compressedSize int64
}

type blobClassification struct {
	kind blobTypeKind
}

func readBlobTypeStatus(dbPath, blobsDir string) ([]blobTypeStatus, error) {
	if !regularFile(dbPath) {
		return nil, nil
	}

	db, err := openExistingDB(dbPath)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	cat := newCatalog(db)
	// The versioned query below reads blob_type_version; older caches migrate to
	// both blob-type columns together, so gate on the one the query depends on.
	hasType, err := entriesHasColumn(ctx, db, "blob_type_version")
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("inspect catalog schema: %w", err)
	}
	outputs, err := cat.listOutputs(ctx, hasType, blobClassifierVersion)
	_ = db.Close()
	if err != nil {
		return nil, fmt.Errorf("list catalog outputs: %w", err)
	}

	byKind, classified := classifyBlobTypes(blobsDir, outputs)
	if len(classified) > 0 {
		// Best effort: cache the classifications so later runs skip
		// decompressing these blobs. Ignore failures (e.g. a read-only cache).
		_ = persistBlobTypes(dbPath, classified)
	}

	statuses := make([]blobTypeStatus, 0, len(byKind))
	for _, status := range byKind {
		statuses = append(statuses, *status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		if statuses[i].size != statuses[j].size {
			return statuses[i].size > statuses[j].size
		}
		return statuses[i].kind.label() < statuses[j].kind.label()
	})
	return statuses, nil
}

func persistBlobTypes(dbPath string, classified map[string]blobTypeKind) error {
	db, err := openDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close() //nolint:errcheck

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	qtx := newCatalog(db).withTx(tx)
	for outputID, kind := range classified {
		if err := qtx.updateBlobType(ctx, outputID, kind, blobClassifierVersion); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

type blobTypeResult struct {
	output         catalogOutput
	classification blobClassification
}

// classifyBlobTypes aggregates the blobs for outputs by classification. Outputs
// carrying a cached classification (blobType) reuse it; the rest are
// decompressed and classified in parallel, and returned so the caller can cache
// them.
func classifyBlobTypes(blobsDir string, outputs []catalogOutput) (map[blobTypeKind]*blobTypeStatus, map[string]blobTypeKind) {
	byKind := make(map[blobTypeKind]*blobTypeStatus)
	classified := make(map[string]blobTypeKind)

	var pending []catalogOutput
	for _, output := range outputs {
		if output.blobType.Valid {
			addBlobType(byKind, blobTypeKind(output.blobType.Int64), output)
			continue
		}
		pending = append(pending, output)
	}
	if len(pending) == 0 {
		return byKind, classified
	}

	workers := min(len(pending), min(max(runtime.GOMAXPROCS(0), 1), 8))
	jobs := make(chan catalogOutput)
	results := make(chan blobTypeResult, workers)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			var decoder *zstd.Decoder
			defer func() {
				if decoder != nil {
					decoder.Close()
				}
			}()
			for output := range jobs {
				var classification blobClassification
				classification, decoder = classifyCompressedBlob(blobPath(blobsDir, output.outputID), decoder)
				results <- blobTypeResult{
					output:         output,
					classification: classification,
				}
			}
		})
	}

	go func() {
		for _, output := range pending {
			jobs <- output
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for result := range results {
		kind := result.classification.kind
		addBlobType(byKind, kind, result.output)
		if kind != blobTypeUnreadable {
			classified[result.output.outputID] = kind
		}
	}
	return byKind, classified
}

func addBlobType(byKind map[blobTypeKind]*blobTypeStatus, kind blobTypeKind, output catalogOutput) {
	status := byKind[kind]
	if status == nil {
		status = &blobTypeStatus{
			kind: kind,
		}
		byKind[kind] = status
	}
	status.count++
	status.size += output.size
	status.compressedSize += output.compressedSize
}

func classifyCompressedBlob(path string, decoder *zstd.Decoder) (blobClassification, *zstd.Decoder) {
	file, err := os.Open(path)
	if err != nil {
		return blobClassification{kind: blobTypeUnreadable}, decoder
	}
	defer file.Close() //nolint:errcheck

	if decoder == nil {
		decoder, err = zstd.NewReader(file, decoderOptions...)
	} else {
		err = decoder.Reset(file)
	}
	if err != nil {
		if decoder != nil {
			decoder.Close()
		}
		return blobClassification{kind: blobTypeUnreadable}, nil
	}

	data, err := io.ReadAll(io.LimitReader(decoder, blobTypePrefixLimit))
	if err != nil {
		decoder.Close()
		return blobClassification{kind: blobTypeUnreadable}, nil
	}
	return classifyBlobData(data), decoder
}

func classifyBlobData(data []byte) blobClassification {
	if len(data) == 0 {
		return blobClassification{kind: blobTypeEmpty}
	}
	if _, ok := packageArchiveExportSize(data); ok {
		return blobClassification{kind: blobTypeGoPackageArchive}
	}
	if bytes.HasPrefix(data, []byte("go index v")) {
		return blobClassification{kind: blobTypeGoPackageIndex}
	}
	if bytes.Contains(data, []byte("gobFact")) {
		return blobClassification{kind: blobTypeGoAnalysisFacts}
	}
	if bytes.HasPrefix(data, []byte(archiveMagic)) {
		return blobClassification{kind: blobTypeUnixArchive}
	}
	if isGeneratedCgoSource(data) {
		return blobClassification{kind: blobTypeGeneratedCgoSource}
	}
	if bytes.HasPrefix(data, []byte{0x7f, 'E', 'L', 'F'}) {
		return blobClassification{kind: blobTypeELFBinary}
	}
	if bytes.HasPrefix(data, []byte{'M', 'Z'}) {
		return blobClassification{kind: blobTypePEBinary}
	}
	if bytes.HasPrefix(data, []byte{0x00, 'a', 's', 'm'}) {
		return blobClassification{kind: blobTypeWasmBinary}
	}
	if hasMachOMagic(data) {
		return blobClassification{kind: blobTypeMachOBinary}
	}
	if isCCompilerID(data) {
		return blobClassification{kind: blobTypeCCompilerID}
	}
	if isGoTestOutput(data) {
		return blobClassification{kind: blobTypeGoTestOutput}
	}
	if isGoTestInputLog(data) {
		return blobClassification{kind: blobTypeGoTestInputLog}
	}
	if isGoCoverageProfile(data) {
		return blobClassification{kind: blobTypeGoCoverageProfile}
	}
	if isGoToolOutput(data) {
		return blobClassification{kind: blobTypeGoToolOutput}
	}
	if isGoSourceFileList(data) {
		return blobClassification{kind: blobTypeGoSourceFileList}
	}
	if isGoToolFlagProbe(data) {
		return blobClassification{kind: blobTypeGoToolFlagProbe}
	}
	if isLikelyText(data) {
		if looksLikeGoSource(data) {
			return blobClassification{kind: blobTypeGoSource}
		}
		return blobClassification{kind: blobTypeText}
	}
	return blobClassification{kind: blobTypeUnknownBinary}
}

func hasMachOMagic(data []byte) bool {
	magic := [][]byte{
		{0xfe, 0xed, 0xfa, 0xce},
		{0xce, 0xfa, 0xed, 0xfe},
		{0xfe, 0xed, 0xfa, 0xcf},
		{0xcf, 0xfa, 0xed, 0xfe},
		{0xca, 0xfe, 0xba, 0xbe},
	}
	for _, prefix := range magic {
		if bytes.HasPrefix(data, prefix) {
			return true
		}
	}
	return false
}

func isCCompilerID(data []byte) bool {
	const idSize = 32
	if len(data) <= idSize {
		return false
	}
	metadata := data[:len(data)-idSize]
	if len(metadata) == 0 || metadata[len(metadata)-1] != 0 {
		return false
	}

	fields := bytes.Split(metadata[:len(metadata)-1], []byte{0})
	if len(fields) == 0 || len(fields)%2 != 0 {
		return false
	}
	for i := 0; i < len(fields); i += 2 {
		if len(fields[i]) == 0 || !isCCompilerIDStat(fields[i+1]) {
			return false
		}
	}
	return true
}

func isCCompilerIDStat(data []byte) bool {
	fields := bytes.Fields(data)
	if len(fields) < 6 ||
		!bytes.Equal(fields[0], []byte("stat")) ||
		(!bytes.Equal(fields[len(fields)-1], []byte("true")) && !bytes.Equal(fields[len(fields)-1], []byte("false"))) ||
		!bytes.HasSuffix(data, []byte("\n")) {
		return false
	}
	if _, err := strconv.ParseInt(string(fields[1]), 10, 64); err != nil {
		return false
	}
	if _, err := strconv.ParseUint(string(fields[2]), 16, 64); err != nil {
		return false
	}
	return true
}

func looksLikeGoSource(data []byte) bool {
	trimmed := bytes.TrimSpace(data)
	return bytes.HasPrefix(trimmed, []byte("package ")) ||
		bytes.Contains(data, []byte("\npackage "))
}

func isLikelyText(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	for _, b := range data {
		if b == 0 {
			return false
		}
		if b < 0x20 && b != '\n' && b != '\r' && b != '\t' && b != '\f' {
			return false
		}
	}
	return true
}

func isGoTestOutput(data []byte) bool {
	markers := [][]byte{
		[]byte("\x16=== RUN"),
		[]byte("\x16=== PAUSE"),
		[]byte("\x16=== NAME"),
		[]byte("\x16=== CONT"),
		[]byte("\x16--- PASS"),
		[]byte("\x16--- FAIL"),
		[]byte("\x16--- SKIP"),
		[]byte("\x16PASS\n"),
		[]byte("\x16FAIL\n"),
		[]byte("ok  \t"),
		[]byte("?   \t"),
		[]byte("FAIL\t"),
		[]byte("PASS\nok  \t"),
		[]byte("PASS\n?   \t"),
		[]byte("FAIL\nFAIL\t"),
	}
	for _, marker := range markers {
		if bytes.HasPrefix(data, marker) || bytes.Contains(data, append([]byte("\n"), marker...)) {
			return true
		}
	}
	return false
}

func isGoTestInputLog(data []byte) bool {
	return bytes.HasPrefix(data, []byte("# test log\n"))
}

func isGoCoverageProfile(data []byte) bool {
	line, _, ok := bytes.Cut(data, []byte("\n"))
	if !ok {
		return false
	}
	return bytes.Equal(line, []byte("mode: set")) ||
		bytes.Equal(line, []byte("mode: count")) ||
		bytes.Equal(line, []byte("mode: atomic"))
}

func isGoToolOutput(data []byte) bool {
	line, _, ok := bytes.Cut(data, []byte("\n"))
	if !ok || !bytes.HasPrefix(line, []byte("# ")) || bytes.Equal(line, []byte("# test log")) {
		return false
	}
	name := line[len("# "):]
	return len(name) > 0 && !bytes.ContainsAny(name, " \t")
}

func isGoSourceFileList(data []byte) bool {
	if !isLikelyText(data) {
		return false
	}
	lines := bytes.Split(data, []byte("\n"))
	switch {
	case len(lines[len(lines)-1]) == 0:
		lines = lines[:len(lines)-1]
	case len(data) == blobTypePrefixLimit:
		lines = lines[:len(lines)-1]
	default:
		return false
	}
	for _, line := range lines {
		if !isGoSourceFileListPath(line) {
			return false
		}
	}
	return len(lines) > 0
}

func isGoSourceFileListPath(path []byte) bool {
	if len(path) == 0 ||
		bytes.HasPrefix(path, []byte("/")) ||
		bytes.HasPrefix(path, []byte("../")) ||
		bytes.ContainsAny(path, " \t\r") {
		return false
	}
	return bytes.HasPrefix(path, []byte("./")) || hasGoSourceFileListExtension(path)
}

func hasGoSourceFileListExtension(path []byte) bool {
	extensions := [][]byte{
		[]byte(".go"),
		[]byte(".s"),
		[]byte(".c"),
	}
	for _, ext := range extensions {
		if bytes.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

func isGoToolFlagProbe(data []byte) bool {
	return bytes.Equal(data, []byte("true")) || bytes.Equal(data, []byte("false"))
}

func (kind blobTypeKind) label() string {
	switch kind {
	case blobTypeGoPackageArchive:
		return "Go package archives"
	case blobTypeGoPackageIndex:
		return "Go package indexes"
	case blobTypeGoAnalysisFacts:
		return "Go analysis facts"
	case blobTypeUnixArchive:
		return "Unix archives"
	case blobTypeGeneratedCgoSource:
		return "Generated cgo sources"
	case blobTypeGoSource:
		return "Go source files"
	case blobTypeELFBinary:
		return "ELF binaries"
	case blobTypeMachOBinary:
		return "Mach-O binaries"
	case blobTypePEBinary:
		return "PE binaries"
	case blobTypeWasmBinary:
		return "WebAssembly binaries"
	case blobTypeCCompilerID:
		return "C compiler IDs"
	case blobTypeGoTestOutput:
		return "Go test outputs"
	case blobTypeGoTestInputLog:
		return "Go test input logs"
	case blobTypeGoCoverageProfile:
		return "Go coverage profiles"
	case blobTypeGoToolOutput:
		return "Go tool outputs"
	case blobTypeGoSourceFileList:
		return "Go source file lists"
	case blobTypeGoToolFlagProbe:
		return "Go tool flag probes"
	case blobTypeText:
		return "Text files"
	case blobTypeEmpty:
		return "Empty files"
	case blobTypeUnknownBinary:
		return "Unknown binary files"
	case blobTypeUnreadable:
		return "Unreadable blobs"
	default:
		return "Unknown files"
	}
}
