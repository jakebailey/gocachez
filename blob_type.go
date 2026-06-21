package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"
)

const blobTypePrefixLimit = 64 << 10

type blobTypeKind int

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
	exportDataSize int64
}

type blobClassification struct {
	kind           blobTypeKind
	exportDataSize int64
}

func readBlobTypeStatus(dbPath, blobsDir string) ([]blobTypeStatus, error) {
	if !regularFile(dbPath) {
		return nil, nil
	}

	db, err := openExistingDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close() //nolint:errcheck

	outputs, err := newCatalog(db).listOutputs(context.Background())
	if err != nil {
		return nil, fmt.Errorf("list catalog outputs: %w", err)
	}

	byKind := classifyBlobTypes(blobsDir, outputs)
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

type blobTypeResult struct {
	output         catalogOutput
	classification blobClassification
}

func classifyBlobTypes(blobsDir string, outputs []catalogOutput) map[blobTypeKind]*blobTypeStatus {
	byKind := make(map[blobTypeKind]*blobTypeStatus)
	if len(outputs) == 0 {
		return byKind
	}

	workers := min(len(outputs), min(max(runtime.GOMAXPROCS(0), 1), 8))
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
		for _, output := range outputs {
			jobs <- output
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for result := range results {
		output := result.output
		classification := result.classification
		status := byKind[classification.kind]
		if status == nil {
			status = &blobTypeStatus{
				kind: classification.kind,
			}
			byKind[classification.kind] = status
		}
		status.count++
		status.size += output.size
		status.compressedSize += output.compressedSize
		status.exportDataSize += classification.exportDataSize
	}
	return byKind
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
	if exportDataSize, ok := packageArchiveExportSize(data); ok {
		return blobClassification{
			kind:           blobTypeGoPackageArchive,
			exportDataSize: exportDataSize,
		}
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
