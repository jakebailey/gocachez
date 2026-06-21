package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestClassifyCompressedBlobReusesDecoderAfterPrefixRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first := filepath.Join(dir, "first.zst")
	second := filepath.Join(dir, "second.zst")
	writeCompressedTestBlob(t, first, bytes.Repeat([]byte("a"), blobTypePrefixLimit+1))
	writeCompressedTestBlob(t, second, bytes.Repeat([]byte("b"), blobTypePrefixLimit+1))

	classification, decoder := classifyCompressedBlob(first, nil)
	if classification.kind != blobTypeText {
		t.Fatalf("first classification = %v, want %v", classification.kind, blobTypeText)
	}
	if decoder == nil {
		t.Fatal("first classification did not return a reusable decoder")
	}
	defer decoder.Close()

	classification, decoder = classifyCompressedBlob(second, decoder)
	if classification.kind != blobTypeText {
		t.Fatalf("second classification = %v, want %v", classification.kind, blobTypeText)
	}
	if decoder == nil {
		t.Fatal("second classification did not return a reusable decoder")
	}
}

func TestClassifyBlobDataRecognizesCCompilerID(t *testing.T) {
	t.Parallel()

	body := append([]byte("/usr/lib/ccache/bin/gcc\x00stat 1448536 1ed 2026-05-13 07:28:29 -0700 PDT false\n\x00"),
		bytes.Repeat([]byte{0x86}, 32)...)

	classification := classifyBlobData(body)
	if classification.kind != blobTypeCCompilerID {
		t.Fatalf("classification = %v, want %v", classification.kind, blobTypeCCompilerID)
	}
}

func writeCompressedTestBlob(t *testing.T, path string, body []byte) {
	t.Helper()

	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	zw, err := zstd.NewWriter(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if _, err := zw.Write(body); err != nil {
		_ = zw.Close()
		_ = file.Close()
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
