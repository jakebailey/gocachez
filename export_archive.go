package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	archiveMagic     = "!<arch>\n"
	archiveHeaderLen = 60
	pkgdefName       = "__.PKGDEF"
)

func stripPackageArchiveToExport(path, exportPath string) (bool, error) {
	in, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("open live file: %w", err)
	}

	prefix := make([]byte, len(archiveMagic)+archiveHeaderLen)
	if _, err := io.ReadFull(in, prefix); err != nil {
		_ = in.Close()
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, fmt.Errorf("read live archive header: %w", err)
	}
	if string(prefix[:len(archiveMagic)]) != archiveMagic {
		_ = in.Close()
		return false, nil
	}

	header := prefix[len(archiveMagic):]
	name := strings.TrimSpace(string(header[:16]))
	name = strings.TrimSuffix(name, "/")
	if name != pkgdefName || !bytes.Equal(header[58:60], []byte("`\n")) {
		_ = in.Close()
		return false, nil
	}
	size, err := strconv.ParseInt(strings.TrimSpace(string(header[48:58])), 10, 64)
	if err != nil || size < 0 {
		_ = in.Close()
		return false, nil
	}

	tmpDir := filepath.Dir(path)
	if exportPath != "" {
		tmpDir = filepath.Dir(exportPath)
		if err := os.MkdirAll(tmpDir, 0o777); err != nil {
			_ = in.Close()
			return false, fmt.Errorf("create export dir: %w", err)
		}
	}
	tmp, err := os.CreateTemp(tmpDir, filepath.Base(path)+".pkgdef-*")
	if err != nil {
		_ = in.Close()
		return false, fmt.Errorf("create stripped export archive: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmp.Write(prefix); err != nil {
		_ = in.Close()
		_ = tmp.Close()
		return false, fmt.Errorf("write stripped export archive header: %w", err)
	}
	copySize := size
	if size%2 != 0 {
		copySize++
	}
	if _, err := io.CopyN(tmp, in, copySize); err != nil {
		_ = in.Close()
		_ = tmp.Close()
		return false, fmt.Errorf("copy package export data: %w", err)
	}
	if err := in.Close(); err != nil {
		_ = tmp.Close()
		return false, fmt.Errorf("close live archive: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return false, fmt.Errorf("close stripped export archive: %w", err)
	}

	if exportPath == "" {
		if err := replaceFile(tmpPath, path); err != nil {
			return false, err
		}
		return true, nil
	}

	if err := installExportArchive(tmpPath, exportPath); err != nil {
		return false, err
	}
	if err := replaceWithExportArchive(exportPath, path); err != nil {
		return false, err
	}

	return true, nil
}

func installExportArchive(tmpPath, exportPath string) error {
	if regularFile(exportPath) {
		return nil
	}
	err := os.Rename(tmpPath, exportPath)
	if err == nil {
		return nil
	}
	if regularFile(exportPath) {
		return nil
	}
	return fmt.Errorf("install export archive: %w", err)
}

func replaceWithExportArchive(exportPath, path string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".export-*")
	if err != nil {
		return fmt.Errorf("create live export placeholder: %w", err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close live export placeholder: %w", err)
	}
	if err := os.Remove(tmpPath); err != nil {
		return fmt.Errorf("remove live export placeholder: %w", err)
	}
	if err := os.Link(exportPath, tmpPath); err != nil {
		if copyErr := copyFile(exportPath, tmpPath); copyErr != nil {
			return fmt.Errorf("copy live export archive: %w", copyErr)
		}
	}
	if err := os.Rename(tmpPath, path); err == nil {
		return nil
	}
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace live archive: %w", removeErr)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace live archive: %w", err)
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer in.Close() //nolint:errcheck

	out, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create destination: %w", err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy data: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close destination: %w", err)
	}
	return nil
}

func replaceFile(tmpPath, path string) error {
	if err := os.Rename(tmpPath, path); err == nil {
		return nil
	}
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return fmt.Errorf("replace live archive: %w", removeErr)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace live archive: %w", err)
	}
	return nil
}

func retainGeneratedCgoSource(path, retainedPath string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("read live file: %w", err)
	}
	if !isGeneratedCgoSource(data) {
		return false, nil
	}
	if err := installRetainedFile(data, retainedPath); err != nil {
		return false, err
	}
	if err := replaceWithExportArchive(retainedPath, path); err != nil {
		return false, err
	}
	return true, nil
}

func isGeneratedCgoSource(data []byte) bool {
	if !bytes.Contains(data, []byte("package ")) {
		return false
	}
	return bytes.Contains(data, []byte("Code generated by cmd/cgo; DO NOT EDIT.")) ||
		bytes.Contains(data, []byte("//go:cgo_"))
}

func installRetainedFile(data []byte, retainedPath string) error {
	if regularFile(retainedPath) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(retainedPath), 0o777); err != nil {
		return fmt.Errorf("create retained dir: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(retainedPath), filepath.Base(retainedPath)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create retained file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write retained file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close retained file: %w", err)
	}
	err = os.Rename(tmpPath, retainedPath)
	if err == nil {
		return nil
	}
	if regularFile(retainedPath) {
		return nil
	}
	return fmt.Errorf("install retained file: %w", err)
}
