package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func idHex(name string, id []byte) (string, error) {
	if len(id) == 0 {
		return "", fmt.Errorf("missing %s", name)
	}
	return hex.EncodeToString(id), nil
}

func regularFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat file: %w", err)
	}
	return info.Size(), nil
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size")
	}
	if s == "0" {
		return 0, nil
	}

	i := 0
	for i < len(s) && (s[i] == '.' || s[i] == '-' || s[i] == '+' || s[i] >= '0' && s[i] <= '9') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("missing number in %q", s)
	}
	value, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return 0, err
	}
	unit := strings.TrimSpace(strings.ToLower(s[i:]))
	multiplier, ok := sizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown unit %q", unit)
	}
	if value < 0 {
		return 0, fmt.Errorf("negative size %q", s)
	}
	return int64(value * float64(multiplier)), nil
}

var sizeUnits = map[string]int64{
	"":    1,
	"b":   1,
	"k":   1 << 10,
	"kb":  1 << 10,
	"kib": 1 << 10,
	"m":   1 << 20,
	"mb":  1 << 20,
	"mib": 1 << 20,
	"g":   1 << 30,
	"gb":  1 << 30,
	"gib": 1 << 30,
	"t":   1 << 40,
	"tb":  1 << 40,
	"tib": 1 << 40,
}

func formatSize(size int64) string {
	if size < 0 {
		return "-" + formatSize(-size)
	}
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%dB", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(size)/float64(div), "KMGTPE"[exp])
}

func formatSavings(uncompressed, compressed int64) string {
	return fmt.Sprintf("%s (%s)", formatSavingsAmount(uncompressed, compressed), formatSavingsPercent(uncompressed, compressed))
}

func formatSavingsAmount(uncompressed, compressed int64) string {
	if uncompressed <= 0 {
		return "0B"
	}
	return formatSize(uncompressed - compressed)
}

func formatSavingsPercent(uncompressed, compressed int64) string {
	if uncompressed <= 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", float64(uncompressed-compressed)/float64(uncompressed)*100)
}

func unixMillis(t time.Time) int64 {
	return t.UnixNano() / int64(time.Millisecond)
}

func millisTime(ms int64) time.Time {
	return time.UnixMilli(ms)
}
