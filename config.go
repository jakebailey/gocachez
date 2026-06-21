package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type config struct {
	dir        string
	maxSize    int64
	verbose    bool
	cpuProfile string
	memProfile string
}

type fileConfig struct {
	CacheDir *string `json:"cacheDir"`
	MaxSize  *string `json:"maxSize"`
	Verbose  *bool   `json:"verbose"`
}

func parseFlags(args []string) (config, error) {
	cfg, operands, err := parseFlagOperands(args)
	if err != nil {
		return config{}, err
	}
	if len(operands) != 0 {
		return config{}, fmt.Errorf("unexpected argument %q", operands[0])
	}
	return cfg, nil
}

func parseFlagOperands(args []string) (config, []string, error) {
	cfg, err := defaultConfig()
	if err != nil {
		return config{}, nil, err
	}
	configPath, configRequired := defaultConfigPath()

	var flagDir, flagMaxSize string
	var flagVerbose bool

	fs := flag.NewFlagSet("gocachez", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&configPath, "config", configPath, "JSON config file")
	fs.StringVar(&flagDir, "dir", "", "cache directory")
	fs.StringVar(&flagMaxSize, "max-size", "", "maximum compressed cache size, or 0 to disable pruning")
	fs.BoolVar(&flagVerbose, "v", false, "log cache maintenance to stderr")
	fs.StringVar(&cfg.cpuProfile, "cpuprofile", "", "write CPU profile to file")
	fs.StringVar(&cfg.memProfile, "memprofile", "", "write memory profile to file")
	if err := fs.Parse(args); err != nil {
		return config{}, nil, err
	}

	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		visited[f.Name] = true
	})
	if visited["config"] {
		configRequired = true
	}

	if configPath != "" {
		if err := applyConfigFile(&cfg, configPath, configRequired); err != nil {
			return config{}, nil, err
		}
	}
	if value := os.Getenv("GOCACHEZ_DIR"); value != "" {
		cfg.dir = value
	}
	if value := os.Getenv("GOCACHEZ_MAX_SIZE"); value != "" {
		if cfg.maxSize, err = parseSize(value); err != nil {
			return config{}, nil, fmt.Errorf("parse GOCACHEZ_MAX_SIZE: %w", err)
		}
	}
	if value := os.Getenv("GOCACHEZ_VERBOSE"); value != "" {
		if cfg.verbose, err = strconv.ParseBool(value); err != nil {
			return config{}, nil, fmt.Errorf("parse GOCACHEZ_VERBOSE: %w", err)
		}
	}

	if visited["dir"] {
		cfg.dir = flagDir
	}
	if visited["max-size"] {
		if cfg.maxSize, err = parseSize(flagMaxSize); err != nil {
			return config{}, nil, fmt.Errorf("parse -max-size: %w", err)
		}
	}
	if visited["v"] {
		cfg.verbose = flagVerbose
	}

	abs, err := filepath.Abs(cfg.dir)
	if err != nil {
		return config{}, nil, fmt.Errorf("resolve cache dir: %w", err)
	}
	cfg.dir = abs
	return cfg, fs.Args(), nil
}

func defaultConfig() (config, error) {
	userCacheDir, err := os.UserCacheDir()
	if err != nil {
		return config{}, fmt.Errorf("find user cache dir: %w", err)
	}
	maxSize, err := parseSize("20GiB")
	if err != nil {
		return config{}, err
	}
	return config{
		dir:     filepath.Join(userCacheDir, "gocachez"),
		maxSize: maxSize,
	}, nil
}

func defaultConfigPath() (string, bool) {
	if value := os.Getenv("GOCACHEZ_CONFIG"); value != "" {
		return value, true
	}
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		// A default config file is optional; absence of a config directory should not disable startup.
		return "", false
	}
	return filepath.Join(userConfigDir, "gocachez", "config.json"), false
}

func applyConfigFile(cfg *config, path string, required bool) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) && !required {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	var fc fileConfig
	if err := json.Unmarshal(data, &fc); err != nil {
		return fmt.Errorf("decode config %s: %w", path, err)
	}
	return applyFileConfig(cfg, fc)
}

func applyFileConfig(cfg *config, fc fileConfig) error {
	if fc.CacheDir != nil {
		cfg.dir = *fc.CacheDir
	}
	if fc.MaxSize != nil {
		maxSize, err := parseSize(*fc.MaxSize)
		if err != nil {
			return fmt.Errorf("parse config maxSize: %w", err)
		}
		cfg.maxSize = maxSize
	}
	if fc.Verbose != nil {
		cfg.verbose = *fc.Verbose
	}
	return nil
}
