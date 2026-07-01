package main

import (
	"fmt"
	"io"
)

func helpMode(args []string) (runMode, bool) {
	for _, arg := range args {
		if arg != "-h" && arg != "--help" && arg != "-help" {
			continue
		}
		for _, candidate := range args {
			if isRunMode(candidate) {
				return runMode(candidate), true
			}
		}
		return runModeProtocol, true
	}
	return runModeProtocol, false
}

func writeHelp(w io.Writer, mode runMode) error {
	var text string
	switch mode {
	case runModeClean:
		text = cleanHelp
	case runModeStatus:
		text = statusHelp
	default:
		text = rootHelp
	}
	if _, err := fmt.Fprint(w, text); err != nil {
		return fmt.Errorf("write help: %w", err)
	}
	return nil
}

const rootHelp = `Usage:
  gocachez [flags]
  gocachez <command> [flags]

Commands:
  clean    Remove inactive cache state
  status   Show cache state

Flags:
  -config path    JSON config file
  -dir path       cache directory
  -max-size size  maximum compressed cache size, or 0 to disable pruning
  -max-age dur    maximum age of unused entries, or 0 to disable age-based pruning
  -v              log cache maintenance to stderr
  -h              show help
`

const cleanHelp = `Usage:
  gocachez clean [flags]

Remove inactive cache state. State currently used by active gocachez processes
is preserved.

Flags:
  -config path  JSON config file
  -dir path     cache directory
  -h            show help
`

const statusHelp = `Usage:
  gocachez status [flags]

Show cache state without modifying it.

Flags:
  -config path  JSON config file
  -dir path     cache directory
  -h            show help
`
