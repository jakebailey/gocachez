# gocachez

`gocachez` is a [`GOCACHEPROG`](https://pkg.go.dev/cmd/go/internal/cacheprog)
helper for the Go build cache. It stores cache artifacts as zstd-compressed
files, materializes uncompressed files for the Go command on demand, and evicts
old compressed entries automatically.

This project was inspired by the discussion on
[`golang/go#76337`](https://github.com/golang/go/issues/76337).

Go `std`:

| Mode             |       Cold time |       Warm time | Disk usage |
| ---------------- | --------------: | --------------: | ---------: |
| No `GOCACHEPROG` |          36.31s |           4.06s |     1.60GB |
| `gocachez`       |          40.97s |           5.65s |      403MB |
|                  | +4.66s (+12.8%) | +1.59s (+39.1%) |     -74.8% |

[`typescript-go`](https://github.com/microsoft/typescript-go):

| Mode             |      Cold time |       Warm time | Disk usage |
| ---------------- | -------------: | --------------: | ---------: |
| No `GOCACHEPROG` |         89.20s |           4.76s |     4.95GB |
| `gocachez`       |         96.84s |           6.97s |      453MB |
|                  | +7.64s (+8.6%) | +2.21s (+46.4%) |     -90.9% |

## Installation

Requires Go 1.25 or newer.

```console
$ go install github.com/jakebailey/gocachez@latest
$ go env -w GOCACHEPROG=gocachez
```

Undo:

```console
$ go env -u GOCACHEPROG
```

## Commands

Show help:

```console
$ gocachez -h
```

Run as a `GOCACHEPROG` helper:

```console
$ gocachez
```

Remove inactive cache state:

```console
$ gocachez clean
```

`clean` removes blobs, live files, and catalog state that no active `gocachez`
process is using. State for active live runs is preserved.

Show cache state:

```console
$ gocachez status
```

`status` reports the current cache configuration and state.

## How it works

`gocachez` implements the Go command's external cache protocol over stdin and
stdout. The Go command sends `put` requests when it wants to store an artifact
and `get` requests when it wants to retrieve one.

On `put`, `gocachez`:

1. streams the artifact body into a zstd encoder,
2. writes the compressed bytes into `blobs/`,
3. writes the uncompressed body into the current run's `live/` directory, and
4. returns that live file path to the Go command as `DiskPath`.

The compressed blob is the durable cache entry. The live file is only the
temporary uncompressed file the Go command needs in order to read the artifact
from disk.

On `get`, `gocachez` looks up the action ID in SQLite. On a hit, it opens the
compressed blob, decompresses it into the current run's `live/` directory, and
returns the materialized path as `DiskPath`.

Live files are removed when the `GOCACHEPROG` process closes. If `gocachez`
exits abnormally, a later run can safely clean up abandoned live files: each run
holds an OS file lock, and another process only reclaims a run directory after
it can acquire that lock.

Some live files can escape the Go command process through `go list` output. For
example, `go list -export` reports an `Export` path that tools such as
`go/packages` and `golangci-lint` may open after the Go command has closed its
`GOCACHEPROG` helper. `go list -compiled` can also report generated cgo source
paths in `CompiledGoFiles`, and `go list -test` can report a generated
`_testmain.go` path in the synthetic test main package's `GoFiles`.

To support those tools without keeping large uncompressed archives around,
`gocachez` treats these escaped files specially on close. Package archives are
replaced with small archives containing only their `__.PKGDEF` export data, and
generated Go source files that can appear in list output are retained as-is.
These retained files are stored under `retained/`, keyed by output ID, and are
cleaned up once no catalog entry references that output.

## Configuration

By default, `gocachez` reads its config from:

```text
os.UserConfigDir()/gocachez/config.json
```

and stores cache data in:

```text
os.UserCacheDir()/gocachez
```

See [`os.UserConfigDir`](https://pkg.go.dev/os#UserConfigDir) and
[`os.UserCacheDir`](https://pkg.go.dev/os#UserCacheDir) for the
platform-specific base directories.

Example config:

```json
{
    "cacheDir": "/path/to/gocachez",
    "maxSize": "20GiB",
    "verbose": false
}
```

Config can also be selected explicitly:

```console
$ go env -w GOCACHEPROG="gocachez -config /path/to/config.json"
```

Supported options:

**Cache directory**

- Config: `cacheDir`
- Environment: `GOCACHEZ_DIR`
- Flag: `-dir`
- Default: `os.UserCacheDir()/gocachez`

**Maximum compressed cache size**

- Config: `maxSize`
- Environment: `GOCACHEZ_MAX_SIZE`
- Flag: `-max-size`
- Default: `20GiB`; `0` disables size-based pruning.

Independent of `maxSize`, entries and retained files that have not been used for
five days are trimmed automatically, matching `cmd/go`'s `GOCACHE`.

**Verbose maintenance logs**

- Config: `verbose`
- Environment: `GOCACHEZ_VERBOSE`
- Flag: `-v`
- Default: `false`

**Config file path**

- Environment: `GOCACHEZ_CONFIG`
- Flag: `-config`
- Default: `os.UserConfigDir()/gocachez/config.json`
- Missing file is an error when explicitly set.
