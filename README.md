# gocachez

`gocachez` is a [`GOCACHEPROG`](https://pkg.go.dev/cmd/go/internal/cacheprog)
helper for the Go build cache. It stores cache artifacts as zstd-compressed
files, materializes uncompressed files for the Go command on demand, and evicts
old compressed entries automatically.

Go `std`:

| Mode             |        Cold time |       Warm time | Disk usage |
| ---------------- | ---------------: | --------------: | ---------: |
| No `GOCACHEPROG` |           37.79s |           3.81s |     1.60GB |
| `gocachez`       |           48.52s |           5.29s |      403MB |
|                  | +10.73s (+28.4%) | +1.48s (+38.8%) |     -74.8% |

[`typescript-go`](https://github.com/microsoft/typescript-go):

| Mode             |       Cold time |       Warm time | Disk usage |
| ---------------- | --------------: | --------------: | ---------: |
| No `GOCACHEPROG` |          81.49s |           4.57s |     4.95GB |
| `gocachez`       |          89.92s |           6.77s |      453MB |
|                  | +8.43s (+10.3%) | +2.20s (+48.1%) |     -90.9% |

## Setup

Requires Go 1.25 or newer.

```sh
go install github.com/jakebailey/gocachez@latest
go env -w GOCACHEPROG=gocachez
```

Undo:

```sh
go env -u GOCACHEPROG
```

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

## Configuration

By default, `gocachez` reads:

```text
~/.config/gocachez/config.json
```

and stores cache data in:

```text
~/.cache/gocachez
```

Example config:

```json
{
    "cache_dir": "/path/to/gocachez",
    "max_size": "20GiB",
    "verbose": false
}
```

Config can also be selected explicitly:

```sh
go env -w GOCACHEPROG="gocachez -config /path/to/config.json"
```

Supported options:

| Config key  | Env var             | Flag        | Default                                   | Meaning                                                         |
| ----------- | ------------------- | ----------- | ----------------------------------------- | --------------------------------------------------------------- |
| `cache_dir` | `GOCACHEZ_DIR`      | `-dir`      | `os.UserCacheDir()/gocachez`              | Cache directory.                                                |
| `max_size`  | `GOCACHEZ_MAX_SIZE` | `-max-size` | `20GiB`                                   | Maximum compressed cache size; `0` disables size-based pruning. |
| `verbose`   | `GOCACHEZ_VERBOSE`  | `-v`        | `false`                                   | Enable maintenance logs on stderr.                              |
|             | `GOCACHEZ_CONFIG`   | `-config`   | `os.UserConfigDir()/gocachez/config.json` | Config file path. Missing file is an error when explicitly set. |
