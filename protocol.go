package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

type command string

const (
	cmdPut   command = "put"
	cmdGet   command = "get"
	cmdClose command = "close"
)

type runMode string

const (
	runModeProtocol runMode = ""
	runModeClean    runMode = "clean"
	runModeStatus   runMode = "status"
)

type request struct {
	ID       int64
	Command  command
	ActionID []byte `json:",omitempty"`
	OutputID []byte `json:",omitempty"`
	BodySize int64  `json:",omitempty"`
}

type response struct {
	ID            int64
	Err           string     `json:",omitempty"`
	KnownCommands []command  `json:",omitempty"`
	Miss          bool       `json:",omitempty"`
	OutputID      []byte     `json:",omitempty"`
	Size          int64      `json:",omitempty"`
	Time          *time.Time `json:",omitempty"`
	DiskPath      string     `json:",omitempty"`
}

func run(args []string, stdin io.Reader, stdout io.Writer) (err error) {
	if mode, ok := helpMode(args); ok {
		return writeHelp(stdout, mode)
	}

	cfg, mode, err := parseRunArgs(args)
	if err != nil {
		return err
	}
	stopProfiling, err := startProfiling(cfg)
	if err != nil {
		return err
	}
	defer func() {
		if profileErr := stopProfiling(); err == nil {
			err = profileErr
		}
	}()

	if mode == runModeClean {
		return cleanCache(cfg)
	}
	if mode == runModeStatus {
		return writeStatus(cfg, stdout)
	}

	st, err := newStore(cfg)
	if err != nil {
		return err
	}
	defer st.close()

	rw := &responseWriter{enc: json.NewEncoder(stdout)}
	if err := rw.write(response{
		KnownCommands: []command{cmdGet, cmdPut, cmdClose},
	}); err != nil {
		return err
	}

	br := bufio.NewReader(stdin)
	var wg sync.WaitGroup
	sem := make(chan struct{}, min(max(runtime.GOMAXPROCS(0), 1), 8))
	for {
		req, err := readRequest(br)
		if errors.Is(err, io.EOF) {
			wg.Wait()
			return rw.err()
		}
		if err != nil {
			return err
		}

		switch req.Command {
		case cmdGet:
			sem <- struct{}{}
			wg.Go(func() {
				defer func() { <-sem }()
				_ = rw.write(st.handle(req, nil))
			})
		case cmdClose:
			wg.Wait()
			if err := rw.write(st.handle(req, nil)); err != nil {
				return err
			}
			return rw.err()
		default:
			if err := rw.write(st.handle(req, br)); err != nil {
				return err
			}
		}
	}
}

func parseRunArgs(args []string) (config, runMode, error) {
	if len(args) != 0 && isRunMode(args[0]) {
		cfg, err := parseFlags(args[1:])
		return cfg, runMode(args[0]), err
	}

	cfg, err := parseFlags(args)
	return cfg, runModeProtocol, err
}

func isRunMode(arg string) bool {
	return arg == string(runModeClean) || arg == string(runModeStatus)
}

func startProfiling(cfg config) (func() error, error) {
	var cpuFile, memFile *os.File
	if cfg.cpuProfile != "" {
		f, err := os.Create(cfg.cpuProfile)
		if err != nil {
			return nil, fmt.Errorf("create CPU profile: %w", err)
		}
		cpuFile = f
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			return nil, fmt.Errorf("start CPU profile: %w", err)
		}
	}
	if cfg.memProfile != "" {
		f, err := os.Create(cfg.memProfile)
		if err != nil {
			if cpuFile != nil {
				pprof.StopCPUProfile()
				_ = cpuFile.Close()
			}
			return nil, fmt.Errorf("create memory profile: %w", err)
		}
		memFile = f
	}
	return func() error {
		var err error
		if cpuFile != nil {
			pprof.StopCPUProfile()
			if closeErr := cpuFile.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close CPU profile: %w", closeErr))
			}
		}
		if memFile != nil {
			runtime.GC()
			if writeErr := pprof.WriteHeapProfile(memFile); writeErr != nil {
				err = errors.Join(err, fmt.Errorf("write memory profile: %w", writeErr))
			}
			if closeErr := memFile.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close memory profile: %w", closeErr))
			}
		}
		return err
	}, nil
}

type responseWriter struct {
	mu       sync.Mutex
	enc      *json.Encoder
	firstErr error
}

func (rw *responseWriter) write(res response) error {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	if rw.firstErr != nil {
		return rw.firstErr
	}
	if err := rw.enc.Encode(res); err != nil {
		rw.firstErr = err
		return err
	}
	return nil
}

func (rw *responseWriter) err() error {
	rw.mu.Lock()
	defer rw.mu.Unlock()
	return rw.firstErr
}

func readRequest(br *bufio.Reader) (request, error) {
	for {
		line, err := br.ReadBytes('\n')
		if err != nil && len(bytes.TrimSpace(line)) == 0 {
			return request{}, err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			if err != nil {
				return request{}, err
			}
			continue
		}
		var req request
		if unmarshalErr := json.Unmarshal(line, &req); unmarshalErr != nil {
			return request{}, unmarshalErr
		}
		if errors.Is(err, io.EOF) {
			err = nil
		}
		return req, err
	}
}

func (st *store) handle(req request, br *bufio.Reader) response {
	res := response{ID: req.ID}
	var err error
	switch req.Command {
	case cmdPut:
		res, err = st.put(req, br)
	case cmdGet:
		res, err = st.get(req)
	case cmdClose:
	default:
		err = fmt.Errorf("unknown command %q", req.Command)
	}
	if err != nil {
		res.ID = req.ID
		res.Err = err.Error()
	}
	return res
}

func bodyReader(br *bufio.Reader, size int64) (io.Reader, error) {
	if size == 0 {
		return strings.NewReader(""), nil
	}
	if size < 0 {
		return nil, fmt.Errorf("negative body size %d", size)
	}
	raw, err := newJSONStringReader(br)
	if err != nil {
		return nil, err
	}
	return &bodyStream{
		decoded: base64.NewDecoder(base64.StdEncoding, raw),
		raw:     raw,
	}, nil
}

type bodyStream struct {
	decoded io.Reader
	raw     *jsonStringReader
}

func (r *bodyStream) Read(p []byte) (int, error) {
	return r.decoded.Read(p)
}

func (r *bodyStream) drain() error {
	_, err := io.Copy(io.Discard, r.raw)
	return err
}

func drainBody(r io.Reader) error {
	if drainer, ok := r.(interface{ drain() error }); ok {
		return drainer.drain()
	}
	_, err := io.Copy(io.Discard, r)
	return err
}

type jsonStringReader struct {
	br      *bufio.Reader
	pending []byte
	done    bool
}

func newJSONStringReader(br *bufio.Reader) (*jsonStringReader, error) {
	if err := skipWhitespace(br); err != nil {
		return nil, err
	}
	b, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != '"' {
		return nil, fmt.Errorf("expected JSON string body, got %q", b)
	}
	return &jsonStringReader{br: br}, nil
}

func (r *jsonStringReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) > 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}

	chunk, err := r.br.ReadSlice('"')
	if err == nil {
		r.done = true
		chunk = chunk[:len(chunk)-1]
	} else if !errors.Is(err, bufio.ErrBufferFull) {
		return 0, err
	}
	if bytes.IndexByte(chunk, '\\') >= 0 {
		return 0, errors.New("unsupported escape sequence in body")
	}
	n := copy(p, chunk)
	if n < len(chunk) {
		r.pending = chunk[n:]
	}
	if n == 0 && r.done {
		return 0, io.EOF
	}
	return n, nil
}

func skipWhitespace(br *bufio.Reader) error {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return err
		}
		switch b {
		case ' ', '\n', '\r', '\t':
			continue
		default:
			return br.UnreadByte()
		}
	}
}
