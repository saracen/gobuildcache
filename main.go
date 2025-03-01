package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
	"golang.org/x/sync/singleflight"
)

type flagArray []string

func (v *flagArray) String() string {
	return fmt.Sprintf("%v", *v)
}

func (v *flagArray) Set(value string) error {
	*v = append(*v, value)
	return nil
}

type cmd string

const (
	cmdGet   = cmd("get")
	cmdPut   = cmd("put")
	cmdClose = cmd("close")
)

type request struct {
	ID       int64
	Command  cmd
	ActionID []byte    `json:",omitempty"`
	ObjectID []byte    `json:",omitempty"` // deprecated: use OutputID
	OutputID []byte    `json:",omitempty"`
	Body     io.Reader `json:"-"`
	BodySize int64     `json:",omitempty"`
}

type response struct {
	ID            int64
	Err           string     `json:",omitempty"`
	KnownCommands []cmd      `json:",omitempty"`
	Miss          bool       `json:",omitempty"`
	OutputID      []byte     `json:",omitempty"`
	Size          int64      `json:",omitempty"`
	Time          *time.Time `json:",omitempty"`
	DiskPath      string     `json:",omitempty"`
}

type Cacher struct {
	disk   *Disk
	bucket *Bucket
	flight singleflight.Group
}

func (c *Cacher) Get(ctx context.Context, req *request) (string, error) {
	actionID := hex.EncodeToString(req.ActionID)

	slog.Debug("get", "action", actionID)

	outputID, err := c.bucket.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", fmt.Errorf("getting output id from action (bucket): %w", err)
	}
	if outputID == "" {
		return "", nil
	}

	slog.Debug("single flight get", "action", actionID, "output", outputID)
	pathname, err, shared := c.flight.Do("get"+outputID, func() (any, error) {
		return c.bucket.GetOutput(ctx, outputID)
	})
	slog.Debug("single flight get done", "action", actionID, "output", outputID)

	if shared {
		slog.Debug("get output shared", "output", outputID)
	}

	return pathname.(string), err
}

func (c *Cacher) Put(ctx context.Context, req *request) (string, error) {
	actionID := hex.EncodeToString(req.ActionID)
	outputID := hex.EncodeToString(req.OutputID)

	slog.Debug("put", "action", actionID, "output", outputID)

	pathname, err, shared := c.flight.Do("put"+outputID, func() (any, error) {
		pathname, _, err := c.bucket.PutOutput(ctx, outputID, req.Body)
		return pathname, err
	})

	if shared {
		slog.Debug("put output shared", "output", outputID)
	}

	if err != nil {
		return "", err
	}

	_, err = c.bucket.LinkActionToOutput(ctx, actionID, outputID)
	if err != nil {
		return pathname.(string), fmt.Errorf("linking action to output: %w", err)
	}

	return pathname.(string), err
}

func run(ctx context.Context, prefix, bucketURL string, readonly bool) error {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return fmt.Errorf("getting cache dir: %w", err)
	}

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return fmt.Errorf("opening bucket: %w", err)
	}
	defer bucket.Close()
	bucket = blob.PrefixedBucket(bucket, prefix)

	cacher := &Cacher{}
	cacher.disk = &Disk{cacheDir: filepath.Join(cacheDir, ".gocachebucket")}
	cacher.bucket = &Bucket{disk: cacher.disk, bucket: bucket}
	cacher.bucket.Start(ctx)

	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, actionDir), 0o777); err != nil {
		return fmt.Errorf("creating cache action dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, outputDir), 0o777); err != nil {
		return fmt.Errorf("creating cache output dir: %w", err)
	}

	caps := []cmd{cmdClose, cmdGet}
	if !readonly {
		caps = append(caps, cmdPut)
	}

	r, w := bufio.NewReader(os.Stdin), bufio.NewWriter(originalStdout)
	dec, enc := json.NewDecoder(r), json.NewEncoder(w)

	enc.Encode(response{KnownCommands: caps})
	if err := w.Flush(); err != nil {
		return err
	}

	var mu sync.Mutex
	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		// handle accidental naming of OutputID prior to Go 1.24
		if req.ObjectID != nil {
			req.OutputID = req.ObjectID
		}

		if req.Command == cmdPut {
			if req.BodySize > 0 {
				var buf []byte
				if err := dec.Decode(&buf); err != nil {
					return fmt.Errorf("decoding body: %w", err)
				}
				if int64(len(buf)) != req.BodySize {
					return fmt.Errorf("incorrect length: %d != %d", len(buf), req.BodySize)
				}
				req.Body = bytes.NewReader(buf)
			} else {
				req.Body = bytes.NewReader(nil)
			}
		}

		go func() {
			resp := &response{ID: req.ID}

			defer func() {
				if req.Command != cmdClose {
					fi, err := os.Stat(resp.DiskPath)
					if err != nil {
						resp.Err = err.Error()
					} else {
						resp.OutputID, err = hex.DecodeString(filepath.Base(resp.DiskPath))
						if err != nil {
							resp.Err = "invalid output id"
						}
						resp.Size = fi.Size()
						modTime := fi.ModTime()
						resp.Time = &modTime
					}
				}

				mu.Lock()
				enc.Encode(resp)
				w.Flush()
				mu.Unlock()
			}()

			var err error
			switch req.Command {
			case cmdClose:
				cacher.bucket.Close()

			case cmdGet:
				now := time.Now()
				resp.DiskPath, err = cacher.Get(ctx, &req)
				if err != nil {
					slog.Error("get", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "err", err, "took", time.Since(now))
				} else {
					slog.Debug("get", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "took", time.Since(now))
				}

				if err != nil {
					resp.Err = err.Error()
				}
				if resp.DiskPath == "" {
					resp.Miss = true
				}

			case cmdPut:
				now := time.Now()
				resp.DiskPath, err = cacher.Put(ctx, &req)
				if err != nil {
					slog.Error("put", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "err", err, "took", time.Since(now))
				} else {
					slog.Debug("put", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "took", time.Since(now))
				}

				if err != nil {
					resp.Err = err.Error()
				}
			}
		}()
	}
}

var originalStdout = os.Stdout

func init() {
	// annoyingly, gocloud.dev prints to stdout messing with the expected JSON output
	os.Stdout = os.Stderr
}

func main() {
	var prefix string
	var verbose bool
	var readonly bool
	var envmap flagArray

	flag.StringVar(&prefix, "p", "", "prefix")
	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.BoolVar(&readonly, "readonly", false, "readonly")
	flag.Var(&envmap, "env", "remap environment variable (example: GOOGLE_APPLICATION_CREDENTIALS=MY_ENV)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s <bucket url>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	for _, env := range envmap {
		key, val, ok := strings.Cut(env, "=")
		if !ok {
			continue
		}
		os.Setenv(key, os.Getenv(val))
	}

	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if err := run(context.Background(), prefix, flag.Arg(0), readonly); err != nil {
		slog.Error("run error", "err", err)
		os.Exit(1)
	}
}
