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

	// claims coordinates misses with other processes sharing the local
	// cache directory; nil to not.
	claims *claims
}

func (c *Cacher) Get(ctx context.Context, req *request) (string, error) {
	actionID := hex.EncodeToString(req.ActionID)

	pathname, err := c.get(ctx, actionID)
	if err != nil || pathname != "" || c.claims == nil {
		return pathname, err
	}

	return c.claims.awaitOrClaim(ctx, actionID, func() string { return c.localHit(actionID) }), nil
}

// localHit returns the path of actionID's output if it's in the local cache.
func (c *Cacher) localHit(actionID string) string {
	outputID, err := c.disk.OutputIDFromAction(context.Background(), actionID)
	if err != nil || outputID == "" {
		return ""
	}

	pathname := filepath.Join(c.disk.cacheDir, outputDir, outputID)
	if _, err := os.Stat(pathname); err != nil {
		return ""
	}
	return pathname
}

func (c *Cacher) get(ctx context.Context, actionID string) (string, error) {
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
	if c.claims != nil {
		c.claims.release(actionID)
	}
	if err != nil {
		return pathname.(string), fmt.Errorf("linking action to output: %w", err)
	}

	return pathname.(string), err
}

type options struct {
	cacheDir     string
	readonly     bool
	stats        bool
	refreshAfter time.Duration
	dedupeWait   time.Duration
}

func defaultCacheDir() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("getting cache dir: %w", err)
	}

	return filepath.Join(cacheDir, ".gocachebucket"), nil
}

func run(ctx context.Context, prefix, bucketURL string, opts options) error {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return fmt.Errorf("opening bucket: %w", err)
	}
	defer bucket.Close()
	bucket = blob.PrefixedBucket(bucket, prefix)

	return serve(ctx, bucket, opts, os.Stdin, originalStdout)
}

func serve(ctx context.Context, bucket *blob.Bucket, opts options, in io.Reader, out io.Writer) error {
	cacher := &Cacher{
		disk: &Disk{cacheDir: opts.cacheDir},
	}
	cacher.bucket = &Bucket{disk: cacher.disk, bucket: bucket, readonly: opts.readonly, refreshAfter: opts.refreshAfter}
	cacher.bucket.stats.Started = time.Now()
	cacher.bucket.Start(ctx)

	// The go command sends close before closing stdin, which waits for
	// uploads, but if stdin is closed without it we still want queued uploads
	// to finish.
	defer func() {
		if cacher.claims != nil {
			cacher.claims.releaseAll()
		}
		cacher.bucket.Close()
		if opts.stats {
			cacher.bucket.stats.Log()
		}
	}()

	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, actionDir), 0o755); err != nil {
		return fmt.Errorf("creating cache action dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, outputDir), 0o755); err != nil {
		return fmt.Errorf("creating cache output dir: %w", err)
	}

	if opts.dedupeWait > 0 {
		claims, err := newClaims(cacher.disk.cacheDir, opts.dedupeWait, &cacher.bucket.stats)
		if err != nil {
			return err
		}
		cacher.claims = claims
	}

	// Puts are accepted even when readonly, and only kept locally: the go
	// command reads back some of what it puts within the same invocation,
	// such as the generated test main of a test package, and fails if the
	// cache doesn't have it.
	caps := []cmd{cmdClose, cmdGet, cmdPut}

	r, w := bufio.NewReader(in), bufio.NewWriter(out)
	dec, enc := json.NewDecoder(r), json.NewEncoder(w)

	if err := enc.Encode(response{KnownCommands: caps}); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}

	var mu sync.Mutex

	// wait for in-flight requests before returning, so their responses and
	// any queued uploads aren't lost
	var inflight sync.WaitGroup
	defer inflight.Wait()

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

		inflight.Add(1)
		go func() {
			defer inflight.Done()

			resp := handleRequest(ctx, cacher, &req)
			mu.Lock()
			enc.Encode(resp)
			w.Flush()
			mu.Unlock()
		}()
	}
}

func handleRequest(ctx context.Context, c *Cacher, req *request) *response {
	resp := &response{ID: req.ID}

	var err error
	switch req.Command {
	case cmdClose:
		c.bucket.Close()

	case cmdGet:
		c.bucket.stats.Gets.Add(1)
		now := time.Now()
		resp.DiskPath, err = c.Get(ctx, req)
		if err == nil && resp.DiskPath != "" {
			resp.Time, err = c.putTime(req)
		}
		switch {
		case err != nil:
			c.bucket.stats.GetErrors.Add(1)
		case resp.DiskPath == "":
			c.bucket.stats.Misses.Add(1)
		default:
			c.bucket.stats.Hits.Add(1)
		}
		if err != nil {
			slog.Error("get", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "err", err, "took", time.Since(now))
			resp.Err = err.Error()
		} else {
			slog.Debug("get", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "took", time.Since(now))
		}
		if resp.DiskPath == "" {
			resp.Miss = true
		}

	case cmdPut:
		c.bucket.stats.Puts.Add(1)
		c.bucket.stats.PutBytes.Add(req.BodySize)
		now := time.Now()
		resp.DiskPath, err = c.Put(ctx, req)
		if err != nil {
			c.bucket.stats.PutErrors.Add(1)
			slog.Error("put", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "err", err, "took", time.Since(now))
			resp.Err = err.Error()
		} else {
			slog.Debug("put", "action", hex.EncodeToString(req.ActionID), "output", resp.DiskPath, "took", time.Since(now))
		}
	}

	populateFileInfo(req, resp)
	return resp
}

// putTime returns when a get hit's entry was put. The go command expires test
// results put before the last "go clean -testcache" by it, so for an entry
// from the bucket it must be the original put, not the download.
func (c *Cacher) putTime(req *request) (*time.Time, error) {
	t, err := c.disk.PutTime(hex.EncodeToString(req.ActionID))
	if err != nil {
		return nil, fmt.Errorf("getting put time: %w", err)
	}
	return &t, nil
}

// populateFileInfo fills Size/OutputID from the on-disk artifact named by
// resp.DiskPath. Skipped for cmdClose (no disk path) and on cache miss
// (empty DiskPath would otherwise produce a spurious stat error).
func populateFileInfo(req *request, resp *response) {
	if req.Command == cmdClose || resp.DiskPath == "" {
		return
	}
	fi, err := os.Stat(resp.DiskPath)
	if err != nil {
		resp.Err = err.Error()
		return
	}
	resp.OutputID, err = hex.DecodeString(filepath.Base(resp.DiskPath))
	if err != nil {
		resp.Err = "invalid output id"
	}
	resp.Size = fi.Size()
}

var originalStdout = os.Stdout

func init() {
	// annoyingly, gocloud.dev prints to stdout messing with the expected JSON output
	os.Stdout = os.Stderr
}

func main() {
	var prefix string
	var verbose bool
	var opts options
	var envmap flagArray

	flag.StringVar(&prefix, "p", "", "prefix")
	flag.BoolVar(&verbose, "v", false, "verbose")
	flag.BoolVar(&opts.readonly, "readonly", false, "never write to the bucket, only to the local cache")
	flag.BoolVar(&opts.stats, "stats", false, "log hit/miss and transfer statistics on exit")
	flag.DurationVar(&opts.dedupeWait, "dedupe-wait", time.Minute, "how long to wait for another process sharing the local cache to put an action it's computing, rather than computing it too (0 disables)")
	flag.DurationVar(&opts.refreshAfter, "refresh-after", 24*time.Hour, "rewrite objects older than this when using them, to restart their expiry (0 disables)")
	flag.StringVar(&opts.cacheDir, "dir", "", "local cache directory (default: <user cache dir>/.gocachebucket)")
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

	if opts.cacheDir == "" {
		var err error
		opts.cacheDir, err = defaultCacheDir()
		if err != nil {
			slog.Error("run error", "err", err)
			os.Exit(1)
		}
	}

	if err := run(context.Background(), prefix, flag.Arg(0), opts); err != nil {
		slog.Error("run error", "err", err)
		os.Exit(1)
	}
}
