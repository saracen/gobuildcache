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
	"strconv"
	"strings"
	"sync"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
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

	// delta keeps this process's puts apart from -dir, and is looked in
	// first; nil to keep puts in -dir. See delta.go.
	delta *delta

	// claims coordinates misses with other processes sharing the local
	// cache directory; nil to not.
	claims *claims

	// expireOthers reports every entry this process didn't put as put at
	// unknownPutTime, so "go clean -testcache" expires every test result
	// this go command didn't produce, including ones put after it ran.
	expireOthers bool
	puts         sync.Map // actionID -> outputID this process put
}

// Get returns the path of the action's output, or "" on a miss, and when its
// entry was put. The go command expires test results put before the last "go
// clean -testcache" by that time, so for an entry from the bucket it must be
// the original put, not the download.
func (c *Cacher) Get(ctx context.Context, req *request) (string, time.Time, error) {
	actionID := hex.EncodeToString(req.ActionID)

	pathname, putTime, err := c.get(ctx, actionID)
	if err == nil && pathname == "" && c.claims != nil {
		pathname = c.claims.awaitOrClaim(ctx, actionID, func() string {
			pathname, putTime = c.localHit(actionID)
			return pathname
		})
	}

	// An entry is this process's if it's the output this process put for
	// the action. Another process's put, from the bucket or sharing the
	// local cache, links a different output unless it has the same bytes.
	if pathname != "" && c.expireOthers {
		if outputID, _ := c.puts.Load(actionID); outputID != filepath.Base(pathname) {
			putTime = unknownPutTime
		}
	}
	return pathname, putTime, err
}

// localHit returns the path of actionID's output and when it was put if it's
// in the delta or the local cache.
func (c *Cacher) localHit(actionID string) (string, time.Time) {
	if c.delta != nil {
		if pathname, putTime := c.delta.hit(actionID); pathname != "" {
			c.bucket.stats.DeltaHits.Add(1)
			return pathname, putTime
		}
	}

	outputID, putTime, err := c.disk.OutputIDFromAction(context.Background(), actionID)
	if err != nil || outputID == "" {
		return "", time.Time{}
	}

	pathname := filepath.Join(c.disk.cacheDir, outputDir, outputID)
	if _, err := os.Stat(pathname); err != nil {
		return "", time.Time{}
	}
	return pathname, putTime
}

func (c *Cacher) get(ctx context.Context, actionID string) (string, time.Time, error) {
	slog.Debug("get", "action", actionID)

	if c.delta != nil {
		if pathname, putTime := c.delta.hit(actionID); pathname != "" {
			c.bucket.stats.DeltaHits.Add(1)
			return pathname, putTime, nil
		}
	}

	outputID, putTime, err := c.bucket.OutputIDFromAction(ctx, actionID)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("getting output id from action (bucket): %w", err)
	}
	if outputID == "" {
		return "", time.Time{}, nil
	}

	slog.Debug("single flight get", "action", actionID, "output", outputID)
	pathname, err, shared := c.flight.Do("get"+outputID, func() (any, error) {
		return c.bucket.GetOutput(ctx, outputID)
	})
	slog.Debug("single flight get done", "action", actionID, "output", outputID)

	if shared {
		slog.Debug("get output shared", "output", outputID)
	}

	return pathname.(string), putTime, err
}

func (c *Cacher) Put(ctx context.Context, req *request) (string, error) {
	actionID := hex.EncodeToString(req.ActionID)
	outputID := hex.EncodeToString(req.OutputID)

	slog.Debug("put", "action", actionID, "output", outputID)

	if c.delta != nil {
		pathname, err := c.putDelta(ctx, actionID, outputID, req.Body)
		if err == nil {
			c.puts.Store(actionID, outputID)
		}
		if c.claims != nil {
			c.claims.release(actionID)
		}
		return pathname, err
	}

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
	if err == nil {
		c.puts.Store(actionID, outputID)
	}
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
	testExpire   time.Time
	expireOthers bool

	// deltaDir, if set, is where this process keeps its puts; see delta.go.
	deltaDir string

	// remote identifies the bucket, keying its remote-disabled marker; see
	// remoteIdentity.
	remote string
}

// validate reports options that can't be used together.
func (o options) validate() error {
	// A writer's puts belong in the bucket, where every later job finds
	// them. Kept in a delta instead, they'd never be uploaded.
	if o.deltaDir != "" && !o.readonly {
		return errors.New("-delta-dir requires -readonly")
	}
	if o.deltaDir != "" && filepath.Clean(o.deltaDir) == filepath.Clean(o.cacheDir) {
		return errors.New("-delta-dir must not be -dir")
	}
	return nil
}

func defaultCacheDir() (string, error) {
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("getting cache dir: %w", err)
	}

	return filepath.Join(cacheDir, ".gocachebucket"), nil
}

// readTestExpire returns when the go command last expired test results, or
// the zero time if it hasn't. "go clean -testcache" writes it to
// testexpire.txt in GOCACHE, which the go command always sets for
// GOCACHEPROG, to the default if it wasn't set already.
func readTestExpire() time.Time {
	dir := os.Getenv("GOCACHE")
	if !filepath.IsAbs(dir) {
		return time.Time{}
	}

	// parsed as the go command does, which ignores a malformed file
	data, err := os.ReadFile(filepath.Join(dir, "testexpire.txt"))
	if err != nil || len(data) == 0 || data[len(data)-1] != '\n' {
		return time.Time{}
	}
	ns, err := strconv.ParseInt(string(data[:len(data)-1]), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func run(ctx context.Context, prefix, bucketURL string, opts options) error {
	bucket, err := openBucket(ctx, bucketURL)
	if err != nil {
		return fmt.Errorf("opening bucket: %w", err)
	}
	// The bucket isn't closed: serve waits for every upload, the process
	// exits once it returns, and closing waits for any call still running,
	// such as a startup check it gave up on (see Bucket.probe), which the go
	// command would then wait for too.
	bucket = blob.PrefixedBucket(bucket, prefix)
	opts.remote = remoteIdentity(bucketURL, prefix)

	return serve(ctx, bucket, opts, os.Stdin, originalStdout)
}

func serve(ctx context.Context, bucket *blob.Bucket, opts options, in io.Reader, out io.Writer) error {
	if err := opts.validate(); err != nil {
		return err
	}

	cacher := &Cacher{
		disk:         &Disk{cacheDir: opts.cacheDir},
		expireOthers: opts.expireOthers,
	}
	cacher.bucket = &Bucket{disk: cacher.disk, bucket: bucket, readonly: opts.readonly, refreshAfter: opts.refreshAfter, testExpire: opts.testExpire}
	cacher.bucket.remote.marker = remoteDisabledMarker(cacher.disk.cacheDir, opts.remote)
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
			if !cacher.bucket.remote.allow() {
				cacher.bucket.stats.RemoteDisabled.Store(1)
			}
			cacher.bucket.stats.Log()
		}
	}()

	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, actionDir), 0o755); err != nil {
		return fmt.Errorf("creating cache action dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cacher.disk.cacheDir, outputDir), 0o755); err != nil {
		return fmt.Errorf("creating cache output dir: %w", err)
	}

	if opts.deltaDir != "" {
		delta, err := newDelta(opts.deltaDir)
		if err != nil {
			return err
		}
		delta.stats = &cacher.bucket.stats
		cacher.delta = delta
	}

	if opts.dedupeWait > 0 {
		claims, err := newClaims(cacher.disk.cacheDir, opts.dedupeWait, &cacher.bucket.stats)
		if err != nil {
			return err
		}
		cacher.claims = claims
	}

	// Puts are accepted even when readonly, and only kept locally, in -dir or
	// the delta: the go command reads back some of what it puts within the
	// same invocation, such as the generated test main of a test package,
	// and fails if the cache doesn't have it.
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
		var putTime time.Time
		resp.DiskPath, putTime, err = c.Get(ctx, req)
		if err == nil && resp.DiskPath != "" {
			resp.Time = &putTime
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
	if len(os.Args) > 1 && os.Args[1] == "prune" {
		os.Exit(pruneMain(os.Args[2:]))
	}

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
	flag.BoolVar(&opts.expireOthers, "expire-others", false, "report entries this process didn't put as put at the Unix epoch, so that after \"go clean -testcache\" the go command reruns every test result it didn't produce itself")
	flag.StringVar(&opts.cacheDir, "dir", "", "local cache directory (default: <user cache dir>/.gocachebucket)")
	flag.StringVar(&opts.deltaDir, "delta-dir", "", "keep this process's puts in this directory rather than -dir, and look there first; requires -readonly")
	flag.Var(&envmap, "env", "remap environment variable (example: GOOGLE_APPLICATION_CREDENTIALS=MY_ENV)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "%s [flags] <bucket url>\n%s prune [flags]\n", os.Args[0], os.Args[0])
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
	if err := opts.validate(); err != nil {
		slog.Error("run error", "err", err)
		os.Exit(2)
	}

	// The go command needs absolute paths to outputs, and a delta dir is
	// naturally given relative to a CI job's project directory, which is
	// where CI caches can save it from.
	if opts.deltaDir != "" {
		var err error
		opts.deltaDir, err = filepath.Abs(opts.deltaDir)
		if err != nil {
			slog.Error("run error", "err", err)
			os.Exit(1)
		}
	}

	opts.testExpire = readTestExpire()

	if err := run(context.Background(), prefix, flag.Arg(0), opts); err != nil {
		slog.Error("run error", "err", err)
		os.Exit(1)
	}
}

// pruneMain runs "gobuildcache prune", which prunes a delta dir; see
// pruneDelta.
func pruneMain(args []string) int {
	flags := flag.NewFlagSet("prune", flag.ContinueOnError)
	deltaDir := flags.String("delta-dir", "", "the delta directory to prune")
	usedSince := flags.String("used-since", "", "remove entries not used since this time, in Unix seconds or RFC 3339, such as when the job started, unless none was")
	maxSize := flags.String("max-size", "", "remove the least recently used entries, after -used-since, until their outputs take at most this size, in bytes or with a KiB, MiB or GiB suffix")
	verbose := flags.Bool("v", false, "verbose")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "%s prune -delta-dir <dir> [-used-since <time>] [-max-size <size>]\n", os.Args[0])
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	var opts pruneOptions
	var err error
	if *usedSince != "" {
		if opts.usedSince, err = parseTime(*usedSince); err != nil {
			slog.Error("prune", "err", err)
			return 2
		}
	}
	if *maxSize != "" {
		if opts.maxSize, err = parseSize(*maxSize); err != nil {
			slog.Error("prune", "err", err)
			return 2
		}
	}
	if *deltaDir == "" || flags.NArg() != 0 || (opts.usedSince.IsZero() && opts.maxSize == 0) {
		flags.Usage()
		return 2
	}

	result, err := pruneDelta(*deltaDir, opts)
	if err != nil {
		slog.Error("prune", "err", err)
		return 1
	}
	if result.Unused {
		slog.Warn("gobuildcache prune: no entry was used since -used-since, so the job's go commands didn't use the delta; only -max-size applied")
	}
	slog.Info("gobuildcache prune", "kept", result.Kept, "kept_bytes", result.KeptBytes, "removed", result.Removed, "removed_bytes", result.RemovedBytes)
	return 0
}
