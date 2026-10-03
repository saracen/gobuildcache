package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"gocloud.dev/blob"
)

// TestEndToEnd drives the real go command through gobuildcache: a writer
// populates a bucket, then a second "machine" with an empty local cache must
// get every build and test result from the bucket.
func TestEndToEnd(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)

	mod := filepath.Join(tmp, "mod")
	writeModule(t, mod, map[string]string{
		"go.mod":               "module example.com/e2e\n\ngo 1.24\n",
		"lib/lib.go":           "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
		"lib/lib_test.go":      "package lib\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n\tif Add(1, 2) != 3 {\n\t\tt.Fatal(\"bad\")\n\t}\n}\n",
		"cmd/app/main.go":      "package main\n\nimport (\n\t\"fmt\"\n\n\t\"example.com/e2e/lib\"\n)\n\nfunc main() { fmt.Println(lib.Add(1, 2)) }\n",
		"cmd/app/main_test.go": "package main\n\nimport \"testing\"\n\nfunc TestMain(t *testing.T) {}\n",
	})

	goTest := func(name string, flags ...string) (string, map[string]int64) {
		t.Helper()

		cmd := exec.Command(goBin, "test", "./...")
		cmd.Dir = mod
		cmd.Env = jobEnv(tmp, name, bin, bucketURL, append([]string{"-stats"}, flags...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: go test: %v\n%s", name, err, out)
		}
		return string(out), parseStats(t, string(out))
	}

	_, writer := goTest("writer")
	if writer["puts"] == 0 || writer["uploads"] == 0 {
		t.Fatalf("writer stored nothing: %v", writer)
	}
	if writer["upload_errors"] != 0 || writer["put_errors"] != 0 {
		t.Fatalf("writer errors: %v", writer)
	}

	out, reader := goTest("reader", "-readonly")
	if reader["misses"] != 0 || reader["get_errors"] != 0 {
		t.Errorf("fresh reader missed: %v\n%s", reader, out)
	}
	if reader["downloads"] == 0 {
		t.Errorf("fresh reader downloaded nothing: %v", reader)
	}
	if got := strings.Count(out, "(cached)"); got != 2 {
		t.Errorf("want 2 cached test results, got %d:\n%s", got, out)
	}

	// Listing a test package's compiled files writes its generated test main
	// to the cache and reads it back, which needs puts even when readonly.
	cmd := exec.Command(goBin, "list", "-e", "-test", "-compiled", "-f", "{{if .Error}}{{.ImportPath}}: {{.Error}}{{end}}", "./...")
	cmd.Dir = mod
	cmd.Env = jobEnv(tmp, "lister", bin, bucketURL, "-readonly")
	if out, err := cmd.CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "" {
		t.Errorf("go list -test -compiled with -readonly: %v\n%s", err, out)
	}
}

// TestEndToEnd_UnreachableBucket checks that go commands don't wait long on a
// bucket nothing answers for, and still build and test with the local cache:
// the first finds out at startup, and the rest of the job's go commands
// skip the bucket.
func TestEndToEnd_UnreachableBucket(t *testing.T) {
	for _, mode := range []string{"read-write", "readonly"} {
		t.Run(mode, func(t *testing.T) {
			tmp := t.TempDir()
			goBin, bin, _ := setupEndToEnd(t, tmp)

			mod := filepath.Join(tmp, "mod")
			writeModule(t, mod, map[string]string{
				"go.mod":          "module example.com/down\n\ngo 1.24\n",
				"lib/lib.go":      "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
				"lib/lib_test.go": "package lib\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n\tif Add(1, 2) != 3 {\n\t\tt.Fatal(\"bad\")\n\t}\n}\n",
			})

			flags := []string{"-stats"}
			if mode == "readonly" {
				flags = append(flags, "-readonly")
			}
			// the GCS client retries a refused connection until its context
			// ends
			env := append(jobEnv(tmp, "job", bin, "gs://bucket", flags...), "STORAGE_EMULATOR_HOST=127.0.0.1:1")

			var out strings.Builder
			start := time.Now()
			for i := range 10 {
				args := [][]string{{"build", "./..."}, {"vet", "./..."}, {"test", "./..."}}[i%3]
				cmd := exec.Command(goBin, args...)
				cmd.Dir = mod
				cmd.Env = env
				cmdStart := time.Now()
				b, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, b)
				}
				t.Logf("go %s took %v", strings.Join(args, " "), time.Since(cmdStart))
				out.Write(b)
			}
			took := time.Since(start)

			// startupTimeout for the first go command, and compiling
			if took > startupTimeout+30*time.Second {
				t.Errorf("10 go commands took %v", took)
			}
			stats := parseStats(t, out.String())
			// go commands that find everything locally never check the bucket
			if stats["remote_disabled"] == 0 {
				t.Errorf("remote not disabled: %v", stats)
			}
			if stats["remote_calls"] != 1 {
				t.Errorf("remote calls = %d, want only the first go command's startup check", stats["remote_calls"])
			}
			if stats["hits"] == 0 {
				t.Errorf("no local cache hits: %v", stats)
			}
			if !strings.Contains(out.String(), "(cached)") {
				t.Errorf("no cached test result from the local cache:\n%s", out.String())
			}
		})
	}
}

// TestEndToEnd_UnreachableBucketKeepsOthers checks that a bucket found
// unreachable isn't turned off for go commands sharing the local cache that
// use another bucket, as on a developer machine or a shell executor where
// every project uses the default -dir.
func TestEndToEnd_UnreachableBucketKeepsOthers(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)

	mod := filepath.Join(tmp, "mod")
	writeModule(t, mod, map[string]string{
		"go.mod":          "module example.com/others\n\ngo 1.24\n",
		"lib/lib.go":      "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
		"lib/lib_test.go": "package lib\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n\tif Add(1, 2) != 3 {\n\t\tt.Fatal(\"bad\")\n\t}\n}\n",
	})
	goCmd := func(env []string, args ...string) map[string]int64 {
		t.Helper()
		cmd := exec.Command(goBin, args...)
		cmd.Dir = mod
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return parseStats(t, string(out))
	}

	goCmd(jobEnv(tmp, "writer", bin, bucketURL, "-stats"), "test", "./...")

	// the GCS client retries a refused connection until its context ends
	down := append(jobEnv(tmp, "shared", bin, "gs://bucket", "-stats"), "STORAGE_EMULATOR_HOST=127.0.0.1:1")
	if stats := goCmd(down, "build", "./..."); stats["remote_disabled"] == 0 {
		t.Fatalf("unreachable bucket not disabled: %v", stats)
	}

	stats := goCmd(jobEnv(tmp, "shared", bin, bucketURL, "-stats"), "test", "./...")
	if stats["remote_disabled"] != 0 || stats["downloads"] == 0 {
		t.Errorf("reachable bucket not used after another was found unreachable: %v", stats)
	}
}

// TestEndToEnd_CleanTestcache checks that "go clean -testcache" expires test
// results from the bucket, which the go command does by the time they were
// put, and that the results of rerunning them are stored for later jobs.
func TestEndToEnd_CleanTestcache(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)
	mod := writeStampModule(t, tmp)

	// job runs the test in a fresh job, with its own local cache and GOCACHE,
	// returning which run's result it got and whether it was cached.
	job := func(name string, clean bool) (string, bool, map[string]int64) {
		t.Helper()

		env := jobEnv(tmp, name, bin, bucketURL, "-stats")
		if clean {
			goCleanTestcache(t, goBin, mod, env)
		}
		return goTestStamp(t, goBin, mod, env)
	}

	first, cached, _ := job("first", false)
	if cached {
		t.Fatal("first job's test result was cached")
	}

	if got, cached, _ := job("before-rerun", false); !cached || got != first {
		t.Errorf("job without go clean: cached=%v from run %s, want cached from the first run %s", cached, got, first)
	}

	rerun, cached, stats := job("clean", true)
	if cached || rerun == first {
		t.Fatalf("job after go clean -testcache: cached=%v from run %s, want a rerun", cached, rerun)
	}
	if stats["uploads"] == 0 || stats["upload_errors"] != 0 {
		t.Errorf("rerun wasn't stored: %v", stats)
	}

	if got, cached, _ := job("later", false); !cached || got != rerun {
		t.Errorf("later job: cached=%v from run %s, want cached from the rerun %s", cached, got, rerun)
	}
}

// TestEndToEnd_ExpireTestResultsWithConcurrentWriters checks what the README
// says about jobs that must rerun every test while other jobs write to the
// bucket. "go clean -testcache" only expires results put before it ran, so
// such a job replays a result another job puts after that; with
// -expire-others as well, it reruns everything, and still stores the results
// with when it put them.
func TestEndToEnd_ExpireTestResultsWithConcurrentWriters(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)
	mod := writeStampModule(t, tmp)
	env := func(name string, flags ...string) []string {
		return jobEnv(tmp, name, bin, bucketURL, append([]string{"-stats"}, flags...)...)
	}

	if _, cached, _ := goTestStamp(t, goBin, mod, env("first")); cached {
		t.Fatal("first job's test result was cached")
	}

	// A job cleans, then another cleans and tests, before the first tests.
	goCleanTestcache(t, goBin, mod, env("cleaned"))
	goCleanTestcache(t, goBin, mod, env("other"))
	other, cached, _ := goTestStamp(t, goBin, mod, env("other"))
	if cached {
		t.Fatal("other job's test result was cached after go clean -testcache")
	}
	if got, cached, _ := goTestStamp(t, goBin, mod, env("cleaned")); !cached || got != other {
		t.Errorf("cleaned job: cached=%v from run %s, want cached from the other job's run %s; if the go command no longer replays it, update the README", cached, got, other)
	}

	// The same, with -expire-others. Another job cleans before it too, and
	// tests after it.
	goCleanTestcache(t, goBin, mod, env("expiring", "-expire-others"))
	goCleanTestcache(t, goBin, mod, env("cleaned2"))
	goCleanTestcache(t, goBin, mod, env("other2"))
	other, cached, _ = goTestStamp(t, goBin, mod, env("other2"))
	if cached {
		t.Fatal("other job's test result was cached after go clean -testcache")
	}
	expiring, cached, stats := goTestStamp(t, goBin, mod, env("expiring", "-expire-others"))
	if cached || expiring == other {
		t.Fatalf("job with -expire-others: cached=%v from run %s, want a rerun, not the other job's run %s", cached, expiring, other)
	}
	if stats["uploads"] == 0 || stats["upload_errors"] != 0 {
		t.Errorf("rerun wasn't stored: %v", stats)
	}

	// Its result was stored with when it was put, after the job that cleaned
	// before it, rather than when -expire-others reports it for other jobs.
	if got, cached, _ := goTestStamp(t, goBin, mod, env("cleaned2")); !cached || got != expiring {
		t.Errorf("job that cleaned before the job with -expire-others tested: cached=%v from run %s, want cached from its rerun %s", cached, got, expiring)
	}
	if got, cached, _ := goTestStamp(t, goBin, mod, env("later")); !cached || got != expiring {
		t.Errorf("later job: cached=%v from run %s, want cached from the rerun %s", cached, got, expiring)
	}
}

// TestEndToEnd_ListTestUploadsOnlyExpiredRePuts checks that the entries the
// go command puts again unchanged every time it uses them, such as a test
// package's generated test main on every "go list -test", aren't uploaded
// again each time, only once "go clean -testcache" has expired them.
func TestEndToEnd_ListTestUploadsOnlyExpiredRePuts(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)
	mod := writeStampModule(t, tmp)
	env := jobEnv(tmp, "lister", bin, bucketURL)

	goList := func() map[string]string {
		t.Helper()

		cmd := exec.Command(goBin, "list", "-e", "-test", "./...")
		cmd.Dir = mod
		cmd.Env = env
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go list -test: %v\n%s", err, out)
		}
		return linkPutTimes(t, bucketURL)
	}

	first := goList()
	if len(first) == 0 {
		t.Fatal("go list -test uploaded nothing")
	}
	if n := reuploaded(first, goList()); n != 0 {
		t.Errorf("go list -test on a warm local cache uploaded %d of %d action links again", n, len(first))
	}

	goCleanTestcache(t, goBin, mod, env)
	if n := reuploaded(first, goList()); n == 0 {
		t.Errorf("go list -test after go clean -testcache uploaded none of its %d expired action links again", len(first))
	}
}

// reuploaded counts the action links in before with a different put time in
// after.
func reuploaded(before, after map[string]string) int {
	n := 0
	for key, putTime := range before {
		if after[key] != putTime {
			n++
		}
	}
	return n
}

// linkPutTimes returns the put time of every action link in the bucket.
func linkPutTimes(t *testing.T, bucketURL string) map[string]string {
	t.Helper()
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	putTimes := map[string]string{}
	iter := bucket.List(&blob.ListOptions{Prefix: actionDir + "/"})
	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		attrs, err := bucket.Attributes(ctx, obj.Key)
		if err != nil {
			t.Fatal(err)
		}
		putTimes[obj.Key] = attrs.Metadata[putTimeKey]
	}
	return putTimes
}

// writeStampModule writes a module whose test's output is different every
// time it runs, but not what the go command keys its result on, so the output
// shows which run a cached result is from.
func writeStampModule(t *testing.T, tmp string) string {
	t.Helper()

	mod := filepath.Join(tmp, "mod")
	writeModule(t, mod, map[string]string{
		"go.mod":        "module example.com/stamp\n\ngo 1.24\n",
		"stamp_test.go": "package stamp\n\nimport (\n\t\"fmt\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestStamp(t *testing.T) { fmt.Printf(\"stamp %d\\n\", time.Now().UnixNano()) }\n",
	})
	return mod
}

var stampLine = regexp.MustCompile(`(?m)^stamp (\d+)$`)

// goTestStamp tests the stamp module in mod, returning which run's result it
// got, whether it was cached, and gobuildcache's stats.
func goTestStamp(t *testing.T, goBin, mod string, env []string) (string, bool, map[string]int64) {
	t.Helper()

	cmd := exec.Command(goBin, "test", "-v", "./...")
	cmd.Dir = mod
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go test: %v\n%s", err, out)
	}
	m := stampLine.FindStringSubmatch(string(out))
	if m == nil {
		t.Fatalf("no stamp in output:\n%s", out)
	}
	return m[1], strings.Contains(string(out), "(cached)"), parseStats(t, string(out))
}

// goCleanTestcache runs "go clean -testcache", creating GOCACHE first: without
// it, the go command silently does nothing.
func goCleanTestcache(t *testing.T, goBin, mod string, env []string) {
	t.Helper()

	cmd := exec.Command(goBin, "env", "GOCACHE")
	cmd.Dir = mod
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go env GOCACHE: %v", err)
	}
	if err := os.MkdirAll(strings.TrimSpace(string(out)), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd = exec.Command(goBin, "clean", "-testcache")
	cmd.Dir = mod
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go clean -testcache: %v\n%s", err, out)
	}
}

// setupEndToEnd builds gobuildcache into tmp and creates a file:// bucket
// there, returning the go command, gobuildcache and the bucket's URL.
func setupEndToEnd(t *testing.T, tmp string) (string, string, string) {
	t.Helper()

	if testing.Short() {
		t.Skip("builds with the go toolchain")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not found")
	}

	bin := filepath.Join(tmp, "gobuildcache")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command(goBin, "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("building gobuildcache: %v\n%s", err, out)
	}

	bucketURL := "file://" + filepath.ToSlash(filepath.Join(tmp, "bucket"))
	if runtime.GOOS == "windows" {
		bucketURL = "file:///" + filepath.ToSlash(filepath.Join(tmp, "bucket"))
	}
	if err := os.MkdirAll(filepath.Join(tmp, "bucket"), 0o755); err != nil {
		t.Fatal(err)
	}

	return goBin, bin, bucketURL
}

// jobEnv is the environment of go commands in a job named name, which has its
// own local cache and GOCACHE, sharing the bucket with other jobs.
func jobEnv(tmp, name, bin, bucketURL string, flags ...string) []string {
	prog := append([]string{bin, "-dir", filepath.Join(tmp, name)}, flags...)
	return append(os.Environ(),
		"GOCACHEPROG="+strings.Join(append(prog, bucketURL), " "),
		"GOCACHE="+filepath.Join(tmp, name+"-gocache"),
		"GOFLAGS=",
		"GOWORK=off",
		"GOTOOLCHAIN=local",
	)
}

// writeModule writes a module's files with old modification times.
func writeModule(t *testing.T, root string, files map[string]string) {
	t.Helper()

	writeFiles(t, root, files)

	// The go command doesn't cache its index of a directory whose files were
	// modified moments ago, so without this every run misses on those. CI
	// systems wanting remote cache hits need stable, old mtimes for the same
	// reason (they also key test results that read files).
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	err := filepath.WalkDir(root, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(p, old, old)
	})
	if err != nil {
		t.Fatal(err)
	}
}

var statsLine = regexp.MustCompile(`msg="gobuildcache stats"(.*)`)
var statsField = regexp.MustCompile(`(\w+)=(\d+)`)

func parseStats(t *testing.T, out string) map[string]int64 {
	t.Helper()

	// Each go command starts its own GOCACHEPROG; sum them.
	total := map[string]int64{}
	for _, m := range statsLine.FindAllStringSubmatch(out, -1) {
		for _, f := range statsField.FindAllStringSubmatch(m[1], -1) {
			n, _ := strconv.ParseInt(f[2], 10, 64)
			total[f[1]] += n
		}
	}
	if len(total) == 0 {
		t.Fatalf("no stats in output:\n%s", out)
	}
	return total
}

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestEndToEnd_Delta follows a merge request's pipelines, which read the
// bucket that trusted writers fill and keep what they compute in a delta.
// The first job puts only what its change needs, the next job gets those
// from the delta and everything else from the bucket, and pruning keeps only
// what that job used. Nothing from a delta reaches the bucket.
func TestEndToEnd_Delta(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)
	bucketDir := filepath.Join(tmp, "bucket")

	test := func(pkg, call string) string {
		return "package " + pkg + "\n\nimport \"testing\"\n\nfunc TestIt(t *testing.T) { t.Log(" + call + ") }\n"
	}
	mod := filepath.Join(tmp, "mod")
	base := map[string]string{
		"go.mod":          "module example.com/delta\n\ngo 1.24\n",
		"lib/lib.go":      "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
		"lib/lib_test.go": test("lib", "Add(1, 2)"),
		"app/app.go":      "package app\n\nimport \"example.com/delta/lib\"\n\nfunc Three() int { return lib.Add(1, 2) }\n",
		"app/app_test.go": test("app", "Three()"),
		"other/other.go":  "package other\n\nfunc One() int { return 1 }\n",
		"other/o_test.go": test("other", "One()"),
		"extra/extra.go":  "package extra\n\nfunc Two() int { return 2 }\n",
		"extra/e_test.go": test("extra", "Two()"),
	}
	// The merge request's first commit changes lib, which app imports, and
	// extra. Its second reverts extra.
	lib := map[string]string{
		"lib/lib.go":      "package lib\n\nfunc Add(a, b int) int { return a + b }\n\nfunc Sub(a, b int) int { return a - b }\n",
		"lib/lib_test.go": test("lib", "Sub(Add(1, 2), 3)"),
	}
	extra := map[string]string{
		"extra/extra.go":  "package extra\n\nfunc Two() int { return 2 }\n\nfunc Four() int { return 4 }\n",
		"extra/e_test.go": test("extra", "Four()"),
	}
	commit := func(changes ...map[string]string) {
		t.Helper()
		writeModule(t, mod, base)
		for _, c := range changes {
			writeModule(t, mod, c)
		}
	}

	goTest := func(name string, flags ...string) (string, map[string]int64) {
		t.Helper()

		cmd := exec.Command(goBin, "test", "./...")
		cmd.Dir = mod
		cmd.Env = jobEnv(tmp, name, bin, bucketURL, append([]string{"-stats"}, flags...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: go test: %v\n%s", name, err, out)
		}
		return string(out), parseStats(t, string(out))
	}
	mrJob := func(name, delta string) (string, map[string]int64) {
		t.Helper()
		return goTest(name, "-readonly", "-delta-dir", delta)
	}
	cached := func(out, pkg string) bool {
		return regexp.MustCompile(`(?m)^ok\s+example\.com/delta/` + pkg + `\s.*\(cached\)`).MatchString(out)
	}

	commit()
	if _, writer := goTest("writer"); writer["uploads"] == 0 || writer["upload_errors"] != 0 {
		t.Fatalf("writer didn't fill the bucket: %v", writer)
	}
	bucket := snapshotTree(t, bucketDir)
	_, bucketBytes := deltaEntries(t, bucketDir)

	// The first job computes lib, app and extra, and puts only those in its
	// delta: exactly what a readonly job without one keeps in -dir that the
	// bucket doesn't have.
	commit(lib, extra)
	delta1 := filepath.Join(tmp, "delta1")
	out, job1 := mrJob("mr1", delta1)
	if !cached(out, "other") || !cached(out, "app") || cached(out, "lib") || cached(out, "extra") {
		t.Errorf("first job: want other's and app's test results cached, app's binary being the same:\n%s", out)
	}
	entries1, bytes1 := deltaEntries(t, delta1)
	t.Logf("first job's delta: %d entries, %d bytes; bucket: %d bytes", len(entries1), bytes1, bucketBytes)
	if len(entries1) == 0 || int64(len(entries1)) != job1["delta_puts"] || bytes1 != job1["delta_put_bytes"] {
		t.Errorf("first job's delta has %d entries, %d bytes, want its %d puts, %d bytes: %v", len(entries1), bytes1, job1["delta_puts"], job1["delta_put_bytes"], job1)
	}
	if bytes1 == 0 || bytes1*20 > bucketBytes {
		t.Errorf("first job's delta takes %d bytes, want a little of the bucket's %d", bytes1, bucketBytes)
	}
	if job1["downloads"] == 0 || job1["uploads"] != 0 {
		t.Errorf("first job: %v", job1)
	}
	for actionID := range entries1 {
		if _, err := os.Stat(filepath.Join(bucketDir, actionDir, actionID)); err == nil {
			t.Errorf("first job put %s in its delta, which the bucket has", actionID)
		}
	}
	if local := localPuts(t, filepath.Join(tmp, "mr1"), bucketDir); len(local) != 0 {
		t.Errorf("first job put %d entries in -dir, not the delta", len(local))
	}
	_, control := goTest("mr1-control", "-readonly")
	if want := localPuts(t, filepath.Join(tmp, "mr1-control"), bucketDir); !sameKeys(entries1, want) {
		t.Errorf("first job's delta has %d entries, want the %d that a readonly job without one puts in -dir (%v)", len(entries1), len(want), control)
	}

	// A retry that fails before its go commands run, such as while setting
	// up, uses nothing, and pruning keeps the delta for the next retry.
	failed := filepath.Join(tmp, "failed")
	copyTree(t, delta1, failed)
	failedStart := time.Now()
	prune := exec.Command(bin, "prune", "-delta-dir", failed, "-used-since", failedStart.Format(time.RFC3339Nano))
	if out, err := prune.CombinedOutput(); err != nil || !strings.Contains(string(out), "didn't use the delta") {
		t.Errorf("prune after a job that ran no go commands: %v\n%s", err, out)
	}
	if entries, size := deltaEntries(t, failed); !sameKeys(entries, entries1) || size != bytes1 {
		t.Errorf("prune after a job that ran no go commands kept %d of %d entries, %d of %d bytes", len(entries), len(entries1), size, bytes1)
	}

	// The next pipeline restores the delta. Its lib and app entries hit the
	// delta, and the reverted extra and everything else hit the bucket, so
	// it puts nothing.
	commit(lib)
	delta2 := filepath.Join(tmp, "delta2")
	copyTree(t, delta1, delta2)
	started := time.Now()
	out, job2 := mrJob("mr2", delta2)
	for _, pkg := range []string{"lib", "app", "other", "extra"} {
		if !cached(out, pkg) {
			t.Errorf("second job: %s's test result wasn't cached:\n%s", pkg, out)
		}
	}
	if job2["delta_hits"] == 0 || job2["downloads"] == 0 || job2["delta_puts"] != 0 || job2["uploads"] != 0 {
		t.Errorf("second job: want hits from both the delta and the bucket, and no puts: %v", job2)
	}
	if entries, size := deltaEntries(t, delta2); !sameKeys(entries, entries1) || size != bytes1 {
		t.Errorf("second job changed the delta: %d entries, %d bytes, from %d, %d", len(entries), size, len(entries1), bytes1)
	}

	// Pruning removes the entries the second job didn't use: extra's, and
	// some of lib's, since the go command stores a test result it ran under
	// two keys, and later jobs only look up the first.
	prune = exec.Command(bin, "prune", "-delta-dir", delta2, "-used-since", started.Format(time.RFC3339Nano))
	if out, err := prune.CombinedOutput(); err != nil {
		t.Fatalf("prune: %v\n%s", err, out)
	}
	entries2, bytes2 := deltaEntries(t, delta2)
	pruned := map[string]bool{}
	for actionID := range entries1 {
		if _, ok := entries2[actionID]; !ok {
			pruned[actionID] = true
		}
	}
	t.Logf("pruned delta: %d entries, %d bytes", len(entries2), bytes2)
	if len(entries2) == 0 || len(pruned) == 0 || len(entries2)+len(pruned) != len(entries1) || bytes2 >= bytes1 {
		t.Errorf("pruning kept %d of %d entries, %d of %d bytes", len(entries2), len(entries1), bytes2, bytes1)
	}
	for actionID := range entries2 {
		if used := readUsed(delta2, actionID); used.Before(started) {
			t.Errorf("pruning kept %s, last used at %v, before the second job started at %v", actionID, used, started)
		}
	}

	// It kept everything the second job used...
	delta3 := filepath.Join(tmp, "delta3")
	copyTree(t, delta2, delta3)
	if _, job3 := mrJob("mr3", delta3); job3["delta_puts"] != 0 || job3["delta_hits"] != job2["delta_hits"] {
		t.Errorf("job after pruning: want the second job's %d delta hits, and no puts: %v", job2["delta_hits"], job3)
	}

	// ...and extra's entries are among those it removed: changing extra
	// again only puts entries it removed.
	commit(lib, extra)
	delta4 := filepath.Join(tmp, "delta4")
	copyTree(t, delta2, delta4)
	_, job4 := mrJob("mr4", delta4)
	entries4, _ := deltaEntries(t, delta4)
	if job4["delta_puts"] == 0 || int64(len(entries4)) != int64(len(entries2))+job4["delta_puts"] {
		t.Errorf("changing extra again put %d entries in a delta of %d, now %d: %v", job4["delta_puts"], len(entries2), len(entries4), job4)
	}
	for actionID := range entries4 {
		if _, kept := entries2[actionID]; !kept && !pruned[actionID] {
			t.Errorf("changing extra again put %s, which pruning didn't remove", actionID)
		}
	}

	if got := snapshotTree(t, bucketDir); !reflect.DeepEqual(got, bucket) {
		t.Errorf("merge request jobs changed the bucket")
	}
}

// deltaEntries returns the action links in dir, a delta, local cache or
// file:// bucket, and the size of its outputs.
func deltaEntries(t *testing.T, dir string) (map[string]struct{}, int64) {
	t.Helper()

	entries := map[string]struct{}{}
	names, err := os.ReadDir(filepath.Join(dir, actionDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range names {
		if isValidID(e.Name()) {
			entries[e.Name()] = struct{}{}
		}
	}

	var size int64
	outputs, err := os.ReadDir(filepath.Join(dir, outputDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range outputs {
		if !isValidID(e.Name()) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		size += fi.Size()
	}
	return entries, size
}

// localPuts returns the action links in the local cache dir that aren't in
// the bucket, which the job must have put.
func localPuts(t *testing.T, dir, bucketDir string) map[string]struct{} {
	t.Helper()

	entries, _ := deltaEntries(t, dir)
	for actionID := range entries {
		if _, err := os.Stat(filepath.Join(bucketDir, actionDir, actionID)); err == nil {
			delete(entries, actionID)
		}
	}
	return entries
}

func sameKeys(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// snapshotTree returns every file under root with its size, modification
// time and contents' hash.
func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()

	files := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files[p] = fmt.Sprintf("%d %d %s", fi.Size(), fi.ModTime().UnixNano(), hashID(data))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// copyTree copies src to dst keeping modification times, as restoring a CI
// cache does.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()

	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		fi, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if err := os.WriteFile(target, data, 0o644); err != nil {
			return err
		}
		return os.Chtimes(target, fi.ModTime(), fi.ModTime())
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestEndToEnd_DeltaDamaged checks that a job whose restored delta has
// damaged outputs, as a failed CI cache extraction leaves them, still builds
// and tests, and puts correct outputs back in their place, so the next job
// gets them from the delta. A truncated compiled archive otherwise crashes the
// linker in every later pipeline of the merge request.
func TestEndToEnd_DeltaDamaged(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)

	mod := filepath.Join(tmp, "mod")
	base := map[string]string{
		"go.mod":          "module example.com/damaged\n\ngo 1.24\n",
		"lib/lib.go":      "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
		"lib/lib_test.go": "package lib\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) { t.Log(Add(1, 2)) }\n",
		"app/app.go":      "package app\n\nimport \"example.com/damaged/lib\"\n\nfunc Three() int { return lib.Add(1, 2) }\n",
		"app/app_test.go": "package app\n\nimport \"testing\"\n\nfunc TestThree(t *testing.T) { t.Log(Three()) }\n",
	}
	writeModule(t, mod, base)

	goTest := func(name string, flags ...string) (string, map[string]int64) {
		t.Helper()

		cmd := exec.Command(goBin, "test", "./...")
		cmd.Dir = mod
		cmd.Env = jobEnv(tmp, name, bin, bucketURL, append([]string{"-stats"}, flags...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: go test: %v\n%s", name, err, out)
		}
		return string(out), parseStats(t, string(out))
	}
	goTest("writer")

	writeModule(t, mod, map[string]string{
		"lib/lib.go": "package lib\n\nfunc Add(a, b int) int { return a + b }\n\nfunc Sub(a, b int) int { return a - b }\n",
	})
	delta1 := filepath.Join(tmp, "delta1")
	goTest("mr1", "-readonly", "-delta-dir", delta1)
	entries1, _ := deltaEntries(t, delta1)

	// every output truncated or with a byte changed
	delta2 := filepath.Join(tmp, "delta2")
	copyTree(t, delta1, delta2)
	outputs, err := os.ReadDir(filepath.Join(delta2, outputDir))
	if err != nil {
		t.Fatal(err)
	}
	damaged := 0
	for i, e := range outputs {
		p := filepath.Join(delta2, outputDir, e.Name())
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 {
			continue
		}
		if i%2 == 0 {
			data = data[:len(data)/2]
		} else {
			data[len(data)/2] ^= 0xff
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		damaged++
	}
	if damaged == 0 {
		t.Fatal("no outputs to damage")
	}

	_, job2 := goTest("mr2", "-readonly", "-delta-dir", delta2)
	if job2["delta_damaged"] == 0 || job2["delta_puts"] == 0 {
		t.Errorf("job with a damaged delta: want damaged outputs found and put again: %v", job2)
	}
	outputs, err = os.ReadDir(filepath.Join(delta2, outputDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range outputs {
		data, err := os.ReadFile(filepath.Join(delta2, outputDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if got := hashID(data); got != e.Name() {
			t.Errorf("output %s is still damaged after the job", e.Name())
		}
	}

	// the next job gets everything it got from the first job's delta again
	delta3 := filepath.Join(tmp, "delta3")
	copyTree(t, delta2, delta3)
	out, job3 := goTest("mr3", "-readonly", "-delta-dir", delta3)
	if job3["delta_damaged"] != 0 || job3["delta_puts"] != 0 || job3["delta_hits"] == 0 || strings.Count(out, "(cached)") != 2 {
		t.Errorf("job after the damaged one: want every test cached, from the delta and the bucket, and no puts: %v\n%s", job3, out)
	}
	if entries, _ := deltaEntries(t, delta3); !sameKeys(entries, entries1) {
		t.Errorf("delta has %d entries after putting the damaged ones again, want the first job's %d", len(entries), len(entries1))
	}
}

// TestEndToEnd_DeltaUnusable checks that a job whose restored delta its user
// can't read or write, as when a CI cache restores another user's files,
// still builds and tests, treating what it can't use as missing, and that
// pruning says what it couldn't remove.
func TestEndToEnd_DeltaUnusable(t *testing.T) {
	skipUnlessPermissionsApply(t)

	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)

	mod := filepath.Join(tmp, "mod")
	writeModule(t, mod, map[string]string{
		"go.mod":          "module example.com/unusable\n\ngo 1.24\n",
		"lib/lib.go":      "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
		"lib/lib_test.go": "package lib\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) { t.Log(Add(1, 2)) }\n",
		"app/app.go":      "package app\n\nimport \"example.com/unusable/lib\"\n\nfunc Three() int { return lib.Add(1, 2) }\n",
		"app/app_test.go": "package app\n\nimport \"testing\"\n\nfunc TestThree(t *testing.T) { t.Log(Three()) }\n",
	})

	goTest := func(t *testing.T, name string, flags ...string) (string, map[string]int64) {
		t.Helper()

		cmd := exec.Command(goBin, "test", "./...")
		cmd.Dir = mod
		cmd.Env = jobEnv(tmp, name, bin, bucketURL, append([]string{"-stats"}, flags...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: go test: %v\n%s", name, err, out)
		}
		return string(out), parseStats(t, string(out))
	}
	goTest(t, "writer")

	writeModule(t, mod, map[string]string{
		"lib/lib.go": "package lib\n\nfunc Add(a, b int) int { return a + b }\n\nfunc Sub(a, b int) int { return a - b }\n",
	})
	delta1 := filepath.Join(tmp, "delta1")
	goTest(t, "mr1", "-readonly", "-delta-dir", delta1)
	entries1, bytes1 := deltaEntries(t, delta1)

	t.Run("unreadable and unwritable", func(t *testing.T) {
		delta := filepath.Join(tmp, "unwritable-delta")
		copyTree(t, delta1, delta)
		chmodTree(t, delta, 0, 0o555)

		out, job := goTest(t, "unwritable", "-readonly", "-delta-dir", delta)
		if job["put_errors"] != 0 || job["get_errors"] != 0 || job["delta_errors"] == 0 || job["delta_hits"] != 0 {
			t.Errorf("want the delta treated as missing, with no errors from the go command's requests: %v\n%s", job, out)
		}
		if !strings.Contains(out, "delta dir isn't writable") {
			t.Errorf("no warning that the delta dir isn't used:\n%s", out)
		}

		prune := exec.Command(bin, "prune", "-delta-dir", delta, "-used-since", time.Now().Format(time.RFC3339Nano))
		out2, err := prune.CombinedOutput()
		if err != nil || !strings.Contains(string(out2), "couldn't remove") {
			t.Errorf("prune of a delta it can't change: want a warning: %v\n%s", err, out2)
		}
	})

	// as a job running as root with umask 0022 saves it for one that isn't
	t.Run("unwritable", func(t *testing.T) {
		delta := filepath.Join(tmp, "readable-delta")
		copyTree(t, delta1, delta)
		before := snapshotTree(t, delta)
		chmodTree(t, delta, 0o444, 0o555)

		// changing lib again puts what the delta doesn't have
		writeModule(t, mod, map[string]string{
			"lib/lib.go": "package lib\n\nfunc Add(a, b int) int { return a + b }\n\nfunc Sub(a, b int) int { return a - b }\n\nfunc Mul(a, b int) int { return a * b }\n",
		})
		defer writeModule(t, mod, map[string]string{
			"lib/lib.go": "package lib\n\nfunc Add(a, b int) int { return a + b }\n\nfunc Sub(a, b int) int { return a - b }\n",
		})
		out, job := goTest(t, "readable", "-readonly", "-delta-dir", delta)
		if job["put_errors"] != 0 || job["get_errors"] != 0 || job["delta_errors"] != 1 || job["delta_puts"] != 0 || job["puts"] == 0 {
			t.Errorf("want puts kept in -dir, and nothing else failing: %v\n%s", job, out)
		}
		if got := snapshotTree(t, delta); !reflect.DeepEqual(got, before) {
			t.Errorf("job changed a delta it can't write to")
		}

		// what it has is still used
		writeModule(t, mod, map[string]string{
			"lib/lib.go": "package lib\n\nfunc Add(a, b int) int { return a + b }\n\nfunc Sub(a, b int) int { return a - b }\n",
		})
		out, job = goTest(t, "readable-same", "-readonly", "-delta-dir", delta)
		if job["put_errors"] != 0 || job["delta_hits"] == 0 || job["delta_puts"] != 0 || strings.Count(out, "(cached)") != 2 {
			t.Errorf("want hits from a delta it can't write to: %v\n%s", job, out)
		}
	})

	t.Run("unreadable files", func(t *testing.T) {
		delta := filepath.Join(tmp, "unreadable-delta")
		copyTree(t, delta1, delta)
		chmodTree(t, delta, 0, 0o755)

		out, job := goTest(t, "unreadable", "-readonly", "-delta-dir", delta)
		if job["put_errors"] != 0 || job["get_errors"] != 0 || job["delta_puts"] == 0 || job["delta_damaged"] == 0 {
			t.Errorf("want what couldn't be read computed and put again: %v\n%s", job, out)
		}
		if entries, size := deltaEntries(t, delta); !sameKeys(entries, entries1) || size != bytes1 {
			t.Errorf("delta has %d entries, %d bytes after putting them again, want the first job's %d, %d", len(entries), size, len(entries1), bytes1)
		}

		// the next job gets them from the delta
		next := filepath.Join(tmp, "unreadable-next-delta")
		copyTree(t, delta, next)
		if out, job := goTest(t, "unreadable-next", "-readonly", "-delta-dir", next); job["delta_hits"] == 0 || job["delta_puts"] != 0 || strings.Count(out, "(cached)") != 2 {
			t.Errorf("job after putting them again: want every test cached and no puts: %v\n%s", job, out)
		}
	})
}

// skipUnlessPermissionsApply skips tests that make files unreadable or
// directories unwritable, which doesn't stop root, and which Windows doesn't
// do with file modes.
func skipUnlessPermissionsApply(t *testing.T) {
	t.Helper()

	if runtime.GOOS == "windows" {
		t.Skip("file modes don't restrict access on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("file modes don't restrict root")
	}
}

// chmodTree sets the mode of every file under root to file, and of every
// directory, root included, to dir, restoring them when the test ends so
// that its temporary directory can be removed.
func chmodTree(t *testing.T, root string, file, dir os.FileMode) {
	t.Helper()

	var files, dirs []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			dirs = append(dirs, p)
		} else {
			files = append(files, p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, p := range dirs {
			os.Chmod(p, 0o755)
		}
		for _, p := range files {
			os.Chmod(p, 0o644)
		}
	})
	for _, p := range files {
		if err := os.Chmod(p, file); err != nil {
			t.Fatal(err)
		}
	}
	// deepest first, so each is still writable when its files are changed
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Chmod(dirs[i], dir); err != nil {
			t.Fatal(err)
		}
	}
}
