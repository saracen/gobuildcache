package main

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
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

// TestEndToEnd_CleanTestcache checks that "go clean -testcache" expires test
// results from the bucket, which the go command does by the time they were
// put, and that the results of rerunning them are stored for later jobs.
func TestEndToEnd_CleanTestcache(t *testing.T) {
	tmp := t.TempDir()
	goBin, bin, bucketURL := setupEndToEnd(t, tmp)

	// The test's output is different every time it runs, but not what the go
	// command keys its result on, so the output shows which run a cached
	// result is from.
	mod := filepath.Join(tmp, "mod")
	writeModule(t, mod, map[string]string{
		"go.mod":        "module example.com/stamp\n\ngo 1.24\n",
		"stamp_test.go": "package stamp\n\nimport (\n\t\"fmt\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestStamp(t *testing.T) { fmt.Printf(\"stamp %d\\n\", time.Now().UnixNano()) }\n",
	})

	stampLine := regexp.MustCompile(`(?m)^stamp (\d+)$`)

	// job runs the test in a fresh job, with its own local cache and GOCACHE,
	// returning which run's result it got and whether it was cached.
	job := func(name string, clean bool) (string, bool, map[string]int64) {
		t.Helper()

		env := jobEnv(tmp, name, bin, bucketURL, "-stats")
		if clean {
			// go clean -testcache silently does nothing without GOCACHE
			if err := os.MkdirAll(filepath.Join(tmp, name+"-gocache"), 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(goBin, "clean", "-testcache")
			cmd.Dir = mod
			cmd.Env = env
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s: go clean -testcache: %v\n%s", name, err, out)
			}
		}

		cmd := exec.Command(goBin, "test", "-v", "./...")
		cmd.Dir = mod
		cmd.Env = env
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: go test: %v\n%s", name, err, out)
		}
		m := stampLine.FindStringSubmatch(string(out))
		if m == nil {
			t.Fatalf("%s: no stamp in output:\n%s", name, out)
		}
		return m[1], strings.Contains(string(out), "(cached)"), parseStats(t, string(out))
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
