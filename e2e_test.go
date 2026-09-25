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
	if testing.Short() {
		t.Skip("builds with the go toolchain")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go toolchain not found")
	}

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "gobuildcache")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command(goBin, "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("building gobuildcache: %v\n%s", err, out)
	}

	mod := filepath.Join(tmp, "mod")
	writeFiles(t, mod, map[string]string{
		"go.mod":               "module example.com/e2e\n\ngo 1.24\n",
		"lib/lib.go":           "package lib\n\nfunc Add(a, b int) int { return a + b }\n",
		"lib/lib_test.go":      "package lib\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n\tif Add(1, 2) != 3 {\n\t\tt.Fatal(\"bad\")\n\t}\n}\n",
		"cmd/app/main.go":      "package main\n\nimport (\n\t\"fmt\"\n\n\t\"example.com/e2e/lib\"\n)\n\nfunc main() { fmt.Println(lib.Add(1, 2)) }\n",
		"cmd/app/main_test.go": "package main\n\nimport \"testing\"\n\nfunc TestMain(t *testing.T) {}\n",
	})

	// The go command doesn't cache its index of a directory whose files were
	// modified moments ago, so without this every run misses on those. CI
	// systems wanting remote cache hits need stable, old mtimes for the same
	// reason (they also key test results that read files).
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	err = filepath.WalkDir(mod, func(p string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chtimes(p, old, old)
	})
	if err != nil {
		t.Fatal(err)
	}

	bucketURL := "file://" + filepath.ToSlash(filepath.Join(tmp, "bucket"))
	if runtime.GOOS == "windows" {
		bucketURL = "file:///" + filepath.ToSlash(filepath.Join(tmp, "bucket"))
	}
	if err := os.MkdirAll(filepath.Join(tmp, "bucket"), 0o755); err != nil {
		t.Fatal(err)
	}

	goTest := func(name string, flags ...string) (string, map[string]int64) {
		t.Helper()

		prog := append([]string{bin, "-stats", "-dir", filepath.Join(tmp, name)}, flags...)
		cmd := exec.Command(goBin, "test", "./...")
		cmd.Dir = mod
		cmd.Env = append(os.Environ(),
			"GOCACHEPROG="+strings.Join(append(prog, bucketURL), " "),
			"GOCACHE="+filepath.Join(tmp, name+"-gocache"),
			"GOFLAGS=",
			"GOWORK=off",
			"GOTOOLCHAIN=local",
		)
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
	cmd.Env = append(os.Environ(),
		"GOCACHEPROG="+strings.Join([]string{bin, "-readonly", "-dir", filepath.Join(tmp, "lister"), bucketURL}, " "),
		"GOCACHE="+filepath.Join(tmp, "lister-gocache"),
		"GOFLAGS=",
		"GOWORK=off",
		"GOTOOLCHAIN=local",
	)
	if out, err := cmd.CombinedOutput(); err != nil || strings.TrimSpace(string(out)) != "" {
		t.Errorf("go list -test -compiled with -readonly: %v\n%s", err, out)
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
