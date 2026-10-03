package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const commitTime = 1700000000

func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-c", "user.name=test", "-c", "user.email=test@example.com", "-c", "commit.gpgsign=false", "-c", "init.defaultBranch=main"}, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_COMMITTER_DATE=@1700000000 +0000", "GIT_AUTHOR_DATE=@1700000000 +0000")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mtime(t *testing.T, path string) time.Time {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.ModTime()
}

func needGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("needs git")
	}
}

// newRepo commits a few files, some with names git quotes, and a nested
// repository it doesn't track.
func newRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	gitIn(t, root, "init", "-q")
	writeFile(t, filepath.Join(root, "top.txt"), "top")
	writeFile(t, filepath.Join(root, "a", "b", "deep.txt"), "deep")
	writeFile(t, filepath.Join(root, "with space.txt"), "space")
	writeFile(t, filepath.Join(root, "a", "naïve.txt"), "quoted")
	writeFile(t, filepath.Join(root, ".gitignore"), "/testrepo/\n")
	gitIn(t, root, "add", "-A")
	gitIn(t, root, "commit", "-q", "-m", "files")

	nested := filepath.Join(root, "testrepo")
	writeFile(t, filepath.Join(nested, "sub", "file"), "x")
	gitIn(t, nested, "init", "-q")
	gitIn(t, nested, "add", "-A")
	gitIn(t, nested, "commit", "-q", "-m", "nested")
	return root
}

func TestTouch(t *testing.T) {
	needGit(t)
	root := newRepo(t)

	symlinks := runtime.GOOS != "windows"
	if symlinks {
		if err := os.Symlink("top.txt", filepath.Join(root, "link")); err != nil {
			t.Fatal(err)
		}
		gitIn(t, root, "add", "link")
		gitIn(t, root, "commit", "-q", "-m", "link")
	}
	// a deleted tracked file stays deleted
	if err := os.Remove(filepath.Join(root, "with space.txt")); err != nil {
		t.Fatal(err)
	}
	var linkTime time.Time
	if symlinks {
		linkTime = mtime(t, filepath.Join(root, "link"))
	}

	// from a subdirectory, the whole repository is touched
	result, err := touch(filepath.Join(root, "a", "b"), []string{filepath.Join(root, "testrepo"), filepath.Join(root, "missing")})
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"top.txt", "a/b/deep.txt", "a/naïve.txt", ".gitignore"} {
		blob := gitIn(t, root, "rev-parse", "HEAD:"+path)
		if got, want := mtime(t, filepath.Join(root, path)), objectTime(blob); !got.Equal(want) {
			t.Errorf("%s: mtime %v, want %v", path, got, want)
		}
	}
	for _, dir := range []string{"a", "a/b"} {
		tree := gitIn(t, root, "rev-parse", "HEAD:"+dir)
		if got, want := mtime(t, filepath.Join(root, dir)), objectTime(tree); !got.Equal(want) {
			t.Errorf("%s: mtime %v, want %v", dir, got, want)
		}
	}
	if got, want := mtime(t, root), objectTime(gitIn(t, root, "rev-parse", "HEAD^{tree}")); !got.Equal(want) {
		t.Errorf("root: mtime %v, want %v", got, want)
	}
	if _, err := os.Lstat(filepath.Join(root, "with space.txt")); !os.IsNotExist(err) {
		t.Errorf("a deleted tracked file was recreated: %v", err)
	}
	if symlinks {
		if got := mtime(t, filepath.Join(root, "link")); !got.Equal(linkTime) {
			t.Errorf("symlink retimed to %v", got)
		}
	}

	nested := filepath.Join(root, "testrepo")
	for _, path := range []string{nested, filepath.Join(nested, "sub"), filepath.Join(nested, "sub", "file"), filepath.Join(nested, ".git", "HEAD")} {
		if got := mtime(t, path); !got.Equal(time.Unix(commitTime, 0)) {
			t.Errorf("%s: mtime %v, want the nested repository's commit time", path, got)
		}
	}

	want := touchResult{Files: 4, Dirs: 3, Repos: 1}
	if result != want {
		t.Errorf("result %+v, want %+v", result, want)
	}
}

func TestTouch_RepoNotARoot(t *testing.T) {
	needGit(t)
	root := newRepo(t)

	// a directory of the outer repository isn't timed as one
	_, err := touch(root, []string{filepath.Join(root, "a")})
	if err == nil || !strings.Contains(err.Error(), "isn't the root of a repository") {
		t.Fatalf("err = %v, want one saying it isn't a repository's root", err)
	}
}

func TestTouchMain(t *testing.T) {
	needGit(t)
	root := newRepo(t)

	// -repo is relative to -C
	if code := touchMain([]string{"-C", root, "-repo", "testrepo"}); code != 0 {
		t.Fatalf("exit code %d", code)
	}
	if got := mtime(t, filepath.Join(root, "testrepo", "sub", "file")); !got.Equal(time.Unix(commitTime, 0)) {
		t.Errorf("nested file mtime %v, want its commit time", got)
	}

	if code := touchMain([]string{"-C", root, "extra"}); code != 2 {
		t.Errorf("exit code %d with an argument, want 2", code)
	}
	outside := t.TempDir()
	t.Setenv("GIT_CEILING_DIRECTORIES", filepath.Dir(outside))
	if code := touchMain([]string{"-C", outside}); code != 1 {
		t.Errorf("exit code %d outside a repository, want 1", code)
	}
}

func TestObjectTime(t *testing.T) {
	for _, tt := range []struct {
		id   string
		want time.Time
	}{
		{"d3f2e382fbfd3339b741f1cfc70ca9b0b0856bcf", time.Unix(0xd3f2e382%1577836800, 0)},
		{"short", time.Unix(0, 0)},
		{"zzzzzzzzzz", time.Unix(0, 0)},
	} {
		if got := objectTime(tt.id); !got.Equal(tt.want) {
			t.Errorf("objectTime(%q) = %v, want %v", tt.id, got, tt.want)
		}
	}
}
