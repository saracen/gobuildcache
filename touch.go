package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// touchMain runs "gobuildcache touch", which gives a checkout stable
// modification times; see touch.
func touchMain(args []string) int {
	flags := flag.NewFlagSet("touch", flag.ContinueOnError)
	dir := flags.String("C", ".", "a directory in the repository to touch")
	var repos flagArray
	flags.Var(&repos, "repo", "a repository in the checkout that git doesn't track, such as one tests use, to give the time of its HEAD commit; relative to -C, and skipped if missing (repeatable)")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "%s touch [-C <dir>] [-repo <path>]...\n", os.Args[0])
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		flags.Usage()
		return 2
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	for i, repo := range repos {
		if !filepath.IsAbs(repo) {
			repos[i] = filepath.Join(*dir, repo)
		}
	}

	result, err := touch(*dir, repos)
	if err != nil {
		slog.Error("touch", "err", err)
		return 1
	}
	slog.Info("gobuildcache touch", "files", result.Files, "dirs", result.Dirs, "repos", result.Repos)
	return 0
}

type touchResult struct {
	Files, Dirs, Repos int
}

// touch gives every file git tracks in the repository containing dir a
// modification time derived from its blob ID, and every directory one from
// its tree ID, so that each only changes when its contents do. Then it gives
// everything in each of repos the time of its HEAD commit.
//
// The go command doesn't cache its index of a directory whose files were
// just modified, and cached test results record the modification time of
// every file and directory a test opens or changes into. A checkout gives
// them all the time of the checkout, so a fresh one never gets test results
// from a shared cache. Commit times aren't stable enough instead: a shallow
// clone dates every file it didn't fetch the history of to the oldest commit
// it fetched, and GitLab's merged results pipelines check out a new merge
// commit every time.
func touch(dir string, repos []string) (touchResult, error) {
	var result touchResult

	out, err := git(dir, "rev-parse", "--show-toplevel")
	if err != nil {
		return result, err
	}
	root := strings.TrimSpace(string(out))

	if result.Files, err = touchFiles(root); err != nil {
		return result, err
	}
	if result.Dirs, err = touchDirs(root); err != nil {
		return result, err
	}
	for _, repo := range repos {
		touched, err := touchRepo(repo)
		if err != nil {
			return result, err
		}
		if touched {
			result.Repos++
		}
	}
	return result, nil
}

// touchFiles skips symlinks, since setting their time would set their
// target's, and submodules, which are other repositories' to time.
func touchFiles(root string) (int, error) {
	out, err := git(root, "ls-files", "-s", "-z")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, entry := range records(out) {
		// <mode> <object> <stage>\t<path>
		meta, path, _ := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if len(fields) != 3 || fields[0] == "120000" || fields[0] == "160000" {
			continue
		}
		touched, err := chtimes(filepath.Join(root, path), objectTime(fields[1]))
		if err != nil {
			return n, err
		}
		if touched {
			n++
		}
	}
	return n, nil
}

func touchDirs(root string) (int, error) {
	out, err := git(root, "ls-tree", "-r", "-t", "-z", "HEAD")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, entry := range records(out) {
		// <mode> <type> <object>\t<path>
		meta, path, _ := strings.Cut(entry, "\t")
		fields := strings.Fields(meta)
		if len(fields) != 3 || fields[1] != "tree" {
			continue
		}
		touched, err := chtimes(filepath.Join(root, path), objectTime(fields[2]))
		if err != nil {
			return n, err
		}
		if touched {
			n++
		}
	}

	tree, err := git(root, "rev-parse", "HEAD^{tree}")
	if err != nil {
		return n, err
	}
	if _, err := chtimes(root, objectTime(strings.TrimSpace(string(tree)))); err != nil {
		return n, err
	}
	return n + 1, nil
}

// touchRepo gives everything in repo, which git doesn't track, the time of
// its HEAD commit, and reports whether repo exists. A path that isn't a
// repository's root is an error, rather than timed by the repository
// containing it. Symlinks are skipped, as the standard library can't time a
// symlink itself.
func touchRepo(repo string) (bool, error) {
	info, err := os.Stat(repo)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	out, err := git(repo, "rev-parse", "--show-toplevel")
	if err != nil {
		return false, err
	}
	top, err := os.Stat(strings.TrimSpace(string(out)))
	if err != nil {
		return false, err
	}
	if !os.SameFile(info, top) {
		return false, fmt.Errorf("-repo %s isn't the root of a repository", repo)
	}

	out, err = git(repo, "log", "-1", "--format=%ct")
	if err != nil {
		return false, err
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return false, fmt.Errorf("commit time of %s: %w", repo, err)
	}
	t := time.Unix(seconds, 0)

	return true, filepath.WalkDir(repo, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.Type()&fs.ModeSymlink != 0 {
			return err
		}
		_, err = chtimes(path, t)
		return err
	})
}

// objectTime is the first 8 hex digits of a git object ID as Unix seconds,
// modulo 2020-01-01, so always in the past.
func objectTime(id string) time.Time {
	if len(id) < 8 {
		return time.Unix(0, 0)
	}
	n, err := strconv.ParseUint(id[:8], 16, 64)
	if err != nil {
		return time.Unix(0, 0)
	}
	return time.Unix(int64(n%1577836800), 0)
}

// chtimes sets both times, as touch(1) does, but leaves a missing path
// missing rather than creating it, and reports whether it existed.
func chtimes(path string, t time.Time) (bool, error) {
	err := os.Chtimes(path, t, t)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func git(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

// records splits git's -z output.
func records(out []byte) []string {
	return strings.FieldsFunc(string(out), func(r rune) bool { return r == 0 })
}
