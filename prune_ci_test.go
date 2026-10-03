package main

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"
)

func TestPruneStarted(t *testing.T) {
	jobStart := time.Now().Add(-time.Minute).Truncate(time.Second)
	during, earlier := jobStart.Add(time.Second), jobStart.Add(-time.Hour)

	for _, tc := range []struct {
		name    string
		started string // contents, or "" for no file
		failed  bool
		opts    pruneOptions
		stray   int // bytes of a file prune doesn't remove
		want    []string
		removed bool // the delta dir itself
	}{
		{
			name:    "the job didn't use the delta",
			removed: true,
		},
		{
			name:    "keeps what the job used",
			started: strconv.FormatInt(jobStart.Unix(), 10) + "\n",
			want:    []string{"used"},
		},
		{
			name:    "a failed job only applies the size limit",
			started: strconv.FormatInt(jobStart.Unix(), 10) + "\n",
			failed:  true,
			want:    []string{"used", "unused"},
		},
		{
			name:    "emptied when it can't tell when the job started",
			started: "yesterday\n",
			want:    []string{},
		},
		{
			name:    "emptied when still over twice the limit",
			started: strconv.FormatInt(jobStart.Unix(), 10) + "\n",
			opts:    pruneOptions{maxSize: 4096},
			stray:   64 << 10,
			want:    []string{},
		},
		{
			name:    "kept under twice the limit",
			started: strconv.FormatInt(jobStart.Unix(), 10) + "\n",
			opts:    pruneOptions{maxSize: 1 << 20},
			stray:   64 << 10,
			want:    []string{"used"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "delta")
			ids := map[string]string{
				"used":   writeDeltaEntry(t, dir, 1, "used", during),
				"unused": writeDeltaEntry(t, dir, 2, "unused", earlier),
			}
			if tc.stray > 0 {
				if err := os.WriteFile(filepath.Join(dir, "stray"), make([]byte, tc.stray), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			started := dir + ".started"
			if tc.started != "" {
				if err := os.WriteFile(started, []byte(tc.started), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			if code := pruneStarted(dir, started, tc.failed, tc.opts); code != 0 {
				t.Fatalf("exit code %d", code)
			}

			if tc.removed {
				if _, err := os.Stat(dir); !os.IsNotExist(err) {
					t.Fatalf("delta not removed: %v", err)
				}
				return
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(tc.want) == 0 {
				if len(entries) != 0 {
					t.Errorf("delta not emptied: %v", entries)
				}
				return
			}
			for name, id := range ids {
				_, err := os.Stat(filepath.Join(dir, actionDir, id))
				if kept, want := err == nil, slices.Contains(tc.want, name); kept != want {
					t.Errorf("%s kept = %v, want %v", name, kept, want)
				}
			}
		})
	}
}

func TestPruneMain_Usage(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{
		{"-delta-dir", dir},
		{"-delta-dir", dir, "-started", dir + ".started", "-used-since", "1"},
		{"-delta-dir", dir, "-failed", "-max-size", "1KiB"},
		{"-delta-dir", dir, "-ci", "gitlab", "-max-size", "1KiB"},
		{"-delta-dir", dir, "-started", dir + ".started", "-ci", "github"},
	} {
		if code := pruneMain(args); code != 2 {
			t.Errorf("pruneMain(%q) = %d, want 2", args, code)
		}
	}
}
