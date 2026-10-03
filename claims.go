package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Several go commands running at once in the same job, such as packages
// tested concurrently or several binaries built at once, share the local cache
// directory. When they miss on the same action, which is common on a cold
// cache because they share dependencies, each would compute it. Instead, the
// first to miss claims the action with a lock file, and the others wait for it
// to be put and then get a hit.
//
// A claim is released when its action is put, and all of a process's claims
// when it exits. Waiting is bounded: the go command looks up some entries it
// never puts, and a process that dies holding a claim is only detected on
// unix. When a wait times out, a marker records that the action isn't being
// put, so nobody waits for it again.

// claimPoll is how often a waiting process checks whether the action it's
// waiting for has been put.
const claimPoll = 50 * time.Millisecond

type claims struct {
	dir     string
	maxWait time.Duration
	pid     int
	stats   *Stats

	mu    sync.Mutex
	owned map[string]struct{}
}

func newClaims(cacheDir string, maxWait time.Duration, stats *Stats) (*claims, error) {
	dir := filepath.Join(cacheDir, "claims")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating claims dir: %w", err)
	}

	return &claims{
		dir:     dir,
		maxWait: maxWait,
		pid:     os.Getpid(),
		stats:   stats,
		owned:   make(map[string]struct{}),
	}, nil
}

func (l *claims) path(actionID string) string {
	return filepath.Join(l.dir, actionID)
}

func (l *claims) noPutPath(actionID string) string {
	return filepath.Join(l.dir, actionID+".noput")
}

func (l *claims) owns(actionID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, ok := l.owned[actionID]
	return ok
}

// tryClaim claims actionID if no process has.
func (l *claims) tryClaim(actionID string) bool {
	f, err := os.OpenFile(l.path(actionID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return false
	}
	fmt.Fprintf(f, "%d", l.pid)
	f.Close()

	l.mu.Lock()
	l.owned[actionID] = struct{}{}
	l.mu.Unlock()

	return true
}

func (l *claims) release(actionID string) {
	l.mu.Lock()
	_, ok := l.owned[actionID]
	delete(l.owned, actionID)
	l.mu.Unlock()

	if ok {
		os.Remove(l.path(actionID))
	}
}

func (l *claims) releaseAll() {
	l.mu.Lock()
	owned := l.owned
	l.owned = make(map[string]struct{})
	l.mu.Unlock()

	for actionID := range owned {
		os.Remove(l.path(actionID))
	}
}

// holderGone reports whether actionID's claim is held by a process that no
// longer exists.
func (l *claims) holderGone(actionID string) bool {
	data, err := os.ReadFile(l.path(actionID))
	if err != nil {
		return false
	}

	// an empty or partial pid means the claim is still being written
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return false
	}

	return !processAlive(pid)
}

// awaitOrClaim is called on a miss. It returns the path of the action's
// output if another process put it while this one waited, or "" if this
// process should compute it, having claimed it if possible.
func (l *claims) awaitOrClaim(ctx context.Context, actionID string, localHit func() string) string {
	// never wait on ourselves: the go command can look up an action it's
	// already computing
	if l.owns(actionID) {
		return ""
	}

	if _, err := os.Stat(l.noPutPath(actionID)); err == nil {
		return ""
	}

	start := time.Now()
	waited := false
	defer func() {
		if waited {
			l.stats.ClaimWaits.Add(1)
			l.stats.ClaimWaitMillis.Add(time.Since(start).Milliseconds())
		}
	}()

	for {
		if pathname := localHit(); pathname != "" {
			if waited {
				l.stats.ClaimHits.Add(1)
			}
			return pathname
		}

		if l.tryClaim(actionID) {
			// the holder may have put it and released its claim between the
			// check above and claiming it
			if pathname := localHit(); pathname != "" {
				l.release(actionID)
				return pathname
			}
			return ""
		}

		if l.holderGone(actionID) {
			os.Remove(l.path(actionID))
			continue
		}

		if time.Since(start) >= l.maxWait {
			l.stats.ClaimTimeouts.Add(1)
			os.WriteFile(l.noPutPath(actionID), nil, 0o644)
			return ""
		}

		waited = true
		select {
		case <-time.After(claimPoll):
		case <-ctx.Done():
			return ""
		}
	}
}
