# gobuildcache

`gobuildcache` is a [`GOCACHEPROG`](https://github.com/golang/go/issues/59719) process using the [The Go Cloud Development Kit](https://gocloud.dev/) to support Azure, GCS and S3 storage providers.

## install

```shell
go install github.com/saracen/gobuildcache@latest
```

## usage

```shell
export GOCACHEPROG="gobuildcache <bucket url>"
```

A readonly mode is supported, which never writes to the bucket. New cache entries are still kept in the local cache (or the [delta dir](#merge-request-deltas)), because the go command reads some of them back within the same command. It works well with `?anonymous=true` passed as a bucket parameter if the bucket is publicly accessible, though this parameter seems to only be supported by GCS and S3.

For more information on supported bucket URL parameters see https://gocloud.dev/howto/blob/#services.

On GCS, gobuildcache authenticates with the credentials file named by `GOOGLE_APPLICATION_CREDENTIALS`, such as a workload identity federation configuration, or else Application Default Credentials, and with nothing for `?anonymous=true`. Unlike gocloud's default `gs://` opener, it never asks the GCE metadata server for a service account to sign URLs with, since it doesn't sign any. On a GCE VM whose job containers can't reach the metadata server, such as GitLab.com's SaaS runners, those lookups otherwise delay every go command by 14 to 42 seconds. With neither `GOOGLE_APPLICATION_CREDENTIALS` nor gcloud's credentials file, Application Default Credentials come from the metadata server; gobuildcache only asks it for tokens, from when a call first needs one, and takes the universe domain from the `universe_domain` URL parameter, defaulting to `googleapis.com`, instead of asking for it.

### flags

- `-v` for verbose logging.
- `-p` to specify key prefix.
- `-readonly` to never write to the bucket.
- `-dedupe-wait` to set how long to wait for another process sharing the local cache to put an action it's computing, rather than computing it too (default `1m`, `0` disables). See [concurrent go commands](#concurrent-go-commands).
- `-refresh-after` to set how old an entry must be before a writer that uses it refreshes it (default `24h`, `0` disables). See [expiring old entries](#expiring-old-entries).
- `-expire-others` to report every entry this process didn't put as put at the Unix epoch, so that after `go clean -testcache` the go command reruns every test result it didn't produce itself. See [rerunning cached tests](#rerunning-cached-tests).
- `-stats` to log a summary of hits, misses and transfers when the process exits. With `-delta-dir`, `delta_hits` counts gets answered from the delta, and `delta_puts` and `delta_put_bytes` what was added to it.
- `-dir` to specify the local cache directory (default `<user cache dir>/.gocachebucket`).
- `-delta-dir` to keep what the process puts in this directory rather than `-dir`, and look there first. Requires `-readonly`. See [merge request deltas](#merge-request-deltas).
- `-env` to remap an environment variable before opening the bucket, for example `-env GOOGLE_APPLICATION_CREDENTIALS=MY_CREDENTIALS_FILE`.

## getting cache hits in CI

The go command runs one `GOCACHEPROG` process per invocation, so `-stats` prints one line per `go build`, `go test` and so on. `GODEBUG=gocachehash=1` prints the inputs hashed into each action ID, and `GODEBUG=gocachetest=1` explains why a test result wasn't reused. Diffing either between two runs is the quickest way to find what changed.

Fresh checkouts won't hit the cache if files have new modification times:

- The go command doesn't cache its index of a directory whose files were just modified.
- Cached test results record the size and modification time of every file a test opens.

Set every checked-out file's modification time to something stable and in the past, such as a time derived from the file's contents, before running `go`.

## merge request deltas

A readonly job only gets from the bucket what trusted writers put there, so a merge request's pipelines recompute everything its change affects every time they run, and a retry recomputes it all again. With `-delta-dir`, a readonly job keeps what it computes itself apart, where a CI cache can save it for the merge request's later pipelines:

- Puts go to the delta dir, not `-dir`, and never to the bucket. Since the go command only puts what it missed, that's what the bucket didn't have. An entry the go command puts again unchanged, such as a test main on every `go list -test`, isn't added if the job already has it, unless `go clean -testcache` has expired its put time.
- Gets look in the delta dir first, then `-dir`, then the bucket. What comes from the bucket is downloaded to `-dir`, never to the delta dir.
- Go commands sharing a delta dir write to it as they do to `-dir`, through temporary files renamed into place, and wait on each other's claims (kept in `-dir`) the same way.
- Put times, and so `go clean -testcache` and `-expire-others`, work as for `-dir`. A put time is the modification time of the entry's action link, so whatever saves and restores the delta dir must keep modification times, as GitLab's cache does.

A delta is small: in gitlab-runner's integration test jobs, 1 to 14 MB, against 1 to 3 GB downloaded from the bucket.

The delta should stay the merge request's working set, not grow with every change pushed to it. Each entry records when it was last used, meaning got or put, and after the job's go commands, `gobuildcache prune` removes the entries the job didn't use, and the outputs nothing links to any more:

```shell
gobuildcache prune -delta-dir <dir> -used-since <when the job started> [-max-size <size>]
```

- `-used-since` takes Unix seconds, as `date +%s` prints, or an RFC 3339 time. Take it on the machine running the job, before its first go command: when an entry was used is by that machine's clock.
- `-max-size` then removes the least recently used entries until their outputs take at most this size, in bytes or with a `KiB`, `MiB` or `GiB` suffix. It can also be used alone.
- Run it after the job's go commands, and before the CI cache saves the delta dir. It only removes what gobuildcache writes there, including temporary files that killed processes left.

A job trusts the delta as much as whatever wrote the CI cache it came from: in GitLab, any pipeline that can write the project's unprotected caches, whichever merge request it's for. Only give one to jobs whose results nothing depends on, such as merge request pipelines that don't gate a merge, and key it to the merge request and the job. Pipelines that gate a merge or a release should never use a delta.

For example, in GitLab CI:

```yaml
test:
  variables:
    GOCACHEPROG: gobuildcache -readonly -delta-dir $CI_PROJECT_DIR/.gobuildcache/delta gs://bucket?anonymous=true
  cache:
    key: gobuildcache-delta-$CI_MERGE_REQUEST_IID-$CI_JOB_NAME
    paths: [.gobuildcache/delta/]
    when: always
  before_script:
    - mkdir -p .gobuildcache && date +%s > .gobuildcache/started
  script:
    - go test ./...
  after_script:
    - gobuildcache prune -delta-dir .gobuildcache/delta -used-since "$(cat .gobuildcache/started)" -max-size 256MiB
```

GitLab saves the cache after `after_script`, which runs even when the job fails. Add `.gobuildcache/` to `.gitignore`: the go command stamps binaries built in a checkout with untracked files as modified.

## rerunning cached tests

`go clean -testcache` expires the test results put before it ran, including ones from the bucket, and unless it's `-readonly`, the job stores the results of rerunning them for later jobs. The go command decides by the time an entry was put, so gobuildcache records it on each action link it uploads (`put_time` metadata) and reports that, not when the entry was downloaded. Refreshing an entry keeps its put time.

- `go clean -testcache` silently does nothing if `GOCACHE` doesn't exist yet, as in a fresh CI job, so create it first: `mkdir -p "$(go env GOCACHE)"`.
- Entries uploaded by versions of gobuildcache that didn't record put times are always expired by `go clean -testcache`, because when they were put isn't known. Their modification time isn't used instead, since refreshing changes it. Results from rerunning them are stored with a put time.
- Results put after `go clean -testcache` ran aren't expired, including ones that other jobs upload while the job runs. So a job doesn't rerun every test if another job writing to the bucket runs the same tests at the same time, such as a pipeline for an overlapping change: for a test with inputs the go command can't see, it can replay the other job's result, from different inputs. To rerun every test, also pass `-expire-others`, which reports every entry the process didn't put itself as put at the Unix epoch, before any `go clean -testcache`. Only the go command's test cache reads when an entry was put, so builds still hit. The job's results are stored with when they were put, so jobs without `-expire-others` reuse them. Without `go clean -testcache`, `-expire-others` changes nothing.
- `-expire-others` only trusts entries its own process put, and the go command runs one process per invocation, so a later go command in the same job reruns the tests an earlier one ran. Entries other processes put are expired however they're found: from the bucket, left in a long-lived `-dir`, or put by a go command sharing `-dir` while this one waited on its claim. With `-readonly`, a process's results are only kept locally, and it still reports their put time.
- The go command puts some entries again unchanged whenever it uses them, such as a test package's generated test main on every `go list -test`, which tools like gopls run. gobuildcache only uploads an entry put again unchanged if it was put before the expiry, so a job that expires test results uploads those again too.
- Rerunning a test needs its test binary, which the go command never puts, so go commands running at the same time with the same `-dir` that rerun the same packages wait on each other's claims to link them until `-dedupe-wait` runs out (see [concurrent go commands](#concurrent-go-commands)). Pass `-dedupe-wait 0` to go commands that do.

## retries and timeouts

Every bucket call is bounded and retried the same way whichever provider is behind it: up to 3 attempts with backoff, each limited to 30 seconds for lookups and small writes, or 5 minutes for transferring an output. The limit also bounds retries done inside a provider's SDK, which for some reads otherwise continue until their context ends.

gocloud reports most server errors and throttling (a GCS 503, most S3 and Azure errors) as `Unknown`, so `Unknown` errors are retried, along with `Internal` and `ResourceExhausted`. An attempt that times out or can't connect isn't retried, as SDKs that retry those have already done so within the attempt; gobuildcache gives up on the bucket instead (see below). That includes an attempt the GCS client spent retrying 503 or 429 answers until its timeout. Some permanent errors are `Unknown` too, such as a malformed credentials file, and are retried as well before gobuildcache gives up on the bucket. Errors that can't change, such as not found or permission denied, are not retried.

On S3, grant `s3:ListBucket` on the bucket as well as reading (and, unless `-readonly`, writing) its objects. Without it, S3 answers a lookup of a missing key with 403, which gobuildcache takes as permission denied, so it gives up on the bucket at the process's first miss.

gobuildcache gives up on the bucket for the rest of the process, and carries on with the local cache, after 5 failed calls in a row, a permission denied error, or a single attempt that can't reach the bucket: one that can't connect, can't resolve the host or times out without an answer. An attempt that the bucket answered, or an upload that was sent to it, and that then ran out of time, such as a large output over a slow link, is an ordinary failure. An answer from the service that issues credentials doesn't count. A single attempt that the GCS client spent retrying error answers, such as 503s, until its timeout gives up on the bucket too. Giving up also ends the process's calls still in flight. Before a process first uses the bucket, it checks it with a single lookup bounded to 15 seconds, enough for a lost DNS query to be resent, which the process's other calls wait for. If the lookup can't reach the bucket, which also covers a token exchange that can't reach its service, the process gives up on the bucket when the lookup fails, or at the bound: the GCS client retries a refused connection until then. If the bucket answered, but with errors that the SDK retried until then, such as 503s, the process gives up on the bucket as well. Any other answer is left to the process's own calls, since how a bucket answers for a missing key doesn't show whether it's usable. Without this, an unreachable bucket costs every go command minutes: some SDKs retry a refused connection until each call's timeout, and a black hole waits it out anyway.

Since the go command starts one gobuildcache per invocation, a process that gives up on the bucket because it can't reach it writes a marker to `-dir`, holding the reason. The marker is named `remote-disabled-` and a hash of the bucket URL, `-p`, and the environment variables that decide how the bucket is reached, such as `GOOGLE_APPLICATION_CREDENTIALS`, endpoints and proxies (for those that can hold a secret, only whether they're set). For 10 minutes after it's written, processes using that `-dir` with the same bucket, reached the same way, don't use the bucket at all, so in a CI job the go commands started after the bucket goes away don't pay for finding out, and those already running stop using the bucket within a second of the marker being written. Processes using another bucket, or reaching it another way, aren't affected. Calls they already had in flight when the bucket went away still run until they fail, which can take an attempt's timeout: 30 seconds for a lookup, or 5 minutes for a transfer. A transfer the bucket had answered, or an upload already sent to it, can't be told from a slow one, so it's the retry that finds the bucket unreachable, and that can take up to twice as long, unless another of the process's calls gives up on the bucket sooner. For GCS, whose connections are checked with pings, the first attempt ends within a minute of the bucket going away. Other reasons to give up aren't shared, and a brief failure doesn't turn the bucket off for the rest of a job: rejected credentials take a process a few quick calls to find, and a bucket answering errors is reachable, even if a few seconds of 503s outlast an attempt, as the GCS client waits up to 30 seconds between tries. Once the marker expires, the next process to use the bucket checks it again. Delete `remote-disabled-*` from `-dir` to try the bucket again sooner. With `-stats`, `remote_disabled=1` shows a process that gave up on the bucket, or skipped it because of the marker. A go command that finds everything it needs locally never checks the bucket, and reports 0.

## concurrent go commands

Go commands running at the same time with the same `-dir`, such as packages tested concurrently or several binaries built at once, coordinate their misses. The first to miss on an action claims it with a lock file in the local cache directory and computes it; the others wait for it to be put, then get a hit instead of computing it too. On a cold cache, where concurrent commands share many dependencies, that avoids compiling the same packages several times over.

A claim is released when its action is put, and all of a process's claims when it exits. A claim held by a process that no longer exists is taken over on unix. Waiting is bounded by `-dedupe-wait`, since the go command looks up some entries it never puts, such as its index of a directory with recently modified files; when a wait times out, a marker stops others waiting for that action again.

## expiring old entries

gobuildcache never deletes anything. Use the storage provider's lifecycle rules to delete entries a number of days after they were last written, for both the `action/` and `output/` prefixes (under `-p`, if you use one):

- GCS: a `Delete` action with an `age` condition.
- S3: an expiration rule.
- Azure: a delete rule with `daysAfterModificationGreaterThan`.

On its own, that deletes entries that are used all the time along with ones that aren't. So when a writer (not `-readonly`) uses an entry older than `-refresh-after`, it rewrites the entry in place, which restarts its expiry: anything a writer uses at least every few days stays. The rewrite is a copy of the object onto itself, done by the provider without transferring the object. If the copy fails, the entry is uploaded again from the local cache.

Keep the lifecycle age comfortably longer than `-refresh-after`, and note:

- **Versioning and soft delete keep what the rewrite replaces.** Every refresh creates a new version of the object, so with versioning or soft delete enabled, each one leaves a copy that you're billed for until it's removed. Disable them on the cache bucket, or keep their retention short.
- **Only writers refresh.** Entries used only by `-readonly` processes expire at the lifecycle age.
- **Entries served from the local cache aren't refreshed**, only those fetched from the bucket, so a long-lived machine with a warm local cache refreshes less than fresh CI machines do.
- **S3** refuses to copy an object onto itself unchanged, so the copy replaces the object's metadata with the same metadata.
- **Azure** copying a blob onto itself hasn't been tested; if it's refused, entries are uploaded again instead.
