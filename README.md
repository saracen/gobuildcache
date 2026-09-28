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

A readonly mode is supported, which never writes to the bucket. New cache entries are still kept in the local cache, because the go command reads some of them back within the same command. It works well with `?anonymous=true` passed as a bucket parameter if the bucket is publicly accessible, though this parameter seems to only be supported by GCS and S3.

For more information on supported bucket URL parameters see https://gocloud.dev/howto/blob/#services.

On GCS, gobuildcache authenticates with the credentials file named by `GOOGLE_APPLICATION_CREDENTIALS`, such as a workload identity federation configuration, or else Application Default Credentials, and with nothing for `?anonymous=true`. Unlike gocloud's default `gs://` opener, it never asks the GCE metadata server for a service account to sign URLs with, since it doesn't sign any. On a GCE VM whose job containers can't reach the metadata server, such as GitLab.com's SaaS runners, those lookups otherwise delay every go command by 14 to 42 seconds. Only Application Default Credentials that come from the metadata server itself still use it.

### flags

- `-v` for verbose logging.
- `-p` to specify key prefix.
- `-readonly` to never write to the bucket.
- `-dedupe-wait` to set how long to wait for another process sharing the local cache to put an action it's computing, rather than computing it too (default `1m`, `0` disables). See [concurrent go commands](#concurrent-go-commands).
- `-refresh-after` to set how old an entry must be before a writer that uses it refreshes it (default `24h`, `0` disables). See [expiring old entries](#expiring-old-entries).
- `-expire-others` to report every entry this process didn't put as put at the Unix epoch, so that after `go clean -testcache` the go command reruns every test result it didn't produce itself. See [rerunning cached tests](#rerunning-cached-tests).
- `-stats` to log a summary of hits, misses and transfers when the process exits.
- `-dir` to specify the local cache directory (default `<user cache dir>/.gocachebucket`).
- `-env` to remap an environment variable before opening the bucket, for example `-env GOOGLE_APPLICATION_CREDENTIALS=MY_CREDENTIALS_FILE`.

## getting cache hits in CI

The go command runs one `GOCACHEPROG` process per invocation, so `-stats` prints one line per `go build`, `go test` and so on. `GODEBUG=gocachehash=1` prints the inputs hashed into each action ID, and `GODEBUG=gocachetest=1` explains why a test result wasn't reused. Diffing either between two runs is the quickest way to find what changed.

Fresh checkouts won't hit the cache if files have new modification times:

- The go command doesn't cache its index of a directory whose files were just modified.
- Cached test results record the size and modification time of every file a test opens.

Set every checked-out file's modification time to something stable and in the past, such as a time derived from the file's contents, before running `go`.

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

gocloud reports most server errors and throttling (a GCS 503, most S3 and Azure errors) as `Unknown`, so `Unknown` errors are retried, along with `Internal`, `ResourceExhausted` and timeouts. Some permanent errors are `Unknown` too, such as a malformed credentials file, and are retried as well before gobuildcache gives up on the bucket. Errors that can't change, such as not found or permission denied, are not retried.

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
