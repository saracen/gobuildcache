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

### flags

- `-v` for verbose logging.
- `-p` to specify key prefix.
- `-readonly` to never write to the bucket.
- `-refresh-after` to set how old an entry must be before a writer that uses it refreshes it (default `24h`, `0` disables). See [expiring old entries](#expiring-old-entries).
- `-stats` to log a summary of hits, misses and transfers when the process exits.
- `-dir` to specify the local cache directory (default `<user cache dir>/.gocachebucket`).
- `-env` to remap an environment variable before opening the bucket, for example `-env GOOGLE_APPLICATION_CREDENTIALS=MY_CREDENTIALS_FILE`.

## getting cache hits in CI

The go command runs one `GOCACHEPROG` process per invocation, so `-stats` prints one line per `go build`, `go test` and so on. `GODEBUG=gocachehash=1` prints the inputs hashed into each action ID, and `GODEBUG=gocachetest=1` explains why a test result wasn't reused. Diffing either between two runs is the quickest way to find what changed.

Fresh checkouts won't hit the cache if files have new modification times:

- The go command doesn't cache its index of a directory whose files were just modified.
- Cached test results record the size and modification time of every file a test opens.

Set every checked-out file's modification time to something stable and in the past, such as a time derived from the file's contents, before running `go`.

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
