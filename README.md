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
- `-stats` to log a summary of hits, misses and transfers when the process exits.
- `-dir` to specify the local cache directory (default `<user cache dir>/.gocachebucket`).
- `-env` to remap an environment variable before opening the bucket, for example `-env GOOGLE_APPLICATION_CREDENTIALS=MY_CREDENTIALS_FILE`.

## getting cache hits in CI

The go command runs one `GOCACHEPROG` process per invocation, so `-stats` prints one line per `go build`, `go test` and so on. `GODEBUG=gocachehash=1` prints the inputs hashed into each action ID, and `GODEBUG=gocachetest=1` explains why a test result wasn't reused. Diffing either between two runs is the quickest way to find what changed.

Fresh checkouts won't hit the cache if files have new modification times:

- The go command doesn't cache its index of a directory whose files were just modified.
- Cached test results record the size and modification time of every file a test opens.

Set every checked-out file's modification time to something stable and in the past, such as a time derived from the file's contents, before running `go`.
