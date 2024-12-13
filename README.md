# gobuildcache

`gobuildcache` is a [`GOCACHEPROG`](https://github.com/golang/go/issues/59719) process using the [The Go Cloud Development Kit](https://gocloud.dev/) to support Azure, GCS and S3 storage providers.

## usage

```shell
export GOCACHEPROG="./gobuildcache <bucket url>"
```

A readonly mode is supported, which works well if `?anonymous=true` is also passed as a bucket parameter to the bucket URL if the bucket is publically accessible. This parameter seems to only be supported by GCS and S3 though.

For more information on supported bucket URL parameters see https://gocloud.dev/howto/blob/#services.

### flags

- `-v` for verbose logging.
- `-p` to specify key prefix.
- `-readonly` to only support fetching of cache items.
