# gobuildcache

`gobuildcache` is a [`GOCACHEPROG`](https://github.com/golang/go/issues/59719) process using the [The Go Cloud Development Kit](https://gocloud.dev/) to support Azure, GCS and S3 storage providers.

## usage

```shell
export GOCACHEPROG="./gobuildcache <bucket url>"
```

A test is performed to see if permissions allow for write or read. For GCS, the bucket URL param `?access_id=-` has to be used to force unauthenicated mode. This is useful in scenarios where you only want to provide read access to your bucket's caches.

For more information on supported bucket URL parameters see https://gocloud.dev/howto/blob/#services.

### flags

- `-v` for verbose logging.
- `-p` to specify key prefix.
