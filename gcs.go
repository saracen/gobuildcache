package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// openBucket opens a bucket URL as blob.OpenBucket does, except that gs://
// URLs are opened with gcsOpener.
func openBucket(ctx context.Context, bucketURL string) (*blob.Bucket, error) {
	mux := new(blob.URLMux)
	for _, scheme := range blob.DefaultURLMux().BucketSchemes() {
		if scheme != gcsblob.Scheme {
			mux.RegisterBucket(scheme, defaultOpener{})
		}
	}
	mux.RegisterBucket(gcsblob.Scheme, gcsOpener{})

	return mux.OpenBucket(ctx, bucketURL)
}

// defaultOpener opens a URL with the opener registered for its scheme by
// default. The mux has already removed the parameters it handles itself,
// such as prefix, so they aren't applied twice.
type defaultOpener struct{}

func (defaultOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	return blob.DefaultURLMux().OpenBucketURL(ctx, u)
}

// gcsOpener opens gs:// URLs like gcsblob's default opener, but never asks
// the GCE metadata server for anything credentials don't need.
//
// The default opener looks up the instance's service account email to sign
// URLs with, and does all its credential lookups even for anonymous=true.
// On a GCE VM whose job containers can't reach the metadata server, such as
// GitLab.com's SaaS runners, each lookup waits out its retries, about 14s,
// before gobuildcache can answer the go command. gobuildcache never signs
// URLs, so it doesn't need them.
type gcsOpener struct{}

func (gcsOpener) OpenBucketURL(ctx context.Context, u *url.URL) (*blob.Bucket, error) {
	opener := &gcsblob.URLOpener{Client: gcp.NewAnonymousHTTPClient(gcp.DefaultTransport())}

	// The emulator replaces the client with an unauthenticated one, and
	// anonymous URLs don't use it, so neither needs credentials. Invalid
	// parameters are reported by the opener.
	q := u.Query()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" && !gcsAnonymous(q) {
		client, universeDomain, err := gcsCredentialsClient(ctx, q.Get("universe_domain"))
		if err != nil {
			return nil, err
		}
		opener.Client = client
		if universeDomain != "" {
			opener.Options.ClientOptions = append(opener.Options.ClientOptions, option.WithUniverseDomain(universeDomain))
		}
	}

	return opener.OpenBucketURL(ctx, u)
}

// gcsAnonymous reports whether a gs:// URL's parameters ask for an
// unauthenticated client, or are invalid, which gcsblob.URLOpener rejects.
func gcsAnonymous(q url.Values) bool {
	if v := q.Get("anonymous"); v != "" {
		anonymous, err := strconv.ParseBool(v)
		if err != nil || anonymous {
			return true
		}
	}
	return q.Get("access_id") == "-"
}

// gcsCredentialsClient returns an HTTP client authenticated with Application
// Default Credentials, and the universe domain they're for. Credentials from
// GOOGLE_APPLICATION_CREDENTIALS or gcloud's file don't use the metadata
// server; on GCE without either, they come from it, as the token must.
//
// If there are no credentials, the client's requests fail, as with gcsblob's
// default opener: the breaker then turns the bucket off, where failing to
// start would fail every go command.
func gcsCredentialsClient(ctx context.Context, universeDomain string) (*gcp.HTTPClient, string, error) {
	// Token requests, such as a workload identity federation token exchange,
	// use this client for as long as the credentials are used. Without a
	// timeout, an exchange that never answers holds up the bucket call that
	// needed the token, whatever that call's own deadline.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: metadataTimeout})

	creds, err := gcp.DefaultCredentialsWithParams(ctx, google.CredentialsParams{UniverseDomain: universeDomain})
	if err != nil {
		slog.Warn("no GCP credentials, the bucket can't be used", "err", err)
		client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), failingTokenSource{err})
		return client, "", err
	}

	universeDomain, err = creds.GetUniverseDomain()
	if err != nil {
		slog.Warn("getting GCP universe domain, using the default", "err", err)
		universeDomain = ""
	}

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), creds.TokenSource)
	return client, universeDomain, err
}

type failingTokenSource struct{ err error }

func (s failingTokenSource) Token() (*oauth2.Token, error) { return nil, s.err }
