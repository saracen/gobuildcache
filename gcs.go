package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

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
	opener := &gcsblob.URLOpener{Client: gcp.NewAnonymousHTTPClient(gcsTransport())}

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

// gcsPingTimeout is how long a GCS connection can go without receiving
// anything before it's pinged, and then how long the ping has to be answered
// before the connection is closed. A ping can wait behind an upload's bytes
// queued on a slow link, so this is generous, but it's well within an
// attempt at a transfer.
var gcsPingTimeout = 30 * time.Second

// gcsTransport returns gcp.DefaultTransport, with HTTP/2 connections checked
// with pings.
//
// The GCS client reaches the bucket over HTTP/2, which sends every request
// to a host over one connection, and nothing else closes a connection that
// stops answering: a request that runs out of time only resets its stream.
// So when the bucket goes away during an upload, which counts as reaching
// it once sent (see attemptTrace.reached), each retry would go out on the
// same dead connection, be sent, and count as reaching the bucket too, and
// the upload would take every attempt's timeout without turning the bucket
// off. Once a ping goes unanswered, the connection is closed, and the retry
// dials a new one, which can't reach the bucket. A connection that's
// working, however slowly, receives the bucket's flow control updates as it
// reads an upload, and its pings are answered.
func gcsTransport() http.RoundTripper {
	// http.DefaultTransport, which needn't be an *http.Transport
	t, ok := gcp.DefaultTransport().(*http.Transport)
	if !ok {
		return gcp.DefaultTransport()
	}
	t = t.Clone()
	t.HTTP2 = &http.HTTP2Config{SendPingTimeout: gcsPingTimeout, PingTimeout: gcsPingTimeout}
	return t
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
// Default Credentials, and the universe domain they're for.
//
// If there are no credentials, the client's requests fail, as with gcsblob's
// default opener: the breaker then turns the bucket off, where failing to
// start would fail every go command.
func gcsCredentialsClient(ctx context.Context, universeDomain string) (*gcp.HTTPClient, string, error) {
	// Without a credentials file, Application Default Credentials come from
	// the metadata server, and looking them up asks it for the project ID
	// and then the universe domain, neither needed for a token. This runs
	// before gobuildcache answers the go command, where nothing bounds it,
	// and where the metadata server can't be reached each lookup waits out
	// the metadata client's retries. A compute token source asks it for
	// nothing until the first call needs a token, which the startup check
	// bounds; off GCE, that token request fails.
	if !adcFromFile() {
		client, err := gcp.NewHTTPClient(gcsTransport(), google.ComputeTokenSource("", gcsScope))
		return client, universeDomain, err
	}

	// Token requests, such as a workload identity federation token exchange,
	// use this client for as long as the credentials are used. Without a
	// timeout, an exchange that never answers holds up the bucket call that
	// needed the token, whatever that call's own deadline.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: metadataTimeout})

	creds, err := gcp.DefaultCredentialsWithParams(ctx, google.CredentialsParams{UniverseDomain: universeDomain})
	if err != nil {
		slog.Warn("no GCP credentials, the bucket can't be used", "err", err)
		client, err := gcp.NewHTTPClient(gcsTransport(), failingTokenSource{err})
		return client, "", err
	}

	// from the file, or the default
	universeDomain, err = creds.GetUniverseDomain()
	if err != nil {
		slog.Warn("getting GCP universe domain, using the default", "err", err)
		universeDomain = ""
	}

	client, err := gcp.NewHTTPClient(gcsTransport(), creds.TokenSource)
	return client, universeDomain, err
}

// gcsScope is the scope gcp.DefaultCredentialsWithParams asks for.
const gcsScope = "https://www.googleapis.com/auth/cloud-platform"

// adcFromFile reports whether Application Default Credentials come from a
// file, rather than the metadata server: the one GOOGLE_APPLICATION_CREDENTIALS
// names, or gcloud's, found as google.FindDefaultCredentials finds it, which
// doesn't export where it looks.
func adcFromFile() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}

	const name = "application_default_credentials.json"
	var pathname string
	if runtime.GOOS == "windows" {
		pathname = filepath.Join(os.Getenv("APPDATA"), "gcloud", name)
	} else {
		home := os.Getenv("HOME")
		if home == "" {
			if u, err := user.Current(); err == nil {
				home = u.HomeDir
			}
		}
		pathname = filepath.Join(home, ".config", "gcloud", name)
	}

	// read, as the lookup does, which moves on if it can't
	_, err := os.ReadFile(pathname)
	return err == nil
}

type failingTokenSource struct{ err error }

func (s failingTokenSource) Token() (*oauth2.Token, error) { return nil, s.err }
