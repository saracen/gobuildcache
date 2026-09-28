package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeGoogle answers token exchanges and object lookups for every request
// made through http.DefaultTransport, recording what it was sent.
type fakeGoogle struct {
	mu            sync.Mutex
	subjectTokens []string
	storageAuth   []string
	other         []string
}

const fakeAccessToken = "fake-access-token"

func (f *fakeGoogle) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	respond := func(body string) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	}

	switch {
	case r.URL.Host == "sts.googleapis.com":
		if err := r.ParseForm(); err != nil {
			return nil, err
		}
		f.subjectTokens = append(f.subjectTokens, r.PostForm.Get("subject_token"))
		return respond(`{"access_token":"` + fakeAccessToken + `","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}`)

	case r.URL.Host == "storage.googleapis.com":
		f.storageAuth = append(f.storageAuth, r.Header.Get("Authorization"))
		return respond(`{"bucket":"bucket","name":"key","metadata":{"output_id":"` + strings.Repeat("b", 64) + `"}}`)
	}

	f.other = append(f.other, r.URL.String())
	return nil, &url.Error{Op: r.Method, URL: r.URL.String(), Err: io.ErrUnexpectedEOF}
}

func TestOpenBucket_GCSDoesNotWaitForTheMetadataServer(t *testing.T) {
	dir := t.TempDir()

	// external_account credentials, as workload identity federation from a
	// CI job's ID token uses
	subjectToken := filepath.Join(dir, "id_token")
	if err := os.WriteFile(subjectToken, []byte("ci-id-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	creds, err := json.Marshal(map[string]any{
		"type":               "external_account",
		"audience":           "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/pool/providers/provider",
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          "https://sts.googleapis.com/v1/token",
		"credential_source":  map[string]string{"file": subjectToken},
	})
	if err != nil {
		t.Fatal(err)
	}
	credsFile := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(credsFile, creds, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := map[string]struct {
		url      string
		wantAuth string
		wantSTS  []string
	}{
		"anonymous":        {url: "gs://bucket?anonymous=true", wantAuth: ""},
		"external account": {url: "gs://bucket", wantAuth: "Bearer " + fakeAccessToken, wantSTS: []string{"ci-id-token"}},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// An address nothing answers makes the metadata client think
			// it's on GCE, and wait out its retries on every lookup, as in
			// a job container on a GCE VM that can't reach it.
			t.Setenv("GCE_METADATA_HOST", "10.255.255.1")
			t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credsFile)
			t.Setenv("STORAGE_EMULATOR_HOST", "")

			fake := &fakeGoogle{}
			transport := http.DefaultTransport
			http.DefaultTransport = fake
			t.Cleanup(func() { http.DefaultTransport = transport })

			ctx := context.Background()
			start := time.Now()
			bucket, err := openBucket(ctx, tc.url)
			if err != nil {
				t.Fatal(err)
			}
			defer bucket.Close()
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("opening the bucket took %v", took)
			}

			start = time.Now()
			if _, err := bucket.Attributes(ctx, "action/"+strings.Repeat("a", 64)); err != nil {
				t.Fatal(err)
			}
			if took := time.Since(start); took > 2*time.Second {
				t.Errorf("first lookup took %v", took)
			}

			fake.mu.Lock()
			defer fake.mu.Unlock()
			if len(fake.storageAuth) != 1 || fake.storageAuth[0] != tc.wantAuth {
				t.Errorf("storage requests' Authorization = %q, want one with %q", fake.storageAuth, tc.wantAuth)
			}
			if strings.Join(fake.subjectTokens, ",") != strings.Join(tc.wantSTS, ",") {
				t.Errorf("token exchanges' subject tokens = %q, want %q", fake.subjectTokens, tc.wantSTS)
			}
			if len(fake.other) != 0 {
				t.Errorf("unexpected requests: %q", fake.other)
			}
		})
	}
}

func TestOpenBucket_OtherSchemesKeepTheirOpeners(t *testing.T) {
	dir := t.TempDir()
	bucket, err := openBucket(context.Background(), "file://"+filepath.ToSlash(dir)+"?prefix=p/1/")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	if err := bucket.WriteAll(context.Background(), "key", []byte("x"), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "p", "1", "key")); err != nil {
		t.Errorf("prefix parameter wasn't applied: %v", err)
	}
}

func TestOpenBucket_GCSRejectsInvalidParameters(t *testing.T) {
	t.Setenv("GCE_METADATA_HOST", "10.255.255.1")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("STORAGE_EMULATOR_HOST", "")

	for _, u := range []string{"gs://bucket?anonymous=maybe", "gs://bucket?unknown=1"} {
		if bucket, err := openBucket(context.Background(), u); err == nil {
			bucket.Close()
			t.Errorf("%s: opened, want an error", u)
		}
	}
}

// Failing to start would fail every go command, so without credentials the
// bucket opens, and calls to it fail for the breaker to turn it off.
func TestOpenBucket_GCSWithoutCredentials(t *testing.T) {
	t.Setenv("GCE_METADATA_HOST", "10.255.255.1")
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	t.Setenv("STORAGE_EMULATOR_HOST", "")

	fake := &fakeGoogle{}
	transport := http.DefaultTransport
	http.DefaultTransport = fake
	t.Cleanup(func() { http.DefaultTransport = transport })

	bucket, err := openBucket(context.Background(), "gs://bucket")
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := bucket.Attributes(ctx, "key"); err == nil || ctx.Err() != nil {
		t.Errorf("lookup: err = %v, ctx = %v; want a credentials error before the deadline", err, ctx.Err())
	}
	if len(fake.storageAuth) != 0 {
		t.Errorf("sent %d storage requests without credentials", len(fake.storageAuth))
	}
}
