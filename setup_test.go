package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func gitlabVars(overrides map[string]string) func(string) string {
	vars := map[string]string{
		"CI_PROJECT_ID":           "250833",
		"CI_COMMIT_REF_PROTECTED": "true",
		"CI_PIPELINE_SOURCE":      "push",
		"CI_COMMIT_BRANCH":        "main",
		"GOBUILDCACHE_ID_TOKEN":   "header.payload.signature",
	}
	for k, v := range overrides {
		vars[k] = v
	}
	return func(name string) string { return vars[name] }
}

func runnerPolicy(t *testing.T) setupConfig {
	t.Helper()
	branches, err := wholeMatch(`main|[0-9]+-[0-9]+-stable`)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := wholeMatch(`v[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?`)
	if err != nil {
		t.Fatal(err)
	}
	return setupConfig{
		ci:             "gitlab",
		bucket:         "gs://bucket",
		prefix:         "p/250833/",
		writeProjectID: "250833",
		branches:       branches,
		tags:           tags,
		sources:        []string{"push", "web", "schedule", "api"},
		idTokenVar:     "GOBUILDCACHE_ID_TOKEN",
		provider:       "projects/1/locations/global/workloadIdentityPools/pool/providers/provider",
		credentialsVar: "GOBUILDCACHE_CREDENTIALS",
		anonymousReads: true,
		shell:          "sh",
	}
}

func TestSetup_WhyReadonly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		vars   map[string]string
		policy func(*setupConfig)
		writes bool
	}{
		{name: "protected main push", writes: true},
		{name: "stable branch", vars: map[string]string{"CI_COMMIT_BRANCH": "19-4-stable"}, writes: true},
		{name: "release tag", vars: map[string]string{"CI_COMMIT_BRANCH": "", "CI_COMMIT_TAG": "v19.4.1"}, writes: true},
		{name: "rc tag", vars: map[string]string{"CI_COMMIT_BRANCH": "", "CI_COMMIT_TAG": "v19.5.0-rc1"}, writes: true},
		{name: "web, schedule and api sources", vars: map[string]string{"CI_PIPELINE_SOURCE": "schedule"}, writes: true},
		{name: "without a provider, no token is needed", vars: map[string]string{"GOBUILDCACHE_ID_TOKEN": ""}, policy: func(c *setupConfig) { c.provider = "" }, writes: true},

		{name: "branch matching only a prefix", vars: map[string]string{"CI_COMMIT_BRANCH": "19-4-stable-evil"}},
		{name: "branch matching only a suffix", vars: map[string]string{"CI_COMMIT_BRANCH": "evil-19-4-stable"}},
		{name: "tag with a newline", vars: map[string]string{"CI_COMMIT_BRANCH": "", "CI_COMMIT_TAG": "v1.2.3\nmain"}},
		{name: "branch name as a tag", vars: map[string]string{"CI_COMMIT_BRANCH": "", "CI_COMMIT_TAG": "main"}},
		{name: "unprotected", vars: map[string]string{"CI_COMMIT_REF_PROTECTED": "false"}},
		{name: "merge request pipeline marked protected", vars: map[string]string{"CI_PIPELINE_SOURCE": "merge_request_event", "CI_COMMIT_BRANCH": ""}},
		{name: "trigger", vars: map[string]string{"CI_PIPELINE_SOURCE": "trigger"}},
		{name: "child pipeline", vars: map[string]string{"CI_PIPELINE_SOURCE": "parent_pipeline"}},
		{name: "multi-project pipeline", vars: map[string]string{"CI_PIPELINE_SOURCE": "pipeline"}},
		{name: "fork", vars: map[string]string{"CI_PROJECT_ID": "123"}},
		{name: "no ID token", vars: map[string]string{"GOBUILDCACHE_ID_TOKEN": ""}},
		{name: "no -write-project-id", policy: func(c *setupConfig) { c.writeProjectID = "" }},
		{name: "no -write-project-id or project", vars: map[string]string{"CI_PROJECT_ID": ""}, policy: func(c *setupConfig) { c.writeProjectID = "" }},
		{name: "no -write-branches", policy: func(c *setupConfig) { c.branches = nil }},
		{name: "no -write-tags", vars: map[string]string{"CI_COMMIT_BRANCH": "", "CI_COMMIT_TAG": "v19.4.1"}, policy: func(c *setupConfig) { c.tags = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := runnerPolicy(t)
			if tc.policy != nil {
				tc.policy(&c)
			}
			why := c.whyReadonly(gitlabVars(tc.vars))
			if got := why == ""; got != tc.writes {
				t.Errorf("writes = %v (%q), want %v", got, why, tc.writes)
			}
		})
	}
}

func testSetupEnv(t *testing.T, vars func(string) string) (setupEnv, *strings.Builder) {
	t.Helper()
	var log strings.Builder
	return setupEnv{
		getenv:    vars,
		exe:       "/opt/gobuildcache",
		goCommand: "go",
		tempDir:   t.TempDir(),
		now:       func() time.Time { return time.Unix(1700000000, 0) },
		logf: func(format string, args ...any) {
			fmt.Fprintf(&log, format+"\n", args...)
		},
	}, &log
}

func lookup(vars []variable, name string) (string, bool) {
	for _, v := range vars {
		if v.name == name {
			return v.value, true
		}
	}
	return "", false
}

func TestSetup_Writer(t *testing.T) {
	c := runnerPolicy(t)
	c.extraFlags = []string{"-stats"}
	c.deltaDir = filepath.Join(t.TempDir(), "delta")
	if err := os.MkdirAll(filepath.Join(c.deltaDir, "action"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deltaStartedFile(c.deltaDir), []byte("1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	e, _ := testSetupEnv(t, gitlabVars(nil))

	vars, err := c.run(e)
	if err != nil {
		t.Fatal(err)
	}

	credentials, ok := lookup(vars, "GOBUILDCACHE_CREDENTIALS")
	if !ok {
		t.Fatalf("no credentials in %v", vars)
	}
	if !strings.HasPrefix(credentials, e.tempDir) {
		t.Errorf("credentials %s aren't in the temp dir %s", credentials, e.tempDir)
	}
	prog, _ := lookup(vars, "GOCACHEPROG")
	if want := "/opt/gobuildcache -stats -p p/250833/ -env GOOGLE_APPLICATION_CREDENTIALS=GOBUILDCACHE_CREDENTIALS gs://bucket"; prog != want {
		t.Errorf("GOCACHEPROG = %q, want %q", prog, want)
	}

	data, err := os.ReadFile(credentials)
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Type             string `json:"type"`
		Audience         string `json:"audience"`
		CredentialSource struct {
			File string `json:"file"`
		} `json:"credential_source"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.Type != "external_account" || config.Audience != "//iam.googleapis.com/"+c.provider {
		t.Errorf("credentials config %s", data)
	}
	token, err := os.ReadFile(config.CredentialSource.File)
	if err != nil || string(token) != "header.payload.signature" {
		t.Errorf("token file = %q, %v", token, err)
	}
	if runtime.GOOS != "windows" {
		for path, want := range map[string]os.FileMode{filepath.Dir(credentials): 0o700, credentials: 0o600, config.CredentialSource.File: 0o600} {
			if info, err := os.Stat(path); err != nil || info.Mode().Perm() != want {
				t.Errorf("%s: mode %v, %v, want %v", path, info.Mode().Perm(), err, want)
			}
		}
	}

	for _, path := range []string{c.deltaDir, deltaStartedFile(c.deltaDir)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("a writer left %s: %v", path, err)
		}
	}
}

func TestSetup_Reader(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config func(*setupConfig, string)
		want   string
	}{
		{
			name: "anonymous",
			want: "/opt/gobuildcache -readonly -p p/250833/ gs://bucket?anonymous=true",
		},
		{
			name:   "read prefix",
			config: func(c *setupConfig, _ string) { c.readPrefix = "p/1/" },
			want:   "/opt/gobuildcache -readonly -p p/1/ gs://bucket?anonymous=true",
		},
		{
			name:   "bucket with parameters",
			config: func(c *setupConfig, _ string) { c.bucket = "s3://bucket?region=eu-west-1" },
			want:   "/opt/gobuildcache -readonly -p p/250833/ s3://bucket?anonymous=true&region=eu-west-1",
		},
		{
			name:   "authenticated reads",
			config: func(c *setupConfig, _ string) { c.anonymousReads = false },
			want:   "/opt/gobuildcache -readonly -p p/250833/ gs://bucket",
		},
		{
			name: "local cache dir and rerun tests",
			config: func(c *setupConfig, dir string) {
				c.cacheDir = filepath.Join(dir, "with space")
				c.rerunTests = true
			},
			want: "/opt/gobuildcache -dir '<cache dir>' -expire-others -readonly -p p/250833/ gs://bucket?anonymous=true",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.name == "local cache dir and rerun tests" {
				if _, err := exec.LookPath("go"); err != nil {
					t.Skip("needs go")
				}
				t.Setenv("GOCACHE", filepath.Join(dir, "gocache"))
			}
			c := runnerPolicy(t)
			if tc.config != nil {
				tc.config(&c, dir)
			}
			e, _ := testSetupEnv(t, gitlabVars(map[string]string{"CI_PIPELINE_SOURCE": "merge_request_event"}))

			vars, err := c.run(e)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := lookup(vars, "GOBUILDCACHE_CREDENTIALS"); ok {
				t.Errorf("a reader got credentials: %v", vars)
			}
			prog, _ := lookup(vars, "GOCACHEPROG")
			if want := strings.ReplaceAll(tc.want, "<cache dir>", filepath.Join(dir, "with space")); prog != want {
				t.Errorf("GOCACHEPROG = %q, want %q", prog, want)
			}
			if c.rerunTests {
				if _, err := os.Stat(filepath.Join(dir, "gocache", "testexpire.txt")); err != nil {
					t.Errorf("go clean -testcache didn't run: %v", err)
				}
			}
		})
	}
}

func TestSetup_ReaderDelta(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	c := runnerPolicy(t)
	c.deltaDir = filepath.Join(".tmp", "delta") + string(filepath.Separator)
	e, _ := testSetupEnv(t, gitlabVars(map[string]string{"CI_COMMIT_REF_PROTECTED": "false"}))

	vars, err := c.run(e)
	if err != nil {
		t.Fatal(err)
	}

	delta := filepath.Join(dir, ".tmp", "delta")
	prog, _ := lookup(vars, "GOCACHEPROG")
	if want := "/opt/gobuildcache -readonly -p p/250833/ -delta-dir " + delta + " gs://bucket?anonymous=true"; prog != want {
		t.Errorf("GOCACHEPROG = %q, want %q", prog, want)
	}
	started, err := os.ReadFile(filepath.Join(dir, ".tmp", "delta.started"))
	if err != nil || string(started) != "1700000000\n" {
		t.Errorf("started file = %q, %v", started, err)
	}
}

func TestSetup_Validate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config func(*setupConfig)
	}{
		{"no -ci", func(c *setupConfig) { c.ci = "" }},
		{"unknown -ci", func(c *setupConfig) { c.ci = "github" }},
		{"no bucket", func(c *setupConfig) { c.bucket = "" }},
		{"unknown shell", func(c *setupConfig) { c.shell = "cmd" }},
		{"credentials variable that isn't a name", func(c *setupConfig) { c.credentialsVar = "X; rm -rf /" }},
		{"ID token variable that isn't a name", func(c *setupConfig) { c.idTokenVar = "1X" }},
		{"delta with rerun tests", func(c *setupConfig) { c.deltaDir, c.rerunTests = "delta", true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := runnerPolicy(t)
			tc.config(&c)
			if err := c.validate(); err == nil {
				t.Error("no error")
			}
		})
	}
	if err := runnerPolicy(t).validate(); err != nil {
		t.Errorf("valid policy: %v", err)
	}
}

func TestSetupMain_Usage(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-ci", "gitlab"},
		{"-bucket", "gs://bucket"},
		{"-ci", "gitlab", "-bucket", "gs://bucket", "-write-branches", "("},
		{"-ci", "gitlab", "-bucket", "gs://bucket", "extra"},
	} {
		if code := setupMain(args); code != 2 {
			t.Errorf("setupMain(%q) = %d, want 2", args, code)
		}
	}
}

func TestSetup_ShellFormats(t *testing.T) {
	vars := []variable{
		{"PLAIN", "/opt/gobuildcache -readonly gs://bucket?anonymous=true"},
		{"TRICKY", `it's "$HOME" and $(echo no) and ` + "`no`"},
	}

	if runtime.GOOS != "windows" {
		out, err := exec.Command("sh", "-c", `eval "$1"; printf '%s\n%s' "$PLAIN" "$TRICKY"`, "sh", shellFormats["sh"](vars)).Output()
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Split(string(out), "\n"); !reflect.DeepEqual(got, []string{vars[0].value, vars[1].value}) {
			t.Errorf("sh read back %q", got)
		}
	}

	pwsh, err := exec.LookPath("pwsh")
	if err != nil {
		t.Log("no pwsh to check its format with")
		return
	}
	cmd := exec.Command(pwsh, "-NoProfile", "-NonInteractive", "-Command", "-")
	cmd.Stdin = strings.NewReader(shellFormats["pwsh"](vars) + "\nWrite-Output $env:PLAIN $env:TRICKY\n")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Split(strings.TrimSpace(strings.ReplaceAll(string(out), "\r", "")), "\n"); !reflect.DeepEqual(got, []string{vars[0].value, vars[1].value}) {
		t.Errorf("pwsh read back %q", got)
	}
}

func TestJoinFields(t *testing.T) {
	got, err := joinFields([]string{"/opt/my tools/gobuildcache", "-dir", "/it's here", "gs://bucket"})
	if want := `'/opt/my tools/gobuildcache' -dir "/it's here" gs://bucket`; got != want || err != nil {
		t.Errorf("joinFields = %s, %v, want %s", got, err, want)
	}
	if _, err := joinFields([]string{"/both ' \""}); err == nil {
		t.Error("no error for a field with a space and both quotes")
	}
}
