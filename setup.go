package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// setupMain runs "gobuildcache setup", which decides how a CI job uses the
// bucket, and prints the variables the job's shell sets; see setupConfig.
func setupMain(args []string) int {
	flags := flag.NewFlagSet("setup", flag.ContinueOnError)
	var c setupConfig
	var branches, tags, sources, extra string
	flags.StringVar(&c.ci, "ci", "", "the CI system whose predefined variables decide whether the job writes: gitlab")
	flags.StringVar(&c.bucket, "bucket", "", "the bucket URL")
	flags.StringVar(&c.prefix, "prefix", "", "the prefix writers write under, and readers read")
	flags.StringVar(&c.readPrefix, "read-prefix", "", "the prefix readers read (default: -prefix)")
	flags.StringVar(&c.writeProjectID, "write-project-id", "", "the project whose jobs may write; without it, none do")
	flags.StringVar(&branches, "write-branches", "", "a regular expression the whole name of a protected branch must match to write")
	flags.StringVar(&tags, "write-tags", "", "a regular expression the whole name of a protected tag must match to write")
	flags.StringVar(&sources, "write-sources", "push,web,schedule,api", "the pipeline sources that may write, separated by commas")
	flags.StringVar(&c.idTokenVar, "id-token-var", "GOBUILDCACHE_ID_TOKEN", "the variable holding the job's ID token, which writers need with -gcp-workload-identity-provider")
	flags.StringVar(&c.provider, "gcp-workload-identity-provider", "", "the Workload Identity Federation provider writers exchange their ID token through, as projects/<number>/locations/global/workloadIdentityPools/<pool>/providers/<provider>")
	flags.StringVar(&c.credentialsVar, "credentials-var", "GOBUILDCACHE_CREDENTIALS", "the variable to export the path of writers' credentials config in")
	flags.BoolVar(&c.anonymousReads, "anonymous-reads", true, "open the bucket with ?anonymous=true when reading only")
	flags.StringVar(&c.deltaDir, "delta-dir", "", "keep the puts of jobs that don't write in this directory, for a CI cache to save; can't be used with -rerun-tests")
	flags.BoolVar(&c.rerunTests, "rerun-tests", false, "rerun every test rather than reuse cached results: run go clean -testcache, and pass -expire-others")
	flags.StringVar(&c.cacheDir, "dir", "", "the local cache directory")
	flags.StringVar(&extra, "flags", "", "more flags for GOCACHEPROG, separated by spaces, such as -stats")
	flags.StringVar(&c.shell, "shell", "sh", "the syntax to print the variables in: sh or pwsh")
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "%s setup -ci gitlab -bucket <url> [flags]\n", os.Args[0])
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		flags.Usage()
		return 2
	}
	logf := newSetupLogger(os.Stderr)

	var err error
	if c.branches, err = wholeMatch(branches); err != nil {
		logf("-write-branches: %v", err)
		return 2
	}
	if c.tags, err = wholeMatch(tags); err != nil {
		logf("-write-tags: %v", err)
		return 2
	}
	for _, source := range strings.Split(sources, ",") {
		if source = strings.TrimSpace(source); source != "" {
			c.sources = append(c.sources, source)
		}
	}
	c.extraFlags = strings.Fields(extra)
	if err := c.validate(); err != nil {
		logf("%v", err)
		flags.Usage()
		return 2
	}

	exe, err := os.Executable()
	if err != nil {
		logf("%v", err)
		return 1
	}
	vars, err := c.run(setupEnv{
		getenv:    os.Getenv,
		exe:       exe,
		goCommand: "go",
		tempDir:   os.TempDir(),
		now:       time.Now,
		logf:      logf,
	})
	if err != nil {
		logf("%v", err)
		return 1
	}
	if _, err := io.WriteString(originalStdout, shellFormats[c.shell](vars)); err != nil {
		logf("%v", err)
		return 1
	}
	return 0
}

func newSetupLogger(w io.Writer) func(string, ...any) {
	return func(format string, args ...any) {
		fmt.Fprintf(w, "gobuildcache setup: "+format+"\n", args...)
	}
}

// setupConfig is a project's policy for its CI jobs: who writes to the
// bucket, where entries go, and which jobs keep a delta or rerun tests.
//
// Only jobs the bucket's identity provider accepts can write, so the policy
// must say exactly what the provider's condition does: a job the provider
// rejects can't authenticate at all, losing its reads too, and one it would
// accept but the policy doesn't only reads. Every other job reads, without
// credentials unless -anonymous-reads=false, and keeps what it puts locally.
// A pipeline that overrides the variables setup reads only makes itself try
// to write, and fail when the provider checks its token's claims.
type setupConfig struct {
	ci                   string
	bucket               string
	prefix, readPrefix   string
	writeProjectID       string
	branches, tags       *regexp.Regexp
	sources              []string
	idTokenVar, provider string
	credentialsVar       string
	anonymousReads       bool
	deltaDir             string
	rerunTests           bool
	cacheDir             string
	extraFlags           []string
	shell                string
}

// setupEnv is what setup needs from the job.
type setupEnv struct {
	getenv func(string) string
	// exe is the gobuildcache binary GOCACHEPROG runs: the one running
	// setup, which the job checked.
	exe       string
	goCommand string
	// tempDir is where writers' credentials go, outside the project
	// directory, so that no CI cache or artifact picks them up.
	tempDir string
	now     func() time.Time
	logf    func(string, ...any)
}

// variable is one the job's shell sets from setup's output.
type variable struct {
	name, value string
}

func (c setupConfig) validate() error {
	switch {
	case c.ci != "gitlab":
		return errors.New("-ci must be gitlab")
	case c.bucket == "":
		return errors.New("-bucket is required")
	case shellFormats[c.shell] == nil:
		return fmt.Errorf("-shell must be sh or pwsh, not %q", c.shell)
	case !variableName.MatchString(c.credentialsVar):
		return fmt.Errorf("-credentials-var %q isn't a variable name", c.credentialsVar)
	case !variableName.MatchString(c.idTokenVar):
		return fmt.Errorf("-id-token-var %q isn't a variable name", c.idTokenVar)
	case c.rerunTests && c.deltaDir != "":
		return errors.New("-delta-dir can't be used with -rerun-tests: a job that reruns tests mustn't trust what a delta's CI cache can hold")
	}
	return nil
}

// variableName is what setup prints variable names as: both shells take
// them, and none needs quoting.
var variableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// wholeMatch compiles a regular expression that only matches a whole name,
// or nil for none.
func wholeMatch(expr string) (*regexp.Regexp, error) {
	if expr == "" {
		return nil, nil
	}
	return regexp.Compile(`^(?:` + expr + `)$`)
}

// matches reports whether re matches the whole value. A value with a newline
// never does, so a ref name can't smuggle in a line that would.
func matches(re *regexp.Regexp, value string) bool {
	return re != nil && value != "" && !strings.Contains(value, "\n") && re.MatchString(value)
}

// whyReadonly says why a GitLab job doesn't write, or "" when it does.
// Pipeline sources other than the writers' run code or variables a ref's
// protection doesn't vouch for: merge request pipelines can be marked
// protected, trigger pipelines take any variables, and child and
// multi-project pipelines carry another pipeline's ref.
func (c setupConfig) whyReadonly(getenv func(string) string) string {
	branch, tag := getenv("CI_COMMIT_BRANCH"), getenv("CI_COMMIT_TAG")
	switch {
	case c.writeProjectID == "":
		return "there's no -write-project-id"
	case getenv("CI_PROJECT_ID") != c.writeProjectID:
		return fmt.Sprintf("project %q isn't -write-project-id", getenv("CI_PROJECT_ID"))
	case getenv("CI_COMMIT_REF_PROTECTED") != "true":
		return "the ref isn't protected"
	case !slices.Contains(c.sources, getenv("CI_PIPELINE_SOURCE")):
		return fmt.Sprintf("pipeline source %q isn't in -write-sources", getenv("CI_PIPELINE_SOURCE"))
	case !matches(c.branches, branch) && !matches(c.tags, tag):
		if tag != "" {
			return fmt.Sprintf("tag %q doesn't match -write-tags", tag)
		}
		return fmt.Sprintf("branch %q doesn't match -write-branches", branch)
	case c.provider != "" && getenv(c.idTokenVar) == "":
		return fmt.Sprintf("there's no ID token in %s", c.idTokenVar)
	}
	return ""
}

// run does what the job's use of the bucket needs, and returns the variables
// the job sets.
func (c setupConfig) run(e setupEnv) ([]variable, error) {
	var started, deltaDir string
	if c.deltaDir != "" {
		var err error
		if deltaDir, err = filepath.Abs(c.deltaDir); err != nil {
			return nil, err
		}
		started = deltaStartedFile(deltaDir)
		if err := os.Remove(started); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}

	prog := append([]string{e.exe}, c.extraFlags...)
	if c.cacheDir != "" {
		dir, err := filepath.Abs(c.cacheDir)
		if err != nil {
			return nil, err
		}
		prog = append(prog, "-dir", dir)
	}
	if c.rerunTests {
		prog = append(prog, "-expire-others")
	}

	var vars []variable
	if why := c.whyReadonly(e.getenv); why == "" {
		// a writer never uses a delta, in case a CI cache restored one
		if deltaDir != "" {
			if err := os.RemoveAll(deltaDir); err != nil {
				return nil, err
			}
		}
		if c.prefix != "" {
			prog = append(prog, "-p", c.prefix)
		}
		if c.provider != "" {
			credentials, err := c.writeCredentials(e)
			if err != nil {
				return nil, err
			}
			vars = append(vars, variable{c.credentialsVar, credentials})
			// test runners that only pass some variables to go test, and
			// so to GOCACHEPROG, need to pass this one
			prog = append(prog, "-env", "GOOGLE_APPLICATION_CREDENTIALS="+c.credentialsVar)
		}
		prog = append(prog, c.bucket)
		e.logf("using %s read-write, under %q", c.bucket, c.prefix)
	} else {
		prefix := c.readPrefix
		if prefix == "" {
			prefix = c.prefix
		}
		prog = append(prog, "-readonly")
		if prefix != "" {
			prog = append(prog, "-p", prefix)
		}
		if deltaDir != "" {
			// prune keeps what the job used since, by this machine's clock
			if err := os.MkdirAll(filepath.Dir(started), 0o777); err != nil {
				return nil, err
			}
			if err := os.WriteFile(started, []byte(strconv.FormatInt(e.now().Unix(), 10)+"\n"), 0o666); err != nil {
				return nil, err
			}
			prog = append(prog, "-delta-dir", deltaDir)
		}
		bucket, err := c.readURL()
		if err != nil {
			return nil, err
		}
		prog = append(prog, bucket)
		e.logf("using %s readonly, under %q, because %s", c.bucket, prefix, why)
		if deltaDir != "" {
			e.logf("keeping this job's puts in %s", deltaDir)
		}
	}
	joined, err := joinFields(prog)
	if err != nil {
		return nil, err
	}
	vars = append(vars, variable{"GOCACHEPROG", joined})

	if c.rerunTests {
		if err := c.expireTestResults(e, vars); err != nil {
			return nil, err
		}
	}
	return vars, nil
}

// deltaStartedFile holds when the job started using the delta, for prune.
// It's next to the delta rather than in it, so that a CI cache saving the
// delta doesn't save it.
func deltaStartedFile(deltaDir string) string {
	return filepath.Clean(deltaDir) + ".started"
}

// readURL is the bucket URL for readers.
func (c setupConfig) readURL() (string, error) {
	if !c.anonymousReads {
		return c.bucket, nil
	}
	u, err := url.Parse(c.bucket)
	if err != nil {
		return "", fmt.Errorf("-bucket: %w", err)
	}
	q := u.Query()
	q.Set("anonymous", "true")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// writeCredentials writes the ID token, and a credentials config for
// exchanging it through Workload Identity Federation, to a new directory only
// the job's user can read, and returns the config's path. A new directory,
// since on a machine running other users' jobs, one with a fixed name could
// be someone else's.
func (c setupConfig) writeCredentials(e setupEnv) (string, error) {
	dir, err := os.MkdirTemp(e.tempDir, "gobuildcache-auth-")
	if err != nil {
		return "", err
	}
	token := filepath.Join(dir, "id_token")
	if err := os.WriteFile(token, []byte(e.getenv(c.idTokenVar)), 0o600); err != nil {
		return "", err
	}

	config, err := json.MarshalIndent(map[string]any{
		"type":               "external_account",
		"audience":           "//iam.googleapis.com/" + c.provider,
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          "https://sts.googleapis.com/v1/token",
		"credential_source":  map[string]string{"file": token},
	}, "", "  ")
	if err != nil {
		return "", err
	}
	credentials := filepath.Join(dir, "credentials.json")
	return credentials, os.WriteFile(credentials, append(config, '\n'), 0o600)
}

// expireTestResults runs go clean -testcache with the variables the job
// sets, as its own go commands run. It creates GOCACHE first, as go clean
// -testcache silently does nothing without it. If it fails, so does setup,
// rather than let the job reuse results it shouldn't.
func (c setupConfig) expireTestResults(e setupEnv, vars []variable) error {
	env := os.Environ()
	for _, v := range vars {
		env = append(env, v.name+"="+v.value)
	}

	cmd := exec.Command(e.goCommand, "env", "GOCACHE")
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("go env GOCACHE: %w", err)
	}
	gocache := strings.TrimSpace(string(out))
	if gocache == "" || gocache == "off" {
		return fmt.Errorf("go env GOCACHE is %q, so go clean -testcache can't expire test results", gocache)
	}
	if err := os.MkdirAll(gocache, 0o777); err != nil {
		return err
	}

	cmd = exec.Command(e.goCommand, "clean", "-testcache")
	cmd.Env = env
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go clean -testcache: %w: %s", err, strings.TrimSpace(string(out)))
	}
	e.logf("expired cached test results, so every test reruns")
	return nil
}

// joinFields joins GOCACHEPROG's fields, which the go command splits at
// spaces, quoting any field with a space in it with whichever quote it
// doesn't contain. It has no escapes, so a field with a space and both
// quotes can't be passed.
func joinFields(fields []string) (string, error) {
	quoted := make([]string, len(fields))
	for i, field := range fields {
		switch {
		case !strings.ContainsAny(field, " \t\n\r"):
			quoted[i] = field
		case !strings.Contains(field, "'"):
			quoted[i] = "'" + field + "'"
		case !strings.Contains(field, `"`):
			quoted[i] = `"` + field + `"`
		default:
			return "", fmt.Errorf("GOCACHEPROG can't hold %q, which has a space and both quotes", field)
		}
	}
	return strings.Join(quoted, " "), nil
}

// shellFormats print the variables in a shell's syntax, with values in
// single quotes, which neither shell expands anything in.
var shellFormats = map[string]func([]variable) string{
	"sh": func(vars []variable) string {
		var b strings.Builder
		for _, v := range vars {
			fmt.Fprintf(&b, "export %s='%s'\n", v.name, strings.ReplaceAll(v.value, "'", `'\''`))
		}
		return b.String()
	},
	"pwsh": func(vars []variable) string {
		var b strings.Builder
		for _, v := range vars {
			fmt.Fprintf(&b, "$env:%s = '%s'\n", v.name, strings.ReplaceAll(v.value, "'", "''"))
		}
		return b.String()
	},
}
