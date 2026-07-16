package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeInstallSource lays out the minimal hev/layer checkout shape the command
// looks for, with an install script that prints the environment it received.
func fakeInstallSource(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{
		filepath.Join(root, "infra", "terraform"),
		filepath.Join(root, "infra", "helm", "layer"),
		filepath.Join(root, "scripts"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	script := "#!/usr/bin/env bash\n" +
		`echo "REGION=$AWS_REGION NODE=$SYSTEM_NODE_INSTANCE_TYPE KEY=$TURBOPUFFER_API_KEY PROFILE=${AWS_PROFILE:-} CLUSTER=$CLUSTER_NAME NS=$NAMESPACE VERSION=$LAYER_VERSION"` + "\n"
	if err := os.WriteFile(filepath.Join(root, "scripts", "install-layer.sh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestInstallAWSDryRun(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{},
		HomeDir: t.TempDir(),
		Version: "test",
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws",
		"--source", source,
		"--turbopuffer-api-key", "tpuf_secret_value_123",
		"--dry-run",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{
		"AWS_REGION=us-east-1",
		"CLUSTER_NAME=layer",
		"SYSTEM_NODE_INSTANCE_TYPE=" + defaultSystemNodeType,
		"install-layer.sh",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("dry-run output missing %q: %s", want, stdout)
		}
	}
	if strings.Contains(stdout, "tpuf_secret_value_123") {
		t.Fatalf("dry-run output leaks the API key: %s", stdout)
	}
	if !strings.Contains(stderr, "Install plan") {
		t.Fatalf("plan summary missing from stderr: %s", stderr)
	}
}

func TestInstallAWSWizardRunsScript(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		// Wizard answers: region (default), node type, Turbopuffer key,
		// cluster name (default), then the confirmation.
		Stdin:            strings.NewReader("\ni4i.xlarge\ntpuf_key\n\ny\n"),
		Env:              map[string]string{},
		HomeDir:          t.TempDir(),
		StdinIsTerminal:  true,
		StdoutIsTerminal: true,
		Version:          "test",
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws",
		"--source", source,
		"--profile", "demo",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	want := "REGION=us-east-1 NODE=i4i.xlarge KEY=tpuf_key PROFILE=demo CLUSTER=layer NS=layer VERSION=latest"
	if !strings.Contains(stdout, want) {
		t.Fatalf("install script env = %q, want %q", stdout, want)
	}
	if !strings.Contains(stderr, "Base node instance type") {
		t.Fatalf("node-type prompt missing from stderr: %s", stderr)
	}
}

func TestInstallAWSWizardAbortsWithoutConfirmation(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin:            strings.NewReader("\n\ntpuf_key\n\nn\n"),
		Env:              map[string]string{},
		HomeDir:          t.TempDir(),
		StdinIsTerminal:  true,
		StdoutIsTerminal: true,
		Version:          "test",
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws", "--source", source, "--profile", "demo",
	})
	if code != ExitFailed {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if strings.Contains(stdout, "REGION=") {
		t.Fatalf("install script ran after aborted confirmation: %s", stdout)
	}
	if !strings.Contains(stderr, "install aborted") {
		t.Fatalf("abort message missing: %s", stderr)
	}
}

func TestInstallAWSNonInteractiveRequiresYes(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{},
		HomeDir: t.TempDir(),
		Version: "test",
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws", "--source", source, "--turbopuffer-api-key", "tpuf_key",
	})
	if code != ExitUsage {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "--yes") {
		t.Fatalf("expected --yes hint, got: %s", stderr)
	}
}

func TestInstallAWSNonInteractiveYesRunsScript(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{"TURBOPUFFER_API_KEY": "tpuf_env_key"},
		HomeDir: t.TempDir(),
		Version: "test",
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws",
		"--source", source,
		"--node-type", "i4i.2xlarge",
		"--region", "us-west-2",
		"--yes",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	want := "REGION=us-west-2 NODE=i4i.2xlarge KEY=tpuf_env_key"
	if !strings.Contains(stdout, want) {
		t.Fatalf("install script env = %q, want prefix %q", stdout, want)
	}
}

func TestInstallAWSMissingKeyNonInteractive(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{},
		HomeDir: t.TempDir(),
		Version: "test",
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws", "--source", source, "--yes",
	})
	if code != ExitUsage {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "TURBOPUFFER_API_KEY") {
		t.Fatalf("expected Turbopuffer key guidance, got: %s", stderr)
	}
}

func TestInstallAWSBadSource(t *testing.T) {
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{},
		HomeDir: t.TempDir(),
		Version: "test",
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "aws", "--source", t.TempDir(), "--turbopuffer-api-key", "k", "--yes",
	})
	if code != ExitUsage {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "git clone https://github.com/hev/layer") {
		t.Fatalf("expected clone guidance, got: %s", stderr)
	}
}
