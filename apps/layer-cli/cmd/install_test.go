package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	for name, contents := range map[string]string{
		"infra/helm/layer/Chart.yaml":           "apiVersion: v2\nname: layer\nversion: 0.1.0\n",
		"infra/helm/layer/values-demo.yaml":     "documentCache:\n  storage:\n    pvc:\n      enabled: true\n",
		"infra/helm/layer/values-indexing.yaml": "documentCache:\n  karpenter:\n    enabled: true\n",
		"scripts/deploy-lb-controller.sh":       "#!/bin/sh\nexit 0\n",
		"scripts/deploy-karpenter.sh":           "#!/bin/sh\nexit 0\n",
	} {
		mode := os.FileMode(0o644)
		if strings.HasSuffix(name, ".sh") {
			mode = 0o755
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), mode); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func fakeInstallTools(t *testing.T) (path, logPath, valuesPath string) {
	t.Helper()
	bin := t.TempDir()
	logPath = filepath.Join(t.TempDir(), "commands.log")
	valuesPath = filepath.Join(t.TempDir(), "values.yaml")
	tools := map[string]string{
		"aws":     "#!/bin/sh\necho aws \"$@\" >> \"$INSTALL_TEST_LOG\"\n",
		"kubectl": "#!/bin/sh\necho kubectl \"$@\" >> \"$INSTALL_TEST_LOG\"\n",
		"helm": `#!/bin/sh
echo helm "$@" >> "$INSTALL_TEST_LOG"
previous=""
for argument in "$@"; do
  if [ "$previous" = "-f" ]; then values="$argument"; fi
  previous="$argument"
done
if [ -n "${values:-}" ] && [ -f "$values" ]; then cp "$values" "$INSTALL_TEST_VALUES"; fi
`,
		"terraform": `#!/bin/sh
echo terraform "$@" >> "$INSTALL_TEST_LOG"
previous=""
for argument in "$@"; do
  if [ "$previous" = "-raw" ]; then
    case "$argument" in
      layer_s3_bucket_name) echo bucket ;;
      cluster_name) echo test-cluster ;;
      layer_namespace) echo test-namespace ;;
      layer_gateway_role_arn) echo gateway-role ;;
      layer_service_account_name) echo gateway-sa ;;
      layer_dashboard_role_arn) echo dashboard-role ;;
      layer_dashboard_service_account_name) echo dashboard-sa ;;
      karpenter_node_instance_profile_name) echo node-profile ;;
    esac
  fi
  previous="$argument"
done
`,
	}
	for name, script := range tools {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return bin + string(os.PathListSeparator) + os.Getenv("PATH"), logPath, valuesPath
}

func TestInstallDryRunDefaultsToDemo(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "--source", source, "--turbopuffer-api-key", "tpuf_secret_value_123", "--dry-run",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{
		"values-demo.yaml",
		"system_node_instance_type=m8g.large",
		"system_node_data_volume_size=80",
		"helm upgrade --install layer",
	} {
		if !strings.Contains(stdout, want) && !strings.Contains(stderr, want) {
			t.Fatalf("dry-run output missing %q\nstdout=%s\nstderr=%s", want, stdout, stderr)
		}
	}
	if strings.Contains(stdout+stderr, "tpuf_secret_value_123") {
		t.Fatalf("dry-run output leaks API key: %s%s", stdout, stderr)
	}
}

func TestInstallDryRunResolvesIndexingOverlay(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "--source", source, "--profile", "indexing", "--aws-profile", "sandbox", "--turbopuffer-api-key", "key", "--dry-run",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "values-indexing.yaml") || !strings.Contains(stderr, "AWS profile       sandbox") {
		t.Fatalf("profile resolution missing\nstdout=%s\nstderr=%s", stdout, stderr)
	}
}

func TestInstallRejectsUnknownProfile(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	_, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "--source", source, "--profile", "production", "--turbopuffer-api-key", "key", "--dry-run",
	})
	if code != ExitUsage || !strings.Contains(stderr, "demo or indexing") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestInstallEnvCompatibility(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin: strings.NewReader(""),
		Env: map[string]string{
			"AWS_REGION": "us-west-2", "CLUSTER_NAME": "env-cluster", "NAMESPACE": "env-ns",
			"HELM_RELEASE": "env-release", "LAYER_VERSION": "v1.2.3", "TURBOPUFFER_API_KEY": "env-key",
			"SYSTEM_NODE_INSTANCE_TYPE": "m8g.xlarge", "AWS_PROFILE": "disposable",
		},
		HomeDir: t.TempDir(), Version: "test",
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", source, "--dry-run"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	for _, want := range []string{"region=us-west-2", "cluster_name=env-cluster", "kubernetes_namespace=env-ns", "system_node_instance_type=m8g.xlarge", "helm upgrade --install env-release"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout missing %q: %s", want, stdout)
		}
	}
}

func TestInstallNonInteractiveRequiresYes(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", source, "--turbopuffer-api-key", "key"})
	if code != ExitUsage || !strings.Contains(stderr, "--yes") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestInstallNonInteractiveRequiresTurbopufferKey(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", source, "--yes"})
	if code != ExitUsage || !strings.Contains(stderr, "TURBOPUFFER_API_KEY") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestInstallRejectsBadSource(t *testing.T) {
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", t.TempDir(), "--turbopuffer-api-key", "key", "--dry-run"})
	if code != ExitUsage || !strings.Contains(stderr, "git clone https://github.com/hev/layer") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestInstallRejectsInvalidSkipTerraformEnv(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{"SKIP_TERRAFORM": "yes"}, HomeDir: t.TempDir(), Version: "test"}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", source, "--turbopuffer-api-key", "key", "--dry-run"})
	if code != ExitUsage || !strings.Contains(stderr, "must be 0 or 1") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestInstallWizardPromptsForMissingSecret(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{
		Stdin: strings.NewReader("\ntpuf-key\nn\n"), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test",
		StdinIsTerminal: true, StdoutIsTerminal: true,
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", source})
	if code != ExitFailed || !strings.Contains(stderr, "Turbopuffer API key") || !strings.Contains(stderr, "install aborted") {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
}

func TestInstallSourceDoesNotDependOnLegacyInstaller(t *testing.T) {
	source := fakeInstallSource(t)
	if !isInstallSource(source) {
		t.Fatal("deployment tree without scripts/install-layer.sh must be accepted")
	}
}

func TestRenderInstallValues(t *testing.T) {
	values, err := renderInstallValues(installOptions{
		region: "us-east-2", tpufKey: "tpuf", layerVersion: "v1.2.3", dashboardUser: "operator", licenseToken: "licensed",
	}, "password", map[string]string{
		"cluster_name": "cluster", "layer_s3_bucket_name": "bucket", "layer_service_account_name": "gateway-sa",
		"layer_gateway_role_arn": "gateway-role", "layer_dashboard_service_account_name": "dashboard-sa",
		"layer_dashboard_role_arn": "dashboard-role", "karpenter_node_instance_profile_name": "nodes",
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(values)
	for _, want := range []string{"apiKey: tpuf", "region: aws-us-east-2", "layer-gateway-pro:v1.2.3", "token: licensed", "instanceProfile: nodes", "storage"} {
		if want == "storage" {
			if strings.Contains(text, want) {
				t.Fatalf("generated values must leave profile-owned storage settings to the overlay: %s", text)
			}
			continue
		}
		if !strings.Contains(text, want) {
			t.Fatalf("rendered values missing %q: %s", want, text)
		}
	}
}

func TestInstallExecutesTerraformProfileOverlayAndHelm(t *testing.T) {
	source := fakeInstallSource(t)
	path, logPath, valuesPath := fakeInstallTools(t)
	t.Setenv("PATH", path)
	opts := Options{
		Stdin: strings.NewReader(""),
		Env: map[string]string{
			"PATH": path, "INSTALL_TEST_LOG": logPath, "INSTALL_TEST_VALUES": valuesPath,
			"TURBOPUFFER_API_KEY": "tpuf-integration", "DASHBOARD_PASSWORD": "dashboard-secret",
		},
		HomeDir: t.TempDir(), Version: "test",
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "--source", source, "--profile", "indexing", "--yes"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	commandText := string(commands)
	for _, want := range []string{
		"aws sts get-caller-identity",
		"terraform -chdir=" + filepath.Join(source, "infra", "terraform") + " apply",
		"system_node_instance_type=m8g.large",
		"system_node_data_volume_size=80",
		"values-indexing.yaml",
		"kubectl -n test-namespace rollout status",
	} {
		if !strings.Contains(commandText, want) {
			t.Fatalf("command log missing %q: %s", want, commandText)
		}
	}
	values, err := os.ReadFile(valuesPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(values), "apiKey: tpuf-integration") || !strings.Contains(string(values), "password: dashboard-secret") {
		t.Fatalf("generated Helm values incomplete: %s", values)
	}
}

func TestInstallStatusReportsHealthAndCacheUtilization(t *testing.T) {
	path, logPath, valuesPath := fakeInstallTools(t)
	t.Setenv("PATH", path)
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{"PATH": path, "INSTALL_TEST_LOG": logPath, "INSTALL_TEST_VALUES": valuesPath},
		HomeDir: t.TempDir(), Version: "test",
	}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{"install", "status", "--cluster-name", "status-cluster"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "Health: healthy") || !strings.Contains(stdout, "Workload health") || !strings.Contains(stdout, "Document-cache node utilization") {
		t.Fatalf("status headings missing: %s", stdout)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"aws eks update-kubeconfig --region us-east-1 --name status-cluster", "helm status layer", "kubectl top nodes -l layer.hev.dev/node-role=document-cache"} {
		if !strings.Contains(string(commands), want) {
			t.Fatalf("status command log missing %q: %s", want, commands)
		}
	}
}

func TestInstallUninstallDryRun(t *testing.T) {
	source := fakeInstallSource(t)
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	stdout, stderr, code := runCLIWithOptions(t, opts, []string{
		"install", "uninstall", "--source", source, "--profile", "indexing", "--dry-run",
	})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "helm uninstall layer") || !strings.Contains(stdout, "terraform") || strings.Contains(stdout, "install-layer.sh") {
		t.Fatalf("unexpected teardown plan: %s", stdout)
	}
}

func TestInstallUninstallRemovesComponentsThenTerraform(t *testing.T) {
	source := fakeInstallSource(t)
	path, logPath, valuesPath := fakeInstallTools(t)
	t.Setenv("PATH", path)
	opts := Options{
		Stdin:   strings.NewReader(""),
		Env:     map[string]string{"PATH": path, "INSTALL_TEST_LOG": logPath, "INSTALL_TEST_VALUES": valuesPath},
		HomeDir: t.TempDir(), Version: "test",
	}
	_, stderr, code := runCLIWithOptions(t, opts, []string{"install", "uninstall", "--source", source, "--yes"})
	if code != ExitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr)
	}
	commands, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(commands)
	for _, want := range []string{
		"helm uninstall layer --namespace layer --ignore-not-found --wait",
		"helm uninstall karpenter --namespace kube-system --ignore-not-found --wait",
		"kubectl delete namespace layer --ignore-not-found --wait=true",
		"terraform -chdir=" + filepath.Join(source, "infra", "terraform") + " destroy -auto-approve",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("teardown command log missing %q: %s", want, text)
		}
	}
	if strings.Index(text, "helm uninstall layer") > strings.Index(text, "terraform ") {
		t.Fatalf("Terraform destroy ran before Helm teardown: %s", text)
	}
}

func TestInstallHelpSurfaces(t *testing.T) {
	opts := Options{Stdin: strings.NewReader(""), Env: map[string]string{}, HomeDir: t.TempDir(), Version: "test"}
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"install", "--help"}, "--profile"},
		{[]string{"install", "status", "--help"}, "workload health"},
		{[]string{"install", "uninstall", "--help"}, "--skip-terraform"},
	}
	for _, tc := range cases {
		args := tc.args
		stdout, stderr, code := runCLIWithOptions(t, opts, args)
		if code != ExitOK {
			t.Fatalf("args=%v code=%d stderr=%s", args, code, stderr)
		}
		if !strings.Contains(stdout, tc.want) {
			t.Fatalf("args=%v help missing %q: %s", args, tc.want, stdout)
		}
	}
}
