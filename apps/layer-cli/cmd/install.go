package cmd

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

const installSourceHelp = `layer install runs from a hev/layer deployment source checkout.

Clone the repository and run the command from inside it:

  git clone https://github.com/hev/layer
  cd layer
  layer install

Or point --source (or LAYER_SRC) at an existing checkout or onboarding artifact.`

const (
	defaultInstallProfile = "demo"
	defaultSystemNodeType = "m8g.large"
	defaultSystemDataGiB  = 80
)

type installOptions struct {
	source            string
	profile           string
	awsProfile        string
	region            string
	clusterName       string
	namespace         string
	helmRelease       string
	nodeType          string
	tpufKey           string
	layerVersion      string
	licenseToken      string
	dashboardUser     string
	dashboardPassword string
	skipTerraform     bool
	assumeYes         bool
	dryRun            bool
}

type installProfile struct {
	name                string
	overlay             string
	systemNodeType      string
	systemDataVolumeGiB int
}

func resolveInstallProfile(name, chartDir string) (installProfile, error) {
	if name == "" {
		name = defaultInstallProfile
	}
	if name != "demo" && name != "indexing" {
		return installProfile{}, cliError{message: fmt.Sprintf("invalid install profile %q: must be demo or indexing", name), code: ExitUsage}
	}
	return installProfile{
		name:                name,
		overlay:             filepath.Join(chartDir, "values-"+name+".yaml"),
		systemNodeType:      defaultSystemNodeType,
		systemDataVolumeGiB: defaultSystemDataGiB,
	}, nil
}

func newInstallCommand(app App, flags *globalFlags) *cobra.Command {
	opts := installOptions{}
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Provision AWS and install Layer",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.runInstall(cmd, opts)
		},
	}
	addInstallFlags(cmd, &opts, true)
	cmd.AddCommand(newInstallStatusCommand(app), newInstallUninstallCommand(app))
	return cmd
}

func addInstallFlags(cmd *cobra.Command, opts *installOptions, includeSecrets bool) {
	cmd.Flags().StringVar(&opts.source, "source", "", "Path to deployment source (env LAYER_SRC; default: walk up from working directory)")
	cmd.Flags().StringVar(&opts.profile, "profile", "", "Install profile: demo or indexing (default demo)")
	cmd.Flags().StringVar(&opts.awsProfile, "aws-profile", "", "AWS credentials profile (env AWS_PROFILE)")
	cmd.Flags().StringVar(&opts.region, "region", "", "AWS region (env AWS_REGION; default us-east-1)")
	cmd.Flags().StringVar(&opts.clusterName, "cluster-name", "", "EKS cluster name (env CLUSTER_NAME; default layer)")
	cmd.Flags().StringVar(&opts.namespace, "namespace", "", "Kubernetes namespace (env NAMESPACE; default layer)")
	cmd.Flags().StringVar(&opts.helmRelease, "helm-release", "", "Helm release name (env HELM_RELEASE; default layer)")
	cmd.Flags().StringVar(&opts.nodeType, "node-type", "", "System node instance type (env SYSTEM_NODE_INSTANCE_TYPE; profile default m8g.large)")
	cmd.Flags().StringVar(&opts.layerVersion, "version", "", "Layer image tag (env LAYER_VERSION; default latest)")
	cmd.Flags().BoolVar(&opts.skipTerraform, "skip-terraform", false, "Reuse existing Terraform outputs; install Helm components only")
	cmd.Flags().BoolVarP(&opts.assumeYes, "yes", "y", false, "Skip the confirmation prompt")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Print the resolved plan and commands without changing anything")
	if includeSecrets {
		cmd.Flags().StringVar(&opts.tpufKey, "turbopuffer-api-key", "", "Upstream store credential (env TURBOPUFFER_API_KEY)")
		cmd.Flags().StringVar(&opts.licenseToken, "license-token", "", "Signed Layer license key (env LICENSE_TOKEN)")
		cmd.Flags().StringVar(&opts.dashboardUser, "dashboard-user", "", "Dashboard Basic Auth user (env DASHBOARD_USER; default admin)")
		cmd.Flags().StringVar(&opts.dashboardPassword, "dashboard-password", "", "Dashboard Basic Auth password (env DASHBOARD_PASSWORD; default generated)")
	}
}

func (app App) applyInstallDefaults(opts *installOptions) {
	env := app.opts.Env
	stringEnvDefault(&opts.profile, env, "LAYER_INSTALL_PROFILE", defaultInstallProfile)
	stringEnvDefault(&opts.awsProfile, env, "AWS_PROFILE", "")
	stringEnvDefault(&opts.region, env, "AWS_REGION", "us-east-1")
	stringEnvDefault(&opts.clusterName, env, "CLUSTER_NAME", "layer")
	stringEnvDefault(&opts.namespace, env, "NAMESPACE", "layer")
	stringEnvDefault(&opts.helmRelease, env, "HELM_RELEASE", "layer")
	stringEnvDefault(&opts.nodeType, env, "SYSTEM_NODE_INSTANCE_TYPE", "")
	stringEnvDefault(&opts.layerVersion, env, "LAYER_VERSION", "latest")
	stringEnvDefault(&opts.tpufKey, env, "TURBOPUFFER_API_KEY", "")
	stringEnvDefault(&opts.licenseToken, env, "LICENSE_TOKEN", "")
	stringEnvDefault(&opts.dashboardUser, env, "DASHBOARD_USER", "admin")
	stringEnvDefault(&opts.dashboardPassword, env, "DASHBOARD_PASSWORD", "")
	if !opts.skipTerraform {
		switch strings.TrimSpace(env["SKIP_TERRAFORM"]) {
		case "", "0":
		case "1":
			opts.skipTerraform = true
		default:
			// Validation is performed by validateSkipTerraform so callers get a usage error.
		}
	}
}

func stringEnvDefault(dst *string, env map[string]string, key, fallback string) {
	if *dst != "" {
		return
	}
	if value := strings.TrimSpace(env[key]); value != "" {
		*dst = value
	} else {
		*dst = fallback
	}
}

func (app App) runInstall(cmd *cobra.Command, opts installOptions) error {
	app.applyInstallDefaults(&opts)
	if err := validateSkipTerraform(app.opts.Env); err != nil {
		return err
	}
	source, err := app.resolveInstallSource(opts.source)
	if err != nil {
		return err
	}
	chartDir := filepath.Join(source, "infra", "helm", "layer")
	profile, err := resolveInstallProfile(opts.profile, chartDir)
	if err != nil {
		return err
	}
	if opts.nodeType == "" {
		opts.nodeType = profile.systemNodeType
	}
	interactive := app.opts.StdinIsTerminal && app.opts.StdoutIsTerminal
	reader := bufio.NewReader(app.opts.Stdin)
	if interactive {
		if err := app.installWizard(cmd, reader, &opts); err != nil {
			return err
		}
	}
	if opts.tpufKey == "" {
		return cliError{message: "a Turbopuffer API key is required: pass --turbopuffer-api-key or set TURBOPUFFER_API_KEY", code: ExitUsage}
	}

	printInstallPlan(cmd.ErrOrStderr(), source, opts, profile)
	if opts.dryRun {
		printInstallDryRun(cmd.OutOrStdout(), source, opts, profile)
		return nil
	}
	if !opts.assumeYes {
		if !interactive {
			return cliError{message: "refusing to provision without confirmation; pass --yes for non-interactive installs", code: ExitUsage}
		}
		answer, err := promptLineW(cmd.ErrOrStderr(), reader, "Proceed? This provisions AWS resources that cost money [y/N]: ")
		if err != nil {
			return err
		}
		if answer = strings.ToLower(strings.TrimSpace(answer)); answer != "y" && answer != "yes" {
			return cliError{message: "install aborted", code: ExitFailed}
		}
	}
	return app.executeInstall(cmd, source, opts, profile)
}

func validateSkipTerraform(env map[string]string) error {
	value := strings.TrimSpace(env["SKIP_TERRAFORM"])
	if value != "" && value != "0" && value != "1" {
		return cliError{message: "SKIP_TERRAFORM must be 0 or 1", code: ExitUsage}
	}
	return nil
}

func printInstallPlan(w io.Writer, source string, opts installOptions, profile installProfile) {
	fmt.Fprintln(w, "\nInstall plan")
	fmt.Fprintf(w, "  Source checkout   %s\n", source)
	fmt.Fprintf(w, "  Install profile   %s (%s)\n", profile.name, profile.overlay)
	fmt.Fprintf(w, "  AWS profile       %s\n", orLabel(opts.awsProfile, "(ambient credentials)"))
	fmt.Fprintf(w, "  Region            %s\n", opts.region)
	fmt.Fprintf(w, "  Cluster           %s\n", opts.clusterName)
	fmt.Fprintf(w, "  Namespace         %s\n", opts.namespace)
	fmt.Fprintf(w, "  Helm release      %s\n", opts.helmRelease)
	fmt.Fprintf(w, "  System node       %s + %d GiB gp3 data volume\n", opts.nodeType, profile.systemDataVolumeGiB)
	fmt.Fprintf(w, "  Layer version     %s\n", opts.layerVersion)
	fmt.Fprintf(w, "  License token     %s\n", orLabel(maskSecret(opts.licenseToken), "(none — open gateway floor)"))
	fmt.Fprintf(w, "  Terraform         %s\n\n", installStageLabel(opts.skipTerraform))
}

func printInstallDryRun(w io.Writer, source string, opts installOptions, profile installProfile) {
	tfDir := filepath.Join(source, "infra", "terraform")
	chartDir := filepath.Join(source, "infra", "helm", "layer")
	fmt.Fprintln(w, "dry run — resolved commands:")
	if !opts.skipTerraform {
		fmt.Fprintf(w, "  terraform -chdir=%s init -input=false\n", tfDir)
		fmt.Fprintf(w, "  terraform -chdir=%s apply -auto-approve -input=false -var region=%s -var cluster_name=%s -var kubernetes_namespace=%s -var bootstrap_cluster=true -var system_node_instance_type=%s -var system_node_data_volume_size=%d\n", tfDir, opts.region, opts.clusterName, opts.namespace, opts.nodeType, profile.systemDataVolumeGiB)
	}
	fmt.Fprintf(w, "  aws eks update-kubeconfig --region %s --name <terraform:cluster_name>\n", opts.region)
	fmt.Fprintf(w, "  %s\n", filepath.Join(source, "scripts", "deploy-lb-controller.sh"))
	fmt.Fprintf(w, "  %s\n", filepath.Join(source, "scripts", "deploy-karpenter.sh"))
	fmt.Fprintf(w, "  helm upgrade --install %s %s --namespace <terraform:layer_namespace> --create-namespace -f %s -f <generated-values.yaml>\n", opts.helmRelease, chartDir, profile.overlay)
	fmt.Fprintln(w, "  kubectl rollout status statefulset --selector app.kubernetes.io/instance="+opts.helmRelease+",app.kubernetes.io/component=gateway --timeout=10m")
}

func (app App) executeInstall(cmd *cobra.Command, source string, opts installOptions, profile installProfile) error {
	if err := requireCommands("aws", "terraform", "helm", "kubectl"); err != nil {
		return err
	}
	env := installEnvironment(app.opts.Env, opts)
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "aws", "sts", "get-caller-identity"); err != nil {
		return fmt.Errorf("AWS credentials not configured: %w", err)
	}
	tfDir := filepath.Join(source, "infra", "terraform")
	if !opts.skipTerraform {
		fmt.Fprintf(cmd.ErrOrStderr(), "\n==> Provisioning AWS resources (terraform apply in %s)\n", tfDir)
		if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "terraform", "-chdir="+tfDir, "init", "-input=false"); err != nil {
			return err
		}
		args := terraformArgs("apply", opts, profile, true)
		if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "terraform", append([]string{"-chdir=" + tfDir}, args...)...); err != nil {
			return err
		}
	}

	fmt.Fprintln(cmd.ErrOrStderr(), "\n==> Reading Terraform outputs")
	outputs := map[string]string{}
	for _, key := range []string{"layer_s3_bucket_name", "cluster_name", "layer_namespace", "layer_gateway_role_arn", "layer_service_account_name", "layer_dashboard_role_arn", "layer_dashboard_service_account_name", "karpenter_node_instance_profile_name"} {
		value, err := captureExternal(cmd.Context(), source, env, cmd.ErrOrStderr(), "terraform", "-chdir="+tfDir, "output", "-raw", key)
		if err != nil {
			return fmt.Errorf("read Terraform output %s: %w", key, err)
		}
		outputs[key] = strings.TrimSpace(value)
	}

	fmt.Fprintf(cmd.ErrOrStderr(), "\n==> Updating kubeconfig for cluster %s\n", outputs["cluster_name"])
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "aws", "eks", "update-kubeconfig", "--region", opts.region, "--name", outputs["cluster_name"]); err != nil {
		return err
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "\n==> Installing cluster networking and autoscaling")
	componentEnv := withEnv(env, "SKIP_TERRAFORM", "1")
	for _, script := range []string{"deploy-lb-controller.sh", "deploy-karpenter.sh"} {
		if err := runExternal(cmd.Context(), source, componentEnv, cmd.OutOrStdout(), cmd.ErrOrStderr(), filepath.Join(source, "scripts", script)); err != nil {
			return err
		}
	}

	password := opts.dashboardPassword
	generatedPassword := false
	if password == "" {
		var err error
		password, err = randomPassword()
		if err != nil {
			return fmt.Errorf("generate dashboard password: %w", err)
		}
		generatedPassword = true
	}
	values, err := renderInstallValues(opts, password, outputs)
	if err != nil {
		return err
	}
	valuesFile, err := os.CreateTemp("", "layer-values-*.yaml")
	if err != nil {
		return err
	}
	valuesPath := valuesFile.Name()
	defer os.Remove(valuesPath)
	if _, err := valuesFile.Write(values); err != nil {
		valuesFile.Close()
		return err
	}
	if err := valuesFile.Close(); err != nil {
		return err
	}

	fmt.Fprintln(cmd.ErrOrStderr(), "\n==> Installing the Layer Helm release")
	chartDir := filepath.Join(source, "infra", "helm", "layer")
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "helm", "upgrade", "--install", opts.helmRelease, chartDir, "--namespace", outputs["layer_namespace"], "--create-namespace", "-f", profile.overlay, "-f", valuesPath); err != nil {
		return err
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "\n==> Waiting for the gateway to become ready")
	selector := "app.kubernetes.io/instance=" + opts.helmRelease + ",app.kubernetes.io/component=gateway"
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "kubectl", "-n", outputs["layer_namespace"], "rollout", "status", "statefulset", "--selector", selector, "--timeout=10m"); err != nil {
		return err
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "\n==> Done. Next steps: https://hevlayer.com/docs/quickstart/")
	if generatedPassword {
		fmt.Fprintf(cmd.ErrOrStderr(), "\n==> Dashboard credentials: %s / %s\n", opts.dashboardUser, password)
	}
	return runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "kubectl", "-n", outputs["layer_namespace"], "get", "pods")
}

func terraformArgs(action string, opts installOptions, profile installProfile, autoApprove bool) []string {
	args := []string{action}
	if autoApprove {
		args = append(args, "-auto-approve")
	}
	args = append(args, "-input=false",
		"-var", "region="+opts.region,
		"-var", "cluster_name="+opts.clusterName,
		"-var", "kubernetes_namespace="+opts.namespace,
		"-var", "bootstrap_cluster=true",
		"-var", "system_node_instance_type="+opts.nodeType,
		"-var", fmt.Sprintf("system_node_data_volume_size=%d", profile.systemDataVolumeGiB),
	)
	return args
}

func installEnvironment(base map[string]string, opts installOptions) []string {
	merged := make(map[string]string, len(base)+2)
	for key, value := range base {
		merged[key] = value
	}
	if opts.awsProfile != "" {
		merged["AWS_PROFILE"] = opts.awsProfile
	}
	merged["AWS_REGION"] = opts.region
	env := make([]string, 0, len(merged))
	for key, value := range merged {
		env = append(env, key+"="+value)
	}
	return env
}

func withEnv(env []string, key, value string) []string {
	prefix := key + "="
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}

func renderInstallValues(opts installOptions, dashboardPassword string, out map[string]string) ([]byte, error) {
	values := map[string]any{
		"vectorStore": map[string]any{
			"credential":  map[string]any{"apiKey": opts.tpufKey},
			"endpoint":    map[string]any{"url": "https://api.turbopuffer.com", "region": "aws-" + opts.region},
			"inboundAuth": map[string]any{"mode": "deriveFromStore"},
		},
		"gateway":  map[string]any{"image": "hevlayer/layer-gateway-pro:" + opts.layerVersion},
		"operator": map[string]any{"enabled": true, "image": "hevlayer/layer-operator:" + opts.layerVersion},
		"dashboard": map[string]any{
			"enabled":        true,
			"image":          "hevlayer/layer-dashboard:" + opts.layerVersion,
			"basicAuth":      map[string]any{"user": opts.dashboardUser, "password": dashboardPassword},
			"serviceAccount": map[string]any{"name": out["layer_dashboard_service_account_name"], "roleArn": out["layer_dashboard_role_arn"]},
		},
		"s3":              map[string]any{"bucket": out["layer_s3_bucket_name"]},
		"serviceAccount":  map[string]any{"name": out["layer_service_account_name"], "roleArn": out["layer_gateway_role_arn"]},
		"workerKarpenter": map[string]any{"enabled": true, "clusterName": out["cluster_name"], "instanceProfile": out["karpenter_node_instance_profile_name"]},
		"documentCache": map[string]any{
			"karpenter": map[string]any{"clusterName": out["cluster_name"], "instanceProfile": out["karpenter_node_instance_profile_name"]},
		},
	}
	if opts.licenseToken != "" {
		values["license"] = map[string]any{"token": opts.licenseToken}
	}
	return yaml.Marshal(values)
}

func randomPassword() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func requireCommands(names ...string) error {
	for _, name := range names {
		if _, err := exec.LookPath(name); err != nil {
			return cliError{message: "missing prerequisite: " + name, code: ExitFailed}
		}
	}
	return nil
}

func runExternal(ctx context.Context, dir string, env []string, stdout, stderr io.Writer, name string, args ...string) error {
	run := exec.CommandContext(ctx, name, args...)
	run.Dir = dir
	run.Env = env
	run.Stdin = nil
	run.Stdout = stdout
	run.Stderr = stderr
	if err := run.Run(); err != nil {
		return fmt.Errorf("%s failed: %w", filepath.Base(name), err)
	}
	return nil
}

func captureExternal(ctx context.Context, dir string, env []string, stderr io.Writer, name string, args ...string) (string, error) {
	run := exec.CommandContext(ctx, name, args...)
	run.Dir = dir
	run.Env = env
	run.Stderr = stderr
	value, err := run.Output()
	return string(value), err
}

func (app App) installWizard(cmd *cobra.Command, reader *bufio.Reader, opts *installOptions) error {
	w := cmd.ErrOrStderr()
	fmt.Fprintln(w, "Layer install — Terraform provisions AWS; Helm installs the Layer release.")
	if opts.awsProfile == "" {
		answer, err := promptLineW(w, reader, "AWS profile [use ambient credentials]: ")
		if err != nil {
			return err
		}
		opts.awsProfile = strings.TrimSpace(answer)
	}
	if opts.tpufKey == "" {
		answer, err := promptLineW(w, reader, "Turbopuffer API key: ")
		if err != nil {
			return err
		}
		opts.tpufKey = strings.TrimSpace(answer)
	}
	return nil
}

func (app App) resolveInstallSource(flagValue string) (string, error) {
	candidates := []string{}
	if flagValue != "" {
		candidates = append(candidates, flagValue)
	}
	if src := strings.TrimSpace(app.opts.Env["LAYER_SRC"]); src != "" {
		candidates = append(candidates, src)
	}
	for _, dir := range candidates {
		abs, err := filepath.Abs(dir)
		if err != nil {
			return "", err
		}
		if isInstallSource(abs) {
			return abs, nil
		}
	}
	if len(candidates) > 0 {
		return "", cliError{message: fmt.Sprintf("%s does not contain complete Layer install assets.\n\n%s", candidates[0], installSourceHelp), code: ExitUsage}
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if isInstallSource(dir) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", cliError{message: installSourceHelp, code: ExitUsage}
		}
		dir = parent
	}
}

func isInstallSource(dir string) bool {
	for _, rel := range []string{
		filepath.Join("infra", "terraform"),
		filepath.Join("infra", "helm", "layer", "Chart.yaml"),
		filepath.Join("infra", "helm", "layer", "values-demo.yaml"),
		filepath.Join("infra", "helm", "layer", "values-indexing.yaml"),
		filepath.Join("scripts", "deploy-lb-controller.sh"),
		filepath.Join("scripts", "deploy-karpenter.sh"),
	} {
		if _, err := os.Stat(filepath.Join(dir, rel)); err != nil {
			return false
		}
	}
	return true
}

func newInstallStatusCommand(app App) *cobra.Command {
	opts := installOptions{}
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report the installed release and workload health",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app.applyInstallDefaults(&opts)
			if err := requireCommands("aws", "helm", "kubectl"); err != nil {
				return err
			}
			env := installEnvironment(app.opts.Env, opts)
			if err := runExternal(cmd.Context(), "", env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "aws", "eks", "update-kubeconfig", "--region", opts.region, "--name", opts.clusterName); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "\nLayer release %s/%s\n", opts.namespace, opts.helmRelease)
			if err := runExternal(cmd.Context(), "", env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "helm", "status", opts.helmRelease, "--namespace", opts.namespace, "--show-desc"); err != nil {
				return fmt.Errorf("Layer is not installed or Helm cannot read it: %w", err)
			}
			selector := "app.kubernetes.io/instance=" + opts.helmRelease
			healthErr := runExternal(cmd.Context(), "", env, io.Discard, cmd.ErrOrStderr(), "kubectl", "-n", opts.namespace, "wait", "pod", "-l", selector, "--for=condition=Ready", "--timeout=5s")
			if healthErr == nil {
				fmt.Fprintln(cmd.OutOrStdout(), "Health: healthy (all release pods Ready)")
			} else {
				fmt.Fprintln(cmd.OutOrStdout(), "Health: unhealthy (one or more release pods are not Ready)")
			}
			fmt.Fprintln(cmd.OutOrStdout(), "\nWorkload health")
			if err := runExternal(cmd.Context(), "", env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "kubectl", "-n", opts.namespace, "get", "pods", "-o", "wide"); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "\nDocument-cache nodes (AGE exposes failed scale-to-zero)")
			cacheSelector := "layer.hev.dev/node-role=document-cache"
			if err := runExternal(cmd.Context(), "", env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "kubectl", "get", "nodes", "-l", cacheSelector, "-L", "node.kubernetes.io/instance-type"); err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "\nDocument-cache node utilization (metrics-server required)")
			if err := runExternal(cmd.Context(), "", env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "kubectl", "top", "nodes", "-l", cacheSelector); err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: cache-node utilization unavailable: %v\n", err)
			}
			return healthErr
		},
	}
	addLifecycleFlags(cmd, &opts)
	return cmd
}

func newInstallUninstallCommand(app App) *cobra.Command {
	opts := installOptions{}
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove Layer and destroy its provisioned AWS footprint",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app.applyInstallDefaults(&opts)
			if err := validateSkipTerraform(app.opts.Env); err != nil {
				return err
			}
			source, err := app.resolveInstallSource(opts.source)
			if err != nil {
				return err
			}
			profile, err := resolveInstallProfile(opts.profile, filepath.Join(source, "infra", "helm", "layer"))
			if err != nil {
				return err
			}
			if opts.nodeType == "" {
				opts.nodeType = profile.systemNodeType
			}
			if opts.dryRun {
				fmt.Fprintf(cmd.OutOrStdout(), "helm uninstall %s --namespace %s --ignore-not-found\n", opts.helmRelease, opts.namespace)
				if !opts.skipTerraform {
					fmt.Fprintf(cmd.OutOrStdout(), "terraform -chdir=%s destroy -auto-approve ...\n", filepath.Join(source, "infra", "terraform"))
				}
				return nil
			}
			if !opts.assumeYes {
				if !app.opts.StdinIsTerminal || !app.opts.StdoutIsTerminal {
					return cliError{message: "refusing to uninstall without confirmation; pass --yes for non-interactive teardown", code: ExitUsage}
				}
				answer, err := promptLineW(cmd.ErrOrStderr(), bufio.NewReader(app.opts.Stdin), "Remove the Helm release and provisioned AWS resources [y/N]: ")
				if err != nil {
					return err
				}
				if answer = strings.ToLower(strings.TrimSpace(answer)); answer != "y" && answer != "yes" {
					return cliError{message: "uninstall aborted", code: ExitFailed}
				}
			}
			return app.executeUninstall(cmd, source, opts, profile)
		},
	}
	addInstallFlags(cmd, &opts, false)
	return cmd
}

func addLifecycleFlags(cmd *cobra.Command, opts *installOptions) {
	cmd.Flags().StringVar(&opts.awsProfile, "aws-profile", "", "AWS credentials profile (env AWS_PROFILE)")
	cmd.Flags().StringVar(&opts.region, "region", "", "AWS region (env AWS_REGION; default us-east-1)")
	cmd.Flags().StringVar(&opts.clusterName, "cluster-name", "", "EKS cluster name (env CLUSTER_NAME; default layer)")
	cmd.Flags().StringVar(&opts.namespace, "namespace", "", "Kubernetes namespace (env NAMESPACE; default layer)")
	cmd.Flags().StringVar(&opts.helmRelease, "helm-release", "", "Helm release name (env HELM_RELEASE; default layer)")
}

func (app App) executeUninstall(cmd *cobra.Command, source string, opts installOptions, profile installProfile) error {
	prereqs := []string{"aws", "helm", "kubectl"}
	if !opts.skipTerraform {
		prereqs = append(prereqs, "terraform")
	}
	if err := requireCommands(prereqs...); err != nil {
		return err
	}
	env := installEnvironment(app.opts.Env, opts)
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "aws", "eks", "update-kubeconfig", "--region", opts.region, "--name", opts.clusterName); err != nil {
		return err
	}
	for _, release := range []struct{ name, namespace string }{
		{opts.helmRelease, opts.namespace},
		{"nvidia-device-plugin", "nvidia-device-plugin"},
		{"karpenter", "kube-system"},
		{"aws-load-balancer-controller", "kube-system"},
	} {
		if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "helm", "uninstall", release.name, "--namespace", release.namespace, "--ignore-not-found", "--wait"); err != nil {
			return err
		}
	}
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "kubectl", "delete", "namespace", opts.namespace, "--ignore-not-found", "--wait=true"); err != nil {
		return err
	}
	if opts.skipTerraform {
		fmt.Fprintln(cmd.ErrOrStderr(), "Layer workloads removed; Terraform-managed AWS resources retained (--skip-terraform).")
		return nil
	}
	tfDir := filepath.Join(source, "infra", "terraform")
	args := terraformArgs("destroy", opts, profile, true)
	if err := runExternal(cmd.Context(), source, env, cmd.OutOrStdout(), cmd.ErrOrStderr(), "terraform", append([]string{"-chdir=" + tfDir}, args...)...); err != nil {
		return err
	}
	fmt.Fprintln(cmd.ErrOrStderr(), "Layer Helm components and Terraform-managed AWS resources removed.")
	return nil
}

func maskSecret(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "****"
	}
	return value[:4] + "…" + value[len(value)-4:]
}

func orLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func installStageLabel(skip bool) string {
	if skip {
		return "skip (reuse existing outputs)"
	}
	return "apply"
}
